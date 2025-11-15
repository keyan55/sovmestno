import time
import random
import socket
import imaplib
import traceback
import ssl
import socks
from dataclasses import dataclass, field
from multiprocessing import Process, Event, Queue, RLock, Manager
from typing import Optional, Dict, Tuple, Any, List

# === Публичные настройки (под твою нагрузку) ===

IMAP_PORT_SSL = 993

# === Оптимизация под целевую нагрузку ===
# Сервер: 16GB RAM, 8 ядер, Ubuntu 24.04
# Нагрузка: 13 пользователей × 50-97 аккаунтов = 650-1261 аккаунтов
# Частота опроса: раз в 10-15 секунд на ящик (увеличено для снижения нагрузки)

# Интервал опроса: 10-15 секунд (средняя частота ~12.5 сек)
# Небольшой разброс для равномерного распределения нагрузки
IMAP_POLL_INTERVAL_MIN = 10.0
IMAP_POLL_INTERVAL_MAX = 15.0

# Таймауты соединений (оптимизированы для стабильности)
IMAP_CONNECTION_TIMEOUT = 15        # Таймаут подключения (уменьшен для быстрого восстановления)
IMAP_SOCKET_TIMEOUT = 15            # Таймаут сокета
IMAP_READ_TIMEOUT = 12              # Таймаут чтения
IMAP_WRITE_TIMEOUT = 8              # Таймаут записи
IMAP_NOOP_TIMEOUT = 5               # Таймаут NOOP (быстрая проверка живости)

# Лимит одновременно открытых IMAP-соединений
# При 1261 аккаунте и 20 процессах: ~63 аккаунта на процесс
# Но не все соединения активны одновременно (постоянные соединения переиспользуются)
# С запасом для пиковых нагрузок и переподключений
MAX_IMAP_CONNECTIONS = 300          # Глобальный лимит (20 процессов × ~15 соединений/процесс)

# Обработка ошибок
IMAP_MAX_ERRORS_PER_10MIN = 5       # Максимум ошибок за 10 минут
IMAP_BACKOFF_DURATION = 1800        # 30 минут в BACKOFF при превышении лимита

# === Пул процессов (Persistent IMAP Runtime) ===
# ОПТИМИЗАЦИЯ ПАМЯТИ: уменьшено количество процессов для экономии RAM
# Расчет: 1261 аккаунт / 20 процессов = ~63 аккаунта на процесс
# Память: 20 процессов × ~180 MB = ~3.6 GB (остается ~12.4 GB для других процессов)
# CPU: 8 ядер, 20 процессов = 2.5 процесса на ядро (приемлемо для I/O-bound операций)
MAX_IMAP_WORKER_PROCESSES = 20      # Максимум процессов в пуле (уменьшено с 30 для экономии памяти)
ACCOUNTS_PER_WORKER = 65            # Аккаунтов на процесс (1261 / 20 ≈ 63, берем 65 с запасом)

# Производительность:
# - 20 процессов × 65 аккаунтов = 1300 аккаунтов максимум (достаточно для 1261)
# - При интервале 10-15 сек: ~101 опроса/сек общая нагрузка (средний интервал 12.5 сек)
# - На процесс: ~5 опроса/сек (очень комфортно для I/O-bound операций)


class SocksIMAP4SSL(imaplib.IMAP4):
    """IMAP4 SSL клиент с поддержкой SOCKS5 прокси"""
    def __init__(
        self,
        host: str,
        port: int = IMAP_PORT_SSL,
        proxy: Optional[Dict[str, Any]] = None,
        timeout: int = IMAP_CONNECTION_TIMEOUT,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self._proxy = proxy or {}
        self._timeout = timeout
        self._ssl_context = ssl_context or ssl.create_default_context()
        # Вызываем базовый конструктор без timeout kwarg — base may not accept it.
        super().__init__(host, port)

    def open(self, host: str, port: int, timeout: Optional[float] = None):
        # Создаём SOCKS5‑сокет с аутентификацией прокси
        s = socks.socksocket()
        s.set_proxy(
            socks.SOCKS5,
            self._proxy["host"],
            int(self._proxy["port"]),
            True,
            self._proxy.get("user") or None,
            self._proxy.get("password") or None,
        )
        s.settimeout(timeout if timeout is not None else self._timeout)
        s.connect((host, port))

        # Оборачиваем в TLS и ПРИСВАИВАЕМ self.sock/self.file (ничего не возвращаем)
        ssock = self._ssl_context.wrap_socket(s, server_hostname=host)
        self.sock = ssock
        self.file = self.sock.makefile("rb")


@dataclass
class ImapAccountConfig:
    user_id: int
    acc_id: int
    email: str
    password: str
    display_name: str
    chat_id: int
    host: str
    proxy: Optional[Dict[str, Any]] = None


@dataclass
class ImapAccountStatus:
    state: str = "NEW"              # NEW|CONNECTING|ACTIVE|BACKOFF|STOPPED
    last_error: Optional[str] = None
    backoff_until: float = 0.0
    last_uid: Optional[int] = None
    error_timestamps: List[float] = field(default_factory=list)
    last_poll_ts: float = 0.0
    last_ok_ts: float = 0.0


# Глобальное состояние
# ВАЖНО: ACCOUNT_STATUS должен быть разделяемым между процессами для синхронизации статуса
# Используем Manager().dict() для создания разделяемого словаря
_manager: Optional[Manager] = None
ACCOUNT_STATUS: Dict[Tuple[int, int], ImapAccountStatus] = {}
CURRENT_IMAP_CONNECTIONS: int = 0
LOCK = RLock()

RESULT_QUEUE: Optional[Queue] = None
GLOBAL_STOP_EVENT: Optional[Event] = None
WORKER_PROCESSES: Dict[Tuple[int, int], Process] = {}

# Новая архитектура: пул процессов
ACCOUNT_QUEUE: Optional[Queue] = None  # Очередь аккаунтов для обработки
WORKER_POOL: List[Process] = []        # Пул воркеров
WORKER_POOL_LOCK = RLock()             # Блокировка для пула


def _log(msg: str) -> None:
    print(f"[IMAP_RUNTIME] {msg}", flush=True)


def _register_error(uid: int, acc_id: int, msg: str, account_status: Optional[Dict] = None) -> None:
    """
    Регистрирует ошибку для аккаунта.
    Если account_status передан, использует его (для воркеров), иначе использует глобальный ACCOUNT_STATUS.
    """
    key = (uid, acc_id)
    status_dict = account_status if account_status is not None else ACCOUNT_STATUS
    with LOCK:
        if key not in status_dict:
            status_dict[key] = {
                "state": "NEW",
                "last_error": None,
                "backoff_until": 0.0,
                "last_uid": None,
                "last_poll_ts": 0.0,
                "last_ok_ts": 0.0,
                "error_timestamps": []
            }
        st = status_dict[key]
        st["last_error"] = msg
        if "error_timestamps" not in st:
            st["error_timestamps"] = []
        st["error_timestamps"].append(time.time())
        cutoff = time.time() - 600
        st["error_timestamps"] = [t for t in st["error_timestamps"] if t >= cutoff]
        if len(st["error_timestamps"]) >= IMAP_MAX_ERRORS_PER_10MIN:
            st["state"] = "BACKOFF"
            st["backoff_until"] = time.time() + IMAP_BACKOFF_DURATION
            _log(f"Account {uid}/{acc_id} entered BACKOFF: {msg}")


def _can_use_account(uid: int, acc_id: int, account_status: Optional[Dict] = None) -> bool:
    """
    Проверяет, можно ли использовать аккаунт.
    Если account_status передан, использует его (для воркеров), иначе использует глобальный ACCOUNT_STATUS.
    """
    key = (uid, acc_id)
    status_dict = account_status if account_status is not None else ACCOUNT_STATUS
    st = status_dict.get(key)
    if not st:
        return True
    if isinstance(st, dict):
        if st.get("state") == "BACKOFF" and time.time() < st.get("backoff_until", 0):
            return False
    else:
        # Обратная совместимость с dataclass
        if st.state == "BACKOFF" and time.time() < st.backoff_until:
            return False
    return True


def _reserve_connection() -> bool:
    global CURRENT_IMAP_CONNECTIONS
    with LOCK:
        if CURRENT_IMAP_CONNECTIONS >= MAX_IMAP_CONNECTIONS:
            return False
        CURRENT_IMAP_CONNECTIONS += 1
        return True


def _release_connection() -> None:
    global CURRENT_IMAP_CONNECTIONS
    with LOCK:
        CURRENT_IMAP_CONNECTIONS = max(0, CURRENT_IMAP_CONNECTIONS - 1)


def _imap_connect(config: ImapAccountConfig) -> imaplib.IMAP4_SSL:
    proxy_info = ""
    if config.proxy:
        proxy_info = f" via proxy {config.proxy.get('host', '?')}:{config.proxy.get('port', '?')}"
    _log(f"Connecting IMAP for {config.user_id}/{config.acc_id} {config.email} @ {config.host}{proxy_info}")
    
    # Используем SocksIMAP4SSL если есть прокси, иначе стандартный IMAP4_SSL
    if config.proxy:
        imap = SocksIMAP4SSL(
            config.host,
            IMAP_PORT_SSL,
            proxy=config.proxy,
            timeout=IMAP_CONNECTION_TIMEOUT
        )
        # Для SocksIMAP4SSL таймаут устанавливается в методе open
        # Дополнительно устанавливаем таймаут на сокет после подключения
        if hasattr(imap, 'sock') and imap.sock:
            imap.sock.settimeout(IMAP_SOCKET_TIMEOUT)
            # Для SSL-сокетов также устанавливаем таймаут на SSL-объект
            if hasattr(imap.sock, '_sslobj') and imap.sock._sslobj:
                # SSL-объект использует таймаут сокета, но можно установить явно
                pass
    else:
        imap = imaplib.IMAP4_SSL(config.host, IMAP_PORT_SSL, timeout=IMAP_CONNECTION_TIMEOUT)
        # Устанавливаем таймаут на сокет (проверяем разные варианты API)
        sock = None
        if hasattr(imap, 'socket'):
            try:
                sock = imap.socket()
            except (AttributeError, TypeError):
                pass
        if not sock and hasattr(imap, 'sock'):
            sock = imap.sock
        if sock:
            sock.settimeout(IMAP_SOCKET_TIMEOUT)
            # Для SSL-сокетов также устанавливаем таймаут на SSL-объект
            if hasattr(sock, '_sslobj') and sock._sslobj:
                # SSL-объект использует таймаут сокета
                pass
    
    # Обертываем login и select в обработку таймаутов
    try:
        imap.login(config.email, config.password)
    except (socket.timeout, TimeoutError, OSError) as e:
        _log(f"IMAP login timeout/error for {config.user_id}/{config.acc_id} {config.email}: {e}")
        try:
            imap.close()
        except:
            pass
        try:
            imap.logout()
        except:
            pass
        raise
    except Exception as e:
        _log(f"IMAP login error for {config.user_id}/{config.acc_id} {config.email}: {e}")
        try:
            imap.close()
        except:
            pass
        try:
            imap.logout()
        except:
            pass
        raise
    
    try:
        imap.select("INBOX")
    except (socket.timeout, TimeoutError, OSError) as e:
        _log(f"IMAP select timeout/error for {config.user_id}/{config.acc_id} {config.email}: {e}")
        try:
            imap.close()
        except:
            pass
        try:
            imap.logout()
        except:
            pass
        raise
    
    return imap


def _get_max_uid(imap: imaplib.IMAP4_SSL) -> Optional[int]:
    """
    Получает максимальный UID всех писем в INBOX.
    Используется при первом запуске для установки начального last_uid.
    """
    try:
        typ, data = imap.uid("SEARCH", None, "ALL")
        if typ != "OK" or not data or not data[0]:
            return None
        raw = data[0].decode() if isinstance(data[0], bytes) else data[0]
        uids = [int(x) for x in raw.split() if x.strip().isdigit()]
        return max(uids) if uids else None
    except Exception:
        return None


def _search_new_uids(imap: imaplib.IMAP4_SSL,
                     last_uid: Optional[int]) -> List[int]:
    """
    Поиск новых UID писем.
    ВАЖНО: Всегда используем UID SEARCH для получения UID, а не sequence numbers.
    Sequence numbers могут меняться при удалении/перемещении писем.
    """
    if last_uid is None:
        # Первый запуск: ищем все UNSEEN письма, используя UID SEARCH
        typ, data = imap.uid("SEARCH", None, "UNSEEN")
    else:
        # Последующие запуски: ищем письма с UID больше last_uid
        typ, data = imap.uid("SEARCH", None, f"UID {last_uid + 1}:*")
    
    if typ != "OK":
        return []
    if not data or not data[0]:
        return []
    raw = data[0].decode() if isinstance(data[0], bytes) else data[0]
    # Парсим UID из ответа (могут быть пустые строки)
    uids = [int(x) for x in raw.split() if x.strip().isdigit()]
    return uids


def _fetch_messages(imap: imaplib.IMAP4_SSL,
                    uids: List[int]) -> List[Tuple[int, bytes]]:
    res: List[Tuple[int, bytes]] = []
    for uid in uids:
        typ, data = imap.uid("FETCH", str(uid), "(RFC822)")
        if typ != "OK" or not data:
            continue
        msg_bytes = data[0][1]
        res.append((uid, msg_bytes))
    return res


def _imap_pool_worker(account_queue: Queue,
                      result_queue: Queue,
                      stop_event: Event,
                      worker_id: int,
                      account_status: Dict):
    """
    Persistent IMAP NOOP/SEARCH Runtime Worker:
    - Держит постоянные IMAP соединения для аккаунтов
    - Периодически делает NOOP для поддержания соединения
    - Периодически делает SEARCH для поиска новых писем
    - Один процесс обрабатывает несколько аккаунтов последовательно
    """
    _log(f"Pool worker {worker_id} started (Persistent IMAP Runtime)")
    
    # Словарь активных соединений: key -> (imap, config, last_used, last_uid)
    persistent_connections: Dict[Tuple[int, int], Tuple[imaplib.IMAP4_SSL, ImapAccountConfig, float, Optional[int]]] = {}
    
    # Словарь конфигураций аккаунтов: key -> config
    account_configs: Dict[Tuple[int, int], ImapAccountConfig] = {}
    
    # Словарь времени последнего опроса: key -> timestamp
    last_poll_time: Dict[Tuple[int, int], float] = {}
    
    # Таймауты для очистки соединений (оптимизированы для экономии памяти)
    # При опросе раз в 10-15 секунд соединения используются менее активно
    # Более агрессивная очистка для освобождения памяти
    IMAP_CONNECTION_IDLE_TIMEOUT = 300.0  # 5 минут простоя - закрыть
    IMAP_CONNECTION_MAX_AGE = 900.0       # 15 минут максимальный возраст
    
    while not stop_event.is_set():
        try:
            # Получаем новые аккаунты из очереди (неблокирующе)
            try:
                while True:
                    config_dict = account_queue.get_nowait()
                    if config_dict is None:  # Сигнал остановки
                        break
                    
                    try:
                        config = ImapAccountConfig(
                            user_id=config_dict["user_id"],
                            acc_id=config_dict["acc_id"],
                            email=config_dict["email"],
                            password=config_dict["password"],
                            display_name=config_dict["display_name"],
                            chat_id=config_dict["chat_id"],
                            host=config_dict["host"],
                            proxy=config_dict.get("proxy")
                        )
                        key = (config.user_id, config.acc_id)
                        account_configs[key] = config
                        _log(f"Pool worker {worker_id}: added account {config.user_id}/{config.acc_id} ({config.email})")
                    except Exception as e:
                        _log(f"Pool worker {worker_id}: failed to deserialize config: {e}")
                        traceback.print_exc()
            except Exception as e_queue:
                # Очередь пуста - это нормально, продолжаем обработку существующих аккаунтов
                pass
            
            now = time.time()
            
            # ВАЖНО: Если нет аккаунтов для обработки, делаем небольшую паузу
            if not account_configs:
                time.sleep(0.5)
                continue
            
            # Обрабатываем все аккаунты с постоянными соединениями
            for key, config in list(account_configs.items()):
                if stop_event.is_set():
                    break
                
                if not _can_use_account(config.user_id, config.acc_id, account_status):
                    # Закрываем соединение для аккаунта в BACKOFF
                    if key in persistent_connections:
                        imap, _, _, _ = persistent_connections[key]
                        try:
                            imap.logout()
                        except:
                            pass
                        del persistent_connections[key]
                    continue
                
                # Проверяем интервал опроса
                last_poll = last_poll_time.get(key, 0)
                time_since_last_poll = now - last_poll
                if time_since_last_poll < IMAP_POLL_INTERVAL_MIN:
                    # Если соединение активно, делаем NOOP для поддержания
                    if key in persistent_connections:
                        stored_imap, _, _, _ = persistent_connections[key]
                        try:
                            stored_imap.sock.settimeout(IMAP_NOOP_TIMEOUT)
                            stored_imap.noop()
                            persistent_connections[key] = (stored_imap, config, now, persistent_connections[key][3])
                        except Exception:
                            # Соединение разорвано, удаляем из кэша
                            if key in persistent_connections:
                                del persistent_connections[key]
                    continue  # Пропускаем, если еще не прошло достаточно времени
                
                # Получаем или создаем соединение
                imap = None
                last_uid = None
                need_reconnect = False
                
                if key in persistent_connections:
                    stored_imap, stored_config, last_used, stored_last_uid = persistent_connections[key]
                    
                    # Проверяем таймауты
                    idle_time = now - last_used
                    if idle_time > IMAP_CONNECTION_IDLE_TIMEOUT or idle_time > IMAP_CONNECTION_MAX_AGE:
                        # Закрываем старое соединение
                        try:
                            stored_imap.logout()
                        except:
                            pass
                        del persistent_connections[key]
                        need_reconnect = True
                    else:
                        # Проверяем живость соединения через NOOP
                        try:
                            stored_imap.sock.settimeout(IMAP_NOOP_TIMEOUT)
                            typ, _ = stored_imap.noop()
                            if typ == "OK":
                                imap = stored_imap
                                last_uid = stored_last_uid
                                # Обновляем last_used (NOOP поддерживает соединение)
                                persistent_connections[key] = (imap, config, now, last_uid)
                            else:
                                need_reconnect = True
                        except Exception:
                            # Соединение мертво, нужно переподключиться
                            need_reconnect = True
                
                
                if need_reconnect or imap is None:
                    # Создаем новое соединение
                    if not _reserve_connection():
                        continue
                    
                    try:
                        # Используем словарь для разделяемого статуса
                        if key not in account_status:
                            account_status[key] = {
                                "state": "NEW",
                                "last_error": None,
                                "backoff_until": 0.0,
                                "last_uid": None,
                                "last_poll_ts": 0.0,
                                "last_ok_ts": 0.0
                            }
                        st = account_status[key]
                        st["state"] = "CONNECTING"
                        imap = _imap_connect(config)
                        st["state"] = "ACTIVE"
                        last_uid = st.get("last_uid")
                        
                        # Инициализация при первом запуске
                        if last_uid is None:
                            try:
                                typ, data = imap.uid("SEARCH", None, "UNSEEN")
                                if typ == "OK" and data and data[0]:
                                    raw = data[0].decode() if isinstance(data[0], bytes) else data[0]
                                    unseen_uids = [int(x) for x in raw.split() if x.strip().isdigit()]
                                    if unseen_uids:
                                        try:
                                            uid_sequence = ",".join(str(uid) for uid in unseen_uids)
                                            imap.uid("STORE", uid_sequence, "+FLAGS", r"(\Seen)")
                                        except Exception:
                                            for uid in unseen_uids:
                                                try:
                                                    imap.uid("STORE", str(uid), "+FLAGS", r"(\Seen)")
                                                except Exception:
                                                    pass
                                
                                max_uid = _get_max_uid(imap)
                                if max_uid is not None:
                                    last_uid = max_uid
                                    st["last_uid"] = last_uid
                                else:
                                    last_uid = 0
                                    st["last_uid"] = last_uid
                            except Exception as e:
                                _log(f"First run init error for {config.user_id}/{config.acc_id}: {e}")
                                last_uid = 0
                                st["last_uid"] = last_uid
                        
                        # Сохраняем соединение
                        persistent_connections[key] = (imap, config, now, last_uid)
                    except Exception as e:
                        _log(f"Connection error for {config.user_id}/{config.acc_id}: {e}")
                        _register_error(config.user_id, config.acc_id, str(e), account_status)
                        _release_connection()
                        continue
                
                # Обрабатываем аккаунт: SEARCH для новых писем
                try:
                    # Обновляем статус в разделяемом словаре
                    if key not in account_status:
                        account_status[key] = {
                            "state": "NEW",
                            "last_error": None,
                            "backoff_until": 0.0,
                            "last_uid": None,
                            "last_poll_ts": 0.0,
                            "last_ok_ts": 0.0
                        }
                    st = account_status[key]
                    # ВАЖНО: Обновляем last_poll_ts ДО выполнения операций, чтобы статус был актуальным
                    st["last_poll_ts"] = now
                    st["state"] = "ACTIVE"  # Обновляем состояние при успешном опросе
                    
                    # Обновляем время последнего опроса
                    last_poll_time[key] = now
                    
                    uids = _search_new_uids(imap, last_uid)
                    if uids:
                        _log(f"Pool worker {worker_id}: found {len(uids)} new messages for {config.user_id}/{config.acc_id}")
                    
                    if uids:
                        msgs = _fetch_messages(imap, uids)
                        
                        if msgs:
                            # Обновляем last_uid
                            new_last_uid = max(last_uid or 0, max(uids))
                            st["last_uid"] = new_last_uid
                            st["last_ok_ts"] = now
                            
                            # Обновляем в persistent_connections
                            persistent_connections[key] = (imap, config, now, new_last_uid)
                            
                            # Помечаем как прочитанные
                            try:
                                uid_sequence = ",".join(str(uid) for uid in uids)
                                imap.uid("STORE", uid_sequence, "+FLAGS", r"(\Seen)")
                            except Exception:
                                for uid in uids:
                                    try:
                                        imap.uid("STORE", str(uid), "+FLAGS", r"(\Seen)")
                                    except Exception:
                                        pass
                            
                            # Отправляем в очередь
                            try:
                                result_queue.put(
                                    (config.user_id, config.acc_id, config.chat_id, config.email, msgs)
                                )
                                _log(f"Pool worker {worker_id}: sent {len(msgs)} messages to result queue for {config.user_id}/{config.acc_id}")
                            except Exception as e_put:
                                _log(f"Pool worker {worker_id}: failed to put messages in result queue for {config.user_id}/{config.acc_id}: {e_put}")
                except Exception as e:
                    error_str = str(e)
                    if "EOF" in error_str or "socket error" in error_str:
                        # Соединение разорвано, удаляем из кэша
                        if key in persistent_connections:
                            del persistent_connections[key]
                        _log(f"Connection lost for {config.user_id}/{config.acc_id}, will reconnect")
                    else:
                        _register_error(config.user_id, config.acc_id, f"SEARCH/FETCH error: {e}", account_status)
            
            # Небольшая задержка перед следующим циклом
            time.sleep(0.5)
            
        except Exception as e:
            _log(f"Pool worker {worker_id} exception: {e}")
            traceback.print_exc()
            time.sleep(1)
    
    # Закрываем все соединения при остановке
    for key, (imap, _, _, _) in list(persistent_connections.items()):
        try:
            imap.logout()
        except:
            pass
        _release_connection()
    
    _log(f"Pool worker {worker_id} stopped (processed {len(account_configs)} accounts)")


def _process_single_account(config: ImapAccountConfig,
                            result_queue: Queue,
                            stop_event: Event,
                            worker_id: int):
    """
    Обрабатывает один аккаунт: подключается, проверяет новые письма, отправляет в очередь.
    """
    key = (config.user_id, config.acc_id)
    if key not in ACCOUNT_STATUS:
        ACCOUNT_STATUS[key] = {
            "state": "NEW",
            "last_error": None,
            "backoff_until": 0.0,
            "last_uid": None,
            "last_poll_ts": 0.0,
            "last_ok_ts": 0.0
        }

    if not _can_use_account(config.user_id, config.acc_id):
        return

    if not _reserve_connection():
        return

    imap = None
    try:
        st = ACCOUNT_STATUS[key]
        if isinstance(st, dict):
            st["state"] = "CONNECTING"
        else:
            st.state = "CONNECTING"
        imap = _imap_connect(config)
        if isinstance(st, dict):
            st["state"] = "ACTIVE"
        else:
            st.state = "ACTIVE"

        last_uid = st.get("last_uid") if isinstance(st, dict) else st.last_uid

        # ВАЖНО: При первом запуске (last_uid is None) помечаем все существующие UNSEEN как прочитанные
        # и устанавливаем last_uid на максимальный UID всех писем, чтобы не публиковать старые письма
        if last_uid is None:
            try:
                # Находим все UNSEEN письма
                typ, data = imap.uid("SEARCH", None, "UNSEEN")
                if typ == "OK" and data and data[0]:
                    raw = data[0].decode() if isinstance(data[0], bytes) else data[0]
                    unseen_uids = [int(x) for x in raw.split() if x.strip().isdigit()]
                    
                    if unseen_uids:
                        # Помечаем все UNSEEN как прочитанные (но НЕ публикуем их)
                        _log(f"First run: marking {len(unseen_uids)} UNSEEN messages as read for {config.user_id}/{config.acc_id}")
                        # Помечаем все письма одним запросом для эффективности
                        try:
                            uid_sequence = ",".join(str(uid) for uid in unseen_uids)
                            imap.uid("STORE", uid_sequence, "+FLAGS", r"(\Seen)")
                        except Exception:
                            # Fallback: помечаем по одному
                            for uid in unseen_uids:
                                try:
                                    imap.uid("STORE", str(uid), "+FLAGS", r"(\Seen)")
                                except Exception:
                                    pass
                        # ВАЖНО: Делаем EXPUNGE или хотя бы закрываем/открываем SELECT,
                        # чтобы сервер применил изменения флагов
                        try:
                            imap.expunge()
                        except Exception:
                            pass
                
                # Устанавливаем last_uid на максимальный UID всех писем в INBOX
                max_uid = _get_max_uid(imap)
                if max_uid is not None:
                    last_uid = max_uid
                    if isinstance(st, dict):
                        st["last_uid"] = last_uid
                    else:
                        st.last_uid = last_uid
                    _log(f"First run: set last_uid={last_uid} for {config.user_id}/{config.acc_id}")
                else:
                    # Если не удалось получить максимальный UID, используем 0
                    last_uid = 0
                    if isinstance(st, dict):
                        st["last_uid"] = last_uid
                    else:
                        st.last_uid = last_uid
            except Exception as e:
                _log(f"First run initialization error for {config.user_id}/{config.acc_id}: {e}")
                # При ошибке устанавливаем last_uid = 0, чтобы начать с начала
                last_uid = 0
                if isinstance(st, dict):
                    st["last_uid"] = last_uid
                else:
                    st.last_uid = last_uid

        # Одна итерация проверки (не бесконечный цикл!)
        if isinstance(st, dict):
            st["last_poll_ts"] = time.time()
        else:
            st.last_poll_ts = time.time()

        try:
            uids = _search_new_uids(imap, last_uid)
        except Exception as e:
            error_str = str(e)
            # EOF ошибки - это обычно временный обрыв соединения, не регистрируем как критическую ошибку
            if "EOF" in error_str or "socket error" in error_str:
                _log(f"Connection error (EOF) for {config.user_id}/{config.acc_id}, will reconnect")
                # Не регистрируем как ошибку, просто переподключаемся
                raise
            _register_error(config.user_id, config.acc_id, f"SEARCH error: {e}")
            raise

        if uids:
            try:
                msgs = _fetch_messages(imap, uids)
            except Exception as e:
                _register_error(config.user_id, config.acc_id, f"FETCH error: {e}")
                raise

            if msgs:
                # ВАЖНО: Обновляем last_uid СРАЗУ после получения писем,
                # чтобы даже при ошибке отправки в очередь не обработать их повторно
                last_uid = max(last_uid or 0, max(uids))
                if isinstance(st, dict):
                    st["last_uid"] = last_uid
                    st["last_ok_ts"] = time.time()
                else:
                    st.last_uid = last_uid
                    st.last_ok_ts = time.time()
                
                # ВАЖНО: Помечаем письма как прочитанные СРАЗУ после получения,
                # чтобы они не попали в следующий поиск по UNSEEN
                try:
                    # Помечаем все полученные письма как прочитанные одним запросом
                    if uids:
                        try:
                            uid_sequence = ",".join(str(uid) for uid in uids)
                            imap.uid("STORE", uid_sequence, "+FLAGS", r"(\Seen)")
                        except Exception:
                            # Fallback: помечаем по одному
                            for uid in uids:
                                try:
                                    imap.uid("STORE", str(uid), "+FLAGS", r"(\Seen)")
                                except Exception:
                                    pass
                except Exception as e:
                    # Логируем, но не прерываем обработку
                    _log(f"Failed to mark messages as read for {config.user_id}/{config.acc_id}: {e}")
                
                # Отправляем в очередь для обработки (только новые письма)
                result_queue.put(
                    (config.user_id,
                     config.acc_id,
                     config.chat_id,
                     config.email,
                     msgs)
                )

        try:
            imap.noop()
        except Exception as e:
            error_str = str(e)
            # EOF ошибки - это обычно временный обрыв соединения, не регистрируем как критическую ошибку
            if "EOF" in error_str or "socket error" in error_str:
                _log(f"Connection error (EOF) during NOOP for {config.user_id}/{config.acc_id}, will reconnect")
                # Не регистрируем как ошибку, просто переподключаемся
                raise
            _register_error(config.user_id, config.acc_id, f"NOOP error: {e}")
            raise

    except Exception as e:
        _log(f"Worker exception for {config.user_id}/{config.acc_id}: {e}")
        traceback.print_exc()
        _register_error(config.user_id, config.acc_id, str(e))
    finally:
        if imap is not None:
            try:
                imap.logout()
            except Exception:
                pass
        _release_connection()
        st = ACCOUNT_STATUS.get(key)
        if st:
            if isinstance(st, dict):
                st["state"] = "IDLE"
            else:
                st.state = "IDLE"


def _imap_account_worker(config: ImapAccountConfig,
                         stop_event: Event,
                         result_queue: Queue):
    """
    Старый воркер (один процесс на аккаунт) - оставлен для обратной совместимости.
    НЕ ИСПОЛЬЗУЕТСЯ в новой архитектуре с пулом процессов.
    """
    key = (config.user_id, config.acc_id)
    if key not in ACCOUNT_STATUS:
        ACCOUNT_STATUS[key] = {
            "state": "NEW",
            "last_error": None,
            "backoff_until": 0.0,
            "last_uid": None,
            "last_poll_ts": 0.0,
            "last_ok_ts": 0.0
        }

    while not stop_event.is_set():
        if not _can_use_account(config.user_id, config.acc_id):
            time.sleep(5)
            continue

        if not _reserve_connection():
            time.sleep(1)
            continue

        imap = None
        try:
            st = ACCOUNT_STATUS[key]
            if isinstance(st, dict):
                st["state"] = "CONNECTING"
            else:
                st.state = "CONNECTING"
            imap = _imap_connect(config)
            if isinstance(st, dict):
                st["state"] = "ACTIVE"
            else:
                st.state = "ACTIVE"

            last_uid = st.get("last_uid") if isinstance(st, dict) else st.last_uid

            # ВАЖНО: При первом запуске (last_uid is None) помечаем все существующие UNSEEN как прочитанные
            # и устанавливаем last_uid на максимальный UID всех писем, чтобы не публиковать старые письма
            if last_uid is None:
                try:
                    # Находим все UNSEEN письма
                    typ, data = imap.uid("SEARCH", None, "UNSEEN")
                    if typ == "OK" and data and data[0]:
                        raw = data[0].decode() if isinstance(data[0], bytes) else data[0]
                        unseen_uids = [int(x) for x in raw.split() if x.strip().isdigit()]
                        
                        if unseen_uids:
                            # Помечаем все UNSEEN как прочитанные (но НЕ публикуем их)
                            _log(f"First run: marking {len(unseen_uids)} UNSEEN messages as read for {config.user_id}/{config.acc_id}")
                            # Помечаем все письма одним запросом для эффективности
                            try:
                                uid_sequence = ",".join(str(uid) for uid in unseen_uids)
                                imap.uid("STORE", uid_sequence, "+FLAGS", r"(\Seen)")
                            except Exception:
                                # Fallback: помечаем по одному
                                for uid in unseen_uids:
                                    try:
                                        imap.uid("STORE", str(uid), "+FLAGS", r"(\Seen)")
                                    except Exception:
                                        pass
                            # ВАЖНО: Делаем EXPUNGE или хотя бы закрываем/открываем SELECT,
                            # чтобы сервер применил изменения флагов
                            try:
                                imap.expunge()
                            except Exception:
                                pass
                    
                    # Устанавливаем last_uid на максимальный UID всех писем в INBOX
                    max_uid = _get_max_uid(imap)
                    if max_uid is not None:
                        last_uid = max_uid
                        if isinstance(st, dict):
                            st["last_uid"] = last_uid
                        else:
                            st.last_uid = last_uid
                        _log(f"First run: set last_uid={last_uid} for {config.user_id}/{config.acc_id}")
                    else:
                        # Если не удалось получить максимальный UID, используем 0
                        last_uid = 0
                        if isinstance(st, dict):
                            st["last_uid"] = last_uid
                        else:
                            st.last_uid = last_uid
                except Exception as e:
                    _log(f"First run initialization error for {config.user_id}/{config.acc_id}: {e}")
                    # При ошибке устанавливаем last_uid = 0, чтобы начать с начала
                    last_uid = 0
                    if isinstance(st, dict):
                        st["last_uid"] = last_uid
                    else:
                        st.last_uid = last_uid

            while not stop_event.is_set():
                if isinstance(st, dict):
                    st["last_poll_ts"] = time.time()
                else:
                    st.last_poll_ts = time.time()

                try:
                    uids = _search_new_uids(imap, last_uid)
                except Exception as e:
                    error_str = str(e)
                    # EOF ошибки - это обычно временный обрыв соединения, не регистрируем как критическую ошибку
                    if "EOF" in error_str or "socket error" in error_str:
                        _log(f"Connection error (EOF) for {config.user_id}/{config.acc_id}, will reconnect")
                        # Не регистрируем как ошибку, просто переподключаемся
                        raise
                    _register_error(config.user_id, config.acc_id, f"SEARCH error: {e}")
                    raise

                if uids:
                    try:
                        msgs = _fetch_messages(imap, uids)
                    except Exception as e:
                        _register_error(config.user_id, config.acc_id, f"FETCH error: {e}")
                        raise

                    if msgs:
                        # ВАЖНО: Обновляем last_uid СРАЗУ после получения писем,
                        # чтобы даже при ошибке отправки в очередь не обработать их повторно
                        last_uid = max(last_uid or 0, max(uids))
                        if isinstance(st, dict):
                            st["last_uid"] = last_uid
                            st["last_ok_ts"] = time.time()
                        else:
                            st.last_uid = last_uid
                            st.last_ok_ts = time.time()
                        
                        # ВАЖНО: Помечаем письма как прочитанные СРАЗУ после получения,
                        # чтобы они не попали в следующий поиск по UNSEEN
                        try:
                            # Помечаем все полученные письма как прочитанные одним запросом
                            if uids:
                                try:
                                    uid_sequence = ",".join(str(uid) for uid in uids)
                                    imap.uid("STORE", uid_sequence, "+FLAGS", r"(\Seen)")
                                except Exception:
                                    # Fallback: помечаем по одному
                                    for uid in uids:
                                        try:
                                            imap.uid("STORE", str(uid), "+FLAGS", r"(\Seen)")
                                        except Exception:
                                            pass
                        except Exception as e:
                            # Логируем, но не прерываем обработку
                            _log(f"Failed to mark messages as read for {config.user_id}/{config.acc_id}: {e}")
                        
                        # Отправляем в очередь для обработки (только новые письма)
                        result_queue.put(
                            (config.user_id,
                             config.acc_id,
                             config.chat_id,
                             config.email,
                             msgs)
                        )

                try:
                    imap.noop()
                except Exception as e:
                    error_str = str(e)
                    # EOF ошибки - это обычно временный обрыв соединения, не регистрируем как критическую ошибку
                    if "EOF" in error_str or "socket error" in error_str:
                        _log(f"Connection error (EOF) during NOOP for {config.user_id}/{config.acc_id}, will reconnect")
                        # Не регистрируем как ошибку, просто переподключаемся
                        raise
                    _register_error(config.user_id, config.acc_id, f"NOOP error: {e}")
                    raise

                sleep_for = random.uniform(IMAP_POLL_INTERVAL_MIN, IMAP_POLL_INTERVAL_MAX)
                time.sleep(sleep_for)

        except Exception as e:
            _log(f"Worker exception for {config.user_id}/{config.acc_id}: {e}")
            traceback.print_exc()
            _register_error(config.user_id, config.acc_id, str(e))
            time.sleep(2)
        finally:
            if imap is not None:
                try:
                    imap.logout()
                except Exception:
                    pass
            _release_connection()

        if stop_event.is_set():
            break

    st = ACCOUNT_STATUS.get(key)
    if st:
        if isinstance(st, dict):
            st["state"] = "STOPPED"
        else:
            st.state = "STOPPED"
    _log(f"Worker stopped for {config.user_id}/{config.acc_id}")


def init_imap_runtime() -> None:
    global RESULT_QUEUE, GLOBAL_STOP_EVENT, ACCOUNT_QUEUE, _manager, ACCOUNT_STATUS
    if RESULT_QUEUE is None:
        RESULT_QUEUE = Queue()
    if GLOBAL_STOP_EVENT is None:
        GLOBAL_STOP_EVENT = Event()
    if ACCOUNT_QUEUE is None:
        ACCOUNT_QUEUE = Queue()
    if _manager is None:
        _manager = Manager()
        # Создаем разделяемый словарь для синхронизации статуса между процессами
        # ВАЖНО: Используем обычный dict, так как dataclass не сериализуется через Manager
        # Будем хранить статус как словарь: {"state": str, "last_poll_ts": float, ...}
        ACCOUNT_STATUS = _manager.dict()
    _log("Runtime initialized (Persistent IMAP NOOP/SEARCH Runtime)")


def shutdown_imap_runtime() -> None:
    global GLOBAL_STOP_EVENT, ACCOUNT_QUEUE
    _log("Shutdown requested")
    if GLOBAL_STOP_EVENT is not None:
        GLOBAL_STOP_EVENT.set()
    
    # Останавливаем пул процессов
    with WORKER_POOL_LOCK:
        # Отправляем сигналы остановки в очередь
        if ACCOUNT_QUEUE is not None:
            for _ in range(len(WORKER_POOL)):
                try:
                    ACCOUNT_QUEUE.put(None)  # Сигнал остановки
                except:
                    pass
        
        # Останавливаем все воркеры пула
        for proc in WORKER_POOL:
            if proc.is_alive():
                proc.terminate()
            proc.join(timeout=5)
        WORKER_POOL.clear()
    
    # Останавливаем старые воркеры (для обратной совместимости)
    for key, proc in list(WORKER_PROCESSES.items()):
        if proc.is_alive():
            proc.terminate()
        proc.join(timeout=5)
        del WORKER_PROCESSES[key]
    
    _log("All workers terminated")


def start_imap_for_account(config: ImapAccountConfig) -> None:
    """
    Добавляет аккаунт в очередь для обработки пулом процессов.
    Если пул процессов не запущен, запускает его.
    """
    assert RESULT_QUEUE is not None, "init_imap_runtime() not called"
    assert GLOBAL_STOP_EVENT is not None
    assert ACCOUNT_QUEUE is not None, "init_imap_runtime() not called"

    key = (config.user_id, config.acc_id)
    
    # Проверяем, не запущен ли уже старый воркер для этого аккаунта
    existing = WORKER_PROCESSES.get(key)
    if existing and existing.is_alive():
        # Останавливаем старый воркер
        try:
            existing.terminate()
            existing.join(timeout=2)
        except:
            pass
        del WORKER_PROCESSES[key]
    
    # Запускаем пул процессов, если еще не запущен
    with WORKER_POOL_LOCK:
        # ВАЖНО: Запускаем воркеры постепенно, по мере добавления аккаунтов
        # Но не более MAX_IMAP_WORKER_PROCESSES
        if len(WORKER_POOL) < MAX_IMAP_WORKER_PROCESSES:
            # Создаем новый воркер пула
            stop_event = Event()
            worker_id = len(WORKER_POOL)
            proc = Process(
                target=_imap_pool_worker,
                args=(ACCOUNT_QUEUE, RESULT_QUEUE, stop_event, worker_id, ACCOUNT_STATUS),
                daemon=True,
            )
            proc.start()
            WORKER_POOL.append(proc)
            _log(f"Pool worker {worker_id} started (total: {len(WORKER_POOL)}/{MAX_IMAP_WORKER_PROCESSES})")
    
    # Добавляем аккаунт в очередь для обработки
    try:
        config_dict = {
            "user_id": config.user_id,
            "acc_id": config.acc_id,
            "email": config.email,
            "password": config.password,
            "display_name": config.display_name,
            "chat_id": config.chat_id,
            "host": config.host,
            "proxy": config.proxy
        }
        ACCOUNT_QUEUE.put(config_dict)
        # ВАЖНО: Обновляем статус в разделяемом словаре
        if key not in ACCOUNT_STATUS:
            ACCOUNT_STATUS[key] = {
                "state": "CONNECTING",
                "last_error": None,
                "backoff_until": 0.0,
                "last_uid": None,
                "last_poll_ts": 0.0,
                "last_ok_ts": 0.0
            }
        else:
            # Обновляем состояние на CONNECTING
            st = ACCOUNT_STATUS[key]
            if isinstance(st, dict):
                st["state"] = "CONNECTING"
                st["last_poll_ts"] = 0.0  # Сбрасываем, чтобы показать, что еще не было опроса
            else:
                st.state = "CONNECTING"
        _log(f"Account {config.user_id}/{config.acc_id} ({config.email}) added to queue (total accounts in queue: {ACCOUNT_QUEUE.qsize()})")
    except Exception as e:
        _log(f"Failed to add account {config.user_id}/{config.acc_id} to queue: {e}")


def stop_imap_for_account(user_id: int, acc_id: int) -> None:
    key = (user_id, acc_id)
    proc = WORKER_PROCESSES.get(key)
    if not proc:
        return
    if proc.is_alive():
        proc.terminate()
    proc.join(timeout=5)
    del WORKER_PROCESSES[key]
    st = ACCOUNT_STATUS.get(key)
    if st:
        if isinstance(st, dict):
            st["state"] = "STOPPED"
        else:
            st.state = "STOPPED"
    _log(f"Worker manually stopped for {user_id}/{acc_id}")


def get_imap_status_for_user(user_id: int) -> Dict[int, Dict[str, Any]]:
    """
    Возвращает статус всех аккаунтов пользователя.
    ВАЖНО: Возвращает словари, так как ACCOUNT_STATUS теперь использует Manager.dict()
    """
    res: Dict[int, Dict[str, Any]] = {}
    with LOCK:
        for (uid, acc_id), st in ACCOUNT_STATUS.items():
            if uid == user_id:
                if isinstance(st, dict):
                    res[acc_id] = st.copy()
                else:
                    # Обратная совместимость с dataclass
                    res[acc_id] = {
                        "state": st.state,
                        "last_error": st.last_error,
                        "backoff_until": st.backoff_until,
                        "last_uid": st.last_uid,
                        "last_poll_ts": st.last_poll_ts,
                        "last_ok_ts": st.last_ok_ts
                    }
    return res


def is_imap_account_active(user_id: int, acc_id: int) -> bool:
    """
    Проверяет, активен ли IMAP воркер для аккаунта.
    
    В новой архитектуре с пулом процессов:
    - Проверяем состояние в ACCOUNT_STATUS (обновляется воркерами через Manager)
    - Проверяем наличие в WORKER_PROCESSES (старая архитектура, для обратной совместимости)
    - Проверяем, что аккаунт не в BACKOFF состоянии
    """
    key = (user_id, acc_id)
    
    # Проверяем состояние в ACCOUNT_STATUS
    with LOCK:
        st = ACCOUNT_STATUS.get(key)
        if st:
            # Поддерживаем обратную совместимость с dataclass и dict
            if isinstance(st, dict):
                state = st.get("state", "NEW")
                backoff_until = st.get("backoff_until", 0)
                last_poll_ts = st.get("last_poll_ts", 0)
            else:
                # Обратная совместимость с dataclass
                state = st.state
                backoff_until = st.backoff_until
                last_poll_ts = st.last_poll_ts
            
            # Если в BACKOFF - неактивен
            if state == "BACKOFF" and time.time() < backoff_until:
                return False
            
            # Активен, если состояние ACTIVE или CONNECTING
            if state in ("ACTIVE", "CONNECTING"):
                # Проверяем, что был недавний опрос (в течение последних 2 минут)
                # или состояние ACTIVE (значит соединение активно)
                if last_poll_ts > 0:
                    time_since_poll = time.time() - last_poll_ts
                    if time_since_poll < 120:  # Был опрос в последние 2 минуты
                        return True
                # Если состояние ACTIVE, но last_poll_ts еще не обновлен или равен 0 - считаем активным
                # (это может быть при первом подключении или если воркер только что установил ACTIVE)
                if state == "ACTIVE":
                    return True
            
            # Если состояние STOPPED - неактивен
            if state == "STOPPED":
                return False
    
    # Fallback: проверяем старую архитектуру (один процесс на аккаунт)
    proc = WORKER_PROCESSES.get(key)
    if proc and proc.is_alive():
        return True
    
    return False