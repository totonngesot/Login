import asyncio
import functools
import gc
import gzip
import inspect
import json
import os
import random
import shutil
import signal
import socket
import statistics
import sys
import threading
import time
import traceback
import tracemalloc
import zlib

from collections import deque
from datetime import datetime
from queue import Empty, Full, Queue

import brotli
import chardet
import psutil
import requests
import websocket

from colorama import Fore, init as colorama_init
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = 10
NAME_BOT = "Sign Ton"


class BoundedSet:
    """LRU-like bounded set to avoid unbounded memory growth for dedupe."""

    def __init__(self, maxlen: int = 10000):
        self._set = set()
        self._dq = deque()
        self.maxlen = maxlen

    def __contains__(self, item):
        return item in self._set

    def add(self, item):
        if item in self._set:
            return
        self._set.add(item)
        self._dq.append(item)
        if len(self._dq) > self.maxlen:
            old = self._dq.popleft()
            try:
                self._set.remove(old)
            except KeyError:
                pass

    def clear(self):
        self._set.clear()
        self._dq.clear()

    def __len__(self):
        return len(self._dq)


colorama_init(autoreset=True)
_global_ua = None


def now_ts():
    return datetime.now().strftime("[%Y:%m:%d ~ %H:%M:%S] |")


class ProxyManager:
    """
    Thread-safe proxy manager.
    - Initialize with a list of proxies (string seperti 'host:port' atau dicts)
    - get_proxy(): ambil 1 proxy
    - release_proxy(proxy): balikin proxy ke pool
    - mark_bad(proxy): karantina proxy yang error
    - snapshot(): lihat kondisi saat ini

    Desain:
    • _available → Queue untuk proxy aktif
    • _in_use → set proxy yang sedang dipakai
    • _quarantine → dict proxy yg diistirahatkan sementara
    """

    def __init__(self, proxies=None, maxsize=0, quarantine_seconds=30):
        self._lock = threading.Lock()
        self._available = Queue(maxsize=maxsize) if maxsize > 0 else Queue()
        self._in_use = set()
        self._quarantine = {}
        self._quarantine_seconds = quarantine_seconds or 30
        proxies = list(proxies or [])
        random.shuffle(proxies)
        for pxy in proxies:
            try:
                self._available.put_nowait(pxy)
            except Full:
                break

    def _requeue_quarantined(self):
        """Balikin proxy yg masa karantinanya udah lewat ke queue."""
        now = time.time()
        expired = []
        with self._lock:
            for pxy, avail_at in list(self._quarantine.items()):
                if avail_at <= now:
                    expired.append(pxy)
                    del self._quarantine[pxy]
        for pxy in expired:
            try:
                self._available.put_nowait(pxy)
            except Full:
                with self._lock:
                    self._quarantine[pxy] = time.time() + min(
                        60, self._quarantine_seconds
                    )

    def get_proxy(self, block=False, timeout=0.1):
        """Ambil 1 proxy dari pool. Kalau kosong, coba requeue dari quarantine."""
        self._requeue_quarantined()
        try:
            pxy = (
                self._available.get(block=block, timeout=timeout)
                if block
                else self._available.get_nowait()
            )
        except Empty:
            return None
        with self._lock:
            self._in_use.add(self._normalize(pxy))
        return pxy

    def release_proxy(self, proxy):
        """Balikin proxy ke pool kalau masih terdaftar di in_use."""
        if proxy is None:
            return
        n = self._normalize(proxy)
        with self._lock:
            if n not in self._in_use:
                return
            self._in_use.remove(n)
        try:
            self._available.put_nowait(proxy)
        except Full:
            with self._lock:
                self._quarantine[proxy] = time.time() + min(
                    60, self._quarantine_seconds
                )

    def mark_bad(self, proxy, quarantine_seconds=None):
        """Karantina proxy selama beberapa detik biar gak langsung dipakai lagi."""
        if proxy is None:
            return
        n = self._normalize(proxy)
        with self._lock:
            if n in self._in_use:
                self._in_use.remove(n)
            self._quarantine[proxy] = time.time() + (
                quarantine_seconds or self._quarantine_seconds
            )

    def available_count(self):
        """Jumlah proxy siap pakai (approximate)."""
        return max(0, self._available.qsize())

    def in_use_count(self):
        """Jumlah proxy yg sedang dipakai."""
        with self._lock:
            return len(self._in_use)

    def _normalize(self, proxy):
        """Ubah proxy jadi string unik biar gampang tracking."""
        if proxy is None:
            return None
        if isinstance(proxy, dict):
            try:
                items = tuple(sorted(proxy.items()))
                return str(items)
            except Exception:
                return str(proxy)
        return str(proxy)

    def snapshot(self):
        """Tampilkan ringkasan state ProxyManager untuk debug/log."""
        with self._lock:
            return {
                "available": self._available.qsize(),
                "in_use": len(self._in_use),
                "quarantine": {k: v for k, v in self._quarantine.items()},
            }


def auto_async(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            asyncio.get_running_loop()
            return func(*args, **kwargs)
        except RuntimeError:
            return asyncio.run(func(*args, **kwargs))

    return wrapper


class signton:
    BASE_URL = "https://api.signton.top/api/"
    HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "br",
        "Accept-Language": "en-GB,en;q=0.9,en-US;q=0.8",
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://web.signton.top",
        "Pragma": "no-cache",
        "Priority": "u=1, i",
        "Referer": "https://web.signton.top/",
        "Sec-Ch-Ua": '"Microsoft Edge";v="142", "Microsoft Edge WebView2";v="142", "Chromium";v="142", "Not_A Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    }

    def __init__(
        self,
        use_proxy: bool = False,
        proxy_list: list | None = None,
        load_on_init: bool = True,
    ):
        """
        load_on_init: pass False for worker instances to avoid duplicate config/query logs.
        """
        self._suppress_local_session_log = not load_on_init
        if load_on_init:
            self.config = self.load_config()
            if self.config.get("reff", False):
                try:
                    self.log("🔁 reff enabled via config.json", Fore.CYAN)
                    self.reff()
                except Exception:
                    pass
            self.query_list = self.load_query("query.txt")
        else:
            self.config = {}
            self.query_list = []
        self.token = None
        self.session = None
        self._session_lock = threading.Lock()
        self.proxy = None
        self.proxy_list = (
            proxy_list
            if proxy_list is not None
            else self.load_proxies()
            if load_on_init
            else []
        )
        self.proxy_manager = (
            ProxyManager(self.proxy_list) if self.config.get("proxy") else None
        )

    def banner(self):
        self.log("")
        self.log("=======================================", Fore.CYAN)
        self.log(f"           🎉  {NAME_BOT} BOT 🎉             ", Fore.CYAN)
        self.log("=======================================", Fore.CYAN)
        self.log("🚀  by LIVEXORDS", Fore.CYAN)
        self.log("📢  t.me/livexordsscript", Fore.CYAN)
        self.log("=======================================", Fore.CYAN)
        self.log("")

    def log(self, message, color=Fore.RESET):
        safe_message = str(message).encode("utf-8", "backslashreplace").decode("utf-8")
        print(Fore.LIGHTBLACK_EX + now_ts() + " " + color + safe_message + Fore.RESET)

    def _request(
        self,
        method: str,
        url_or_path: str,
        *,
        headers: dict | None = None,
        params: dict | None = None,
        data=None,
        json_data=None,
        timeout: float | None = None,
        use_session: bool = True,
        allow_redirects: bool = True,
        stream: bool = False,
        parse: bool = False,
        retries: int = 2,
        backoff: float = 0.5,
        allow_proxy: bool = True,
        debug: bool = False,
    ):
        method = method.upper()
        headers = headers or {}
        timeout = timeout or DEFAULT_TIMEOUT
        if not url_or_path.lower().startswith("http"):
            url = self.BASE_URL.rstrip("/") + "/" + url_or_path.lstrip("/")
        else:
            url = url_or_path
        hdr = dict(getattr(self, "HEADERS", {}) or {})
        if headers:
            hdr.update(headers)
        last_exc = None
        for attempt in range(1, retries + 2):
            chosen_proxy = None
            resp = None
            try:
                proxies = None
                if allow_proxy and getattr(self, "proxy_manager", None):
                    if getattr(self, "proxy", None):
                        chosen_proxy = self.proxy
                        proxies = {"http": chosen_proxy, "https": chosen_proxy}
                    else:
                        chosen_proxy = self.proxy_manager.get_proxy(
                            block=True, timeout=0.5
                        )
                        if chosen_proxy:
                            self.proxy = chosen_proxy
                            self._proxy_use_count = 0
                            proxies = {"http": chosen_proxy, "https": chosen_proxy}
                if debug:
                    self.log(f"[DEBUG] {method} {url}", Fore.MAGENTA)
                    if proxies:
                        self.log(f"[DEBUG] Using proxy: {chosen_proxy}", Fore.MAGENTA)
                    if params:
                        self.log(f"[DEBUG] Params: {params}", Fore.MAGENTA)
                    if json_data:
                        self.log(f"[DEBUG] JSON: {json_data}", Fore.MAGENTA)
                    elif data:
                        self.log(f"[DEBUG] Data: {data}", Fore.MAGENTA)
                call_args = dict(
                    headers=hdr,
                    params=params,
                    data=data,
                    json=json_data,
                    timeout=timeout,
                    allow_redirects=allow_redirects,
                    stream=stream,
                )
                if use_session and getattr(self, "session", None):
                    if proxies:
                        call_args["proxies"] = proxies
                    resp = self.session.request(method, url, **call_args)
                else:
                    if proxies:
                        call_args["proxies"] = proxies
                    resp = requests.request(method, url, **call_args)
                resp.raise_for_status()
                if debug:
                    ct = (resp.headers.get("Content-Type") or "").lower()
                    is_text = any(
                        x in ct for x in ("text", "json", "xml", "html", "javascript")
                    )
                    if not stream and is_text:
                        try:
                            preview = next(resp.iter_content(chunk_size=512))[
                                :250
                            ].decode("utf-8", "replace")
                        except Exception:
                            preview = (resp.text or "")[:250].replace("\n", " ")
                        self.log(f"[DEBUG] Response preview: {preview}", Fore.MAGENTA)
                    else:
                        preview = (resp.text or "")[:250].replace("\n", " ")
                        self.log(f"[DEBUG] Response preview: {preview}", Fore.MAGENTA)
                if chosen_proxy:
                    self._proxy_use_count = getattr(self, "_proxy_use_count", 0) + 1
                    rotate_after = (
                        int(self.config.get("proxy_rotate_every", 0))
                        if getattr(self, "config", None)
                        else 0
                    )
                    if rotate_after > 0 and self._proxy_use_count >= rotate_after:
                        try:
                            if getattr(self, "proxy_manager", None):
                                self.proxy_manager.release_proxy(self.proxy)
                        except Exception:
                            pass
                        self.proxy = None
                        self._proxy_use_count = 0
                decoded = None
                if parse:
                    try:
                        decoded = self.decode_response(resp)
                    except Exception as e:
                        self.log(f"[DEBUG] Parse failed: {e}", Fore.MAGENTA)
                return (resp, decoded)
            except requests.exceptions.RequestException as e:
                last_exc = e
                if chosen_proxy and getattr(self, "proxy_manager", None):
                    try:
                        if (
                            hasattr(self, "proxy_manager")
                            and self.proxy_manager is not None
                        ):
                            self.proxy_manager.mark_bad(chosen_proxy)
                            try:
                                self.log(
                                    f"🚫 Marked bad proxy: {chosen_proxy} | snapshot={self.proxy_manager.snapshot()}",
                                    Fore.YELLOW,
                                )
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        self.log(
                            f"⚠️ Proxy {chosen_proxy} failure on attempt {attempt}: {e}",
                            Fore.YELLOW,
                        )
                    except Exception:
                        pass
                    self.proxy = None
                    self._proxy_use_count = 0
                else:
                    self.log(
                        f"⚠️ Request error attempt {attempt} for {method} {url}: {e}",
                        Fore.YELLOW,
                    )
                if attempt >= retries + 1:
                    self.log(
                        f"❌ Giving up on {method} {url} after {attempt} attempts",
                        Fore.RED,
                    )
                    raise
                sleep_for = (
                    backoff * 2 ** (attempt - 1) * random.uniform(0.9, 1.1)
                    + random.random() * 0.2
                )
                time.sleep(sleep_for)
                continue
            except Exception as e:
                if debug:
                    self.log(
                        f"[DEBUG] Unexpected error: {traceback.format_exc()}",
                        Fore.MAGENTA,
                    )
                if getattr(self, "proxy", None) and getattr(
                    self, "proxy_manager", None
                ):
                    try:
                        if (
                            hasattr(self, "proxy_manager")
                            and self.proxy_manager is not None
                        ):
                            self.proxy_manager.mark_bad(self.proxy)
                            try:
                                self.log(
                                    f"🚫 Marked bad proxy: {self.proxy} | snapshot={self.proxy_manager.snapshot()}",
                                    Fore.YELLOW,
                                )
                            except Exception:
                                pass
                    except Exception:
                        pass
                    self.proxy = None
                self.log(
                    f"❌ Unexpected error during request {method} {url}: {e}", Fore.RED
                )
                raise
        if last_exc:
            raise last_exc
        raise RuntimeError("request failed unexpectedly")

    def memory_monitor(self):
        try:
            p = psutil.Process()
            mem = p.memory_info().rss
            return f"RSS={mem / 1024 / 1024:.2f} MB"
        except Exception:
            tracemalloc.start()
            s = tracemalloc.take_snapshot()
            total = sum([stat.size for stat in s.statistics("filename")])
            return f"tracemalloc_total={total / 1024 / 1024:.2f} MB"

    def clear_locals(self):
        try:
            self.log(f"🔎 Memory before clear: {self.memory_monitor()}", Fore.MAGENTA)
        except Exception:
            pass
        frame = inspect.currentframe()
        try:
            caller = frame.f_back
            if not caller:
                return
            names = list(caller.f_locals.keys())
            for name in names:
                if not name.startswith("__") and name != "self":
                    try:
                        del caller.f_locals[name]
                    except Exception:
                        try:
                            caller.f_locals[name] = None
                        except Exception:
                            pass
            try:
                gc.collect()
            except Exception:
                pass
            try:
                self.log(
                    f"✅ Memory after clear: {self.memory_monitor()}",
                    Fore.LIGHTBLACK_EX,
                )
            except Exception:
                pass
        finally:
            try:
                del frame
                del caller
            except Exception:
                pass

    def get_ua(self):
        """
        Return a shared fake UserAgent instance (or None).
        Uses the module-global _global_ua so we don't recreate the UserAgent object.
        """
        global _global_ua
        if _global_ua is None:
            try:
                _global_ua = UserAgent()
            except Exception:
                _global_ua = None
        return _global_ua

    def rotate_proxy_and_ua(
        self, force_new_proxy: bool = True, quick_test: bool = True
    ):
        """
        Safer rotate: DOES NOT perform network I/O while holding self._session_lock.
        - force_new_proxy: force picking a new proxy even if one exists
        - quick_test: perform a quick connectivity check (HEAD/GET) with a temp session
        """
        test_url = getattr(self, "BASE_URL", "") or "https://httpbin.org/ip"

        def _make_test_session(timeout=5, max_retries=1):
            s = requests.Session()
            retry = Retry(
                total=max_retries,
                backoff_factor=0.1,
                status_forcelist=(429, 500, 502, 503, 504),
            )
            s.mount("http://", HTTPAdapter(max_retries=retry))
            s.mount("https://", HTTPAdapter(max_retries=retry))
            s.request_timeout = timeout
            return s

        if not force_new_proxy and getattr(self, "proxy", None):
            return
        attempts = 0
        max_attempts = max(3, len(getattr(self, "proxy_list", []) or []) + 1)
        while attempts < max_attempts:
            attempts += 1
            candidate = None
            try:
                if hasattr(self, "proxy_manager") and self.proxy_manager is not None:
                    candidate = self.proxy_manager.get_proxy(block=True, timeout=0.5)
                else:
                    plist = getattr(self, "proxy_list", []) or []
                    if not plist:
                        candidate = None
                    else:
                        candidate = random.choice(plist)
            except Exception:
                candidate = None
            if not candidate:
                with self._session_lock:
                    try:
                        if getattr(self, "session", None) is not None:
                            self.session.proxies = {}
                        self.proxy = None
                    except Exception:
                        pass
                return
            if quick_test:
                ok = False
                test_session = _make_test_session(
                    timeout=min(5, getattr(self, "TEST_TIMEOUT", 5)), max_retries=1
                )
                try:
                    if isinstance(candidate, dict):
                        test_session.proxies.update(candidate)
                    else:
                        test_session.proxies.update(
                            {"http": candidate, "https": candidate}
                        )
                    try:
                        resp = test_session.head(
                            test_url,
                            timeout=getattr(test_session, "request_timeout", 5),
                        )
                        ok = resp.status_code < 400
                    except Exception:
                        resp = test_session.get(
                            test_url,
                            timeout=getattr(test_session, "request_timeout", 5),
                        )
                        ok = resp.status_code < 400
                except Exception:
                    ok = False
                finally:
                    try:
                        test_session.close()
                    except Exception:
                        pass
                if not ok:
                    try:
                        if (
                            hasattr(self, "proxy_manager")
                            and self.proxy_manager is not None
                        ):
                            self.proxy_manager.mark_bad(candidate)
                            try:
                                self.log(
                                    f"🚫 Marked bad proxy: {candidate} | snapshot={self.proxy_manager.snapshot()}",
                                    Fore.YELLOW,
                                )
                            except Exception:
                                pass
                    except Exception:
                        pass
                    time.sleep(0.1)
                    continue
            with self._session_lock:
                try:
                    if getattr(self, "session", None) is None:
                        try:
                            self.prepare_session()
                        except Exception:
                            self.session = requests.Session()
                    if isinstance(candidate, dict):
                        self.session.proxies.update(candidate)
                    else:
                        self.session.proxies.update(
                            {"http": candidate, "https": candidate}
                        )
                    self.proxy = candidate
                    if hasattr(self, "get_ua"):
                        try:
                            ua = self.get_ua()
                            if ua:
                                self.HEADERS["user-agent"] = ua
                                self.session.headers.update(self.HEADERS)
                        except Exception:
                            pass
                    else:
                        try:
                            self.session.headers.update(getattr(self, "HEADERS", {}))
                        except Exception:
                            pass
                except Exception:
                    try:
                        if (
                            hasattr(self, "proxy_manager")
                            and self.proxy_manager is not None
                        ):
                            self.proxy_manager.mark_bad(candidate)
                            try:
                                self.log(
                                    f"🚫 Marked bad proxy: {candidate} | snapshot={self.proxy_manager.snapshot()}",
                                    Fore.YELLOW,
                                )
                            except Exception:
                                pass
                    except Exception:
                        pass
                    continue
            return
        with self._session_lock:
            try:
                if getattr(self, "session", None) is not None:
                    self.session.proxies = {}
                self.proxy = None
            except Exception:
                pass

    def reff(self):
        """Placeholder untuk fitur referral.
        Akan otomatis aktif kalau config.json berisi "reff": true.
        Sudah dilengkapi mekanisme clear untuk menjaga memori bersih.
        """
        try:
            self.log("🎯 Running reff placeholder...", Fore.CYAN)
            pass
        except Exception as e:
            self.log(f"❌ reff error: {e}", Fore.RED)
        finally:
            try:
                self.clear_locals()
            except Exception:
                pass

    def load_config(self, suppress_log: bool = False):
        """
        Load config.json. If suppress_log=True, don't print the 'Config loaded' message.
        """
        try:
            with open("config.json", encoding="utf-8") as f:
                cfg = json.load(f)
            if not suppress_log:
                self.log("✅ Config loaded", Fore.GREEN)
            return cfg
        except FileNotFoundError:
            if not suppress_log:
                self.log("⚠️ config.json not found (using minimal)", Fore.YELLOW)
            return {}
        except Exception as e:
            if not suppress_log:
                self.log(f"❌ Config parse error: {e}", Fore.RED)
            return {}

    def load_query(self, path_file: str = "query.txt") -> list:
        """
        Load queries from `path_file`. If `self.config` indicates reff is enabled,
        also attempt to load supplemental queries from `reff_result.txt` and
        append them (preserving order, removing duplicates).
        """
        try:
            queries = []
            if os.path.exists(path_file):
                with open(path_file, encoding="utf-8") as file:
                    for line in file:
                        v = line.strip()
                        if v:
                            queries.append(v)
            else:
                self.log(f"❌ {path_file} not found", Fore.RED)
                return []
            if getattr(self, "config", None) and self.config.get("reff", False):
                reff_file = "reff_result.txt"
                try:
                    if os.path.exists(reff_file):
                        with open(reff_file, encoding="utf-8") as rf:
                            reff_lines = [ln.strip() for ln in rf if ln.strip()]
                        if reff_lines:
                            seen = set(queries)
                            appended = 0
                            for item in reff_lines:
                                if item not in seen:
                                    queries.append(item)
                                    seen.add(item)
                                    appended += 1
                            self.log(
                                f"🔁 Loaded {len(reff_lines)} from {reff_file} (+{appended} new)",
                                Fore.CYAN,
                            )
                    else:
                        self.log(
                            f"⚪ {reff_file} not found (reff enabled but no results yet)",
                            Fore.YELLOW,
                        )
                except Exception as e:
                    self.log(f"❌ Error loading {reff_file}: {e}", Fore.RED)
            if not queries:
                self.log(f"⚠️ {path_file} empty", Fore.YELLOW)
            else:
                self.log(
                    f"✅ {len(queries)} entries loaded from {path_file}", Fore.GREEN
                )
            return queries
        except Exception as e:
            self.log(f"❌ Query load error: {e}", Fore.RED)
            return []

    def login(self, index: int) -> None:
        self.log("🔐 Attempting to log in...", Fore.GREEN)
        try:
            if index >= len(self.query_list):
                self.log("❌ Invalid login index. Please check again.", Fore.RED)
                return
            self.prepare_session()
            raw_token = self.query_list[index]
            try:
                self.HEADERS["authorization"] = raw_token
            except Exception:
                self.HEADERS = getattr(self, "HEADERS", {}) or {}
                self.HEADERS["authorization"] = raw_token
            self.log(
                f"📋 Using raw token in header: {str(raw_token)[:10]}... (truncated for security)",
                Fore.CYAN,
            )
            reff_code = None
            try:
                reff_code = (
                    self.config.get("reff_code")
                    if getattr(self, "config", None) is not None
                    else None
                )
            except Exception:
                reff_code = None
            payload = None
            if reff_code:
                payload = {"startapp": reff_code}
                display_val = (
                    str(reff_code)
                    if len(str(reff_code)) <= 12
                    else f"{str(reff_code)[:8]}..."
                )
                self.log(f"📦 Will send payload startapp={display_val}", Fore.CYAN)
            else:
                self.log(
                    "📦 No reff_code configured — no payload will be sent.", Fore.CYAN
                )
            login_resp = None
            try:
                if payload:
                    self.log("📡 Sending POST to user/login with payload...", Fore.CYAN)
                else:
                    self.log("📡 Sending POST to user/login (no payload)...", Fore.CYAN)
                login_resp, login_data = self._request(
                    "POST",
                    "user/login",
                    data=payload,
                    timeout=DEFAULT_TIMEOUT,
                    parse=True,
                    retries=2,
                    backoff=0.5,
                )
            except requests.exceptions.RequestException as e:
                self.log(f"❌ Failed to send login request: {e}", Fore.RED)
                if login_resp is not None:
                    try:
                        self.log(f"📄 Response content: {login_resp.text}", Fore.RED)
                    except Exception:
                        pass
                return
            except Exception as e:
                self.log(f"❌ Unexpected error during login request: {e}", Fore.RED)
                if login_resp is not None:
                    try:
                        self.log(f"📄 Response content: {login_resp.text}", Fore.RED)
                    except Exception:
                        pass
                return
            try:
                if not isinstance(login_data, dict):
                    raise ValueError("Login response not parsed as JSON/dict")
                code = login_data.get("code")
                if code != 1:
                    msg = login_data.get("msg", "No message")
                    raise ValueError(
                        f"login failed or unexpected code: {code} msg={msg}"
                    )
                data = login_data.get("data", {})
                if not isinstance(data, dict) or not data:
                    raise ValueError("login 'data' missing or not an object")
                username = data.get("username", "N/A")
                first_name = data.get("first_name", "")
                last_name = data.get("last_name", "")
                points = data.get("points", "N/A")
                wallet = data.get("wallet_address", "N/A")
                tg_id = data.get("tg_id", "N/A")
                login_day = data.get("login_day", "N/A")
                is_new = data.get("is_new", "N/A")
                avatar = data.get("avatar", "")
                server_token = data.get("token", "")
                self.log("👤 Login Result Summary:", Fore.GREEN)
                self.log(f"    - Username: {username}", Fore.CYAN)
                if first_name or last_name:
                    self.log(f"    - Name: {first_name} {last_name}".strip(), Fore.CYAN)
                self.log(f"    - Points: {points}", Fore.CYAN)
                self.log(f"    - Wallet: {wallet}", Fore.CYAN)
                self.log(f"    - Telegram ID: {tg_id}", Fore.CYAN)
                self.log(f"    - Login Day: {login_day}", Fore.CYAN)
                self.log(f"    - Is New: {is_new}", Fore.CYAN)
                if avatar:
                    self.log(f"    - Avatar URL: {avatar}", Fore.CYAN)
                if server_token:
                    try:
                        self.HEADERS["authorization"] = server_token
                        self.log("✅ Server token stored in headers.", Fore.GREEN)
                        self.log(
                            f"    - Token (truncated): {server_token[:10]}...",
                            Fore.CYAN,
                        )
                    except Exception as e:
                        self.log(
                            f"❌ Failed to set server token in headers: {e}", Fore.RED
                        )
                else:
                    raise ValueError("token not found in login response data")
            except Exception as e:
                self.log(f"❌ Error processing login response: {e}", Fore.RED)
                return
        finally:
            try:
                self.clear_locals()
            except Exception:
                pass

    def daily(self) -> None:
        """
        Flow:
        1) POST sign/signCheck (no payload)
        - if code == 1 -> success, continue to claim
        - if code == 3 and msg == "Signed in" -> already claimed today
        - if code == 3 and msg indicates missing wallet -> set wallet then retry signCheck
        - otherwise -> log and exit
        2) If signCheck successful, POST sign/claim with year/month/day from signCheck data
        """
        self.log(
            "📅 Running daily tasks... (signCheck -> set wallet if needed -> claim)",
            Fore.GREEN,
        )
        sign_resp = None
        try:
            self.log("📡 Requesting sign/signCheck...", Fore.CYAN)
            sign_resp, sign_data = self._request(
                "POST", "sign/signCheck", timeout=DEFAULT_TIMEOUT, parse=True, retries=1
            )
            if not isinstance(sign_data, dict):
                raise ValueError("sign/signCheck response not parsed as JSON/dict")
            code = sign_data.get("code")
            msg = sign_data.get("msg", "")
            if code == 3 and msg.strip().lower() == "signed in":
                self.log("✅ Already signed in today.", Fore.GREEN)
                return
            if code == 1:
                self.log("✅ sign/signCheck returned success.", Fore.CYAN)
                signcheck_success = sign_data.get("data", {}) or {}
            elif code == 3 and "wallet" in msg.lower():
                self.log(f"⚠️ sign/signCheck returned code=3 msg={msg}", Fore.YELLOW)
                wallet = self.config.get("ton_address")
                if not wallet:
                    self.log(
                        "❌ No wallet in self.config['ton_address'] — cannot set wallet.",
                        Fore.RED,
                    )
                    return
                set_resp = None
                try:
                    self.log(
                        "📡 Attempting to set wallet via user/setWalletAddress...",
                        Fore.CYAN,
                    )
                    set_resp, set_data = self._request(
                        "POST",
                        "user/setWalletAddress",
                        data={"wallet_address": wallet},
                        timeout=DEFAULT_TIMEOUT,
                        parse=True,
                        retries=1,
                    )
                    if not isinstance(set_data, dict):
                        raise ValueError(
                            "user/setWalletAddress response not parsed as JSON/dict"
                        )
                    if set_data.get("code") != 1:
                        self.log(
                            f"❌ user/setWalletAddress failed code={set_data.get('code')} msg={set_data.get('msg')}",
                            Fore.RED,
                        )
                        return
                    else:
                        self.log("✅ user/setWalletAddress succeeded.", Fore.CYAN)
                    self.log(
                        "📡 Re-requesting sign/signCheck after setting wallet...",
                        Fore.CYAN,
                    )
                    sign_resp, sign_data = self._request(
                        "POST",
                        "sign/signCheck",
                        timeout=DEFAULT_TIMEOUT,
                        parse=True,
                        retries=1,
                    )
                    if not isinstance(sign_data, dict):
                        raise ValueError(
                            "sign/signCheck (retry) response not parsed as JSON/dict"
                        )
                    code = sign_data.get("code")
                    msg = sign_data.get("msg", "")
                    if code != 1:
                        self.log(
                            f"❌ sign/signCheck retry returned code={code} msg={msg}",
                            Fore.RED,
                        )
                        return
                    else:
                        self.log("✅ sign/signCheck retry succeeded.", Fore.CYAN)
                        signcheck_success = sign_data.get("data", {}) or {}
                except Exception as e:
                    self.log(
                        f"❌ Error processing user/setWalletAddress or retry: {e}",
                        Fore.RED,
                    )
                    return
            else:
                self.log(
                    f"⚠️ sign/signCheck returned code={code} msg={msg}", Fore.YELLOW
                )
                return
            status = signcheck_success.get("status", "N/A")
            sign_points = signcheck_success.get("sign_points", "N/A")
            month = signcheck_success.get("month")
            day = signcheck_success.get("day")
            self.log("🔎 sign/signCheck result:", Fore.GREEN)
            self.log(f"    - Status: {status}", Fore.CYAN)
            self.log(f"    - Sign Points: {sign_points}", Fore.CYAN)
            self.log(f"    - Month: {month}", Fore.CYAN)
            self.log(f"    - Day: {day}", Fore.CYAN)
            year = str(datetime.now().year)
            if not month or not day:
                now = datetime.now()
                month, day = (now.month, now.day)
            try:
                self.log("📡 Requesting sign/claim...", Fore.CYAN)
                claim_resp, claim_data = self._request(
                    "POST",
                    "sign/claim",
                    data={"year": str(year), "month": str(month), "day": str(day)},
                    timeout=DEFAULT_TIMEOUT,
                    parse=True,
                    retries=1,
                )
                if not isinstance(claim_data, dict):
                    raise ValueError("sign/claim response not parsed as JSON/dict")
                claim_code = claim_data.get("code")
                if claim_code == 1:
                    countdown = claim_data.get("data", {}).get("countdown", "N/A")
                    self.log(
                        f"🎉 sign/claim success — countdown: {countdown}", Fore.CYAN
                    )
                else:
                    self.log(
                        f"⚠️ sign/claim returned code={claim_code} msg={claim_data.get('msg')}",
                        Fore.YELLOW,
                    )
            except Exception as e:
                self.log(f"❌ Error processing sign/claim: {e}", Fore.RED)
        except Exception as e:
            self.log(f"❌ Error in daily flow: {e}", Fore.RED)
        finally:
            try:
                self.clear_locals()
            except Exception:
                pass

    def load_proxies(self, filename="proxy.txt"):
        try:
            if not os.path.exists(filename):
                return []
            with open(filename, encoding="utf-8") as file:
                proxies = list(
                    dict.fromkeys([line.strip() for line in file if line.strip()])
                )
            if not proxies:
                raise ValueError("Proxy file is empty.")
            return proxies
        except Exception as e:
            self.log(f"❌ Proxy load error: {e}", Fore.RED)
            return []

    def decode_response(self, response: object) -> object:
        if isinstance(response, str):
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                return response
        content_encoding = getattr(response.headers, "get", lambda k, d=None: d)(
            "Content-Encoding", ""
        ).lower()
        data = response.content
        try:
            if content_encoding == "gzip":
                data = gzip.decompress(data)
            elif content_encoding in ["br", "brotli"]:
                data = brotli.decompress(data)
            elif content_encoding in ["deflate", "zlib"]:
                data = zlib.decompress(data)
        except Exception:
            pass
        content_type = getattr(response.headers, "get", lambda k, d=None: d)(
            "Content-Type", ""
        ).lower()
        charset = "utf-8"
        if "charset=" in content_type:
            charset = content_type.split("charset=")[-1].split(";")[0].strip()
        try:
            text = data.decode(charset)
        except Exception:
            detected = chardet.detect(data)
            text = data.decode(detected.get("encoding", "utf-8"), errors="replace")
        stripped = text.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                pass
        return text

    def prepare_session(self) -> None:
        try:
            if self.config.get("proxy") and (not getattr(self, "proxy_manager", None)):
                try:
                    self.proxy_manager = ProxyManager(
                        self.proxy_list or [], test_url=self.BASE_URL, test_timeout=4.0
                    )
                except Exception:
                    self.proxy_manager = None
        except Exception:
            pass

        class TimeoutHTTPAdapter(HTTPAdapter):
            def __init__(self, *args, **kwargs):
                self.timeout = kwargs.pop("timeout", 10)
                super().__init__(*args, **kwargs)

            def send(self, request, **kwargs):
                kwargs["timeout"] = kwargs.get("timeout", self.timeout)
                return super().send(request, **kwargs)

        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            raise_on_status=False,
        )
        adapter = TimeoutHTTPAdapter(max_retries=retries, timeout=10)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        try:
            ua = self.get_ua()
            headers = (
                {**self.HEADERS, "User-Agent": ua.random} if ua else {**self.HEADERS}
            )
            session.headers.update(headers)
        except Exception as e:
            self.log(f"⚠️ UA warning: {e}", Fore.YELLOW)
        self.proxy = None
        if self.config.get("proxy") and self.proxy_manager:
            tried_proxies = set()
            total_proxies = len(self.proxy_list)
            result = {"proxy": None}

            def test_proxy(proxy: str):
                if result["proxy"]:
                    return
                test_sess = requests.Session()
                test_sess.headers.update(session.headers)
                test_sess.proxies = {"http": proxy, "https": proxy}
                try:
                    resp = test_sess.get("https://httpbin.org/ip", timeout=5)
                    resp.raise_for_status()
                    ip = resp.json().get("origin", "Unknown")
                    if not result["proxy"]:
                        result["proxy"] = proxy
                        self.log(f"✅ Proxy ok: {proxy}", Fore.GREEN)
                        time.sleep(0.5)
                except Exception:
                    pass

            threads = []
            shuffled_proxies = self.proxy_list[:]
            random.shuffle(shuffled_proxies)
            proxy_iter = iter(shuffled_proxies)
            while not result["proxy"] and len(tried_proxies) < total_proxies:
                threads.clear()
                for _ in range(2):
                    try:
                        proxy = next(proxy_iter)
                        if proxy in tried_proxies:
                            continue
                        tried_proxies.add(proxy)
                        t = threading.Thread(target=test_proxy, args=(proxy,))
                        threads.append(t)
                        t.start()
                    except StopIteration:
                        break
                for t in threads:
                    t.join()
            if result["proxy"]:
                session.proxies = {"http": result["proxy"], "https": result["proxy"]}
                self.proxy = result["proxy"]
                self._proxy_use_count = 0
            else:
                if not self._suppress_local_session_log:
                    self.log("⚠️ No working proxy, using local", Fore.YELLOW)
                session.proxies = {}
        else:
            session.proxies = {}
            if not self._suppress_local_session_log:
                self.log("🌐 Using local IP (no proxy)", Fore.YELLOW)
        self.session = session

    class _WSHandle:
        """Lightweight handle returned by ws_connect().
        Designed to be used from async code (methods are awaitable) even though
        websocket-client is sync: operations run in an executor.
        """

        def __init__(self, parent, raw, proxy_taken=None, prev_session_proxies=None):
            self._parent = parent
            self.lib = "websocket-client"
            self.raw = raw
            self.proxy_taken = proxy_taken
            self._prev_session_proxies = prev_session_proxies
            self._closed = False

        @property
        def is_open(self):
            try:
                if hasattr(self.raw, "connected"):
                    return bool(getattr(self.raw, "connected", True)) and (
                        not self._closed
                    )
                return not self._closed
            except Exception:
                return False

        async def send(self, data, *, timeout=None):
            parent = self._parent

            def _sync_send():
                try:
                    self.raw.send(data)
                    return True
                except Exception as e:
                    parent.log(f"❌ ws.send error: {e}", Fore.RED)
                    raise

            loop = asyncio.get_running_loop()
            if timeout is None:
                return await loop.run_in_executor(None, _sync_send)
            else:
                return await asyncio.wait_for(
                    loop.run_in_executor(None, _sync_send), timeout=timeout
                )

        async def recv(self, *, timeout: float | None = None):
            parent = self._parent

            def _sync_recv():
                try:
                    return self.raw.recv()
                except Exception as e:
                    parent.log(f"⚠️ ws.recv error: {e}", Fore.YELLOW)
                    return None

            loop = asyncio.get_running_loop()
            try:
                if timeout is None:
                    return await loop.run_in_executor(None, _sync_recv)
                else:
                    return await asyncio.wait_for(
                        loop.run_in_executor(None, _sync_recv), timeout=timeout
                    )
            except TimeoutError:
                return None

        async def close(self, *, reason: str | None = None):
            parent = self._parent
            if self._closed:
                return

            def _sync_close():
                try:
                    self.raw.close()
                except Exception:
                    pass

            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(None, _sync_close)
            finally:
                try:
                    if (
                        self._prev_session_proxies is not None
                        and getattr(parent, "session", None) is not None
                    ):
                        parent.session.proxies = self._prev_session_proxies
                except Exception:
                    pass
                try:
                    if self.proxy_taken and getattr(parent, "proxy_manager", None):
                        parent.proxy_manager.release_proxy(self.proxy_taken)
                        parent.log(
                            f"🔁 Proxy released for ws ({self.proxy_taken})", Fore.GREEN
                        )
                except Exception:
                    pass
                self._closed = True

    def _derive_ws_proxy(self):
        """
        Derive proxy string to use for websocket-client connection.
        Priority:
          1. Use self.session.proxies if present (prefer 'http' then 'https')
          2. Else use self.proxy
          3. Else try a non-blocking peek from proxy_manager
        If none found, returns None (direct connection / no proxy).
        """
        try:
            sess = getattr(self, "session", None)
            if sess is not None:
                proxies = getattr(sess, "proxies", None)
                if isinstance(proxies, dict):
                    p = proxies.get("http") or proxies.get("https")
                    if p:
                        return p
            px = getattr(self, "proxy", None)
            if px:
                if isinstance(px, dict):
                    p = px.get("http") or px.get("https")
                    if p:
                        return p
                elif isinstance(px, str):
                    return px
            pm = getattr(self, "proxy_manager", None)
            if pm:
                candidate = pm.get_proxy(block=False, timeout=0.01)
                if candidate:
                    pm.release_proxy(candidate)
                    if isinstance(candidate, dict):
                        p = candidate.get("http") or candidate.get("https")
                        if p:
                            return p
                    elif isinstance(candidate, str):
                        return candidate
        except Exception:
            pass
        return None

    @auto_async
    async def ws_connect(
        self,
        url: str,
        *,
        proxy: str | None = None,
        timeout: float = 30.0,
        use_proxy_from_pool: bool = False,
        debug: bool = False,
    ):
        """Open a websocket-client connection in executor and return _WSHandle.

        - By default this will respect session.proxies (if set) or self.proxy.
        - If use_proxy_from_pool=True it will get a proxy from proxy_manager (blocking) and
          temporarily set session.proxies (restored on close).
        """
        chosen_proxy = proxy or self._derive_ws_proxy()
        proxy_taken = None
        prev_session_proxies = None
        if use_proxy_from_pool and getattr(self, "proxy_manager", None):
            try:
                candidate = self.proxy_manager.get_proxy(block=True, timeout=2.0)
                if candidate:
                    proxy_taken = candidate
                    if isinstance(candidate, dict):
                        p = candidate.get("http") or candidate.get("https")
                        if p:
                            chosen_proxy = p
                    elif isinstance(candidate, str):
                        chosen_proxy = candidate
                    if getattr(self, "session", None) is not None:
                        prev_session_proxies = getattr(self.session, "proxies", None)
                        self.session.proxies = {
                            "http": chosen_proxy,
                            "https": chosen_proxy,
                        }
                    self.log(
                        f"🧵 Using proxy {chosen_proxy} from pool for WS", Fore.CYAN
                    )
            except Exception:
                proxy_taken = None
        if debug:
            self.log(
                f"[DEBUG] ws_connect url={url} proxy={chosen_proxy} use_pool={use_proxy_from_pool}",
                Fore.MAGENTA,
            )

        def _sync_connect():
            connect_kwargs = {}
            px = chosen_proxy
            if px:
                if "//" in px:
                    px = px.split("//", 1)[1]
                if ":" in px:
                    host, port = px.split(":", 1)
                    connect_kwargs["http_proxy_host"] = host
                    try:
                        connect_kwargs["http_proxy_port"] = int(port)
                    except Exception:
                        connect_kwargs["http_proxy_port"] = port
                else:
                    connect_kwargs["http_proxy_host"] = px
            ws = websocket.create_connection(url, timeout=timeout, **connect_kwargs)
            return ws

        loop = asyncio.get_running_loop()
        try:
            ws_raw = await loop.run_in_executor(None, _sync_connect)
            self.log(f"🐾 WebSocket connected -> {url}", Fore.CYAN)
            return self._WSHandle(
                self,
                ws_raw,
                proxy_taken=proxy_taken,
                prev_session_proxies=prev_session_proxies,
            )
        except Exception as e:
            if proxy_taken and getattr(self, "proxy_manager", None):
                self.proxy_manager.release_proxy(proxy_taken)
            self.log(f"❌ ws_connect failed: {e}", Fore.RED)
            raise

    @auto_async
    async def ws_send(
        self,
        handle,
        message,
        *,
        close_after: bool = False,
        close_delay: float = 0.0,
        wait_ack: bool = False,
        ack_timeout: float | None = None,
    ):
        """Send a message and optionally wait for ack and/or close the connection after a delay."""
        try:
            await handle.send(message)
            self.log("📤 message sent", Fore.GREEN)
            resp = None
            if wait_ack:
                resp = await handle.recv(timeout=ack_timeout)
            if close_after:
                if close_delay and close_delay > 0:
                    await asyncio.sleep(close_delay)
                await handle.close()
            return resp
        except Exception as e:
            self.log(f"❌ ws_send error: {e}", Fore.RED)
            raise

    @auto_async
    async def ws_recv(self, handle, *, timeout: float | None = None):
        """Receive one message from the websocket handle. Returns None on timeout/closed."""
        try:
            msg = await handle.recv(timeout=timeout)
            if msg is None:
                self.log("⌛ ws.recv timeout/no-data", Fore.YELLOW)
            else:
                self.log("📥 ws.recv got data", Fore.CYAN)
            return msg
        except Exception as e:
            self.log(f"⚠️ ws_recv error: {e}", Fore.YELLOW)
            return None

    @auto_async
    async def ws_close(self, handle, *, reason: str | None = None):
        """Close the handle and cleanup resources."""
        try:
            await handle.close(reason=reason)
            self.log("🔐 WebSocket closed", Fore.MAGENTA)
        except Exception as e:
            self.log(f"⚠️ ws_close error: {e}", Fore.YELLOW)
            raise

    def close(self):
        try:
            if hasattr(self, "session") and self.session:
                try:
                    self.session.close()
                except Exception:
                    pass
                self.session = None
        finally:
            self.proxy = None
            if hasattr(self, "proxy_list"):
                self.proxy_list = []


tasks_config = {"daily": "Auto claim daily"}


async def process_account(account, original_index, account_label, blu: signton):
    display_account = account[:12] + "..." if len(account) > 12 else account
    blu.log(f"👤 {account_label}: {display_account}", Fore.YELLOW)
    try:
        blu.config = blu.load_config(suppress_log=True)
    except Exception:
        blu.config = blu.config or {}
    if blu.config.get("proxy") and (not getattr(blu, "proxy_manager", None)):
        try:
            plist = list(dict.fromkeys(blu.proxy_list or []))
            blu.proxy_manager = ProxyManager(proxies=plist, maxsize=len(plist))
            blu.log(
                f"🔌 ProxyManager initialized ({blu.proxy_manager.available_count()} available)",
                Fore.GREEN,
            )
        except Exception:
            blu.proxy_manager = None
            blu.log("⚠️ Failed to initialize ProxyManager", Fore.YELLOW)
    await run_in_thread(blu.login, original_index)
    cfg = blu.config or {}
    enabled = [name for key, name in tasks_config.items() if cfg.get(key, False)]
    if enabled:
        blu.log("🛠️ Tasks enabled: " + ", ".join(enabled), Fore.CYAN)
    else:
        blu.log("🛠️ Tasks enabled: (none)", Fore.RED)
    for task_key, task_name in tasks_config.items():
        task_status = cfg.get(task_key, False)
        if task_status:
            if not hasattr(blu, task_key) or not callable(getattr(blu, task_key)):
                blu.log(f"⚠️ {task_key} missing", Fore.YELLOW)
                continue
            try:
                await run_in_thread(getattr(blu, task_key))
            except Exception as e:
                blu.log(f"❌ {task_key} error: {e}", Fore.RED)
    delay_switch = cfg.get("delay_account_switch", 10)
    blu.log(f"➡️ Done {account_label}. wait {delay_switch}s", Fore.CYAN)
    await asyncio.sleep(delay_switch)
    if blu.config.get("proxy") and getattr(blu, "proxy_manager", None) and blu.proxy:
        try:
            blu.proxy_manager.release_proxy(blu.proxy)
            blu.log(
                f"🔁 Proxy released ({blu.proxy}) | avail={blu.proxy_manager.available_count()} in_use={blu.proxy_manager.in_use_count()}",
                Fore.GREEN,
            )
        except Exception:
            blu.log("⚠️ release proxy failed", Fore.YELLOW)
        finally:
            blu.proxy = None


async def stream_producer(
    file_path: str,
    queue: asyncio.Queue,
    stop_event: asyncio.Event,
    base_blu: signton,
    poll_interval=0.8,
    dedupe=True,
):
    """Stream file producer (MODIFIED):
    - Now treats every non-empty line as a task entry (skips blank lines).
    - Keeps dedupe behavior when enabled.
    """
    idx = 0
    seen = BoundedSet(maxlen=10000) if dedupe else None
    f = None
    inode = None
    first_open = True
    while not stop_event.is_set():
        if f is None:
            try:
                f = open(file_path, encoding="utf-8")
                try:
                    inode = os.fstat(f.fileno()).st_ino
                except Exception:
                    inode = None
                if first_open:
                    first_open = False
                    f.seek(0)
                    for line in f:
                        line = line.strip()
                        if not line:
                            idx += 1
                            continue
                        if seen is not None:
                            if line in seen:
                                idx += 1
                                continue
                            seen.add(line)
                        await queue.put((idx, line))
                        idx += 1
                    f.seek(0, os.SEEK_END)
                else:
                    f.seek(0, os.SEEK_END)
            except FileNotFoundError:
                await asyncio.sleep(poll_interval)
                continue
        line = f.readline()
        if not line:
            await asyncio.sleep(poll_interval)
            try:
                st = os.stat(file_path)
                if inode is not None and st.st_ino != inode:
                    try:
                        f.close()
                    except:
                        pass
                    f = open(file_path, encoding="utf-8")
                    inode = os.fstat(f.fileno()).st_ino
                    f.seek(0, os.SEEK_END)
                elif f.tell() > st.st_size:
                    f.seek(0, os.SEEK_END)
            except FileNotFoundError:
                try:
                    if f:
                        f.close()
                except:
                    pass
                f = None
                inode = None
            continue
        line = line.strip()
        if not line:
            continue
        if seen is not None:
            if line in seen:
                continue
            seen.add(line)
        await queue.put((idx, line))
        idx += 1
    try:
        if f:
            f.close()
    except:
        pass


async def once_producer(file_path: str, queue: asyncio.Queue):
    """One-shot file producer (MODIFIED):
    - Emits every non-empty line to the queue.
    """
    idx = 0
    try:
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    idx += 1
                    continue
                await queue.put((idx, line))
                idx += 1
    except FileNotFoundError:
        return


_BG_THREAD_TASKS: set = set()


async def run_in_thread(fn, *args, **kwargs):
    """
    Wrapper around asyncio.to_thread that registers the created task into
    _BG_THREAD_TASKS so main dapat menunggu semua background threads finish.
    Use this instead of direct asyncio.to_thread(...) for long-running background ops.
    """
    coro = asyncio.to_thread(fn, *args, **kwargs)
    task = asyncio.create_task(coro)
    _BG_THREAD_TASKS.add(task)
    try:
        return await task
    finally:
        _BG_THREAD_TASKS.discard(task)


async def worker(worker_id: int, base_blu: signton, queue: asyncio.Queue):
    blu = signton(
        use_proxy=base_blu.config.get("proxy", False),
        proxy_list=base_blu.proxy_list,
        load_on_init=False,
    )
    try:
        blu.query_list = list(base_blu.query_list)
    except Exception:
        blu.query_list = []
    blu.log(f"👷 Worker-{worker_id} started", Fore.CYAN)
    while True:
        try:
            original_index, account = await queue.get()
        except asyncio.CancelledError:
            break
        account_label = f"W{worker_id}-A{original_index + 1}"
        try:
            await process_account(account, original_index, account_label, blu)
        except Exception as e:
            blu.log(f"❌ {account_label} error: {e}", Fore.RED)
        finally:
            try:
                queue.task_done()
            except Exception:
                pass
    await run_in_thread(blu.close)
    base_blu.log(f"🧾 Worker-{worker_id} stopped", Fore.CYAN)


def estimate_network_latency(host="1.1.1.1", port=53, attempts=2, timeout=0.6):
    latencies = []
    for _ in range(attempts):
        try:
            t0 = time.time()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((host, port))
            s.close()
            latencies.append(time.time() - t0)
        except Exception:
            latencies.append(timeout)
    try:
        return statistics.median(latencies)
    except:
        return timeout


def auto_tune_config_respecting_thread(
    existing_cfg: dict | None = None, prefer_network_check: bool = True
) -> dict:
    cfg = dict(existing_cfg or {})
    try:
        phys = psutil.cpu_count(logical=False) or 1
        logical = psutil.cpu_count(logical=True) or phys
    except:
        phys = logical = 1
    try:
        total_mem_gb = psutil.virtual_memory().total / 1024**3
    except:
        total_mem_gb = 1.0
    try:
        disk_free_gb = shutil.disk_usage(os.getcwd()).free / 1024**3
    except:
        disk_free_gb = 1.0
    net_lat = estimate_network_latency() if prefer_network_check else 1.0
    user_thread = cfg.get("thread", None)
    if user_thread is None:
        if logical >= 8:
            rec_thread = min(32, logical * 2)
        else:
            rec_thread = max(1, logical)
    else:
        rec_thread = int(user_thread)
    if total_mem_gb < 1.0:
        per_t = 20
    elif total_mem_gb < 2.5:
        per_t = 75
    elif total_mem_gb < 8:
        per_t = 200
    else:
        per_t = 1000
    q_recommend = int(min(max(50, rec_thread * per_t), 1000))
    if total_mem_gb < 1 or phys <= 1:
        poll = 1.0
    elif net_lat < 0.05:
        poll = 0.2
    elif net_lat < 0.2:
        poll = 0.5
    else:
        poll = 0.8
    dedupe = bool(total_mem_gb < 2.0)
    run_mode = cfg.get("run_mode", "continuous")
    merged = dict(cfg)
    if "queue_maxsize" not in merged:
        merged["queue_maxsize"] = q_recommend
    if "poll_interval" not in merged:
        merged["poll_interval"] = poll
    if "dedupe" not in merged:
        merged["dedupe"] = dedupe
    if "run_mode" not in merged:
        merged["run_mode"] = run_mode
    merged["_autotune_meta"] = {
        "phys_cores": int(phys),
        "logical_cores": int(logical),
        "total_mem_gb": round(total_mem_gb, 2),
        "disk_free_gb": round(disk_free_gb, 2),
        "net_latency_s": round(net_lat, 3),
        "queue_recommendation": int(q_recommend),
        "poll_recommendation": float(poll),
    }
    return merged


async def dynamic_tuner_nonthread(
    base_blu, queue: asyncio.Queue, stop_event: asyncio.Event, interval=6.0
):
    base_blu.log("🤖 Tuner started (non-thread)", Fore.CYAN)
    while not stop_event.is_set():
        try:
            cpu = psutil.cpu_percent(interval=None)
            qsize = queue.qsize() if queue is not None else 0
            cur_q = int(base_blu.config.get("queue_maxsize", 200))
            cur_poll = float(base_blu.config.get("poll_interval", 0.8))
            cur_dedupe = bool(base_blu.config.get("dedupe", True))
            cap = max(50, cur_q)
            if qsize > 0.8 * cap and cpu < 85:
                new_q = min(cur_q * 2, 10000)
            elif qsize < 0.2 * cap and cur_q > 100:
                new_q = max(int(cur_q / 2), 50)
            else:
                new_q = cur_q
            if cpu > 80:
                new_poll = min(cur_poll + 0.2, 2.0)
            elif cpu < 30 and qsize > 0.2 * cap:
                new_poll = max(cur_poll - 0.1, 0.1)
            else:
                new_poll = cur_poll
            vm = psutil.virtual_memory()
            if vm.available / 1024**2 < 200 and cur_dedupe:
                new_dedupe = False
            else:
                new_dedupe = cur_dedupe
            changed = []
            if new_q != cur_q:
                base_blu.config["queue_maxsize"] = int(new_q)
                changed.append(f"q:{cur_q}->{new_q}")
            if abs(new_poll - cur_poll) > 0.01:
                base_blu.config["poll_interval"] = float(round(new_poll, 3))
                changed.append(f"p:{cur_poll}->{round(new_poll, 3)}")
            if new_dedupe != cur_dedupe:
                base_blu.config["dedupe"] = bool(new_dedupe)
                changed.append(f"d:{cur_dedupe}->{new_dedupe}")
            if changed:
                base_blu.log("🔧 Tuner: " + ", ".join(changed), Fore.MAGENTA)
        except Exception:
            base_blu.log("⚠️ tuner error", Fore.YELLOW)
        await asyncio.sleep(interval)
    base_blu.log("🤖 Tuner stopped", Fore.MAGENTA)


async def producer_once(queue: asyncio.Queue, base_blu: signton):
    idx = 0
    try:
        queries = base_blu.query_list
        if not queries:
            base_blu.log("⚠️ No queries to enqueue.", Fore.YELLOW)
            return
        for line in queries:
            if not line:
                idx += 1
                continue
            await queue.put((idx, line))
            idx += 1
        base_blu.log(f"📦 Enqueued {idx} queries total.", Fore.CYAN)
    except Exception as e:
        base_blu.log(f"❌ Producer error: {e}", Fore.RED)


def cleanup_after_batch(base_blu, keep_refs: dict | None = None, deep=False):
    """
    Best-effort cleanup between batches:
    - clear caches, temp attrs, finished tasks
    - reset cookies
    - optional deep mode (reset all attrs except core)
    - run gc.collect()
    """
    try:
        try:
            for t in list(_BG_THREAD_TASKS):
                if getattr(t, "done", lambda: False)():
                    _BG_THREAD_TASKS.discard(t)
        except Exception:
            pass
        sess = getattr(base_blu, "session", None)
        if sess is not None:
            try:
                sess.cookies.clear()
            except Exception:
                pass
        if keep_refs is None:
            keep_refs = {}
        for k in ("last_items", "promo_data", "last_shop", "items_data"):
            if k not in keep_refs:
                try:
                    setattr(base_blu, k, None)
                except Exception:
                    pass
        if deep:
            for name in list(vars(base_blu).keys()):
                if name.startswith("_") or name in (
                    "logger",
                    "log",
                    "session",
                    "config",
                ):
                    continue
                try:
                    setattr(base_blu, name, None)
                except Exception:
                    pass
        try:
            gc.collect()
        except Exception:
            pass
        try:
            pm = getattr(base_blu, "proxy_manager", None)
            if pm:
                snap = pm.snapshot()
                if snap.get("in_use", 0) > 0:
                    base_blu.log(
                        f"⚠️ cleanup: {snap['in_use']} proxies still in-use; snapshot={snap}",
                        Fore.YELLOW,
                    )
        except Exception:
            pass
        try:
            emoji = random.choice(["🧹", "♻️", "🧽", "🌀", "🚿"])
            base_blu.log(
                f"{emoji} cleanup done | {base_blu.memory_monitor()}",
                Fore.LIGHTBLACK_EX,
            )
        except Exception:
            pass
    except Exception as e:
        try:
            base_blu.log(f"⚠️ cleanup error: {e}", Fore.YELLOW)
        except Exception:
            pass


async def main():
    base_blu = signton()
    cfg_file = base_blu.config
    effective = auto_tune_config_respecting_thread(cfg_file)
    run_mode = "repeat"
    base_blu.config = effective
    base_blu.log(
        f"🎉 [LIVEXORDS] === Welcome to {NAME_BOT} Automation === [LIVEXORDS]",
        Fore.YELLOW,
    )
    cfg_summary = {
        "thread": int(effective.get("thread", 1)),
        "queue_maxsize": int(effective.get("queue_maxsize", 200)),
        "poll_interval": float(effective.get("poll_interval", 0.8)),
        "dedupe": bool(effective.get("dedupe", True)),
        "delay_loop": int(effective.get("delay_loop", 30)),
        "delay_account_switch": int(effective.get("delay_account_switch", 10)),
        "proxy": bool(effective.get("proxy", False)),
    }
    base_blu.log("")
    base_blu.log("🔧 Effective config:", Fore.CYAN)
    for k, v in cfg_summary.items():
        base_blu.log(f"    • {k:<20}: {v}", Fore.CYAN)
    base_blu.log("📊 Autotune metadata:", Fore.MAGENTA)
    meta = effective.get("_autotune_meta", {})
    for k, v in meta.items():
        base_blu.log(f"    • {k:<20}: {v}", Fore.MAGENTA)
    query_file = effective.get("query_file", "query.txt")
    queue_maxsize = int(effective.get("queue_maxsize", 200))
    poll_interval = float(effective.get("poll_interval", 0.8))
    dedupe = bool(effective.get("dedupe", True))
    num_threads = int(effective.get("thread", 1))
    base_blu.banner()
    base_blu.log(f"📂 {query_file} | q={queue_maxsize} | mode={run_mode}", Fore.YELLOW)
    stop_event = asyncio.Event()
    try:
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, lambda: stop_event.set())
            loop.add_signal_handler(signal.SIGTERM, lambda: stop_event.set())
        except Exception:
            pass
    except Exception:
        pass
    while True:
        try:
            base_blu.query_list = base_blu.load_query(
                base_blu.config.get("query_file", query_file)
            )
        except Exception:
            base_blu.query_list = base_blu.load_query(query_file)
        queue = asyncio.Queue(maxsize=queue_maxsize)
        tuner_task = asyncio.create_task(
            dynamic_tuner_nonthread(base_blu, queue, stop_event)
        )
        prod_task = asyncio.create_task(producer_once(queue, base_blu))
        workers = [
            asyncio.create_task(worker(i + 1, base_blu, queue))
            for i in range(num_threads)
        ]
        try:
            await prod_task
            await queue.join()
        except asyncio.CancelledError:
            pass
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        tuner_task.cancel()
        try:
            await tuner_task
        except:
            pass
        if _BG_THREAD_TASKS:
            base_blu.log(
                f"⏳ waiting for {len(_BG_THREAD_TASKS)} background thread(s) to finish...",
                Fore.CYAN,
            )
            await asyncio.gather(*list(_BG_THREAD_TASKS), return_exceptions=True)
        try:
            sys.stdout.flush()
        except Exception:
            pass
        try:
            cleanup_after_batch(base_blu)
        except Exception:
            pass
        try:
            prod_task = None
            workers = None
            queue = None
        except Exception:
            pass
        base_blu.log("🔁 batch done", Fore.CYAN)
        base_blu.log(f"🧾 {base_blu.memory_monitor()}", Fore.MAGENTA)
        delay_loop = int(effective.get("delay_loop", 30))
        base_blu.log(f"⏳ sleep {delay_loop}s before next batch", Fore.CYAN)
        for _ in range(delay_loop):
            if stop_event.is_set():
                break
            await asyncio.sleep(1)
        if stop_event.is_set():
            break
    stop_event.set()
    base_blu.log("✅ shutdown", Fore.MAGENTA)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user. Exiting...")
