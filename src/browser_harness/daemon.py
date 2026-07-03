"""CDP WS holder + IPC relay (Unix socket on POSIX, TCP loopback on Windows). One daemon per BU_NAME."""
import asyncio, json, os, socket, sys, time, urllib.error, urllib.request
from urllib.parse import urlparse
from collections import deque
from pathlib import Path

from . import _ipc as ipc
from . import auth
from . import paths
from cdp_use.client import CDPClient
from websockets.exceptions import ConnectionClosed


def _load_env():
    repo_root = Path(__file__).resolve().parents[2]
    workspace = paths.workspace_dir()
    for p in (repo_root / ".env", workspace / ".env"):
        if not p.exists():
            continue
        _load_env_file(p)


def _load_env_file(p):
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

NAME = os.environ.get("BU_NAME", "default")
SOCK = ipc.sock_addr(NAME)
LOG = str(ipc.log_path(NAME))
PID = str(ipc.pid_path(NAME))
BUF = 500
PROFILES = [
    Path.home() / "Library/Application Support/Google/Chrome",
    Path.home() / "Library/Application Support/Google/Chrome Canary",
    Path.home() / "Library/Application Support/Comet",
    Path.home() / "Library/Application Support/Arc/User Data",
    Path.home() / "Library/Application Support/Dia/User Data",
    Path.home() / "Library/Application Support/Microsoft Edge",
    Path.home() / "Library/Application Support/Microsoft Edge Beta",
    Path.home() / "Library/Application Support/Microsoft Edge Dev",
    Path.home() / "Library/Application Support/Microsoft Edge Canary",
    Path.home() / "Library/Application Support/BraveSoftware/Brave-Browser",
    Path.home() / ".config/google-chrome",
    Path.home() / ".config/chromium",
    Path.home() / ".config/chromium-browser",
    Path.home() / ".config/microsoft-edge",
    Path.home() / ".config/microsoft-edge-beta",
    Path.home() / ".config/microsoft-edge-dev",
    Path.home() / ".var/app/org.chromium.Chromium/config/chromium",
    Path.home() / ".var/app/com.google.Chrome/config/google-chrome",
    Path.home() / ".var/app/com.brave.Browser/config/BraveSoftware/Brave-Browser",
    Path.home() / ".var/app/com.microsoft.Edge/config/microsoft-edge",
    Path.home() / "AppData/Local/Google/Chrome/User Data",
    Path.home() / "AppData/Local/Google/Chrome SxS/User Data",
    Path.home() / "AppData/Local/Chromium/User Data",
    Path.home() / "AppData/Local/Microsoft/Edge/User Data",
    Path.home() / "AppData/Local/Microsoft/Edge Beta/User Data",
    Path.home() / "AppData/Local/Microsoft/Edge Dev/User Data",
    Path.home() / "AppData/Local/Microsoft/Edge SxS/User Data",
    Path.home() / "AppData/Local/BraveSoftware/Brave-Browser/User Data",
]
INTERNAL = ("chrome://", "chrome-untrusted://", "devtools://", "chrome-extension://", "about:")
BU_API = "https://api.browser-use.com/api/v3"
REMOTE_ID = os.environ.get("BU_BROWSER_ID")
RECONNECT_DELAYS = (0, 1, 2, 4, 8)  # ~15s of re-dial attempts before declaring the browser gone
RECONNECT_COOLDOWN = 30  # after a failed reconnect, fail fast for this long instead of re-burning ~15s per request
# Cap per CDP call so a hung page surfaces an error instead of an IPC stall.
# 20s, not 60: a wedged renderer answers no faster at 60, and on a long task a
# streak of hung evals at 60s each is instant wall-clock death (observed on
# BU_Bench_V1 v2: repeated 60s Runtime.evaluate timeouts drove 9 task timeouts).
CDP_CALL_TIMEOUT = int(os.environ.get("BH_CDP_TIMEOUT", "20"))
CDP_CALL_TIMEOUTS = {  # per-method overrides: legitimately slow calls get more room
    "Page.captureScreenshot": 45,
    "Page.printToPDF": 60,
}
# Per-command CDP errors worth one inline retry on the same session — navigation
# races where the page moved under the call and the context comes right back.
TRANSIENT_CDP_ERRORS = (
    "Execution context was destroyed",
    "Cannot find context",
    "Inspected target navigated or closed",
)
# The attached tab itself died — re-attach to a real page and retry there.
TAB_GONE_ERRORS = (
    "Session with given id not found",
    "Target closed",
    "Session closed",
)


def log(msg):
    open(LOG, "a").write(f"{msg}\n")


async def _silent(coro):
    try:
        await coro
    except Exception:
        pass


def _ws_from_devtools_active_port(http_url: str) -> str | None:
    """When /json/version returns 404 (Chrome 147+ default profile), match DevToolsActivePort by port."""
    p = urlparse(http_url)
    want_port = str(p.port) if p.port else ""
    if not want_port:
        return None
    host = p.hostname or "127.0.0.1"
    if ":" in host:  # urlparse strips IPv6 brackets; restore them for the ws:// URL
        host = f"[{host}]"
    for base in PROFILES:
        try:
            active = (base / "DevToolsActivePort").read_text().splitlines()
        except (FileNotFoundError, NotADirectoryError):
            continue
        port = active[0].strip() if active else ""
        ws_path = active[1].strip() if len(active) > 1 else ""
        if port == want_port and ws_path:
            return f"ws://{host}:{port}{ws_path}"
    return None


def get_ws_url(wait=30):
    if url := os.environ.get("BU_CDP_WS"):
        return url
    if url := os.environ.get("BU_CDP_URL"):
        # HTTP DevTools endpoint (e.g. http://127.0.0.1:9333) — resolve to ws via /json/version.
        # Use this for a dedicated automation Chrome on a non-default profile, which avoids the
        # M144 "Allow remote debugging" dialog and the M136 default-profile lockdown.
        deadline = time.time() + wait
        last_err = None
        base_url = url.rstrip("/")
        while time.time() < deadline:
            try:
                return json.loads(urllib.request.urlopen(f"{base_url}/json/version", timeout=5).read())["webSocketDebuggerUrl"]
            except urllib.error.HTTPError as e:
                last_err = e
                if e.code == 403:
                    raise RuntimeError("permission-blocked: Chrome is reachable, but the per-session Allow remote debugging popup has not been accepted")
                if e.code == 404 and (ws := _ws_from_devtools_active_port(url)):
                    return ws
                time.sleep(1)
            except Exception as e:
                last_err = e
                time.sleep(1)
        raise RuntimeError(f"BU_CDP_URL={url} unreachable after {wait}s: {last_err} -- is the dedicated automation Chrome running?")
    deadline = time.time() + wait
    while time.time() < deadline:
        for base in PROFILES:
            try:
                active = (base / "DevToolsActivePort").read_text().splitlines()
            except (FileNotFoundError, NotADirectoryError):
                continue
            port = active[0].strip() if active else ""
            ws_path = active[1].strip() if len(active) > 1 else ""
            if not port:
                continue
            # Resolve the live WS URL via /json/version instead of trusting the path stored
            # alongside the port in DevToolsActivePort: if Chrome was previously launched
            # with a different --user-data-dir on the same port, that file is left behind
            # with a stale browser UUID and the WS upgrade returns 404.
            try:
                return json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1).read())["webSocketDebuggerUrl"]
            except urllib.error.HTTPError as e:
                if e.code == 403:
                    raise RuntimeError("permission-blocked: Chrome is reachable, but the per-session Allow remote debugging popup has not been accepted")
                # Chrome 147+ disables /json/* HTTP discovery on the default user-data-dir;
                # the ws path Chrome wrote to DevToolsActivePort still works.
                if e.code == 404 and ws_path:
                    return f"ws://127.0.0.1:{port}{ws_path}"
            except (OSError, KeyError, ValueError):
                pass
        time.sleep(0.2)
    for probe_port in (9222, 9223):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{probe_port}/json/version", timeout=1) as r:
                return json.loads(r.read())["webSocketDebuggerUrl"]
        except urllib.error.HTTPError as e:
            if e.code == 403:
                raise RuntimeError("permission-blocked: Chrome is reachable, but the per-session Allow remote debugging popup has not been accepted")
        except (OSError, KeyError, ValueError):
            continue
    raise RuntimeError(f"DevToolsActivePort not found in {[str(p) for p in PROFILES]} — enable chrome://inspect/#remote-debugging, or set BU_CDP_WS for a remote browser")


def stop_remote(browser_id=None):
    browser_id = browser_id or REMOTE_ID
    if not browser_id:
        return
    try:
        key = auth.get_browser_use_api_key()
        req = urllib.request.Request(
            f"{BU_API}/browsers/{browser_id}",
            data=json.dumps({"action": "stop"}).encode(),
            method="PATCH",
            headers={"X-Browser-Use-API-Key": key, "Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=15).read()
        log(f"stopped remote browser {browser_id}")
    except Exception as e:
        log(f"stop_remote failed ({browser_id}): {e}")


def _conn_dead(e):
    """True when the CDP websocket is gone, as opposed to a normal CDP protocol error."""
    if isinstance(e, TimeoutError):  # TimeoutError ⊂ OSError, but a slow call is not a dead socket
        return False
    if isinstance(e, (OSError, ConnectionClosed)):  # ConnectionError ⊂ OSError
        return True
    return isinstance(e, RuntimeError) and "Client is not started" in str(e)


def _disc_err(e):
    """Error string for a failed target probe, without double-prefixing a reconnect failure."""
    msg = str(e)
    return msg if msg.startswith("cdp_disconnected") else f"cdp_disconnected: {msg}"


class BrowserGone(RuntimeError):
    """The backing browser session ended — reconnecting cannot help."""


def read_descriptor():
    try:
        return json.loads(ipc.conn_path(NAME).read_text())
    except (FileNotFoundError, ValueError, OSError):
        return {}


def write_descriptor(d):
    try:
        ipc.conn_path(NAME).write_text(json.dumps(d))
    except OSError as e:
        log(f"write_descriptor failed: {e}")


def clear_descriptor():
    try:
        ipc.conn_path(NAME).unlink()
    except (FileNotFoundError, OSError):
        pass


def fetch_remote_browser(browser_id):
    """GET /browsers/{id}, or None when the API can't be reached."""
    try:
        key = auth.get_browser_use_api_key()
        req = urllib.request.Request(f"{BU_API}/browsers/{browser_id}", headers={"X-Browser-Use-API-Key": key})
        return json.loads(urllib.request.urlopen(req, timeout=15).read())
    except Exception as e:
        log(f"fetch_remote_browser failed ({browser_id}): {e}")
        return None


def _ws_from_cloud_browser(browser_id):
    """Fresh WS URL for a live cloud browser; BrowserGone once it is stopped; None when unknown."""
    browser = fetch_remote_browser(browser_id)
    if browser is None:
        return None
    if browser.get("status") == "stopped":
        raise BrowserGone(f"cloud browser {browser_id} is stopped (timed out or was stopped)")
    if cdp_url := browser.get("cdpUrl"):
        try:
            return json.loads(
                urllib.request.urlopen(f"{cdp_url.rstrip('/')}/json/version", timeout=15).read()
            )["webSocketDebuggerUrl"]
        except Exception as e:
            log(f"cdpUrl re-resolve failed ({browser_id}): {e}")
    return None


def is_real_page(t):
    return t["type"] == "page" and not t.get("url", "").startswith(INTERNAL)


class Daemon:
    def __init__(self):
        self.cdp = None
        self.session = None
        self.target_id = None
        self.events = deque(maxlen=BUF)
        self.dialog = None
        self.stop = None  # asyncio.Event, set inside start()
        self.gen = 0  # bumps on every successful (re)connect; guards duplicate reconnects
        self.relock = asyncio.Lock()
        self.dead_at = 0.0  # time.time() of the last failed reconnect (cooldown anchor)
        self.dead_err = ""
        self.notice = None  # one-shot warning attached to the next response (e.g. "your tab changed")
        self.browser_id = REMOTE_ID
        self.browser_id_from_env = bool(REMOTE_ID)
        self.stop_browser_on_exit = True  # cleared by a keep-browser shutdown (daemon restart)

    async def attach_first_page(self):
        """Attach to a real page (or any page). Sets self.session. Returns attached target or None."""
        targets = (await self.cdp.send_raw("Target.getTargets"))["targetInfos"]
        pages = [t for t in targets if is_real_page(t)]
        if not pages:
            # No real pages - create one instead of attaching to omnibox popup.
            tid = (await self.cdp.send_raw("Target.createTarget", {"url": "about:blank"}))["targetId"]
            log(f"no real pages found, created about:blank ({tid})")
            pages = [{"targetId": tid, "url": "about:blank", "type": "page"}]
        self.session = (await self.cdp.send_raw(
            "Target.attachToTarget", {"targetId": pages[0]["targetId"], "flatten": True}
        ))["sessionId"]
        self.target_id = pages[0]["targetId"]
        log(f"attached {pages[0]['targetId']} ({pages[0].get('url','')[:80]}) session={self.session}")
        await self._enable_default_domains(self.session)
        return pages[0]

    async def _enable_default_domains(self, session_id):
        """Enable Page/DOM/Runtime/Network on a CDP session.

        Used by both initial attach and set_session (called after switch_tab/
        new_tab). Without this, helpers that depend on Network.* events —
        notably wait_for_network_idle() — silently stop receiving events
        after a tab switch, because each fresh CDP session starts with all
        domains disabled.

        Runs the four enables in parallel via gather so the worst-case time is
        bounded by a single CDP round trip rather than four sequential ones —
        important on the set_session path, where the helper's IPC socket has
        a 5s read timeout.
        """
        async def enable_one(d):
            try:
                await asyncio.wait_for(
                    self.cdp.send_raw(f"{d}.enable", session_id=session_id),
                    timeout=4,
                )
            except Exception as e:
                log(f"enable {d} on {session_id}: {e}")
        await asyncio.gather(*(enable_one(d) for d in ("Page", "DOM", "Runtime", "Network")))

    async def start(self):
        self.stop = asyncio.Event()
        if not self.browser_id and (bid := read_descriptor().get("browser_id")):
            # A previous daemon for this BU_NAME was attached to a cloud browser —
            # reattach to it (the env vars died with that process; the descriptor didn't).
            self.browser_id = bid
        try:
            await self._dial()
        except RuntimeError:
            raise  # resolve_ws_url()/get_ws_url() errors already carry actionable messages
        except Exception as e:
            if os.environ.get("BU_CDP_WS"):
                raise RuntimeError(
                    f"CDP WS handshake failed: {e} -- remote browser WebSocket connection failed. "
                    "This can happen when network policy blocks the connection, the WS URL is wrong or expired, or the remote endpoint is down. "
                    "If you use Browser Use cloud, verify auth and get a fresh URL via start_remote_daemon()."
                )
            raise RuntimeError(f"CDP WS handshake failed: {e} -- click Allow in Chrome if prompted, then retry")

    def _resolve_ws_url(self, wait):
        """WS URL for (re)connecting. First dial honours the env exactly like before;
        reconnects prefer a live API lookup by browser id, so they get a FRESH URL
        (and a terminal BrowserGone once the cloud browser is stopped) instead of
        re-dialing a frozen one forever."""
        first = self.gen == 0
        env_ws = os.environ.get("BU_CDP_WS")
        if first and env_ws:
            return env_ws
        if self.browser_id:
            try:
                if ws := _ws_from_cloud_browser(self.browser_id):
                    return ws
            except BrowserGone:
                if first and not self.browser_id_from_env:
                    # Stale descriptor from an old cloud run — forget it and fall
                    # through to normal discovery instead of bricking startup.
                    log(f"descriptor browser {self.browser_id} is stopped; clearing")
                    clear_descriptor()
                    self.browser_id = None
                else:
                    raise
        if env_ws:
            return env_ws
        if not os.environ.get("BU_CDP_URL") and (ws := read_descriptor().get("ws_url")):
            return ws
        return get_ws_url(wait=wait)

    async def _dial(self, wait=30):
        """One full connect: resolve the WS URL, handshake, tap events, attach a page."""
        url = await asyncio.to_thread(self._resolve_ws_url, wait)  # can block polling for Chrome
        log(f"connecting to {url}")
        cdp = CDPClient(url)
        await cdp.start()
        self.cdp = cdp
        try:
            self._tap_events()
            await self.attach_first_page()
        except BaseException:
            await _silent(cdp.stop())
            raise
        self.gen += 1
        self.dead_at = 0.0
        if self.browser_id or os.environ.get("BU_CDP_WS"):
            # Persist how we're attached so a restarted daemon reattaches to the SAME
            # browser instead of falling back to local discovery. Local-discovery
            # connections are deliberately not persisted: their ws paths go stale on
            # every Chrome restart and rediscovery is cheap.
            write_descriptor({"browser_id": self.browser_id, "ws_url": url})
        else:
            clear_descriptor()

    def _tap_events(self):
        orig = self.cdp._event_registry.handle_event
        mark_js = "if(!document.title.startsWith('\U0001F434'))document.title='\U0001F434 '+document.title"
        async def tap(method, params, session_id=None):
            self.events.append({"method": method, "params": params, "session_id": session_id})
            if method == "Page.javascriptDialogOpening":
                self.dialog = params
            elif method == "Page.javascriptDialogClosed":
                self.dialog = None
            elif method in ("Page.loadEventFired", "Page.domContentEventFired"):
                asyncio.create_task(_silent(asyncio.wait_for(self.cdp.send_raw("Runtime.evaluate", {"expression": mark_js}, session_id=self.session), timeout=2)))
            return await orig(method, params, session_id)
        self.cdp._event_registry.handle_event = tap

    def _dead_msg(self, detail):
        return (
            f"cdp_disconnected: lost the browser connection and automatic reconnect failed ({detail}). "
            "Do NOT retry in a sleep loop. Call reconnect() once to force another attempt; if that "
            "fails too the browser session is gone -- start a fresh one (start_remote_daemon() for "
            "cloud, or restart Chrome / get a new BU_CDP_WS), then redo the task from navigation."
        )

    async def _reconnect(self, gen, why, force=False):
        """Serialized re-dial after a dead WS. No-op when another request already reconnected."""
        async with self.relock:
            if self.gen != gen:
                return
            if not force and time.time() - self.dead_at < RECONNECT_COOLDOWN:
                raise RuntimeError(self._dead_msg(self.dead_err))
            log(f"cdp connection lost ({why}); reconnecting")
            old_target = self.target_id
            await _silent(self.cdp.stop())
            last = None
            for delay in RECONNECT_DELAYS:
                if delay:
                    await asyncio.sleep(delay)
                try:
                    await self._dial(wait=3)
                    log(f"reconnected (attached {self.target_id})")
                    if old_target and self.target_id != old_target:
                        self.notice = (
                            "browser connection recovered, but the tab you were on is gone -- "
                            "now attached to a different tab. Use list_tabs()/switch_tab()/page_info() to reorient."
                        )
                    return
                except BrowserGone as e:
                    last = e
                    break
                except Exception as e:
                    last = e
                    log(f"reconnect attempt failed: {e}")
                    if "permission-blocked" in str(e):
                        break  # needs the user to click Allow; retrying can't help
            self.dead_at = time.time()
            self.dead_err = str(last)
            raise RuntimeError(self._dead_msg(self.dead_err))

    async def _cdp_send(self, method, params=None, session_id=None, use_default=False):
        """send_raw with a per-call timeout and one transparent reconnect+retry when the WS died.

        use_default marks requests that fell back to the daemon's default session:
        after a reconnect the old session id is meaningless, so the retry must use
        the freshly attached session rather than replaying the stale one."""
        gen = self.gen
        timeout = CDP_CALL_TIMEOUTS.get(method, CDP_CALL_TIMEOUT)
        try:
            return await asyncio.wait_for(self.cdp.send_raw(method, params, session_id=session_id), timeout)
        except TimeoutError:
            await self._unhang(session_id)
            raise RuntimeError(
                f"cdp call {method} timed out after {timeout}s -- the page is busy or hung "
                "(this is NOT a disconnect). I terminated any running JS on it. "
                "capture_screenshot() to see the current state, then try a smaller/simpler action -- "
                "avoid heavy JS in one call. If the page stays stuck: close_tab() + new_tab(url), or reconnect()."
            )
        except Exception as e:
            if not _conn_dead(e):
                raise
            await self._reconnect(gen, str(e))
            if use_default:
                session_id = self.session
            return await asyncio.wait_for(self.cdp.send_raw(method, params, session_id=session_id), timeout)

    async def _unhang(self, session_id):
        """Best-effort rescue of a wedged page after a call timeout: kill the running
        JS and stop any load, so the NEXT call meets a live main thread instead of
        re-rolling the dice on the same hung renderer."""
        if not session_id:
            return
        for m in ("Runtime.terminateExecution", "Page.stopLoading"):
            await _silent(asyncio.wait_for(self.cdp.send_raw(m, session_id=session_id), 3))

    async def handle(self, req):
        # Token guard for Windows TCP loopback: any local process can otherwise
        # connect and issue CDP commands. expected_token() is None on POSIX so
        # this check is a no-op there (AF_UNIX + chmod 600 is the boundary).
        expected = ipc.expected_token()
        if expected is not None and req.get("token") != expected:
            return {"error": "unauthorized"}
        meta = req.get("meta")
        # Liveness probe — lets clients confirm the listener is actually this
        # daemon and not an unrelated process that reused our port post-crash.
        # `pid` lets restart_daemon() verify the live daemon's identity before
        # signaling — protects against SIGTERM-by-stale-pid-file after PID reuse.
        if meta == "ping":        return {"pong": True, "pid": os.getpid()}
        if meta == "drain_events":
            out = list(self.events); self.events.clear()
            return {"events": out}
        if meta == "session":     return {"session_id": self.session}
        if meta == "current_tab":
            # Resolve the attached page's target info server-side. Helpers can't
            # send Target.getTargetInfo themselves: daemon strips session_id for
            # any Target.* method (browser-level call), and without a targetId
            # Chrome silently returns the *browser* target.
            if not self.target_id:
                return {"error": "not_attached"}
            try:
                info = (await self._cdp_send("Target.getTargetInfo", {"targetId": self.target_id}))["targetInfo"]
            except Exception as e:
                return {"error": _disc_err(e)}
            return {"targetId": info.get("targetId"), "url": info.get("url", ""), "title": info.get("title", "")}
        if meta == "connection_status":
            if not self.target_id:
                return {"error": "not_attached"}
            try:
                info = (await self._cdp_send("Target.getTargetInfo", {"targetId": self.target_id}))["targetInfo"]
            except Exception as e:
                return {"error": _disc_err(e)}
            page = None
            if is_real_page(info):
                page = {
                    "targetId": info.get("targetId"),
                    "title": info.get("title") or "(untitled)",
                    "url": info.get("url") or "",
                }
            return {"connected": True, "browser_id": self.browser_id, "target_id": self.target_id, "session_id": self.session, "page": page}
        if meta == "reconnect":
            # Explicit agent-initiated reconnect: bypasses the failure cooldown and
            # re-dials even if the current connection looks alive (the agent knows
            # something we may not). Same recovery primitive as connecting.
            try:
                await self._reconnect(self.gen, "explicit reconnect()", force=True)
            except Exception as e:
                return {"error": str(e)}
            out = {"reconnected": True, "browser_id": self.browser_id}
            try:
                info = (await self._cdp_send("Target.getTargetInfo", {"targetId": self.target_id}))["targetInfo"]
                out["page"] = {"targetId": info.get("targetId"), "url": info.get("url", ""), "title": info.get("title", "")}
            except Exception:
                pass
            if self.notice:
                out["notice"], self.notice = self.notice, None
            return out
        if meta == "set_session":
            old_session = self.session
            self.session = req.get("session_id")
            self.target_id = req.get("target_id") or self.target_id
            # Run the old-session Network.disable (defense in depth — keeps
            # background-tab traffic out of the global event buffer; the
            # consumer-side filter in wait_for_network_idle is the actual
            # correctness gate) in parallel with the four enables on the new
            # session. Different sessions, independent CDP requests. Keeps
            # the synchronous reply under the helper's 5s IPC read timeout
            # even on a remote daemon — sequentially these would have stacked
            # to ~22s worst case.
            tasks = []
            if old_session and old_session != self.session:
                async def disable_old():
                    try:
                        await asyncio.wait_for(
                            self.cdp.send_raw("Network.disable", session_id=old_session),
                            timeout=2,
                        )
                    except Exception: pass
                tasks.append(disable_old())
            tasks.append(self._enable_default_domains(self.session))
            await asyncio.gather(*tasks)
            # 🐴 tab-marker title prefix is purely cosmetic — fire-and-forget so
            # it doesn't add to the synchronous IPC budget.
            asyncio.create_task(_silent(asyncio.wait_for(
                self.cdp.send_raw(
                    "Runtime.evaluate",
                    {"expression": "if(!document.title.startsWith('\U0001F434'))document.title='\U0001F434 '+document.title"},
                    session_id=self.session,
                ),
                timeout=2,
            )))
            return {"session_id": self.session}
        if meta == "pending_dialog": return {"dialog": self.dialog}
        if meta == "shutdown":
            # stop_browser=False (daemon restart/self-heal) keeps the cloud browser
            # alive so the next daemon reattaches via the descriptor. Default True
            # preserves the old billing-safety behavior for explicit stops/crashes.
            self.stop_browser_on_exit = bool(req.get("stop_browser", True))
            self.stop.set()
            return {"ok": True}

        method = req["method"]
        params = req.get("params") or {}
        # Browser-level Target.* calls must not use a session (stale or otherwise).
        # For everything else, explicit session in req wins; else default.
        explicit = req.get("session_id")
        sid = None if method.startswith("Target.") else (explicit or self.session)
        try:
            out = {"result": await self._cdp_send(method, params, session_id=sid, use_default=bool(sid and not explicit))}
        except Exception as e:
            msg = str(e)
            on_default = bool(sid and sid == self.session)
            try:
                if on_default and any(t in msg for t in TRANSIENT_CDP_ERRORS):
                    # Navigation race: the page moved under the call and destroyed
                    # its JS context. The context comes right back — retry once.
                    log(f"transient cdp error ({msg[:80]}), retrying once")
                    await asyncio.sleep(0.5)
                    out = {"result": await self.cdp.send_raw(method, params, session_id=sid)}
                elif on_default and any(t in msg for t in TAB_GONE_ERRORS):
                    log(f"attached tab gone ({msg[:80]}), re-attaching")
                    if not await self.attach_first_page():
                        return {"error": msg}
                    self.notice = (
                        "the tab you were on is gone -- re-attached to another tab. "
                        "Use list_tabs()/switch_tab()/page_info() to reorient."
                    )
                    out = {"result": await self.cdp.send_raw(method, params, session_id=self.session)}
                else:
                    return {"error": msg}
            except Exception as retry_err:
                return {"error": str(retry_err)}
        if self.notice:
            out["notice"], self.notice = self.notice, None
        return out


async def serve(d):
    async def handler(reader, writer):
        try:
            line = await reader.readline()
            if not line: return
            resp = await d.handle(json.loads(line))
            writer.write((json.dumps(resp, default=str) + "\n").encode())
            await writer.drain()
        except Exception as e:
            log(f"conn: {e}")
            try:
                writer.write((json.dumps({"error": str(e)}) + "\n").encode())
                await writer.drain()
            except Exception:
                pass
        finally:
            writer.close()

    serve_task = asyncio.create_task(ipc.serve(NAME, handler))
    stop_task = asyncio.create_task(d.stop.wait())
    await asyncio.sleep(0.05)  # let serve() bind so sock_addr() resolves to the live endpoint
    log(f"listening on {ipc.sock_addr(NAME)} (name={NAME}, remote={REMOTE_ID or 'local'})")
    try:
        await asyncio.wait({serve_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if serve_task.done(): await serve_task  # surfaces a serve crash
    finally:
        for t in (serve_task, stop_task):
            t.cancel()
            try: await t
            except (asyncio.CancelledError, Exception): pass
        ipc.cleanup_endpoint(NAME)


DAEMON = None


async def main():
    global DAEMON
    DAEMON = d = Daemon()
    await d.start()
    await serve(d)


def already_running():
    # Ping handshake (not a bare connect) so a stale .port file + port reuse
    # after a daemon crash doesn't make us mistake an unrelated listener for ours.
    return ipc.ping(NAME, timeout=1.0)


if __name__ == "__main__":
    if already_running():
        print(f"daemon already running on {SOCK}", file=sys.stderr)
        sys.exit(0)
    open(LOG, "w").close()
    open(PID, "w").write(str(os.getpid()))
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log(f"fatal: {e}")
        sys.exit(1)
    finally:
        # Keep-browser shutdowns (daemon restart/self-heal) skip the stop so the next
        # daemon can reattach via the descriptor; crashes and explicit stops keep the
        # old kill-the-billed-browser safety behavior.
        if DAEMON is None or DAEMON.stop_browser_on_exit:
            stop_remote(DAEMON.browser_id if DAEMON else None)
            clear_descriptor()
        try: os.unlink(PID)
        except FileNotFoundError: pass
