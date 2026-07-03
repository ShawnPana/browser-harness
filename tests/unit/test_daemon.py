import asyncio

from browser_harness import daemon


class _FakeCDP:
    """Records send_raw calls so tests can assert which CDP methods fired."""

    def __init__(self):
        self.calls = []  # list of (method, params, session_id)

    async def send_raw(self, method, params=None, session_id=None):
        self.calls.append((method, params, session_id))
        # Set-session/initial-attach paths only need a benign response.
        return {}


def _fresh_daemon():
    d = daemon.Daemon()
    d.cdp = _FakeCDP()
    return d


def test_set_session_enables_all_four_default_domains_on_new_session():
    """Regression: switch_tab() / new_tab() in helpers.py route through the
    `set_session` IPC, which previously only enabled Page on the new
    session. With Network disabled, wait_for_network_idle() silently stops
    receiving events after a tab switch. Initial attach enables all four
    (Page, DOM, Runtime, Network); set_session must enable the same set."""
    d = _fresh_daemon()
    new_session = "session-AFTER-switch"

    asyncio.run(d.handle({
        "meta": "set_session",
        "session_id": new_session,
        "target_id": "target-2",
    }))

    enabled_on_new = [
        method for (method, _params, sid) in d.cdp.calls
        if sid == new_session and method.endswith(".enable")
    ]
    assert set(enabled_on_new) == {"Page.enable", "DOM.enable", "Runtime.enable", "Network.enable"}, (
        f"set_session must enable Page/DOM/Runtime/Network on the new session "
        f"(parity with initial attach). Got: {enabled_on_new}"
    )
    assert d.session == new_session
    assert d.target_id == "target-2"


def test_set_session_falls_back_to_existing_target_id_when_not_provided():
    """If a caller forgets target_id (passes None), the daemon should keep its
    existing target_id rather than overwriting it with None — otherwise
    subsequent calls that depend on self.target_id would break."""
    d = _fresh_daemon()
    d.target_id = "original-target"

    asyncio.run(d.handle({
        "meta": "set_session",
        "session_id": "session-AFTER",
        "target_id": None,
    }))

    assert d.target_id == "original-target"
    assert d.session == "session-AFTER"


def test_enable_default_domains_swallows_errors_per_domain():
    """A single domain failing to enable must not prevent the others from
    being attempted — that would leave the daemon in a partially-configured
    state. Each Domain.enable call has its own try/except inside the helper."""
    class _PartialFailureCDP(_FakeCDP):
        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            if method == "DOM.enable":
                raise RuntimeError("simulated DOM failure")
            return {}

    d = daemon.Daemon()
    d.cdp = _PartialFailureCDP()

    asyncio.run(d._enable_default_domains("session-X"))

    attempted = [m for (m, _p, _s) in d.cdp.calls]
    assert "Page.enable" in attempted
    assert "DOM.enable" in attempted  # attempted, but raised
    assert "Runtime.enable" in attempted
    assert "Network.enable" in attempted


def test_set_session_disables_network_on_old_session_before_enabling_new():
    """When switching tabs, the previous session's Network domain must be
    disabled so background tabs (polling, SSE, etc.) stop emitting events
    into the global buffer that wait_for_network_idle reads. Initial attach
    has no `old_session` so this disable doesn't fire then."""
    d = _fresh_daemon()
    d.session = "session-OLD"
    d.target_id = "target-OLD"

    asyncio.run(d.handle({
        "meta": "set_session",
        "session_id": "session-NEW",
        "target_id": "target-NEW",
    }))

    disabled = [
        (method, sid) for (method, _params, sid) in d.cdp.calls
        if method == "Network.disable"
    ]
    assert disabled == [("Network.disable", "session-OLD")], (
        f"Network.disable must fire on the old session before re-enabling on "
        f"the new one. Got: {disabled}"
    )

    # Sanity: the new session still gets Network.enable.
    enabled_on_new = {
        method for (method, _p, sid) in d.cdp.calls
        if sid == "session-NEW" and method.endswith(".enable")
    }
    assert "Network.enable" in enabled_on_new


def test_set_session_does_not_disable_network_when_no_previous_session():
    """First set_session call (e.g. very early in startup before any attach)
    has no old_session — the Network.disable path must be skipped."""
    d = _fresh_daemon()
    d.session = None  # no prior attach

    asyncio.run(d.handle({
        "meta": "set_session",
        "session_id": "session-FIRST",
        "target_id": "target-FIRST",
    }))

    disables = [m for (m, _p, _s) in d.cdp.calls if m == "Network.disable"]
    assert disables == [], (
        f"Network.disable must not fire when there's no previous session "
        f"to disable. Got: {disables}"
    )


def test_set_session_runs_disable_and_enables_in_parallel():
    """The four Domain.enable calls (plus Network.disable on the old session)
    must run concurrently via asyncio.gather, not sequentially. With the old
    sequential code, helpers.switch_tab() would block in _send() for up to
    ~22s on a slow/remote daemon while the helper's IPC socket has a 5s
    read timeout, causing client-side socket timeouts. Verifying that all
    five CDP calls reach send_raw before any returns proves parallelization."""
    class _ConcurrencyProbeCDP:
        def __init__(self):
            self.calls = []
            self.in_flight = 0
            self.max_concurrent = 0
            self.release = None  # asyncio.Event, set inside the test loop

        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            self.in_flight += 1
            self.max_concurrent = max(self.max_concurrent, self.in_flight)
            try:
                await self.release.wait()
            finally:
                self.in_flight -= 1
            return {}

    async def run():
        d = daemon.Daemon()
        d.cdp = _ConcurrencyProbeCDP()
        d.session = "session-OLD"  # ensures Network.disable on old fires
        d.cdp.release = asyncio.Event()

        handle_task = asyncio.create_task(d.handle({
            "meta": "set_session",
            "session_id": "session-NEW",
            "target_id": "target-NEW",
        }))
        # Yield repeatedly until everything that's going to be in-flight is
        # in-flight. Cap iterations to avoid hanging if parallelization breaks.
        for _ in range(50):
            await asyncio.sleep(0)
            # 5 = Network.disable on OLD + 4 enables on NEW.
            if d.cdp.in_flight >= 5:
                break
        peak = d.cdp.max_concurrent
        d.cdp.release.set()
        await handle_task
        return peak, d.cdp.calls

    peak, calls = asyncio.run(run())
    assert peak == 5, (
        f"set_session must run disable + 4 enables concurrently via gather "
        f"(observed peak in-flight = {peak}; expected 5 = 1 disable on OLD + "
        f"4 enables on NEW). Sequential await would peak at 1."
    )
    # Sanity: the right calls were made.
    methods = sorted({m for (m, _p, _s) in calls})
    assert "Network.disable" in methods
    assert {"Page.enable", "DOM.enable", "Runtime.enable", "Network.enable"}.issubset(methods)


def test_set_session_first_attach_runs_four_enables_in_parallel():
    """When there's no previous session, the disable path is skipped — only
    the four enables run, still in parallel."""
    class _ConcurrencyProbeCDP:
        def __init__(self):
            self.calls = []
            self.in_flight = 0
            self.max_concurrent = 0
            self.release = None

        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            self.in_flight += 1
            self.max_concurrent = max(self.max_concurrent, self.in_flight)
            try:
                await self.release.wait()
            finally:
                self.in_flight -= 1
            return {}

    async def run():
        d = daemon.Daemon()
        d.cdp = _ConcurrencyProbeCDP()
        d.session = None  # no previous session
        d.cdp.release = asyncio.Event()

        handle_task = asyncio.create_task(d.handle({
            "meta": "set_session",
            "session_id": "session-FIRST",
            "target_id": "target-FIRST",
        }))
        for _ in range(50):
            await asyncio.sleep(0)
            if d.cdp.in_flight >= 4:
                break
        peak = d.cdp.max_concurrent
        d.cdp.release.set()
        await handle_task
        return peak

    peak = asyncio.run(run())
    assert peak == 4, (
        f"first set_session must run 4 enables concurrently "
        f"(observed peak = {peak}). No Network.disable should fire."
    )


def test_current_tab_meta_passes_attached_target_id():
    """Regression for issue #304: helpers.current_tab() previously sent
    Target.getTargetInfo with no targetId. The daemon strips session_id for
    Target.* methods, so the call hit the browser-level connection with empty
    params, and Chrome returned info about the *browser* target (empty
    url/title) instead of the attached page. The daemon now resolves this
    server-side using its tracked target_id."""
    class _TargetInfoCDP(_FakeCDP):
        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            if method == "Target.getTargetInfo":
                return {"targetInfo": {
                    "targetId": params["targetId"],
                    "url": "https://example.com/",
                    "title": "Example Domain",
                    "type": "page",
                }}
            return {}

    d = daemon.Daemon()
    d.cdp = _TargetInfoCDP()
    d.target_id = "page-target-abc"

    result = asyncio.run(d.handle({"meta": "current_tab"}))

    assert result == {
        "targetId": "page-target-abc",
        "url": "https://example.com/",
        "title": "Example Domain",
    }
    # The targetId must be passed through — that's the whole point of the fix.
    get_info_calls = [(p, s) for (m, p, s) in d.cdp.calls if m == "Target.getTargetInfo"]
    assert get_info_calls == [({"targetId": "page-target-abc"}, None)]


def test_current_tab_meta_returns_not_attached_when_no_target_id():
    """Without an attached page, current_tab() has no meaningful answer.
    Returning {error: not_attached} causes _send() to raise in helpers, which
    is the right signal for callers like ensure_real_tab() that wrap the call
    in try/except."""
    d = _fresh_daemon()
    d.target_id = None

    result = asyncio.run(d.handle({"meta": "current_tab"}))

    assert result == {"error": "not_attached"}
    # No CDP call should have been issued.
    assert d.cdp.calls == []


# --- connection recovery (issue: remote WS death had no reconnect path) ---

import time

from websockets.exceptions import ConnectionClosedError


class _DeadCDP:
    """send_raw always raises like a closed websocket; records stop()."""

    def __init__(self):
        self.calls = []
        self.stopped = False

    async def send_raw(self, method, params=None, session_id=None):
        self.calls.append((method, params, session_id))
        raise ConnectionError("WebSocket connection closed")

    async def stop(self):
        self.stopped = True


def test_conn_dead_classification():
    """Only genuinely-dead-socket errors may trigger a reconnect. A CDP protocol
    error or a slow call must NOT — reconnecting on those would tear down a
    healthy connection mid-task."""
    assert daemon._conn_dead(ConnectionError("WebSocket connection closed"))
    assert daemon._conn_dead(RuntimeError("Client is not started. Call start() first."))
    assert daemon._conn_dead(ConnectionClosedError(None, None))
    assert not daemon._conn_dead(TimeoutError())
    assert not daemon._conn_dead(RuntimeError("{'code': -32000, 'message': 'Node not found'}"))


def test_dispatch_reconnects_retries_and_notices_tab_change(monkeypatch):
    """A dead WS mid-dispatch must trigger one transparent reconnect + retry.
    The retry must use the NEW default session (the old one died with the WS),
    and the response must carry a one-shot notice when the attached tab changed."""
    monkeypatch.setattr(daemon, "RECONNECT_DELAYS", (0,))
    d = daemon.Daemon()
    dead = _DeadCDP()
    d.cdp = dead
    d.session = "s-old"
    d.target_id = "t-old"
    healthy = _FakeCDP()

    async def fake_dial(wait=30):
        d.cdp = healthy
        d.session = "s-new"
        d.target_id = "t-new"
        d.gen += 1

    monkeypatch.setattr(d, "_dial", fake_dial)

    resp = asyncio.run(d.handle({"method": "Runtime.evaluate", "params": {"expression": "1"}}))

    assert resp["result"] == {}
    assert healthy.calls == [("Runtime.evaluate", {"expression": "1"}, "s-new")], (
        "retry must run on the freshly attached session, not replay the dead one"
    )
    assert dead.stopped, "the dead client must be stopped before re-dialing"
    assert "tab" in resp.get("notice", ""), "tab change during recovery must be surfaced"
    assert d.notice is None, "notice is one-shot"


def test_reconnect_failure_returns_cdp_disconnected_guidance(monkeypatch):
    """When every re-dial fails, the agent gets ONE actionable error naming the
    recovery steps (reconnect(), then fresh browser) — not a generic traceback."""
    monkeypatch.setattr(daemon, "RECONNECT_DELAYS", (0, 0))
    d = daemon.Daemon()
    d.cdp = _DeadCDP()
    d.session = "s"
    attempts = []

    async def fail_dial(wait=30):
        attempts.append(1)
        raise ConnectionError("connection refused")

    monkeypatch.setattr(d, "_dial", fail_dial)

    resp = asyncio.run(d.handle({"method": "Page.navigate", "params": {"url": "https://x.test"}}))

    assert resp["error"].startswith("cdp_disconnected")
    assert "reconnect()" in resp["error"]
    assert "sleep loop" in resp["error"]
    assert len(attempts) == 2, "one dial per configured delay"
    assert d.dead_at > 0, "failed reconnect must arm the cooldown"


def test_reconnect_cooldown_fails_fast_without_redialing(monkeypatch):
    """Within the cooldown after a failed reconnect, requests fail immediately
    with the same guidance instead of burning ~15s of re-dials each."""
    d = daemon.Daemon()
    d.cdp = _DeadCDP()
    d.session = "s"
    d.dead_at = time.time()
    d.dead_err = "connection refused"
    attempts = []

    async def fail_dial(wait=30):
        attempts.append(1)
        raise ConnectionError("connection refused")

    monkeypatch.setattr(d, "_dial", fail_dial)

    resp = asyncio.run(d.handle({"method": "Page.navigate", "params": {"url": "https://x.test"}}))

    assert resp["error"].startswith("cdp_disconnected")
    assert attempts == [], "cooldown must skip re-dialing entirely"


def test_meta_reconnect_bypasses_cooldown(monkeypatch):
    """reconnect() is the agent's explicit recovery action — it must re-dial even
    when the automatic path is in its failure cooldown."""
    monkeypatch.setattr(daemon, "RECONNECT_DELAYS", (0,))
    d = daemon.Daemon()
    d.cdp = _DeadCDP()
    d.session = "s-old"
    d.target_id = "t-old"
    d.dead_at = time.time()
    d.dead_err = "connection refused"

    class _TargetInfoCDP(_FakeCDP):
        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            if method == "Target.getTargetInfo":
                return {"targetInfo": {"targetId": params["targetId"], "url": "https://x.test/", "title": "X", "type": "page"}}
            return {}

    async def fake_dial(wait=30):
        d.cdp = _TargetInfoCDP()
        d.session = "s-new"
        d.target_id = "t-new"
        d.gen += 1

    monkeypatch.setattr(d, "_dial", fake_dial)

    resp = asyncio.run(d.handle({"meta": "reconnect"}))

    assert resp["reconnected"] is True
    assert resp["page"]["url"] == "https://x.test/"
    assert "tab" in resp.get("notice", "")


def test_browser_gone_stops_retrying_immediately(monkeypatch):
    """A stopped cloud browser is terminal — later delays must not be burned."""
    monkeypatch.setattr(daemon, "RECONNECT_DELAYS", (0, 0, 0))
    d = daemon.Daemon()
    d.cdp = _DeadCDP()
    d.session = "s"
    attempts = []

    async def gone_dial(wait=30):
        attempts.append(1)
        raise daemon.BrowserGone("cloud browser b-1 is stopped (timed out or was stopped)")

    monkeypatch.setattr(d, "_dial", gone_dial)

    resp = asyncio.run(d.handle({"method": "Page.navigate", "params": {"url": "https://x.test"}}))

    assert resp["error"].startswith("cdp_disconnected")
    assert "stopped" in resp["error"]
    assert len(attempts) == 1


def test_protocol_error_does_not_trigger_reconnect(monkeypatch):
    """CDP protocol errors (bad selector, dead node, ...) are normal traffic —
    they must surface unchanged, with zero re-dial attempts."""
    class _ProtocolErrorCDP(_FakeCDP):
        async def send_raw(self, method, params=None, session_id=None):
            raise RuntimeError("{'code': -32000, 'message': 'Node not found'}")

    d = daemon.Daemon()
    d.cdp = _ProtocolErrorCDP()
    d.session = "s"
    attempts = []

    async def fail_dial(wait=30):
        attempts.append(1)

    monkeypatch.setattr(d, "_dial", fail_dial)

    resp = asyncio.run(d.handle({"method": "DOM.getDocument", "params": {}}))

    assert "Node not found" in resp["error"]
    assert attempts == []


def test_shutdown_meta_stop_browser_flag():
    """restart/self-heal shutdowns (stop_browser=False) must keep the cloud
    browser; plain shutdowns keep the old stop-the-browser billing safety."""
    d = daemon.Daemon()
    d.stop = asyncio.Event()
    asyncio.run(d.handle({"meta": "shutdown", "stop_browser": False}))
    assert d.stop_browser_on_exit is False
    assert d.stop.is_set()

    d2 = daemon.Daemon()
    d2.stop = asyncio.Event()
    asyncio.run(d2.handle({"meta": "shutdown"}))
    assert d2.stop_browser_on_exit is True


def test_descriptor_roundtrip(tmp_path, monkeypatch):
    """The connection descriptor must survive daemon death (that's its job) and
    be removable on explicit browser stop."""
    monkeypatch.setattr(daemon.ipc, "conn_path", lambda name: tmp_path / f"{name}.conn")
    daemon.write_descriptor({"browser_id": "b-1", "ws_url": "ws://cloud.test/devtools/browser/x"})
    assert daemon.read_descriptor() == {"browser_id": "b-1", "ws_url": "ws://cloud.test/devtools/browser/x"}
    daemon.clear_descriptor()
    assert daemon.read_descriptor() == {}


def test_transient_context_destroyed_retries_once_same_session():
    """Navigation races ('Execution context was destroyed') are the bulk of real
    per-command eval failures — one inline retry on the same session turns them
    from agent-visible exit-1s into non-events."""
    class _FlakyCDP(_FakeCDP):
        def __init__(self):
            super().__init__()
            self.failed = False
        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            if not self.failed:
                self.failed = True
                raise RuntimeError("{'code': -32000, 'message': 'Execution context was destroyed.'}")
            return {"ok": True}

    d = daemon.Daemon()
    d.cdp = _FlakyCDP()
    d.session = "s-1"

    resp = asyncio.run(d.handle({"method": "Runtime.evaluate", "params": {"expression": "1"}}))

    assert resp["result"] == {"ok": True}
    assert [s for (_, _, s) in d.cdp.calls] == ["s-1", "s-1"], "retry stays on the same session"
    assert "notice" not in resp, "a healed navigation race needs no agent-facing notice"


def test_tab_gone_reattaches_retries_and_notices(monkeypatch):
    """'Target closed' means the attached tab died while the browser lives — the
    daemon must re-attach to a real page, retry there, and TELL the agent."""
    class _TabDeadCDP(_FakeCDP):
        def __init__(self):
            super().__init__()
            self.failed = False
        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            if not self.failed:
                self.failed = True
                raise RuntimeError("{'code': -32000, 'message': 'Target closed.'}")
            return {"ok": True}

    d = daemon.Daemon()
    d.cdp = _TabDeadCDP()
    d.session = "s-dead"
    d.target_id = "t-dead"

    async def fake_attach():
        d.session = "s-fresh"
        d.target_id = "t-fresh"
        return {"targetId": "t-fresh", "url": "about:blank", "type": "page"}

    monkeypatch.setattr(d, "attach_first_page", fake_attach)

    resp = asyncio.run(d.handle({"method": "Page.captureScreenshot", "params": {}}))

    assert resp["result"] == {"ok": True}
    assert d.cdp.calls[-1][2] == "s-fresh", "retry must run on the re-attached session"
    assert "tab" in resp.get("notice", ""), "silent tab teleport is the failure mode we're killing"


def test_unknown_protocol_error_is_not_retried():
    """Genuine CDP errors (bad node id, etc.) must surface immediately — one
    call, no retry, no reconnect."""
    class _OnceCDP(_FakeCDP):
        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            raise RuntimeError("{'code': -32000, 'message': 'No node with given id found'}")

    d = daemon.Daemon()
    d.cdp = _OnceCDP()
    d.session = "s-1"

    resp = asyncio.run(d.handle({"method": "DOM.resolveNode", "params": {"nodeId": 9}}))

    assert "No node with given id found" in resp["error"]
    assert len(d.cdp.calls) == 1


class _HungCDP(_FakeCDP):
    """Page-level calls hang forever; rescue calls answer instantly."""

    async def send_raw(self, method, params=None, session_id=None):
        self.calls.append((method, params, session_id))
        if method in ("Runtime.terminateExecution", "Page.stopLoading"):
            return {}
        await asyncio.sleep(30)


def test_hung_call_times_out_unhangs_page_and_says_not_a_disconnect(monkeypatch):
    """BU_Bench_V1 v2 finding: 60s timeouts on a wedged renderer burned task
    budgets, and nothing unwedged the page, so the next call hung too. The
    timeout must be short, actively terminate the running JS, and tell the
    agent this is a hang (screenshot + simpler action), not a disconnect."""
    monkeypatch.setattr(daemon, "CDP_CALL_TIMEOUT", 0.05)
    monkeypatch.setattr(daemon, "CDP_CALL_TIMEOUTS", {})
    d = daemon.Daemon()
    d.cdp = _HungCDP()
    d.session = "s-1"
    dials = []

    async def no_dial(wait=30):
        dials.append(1)

    monkeypatch.setattr(d, "_dial", no_dial)

    resp = asyncio.run(d.handle({"method": "Runtime.evaluate", "params": {"expression": "while(1){}"}}))

    assert "timed out" in resp["error"]
    assert "NOT a disconnect" in resp["error"]
    rescue = [(m, s) for (m, _, s) in d.cdp.calls if m in ("Runtime.terminateExecution", "Page.stopLoading")]
    assert rescue == [("Runtime.terminateExecution", "s-1"), ("Page.stopLoading", "s-1")], (
        "the wedged page must be actively rescued, on the hung call's session"
    )
    assert dials == [], "a hang is not a dead socket -- no reconnect"


def test_slow_call_methods_get_their_own_timeout(monkeypatch):
    """Legitimately slow calls (full-page screenshots) must not be killed by the
    short default cap — the per-method table must be consulted."""
    monkeypatch.setattr(daemon, "CDP_CALL_TIMEOUT", 0.05)
    monkeypatch.setattr(daemon, "CDP_CALL_TIMEOUTS", {"Page.captureScreenshot": 5})

    class _SlowShotCDP(_FakeCDP):
        async def send_raw(self, method, params=None, session_id=None):
            self.calls.append((method, params, session_id))
            await asyncio.sleep(0.2)  # slower than default cap, well under its own
            return {"data": "iVBOR..."}

    d = daemon.Daemon()
    d.cdp = _SlowShotCDP()
    d.session = "s-1"

    resp = asyncio.run(d.handle({"method": "Page.captureScreenshot", "params": {}}))

    assert resp["result"] == {"data": "iVBOR..."}
