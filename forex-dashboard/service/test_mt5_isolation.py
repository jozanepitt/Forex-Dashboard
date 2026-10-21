"""Whole-process freezes (2026-10-18/19).

The guardian killed a hung app.py at 19:33, 19:48, 19:58 (10-18) and 02:04,
09:48, 11:18 (10-19). Each time every thread stopped mid-fetch, including the
in-process /health self-check, so hang_dump.log was never written. Every
mt5.* call was already under MT5Client._lock: the cause is that the
MetaTrader5 binding holds the GIL for the whole of a call. Measured on
10-19: a 1.1s copy_rates_from_pos let a 1ms ticker thread run zero times. So
one stalled IPC call to the terminal froze the interpreter, and no Python-level
timeout (_fetch_guarded's join, the self-check) could ever fire.

Fix: every MT5 call runs in a child process (providers/mt5_worker.py) that
can be killed on timeout, and a faulthandler watchdog (C thread, no GIL
needed) dumps every stack if the interpreter ever stops again.
"""
from __future__ import annotations

import ctypes
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from providers import mt5_worker

windows_only = pytest.mark.skipif(sys.platform != "win32", reason="Windows process/GIL behaviour")


FAKE_MT5 = textwrap.dedent('''
    import collections, os, time
    TIMEFRAME_M15 = 15
    Acct = collections.namedtuple("Acct", "login server trade_mode")
    calls = []
    def initialize(*a, **k):
        calls.append("initialize")
        return True
    def calls_so_far():
        return list(calls)
    def account_info():
        return Acct(123, "Demo-Server", 0)
    def echo(*a, **k):
        return (a, k)
    def pid():
        return os.getpid()
    def hang():
        time.sleep(1000)
    def boom():
        raise RuntimeError("terminal said no")
    def die():
        os._exit(7)
''')


@pytest.fixture
def fake_module(tmp_path, monkeypatch):
    (tmp_path / "fake_mt5.py").write_text(FAKE_MT5)
    sys.path.insert(0, str(tmp_path))
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    import importlib
    mod = importlib.import_module("fake_mt5")
    yield mod
    sys.path.remove(str(tmp_path))
    sys.modules.pop("fake_mt5", None)


@pytest.fixture
def proxy(fake_module):
    p = mt5_worker.WorkerProxy(fake_module, module_name="fake_mt5", call_timeout=2.0, init_timeout=5.0)
    yield p
    p.close()


def test_calls_run_in_a_child_process_and_results_round_trip(proxy):
    import os
    assert proxy.pid() != os.getpid()
    assert proxy.echo(1, "a", k=2) == ((1, "a"), {"k": 2})
    acct = proxy.account_info()
    assert (acct.login, acct.server, acct.trade_mode) == (123, "Demo-Server", 0)


def test_constants_are_read_locally_without_a_worker(proxy):
    assert proxy.TIMEFRAME_M15 == 15
    assert proxy._proc is None          # no worker spawned just to read a constant


def test_errors_in_the_binding_come_back_as_worker_errors(proxy):
    with pytest.raises(mt5_worker.WorkerError, match="terminal said no"):
        proxy.boom()
    assert proxy.echo(1) == ((1,), {})  # the worker survives an ordinary error


def test_hung_call_times_out_without_freezing_this_process(proxy):
    first_pid = proxy.pid()
    ticks = []
    stop = threading.Event()

    def ticker():
        while not stop.is_set():
            ticks.append(time.perf_counter())
            time.sleep(0.01)

    t = threading.Thread(target=ticker, daemon=True)
    t.start()
    t0 = time.perf_counter()
    with pytest.raises(mt5_worker.WorkerTimeout):
        proxy.hang()
    elapsed = time.perf_counter() - t0
    stop.set()
    t.join()
    assert 1.5 < elapsed < 6, elapsed
    gaps = [b - a for a, b in zip(ticks, ticks[1:])]
    assert gaps and max(gaps) < 0.5, max(gaps)      # other threads kept running
    # the hung worker was killed; the next call gets a fresh one
    assert proxy.pid() != first_pid


def test_replacement_worker_is_initialized_with_the_last_initialize_args(proxy):
    assert proxy.initialize(path="X") is True
    with pytest.raises(mt5_worker.WorkerTimeout):
        proxy.hang()
    # fresh worker: initialize replayed before the call, exactly once
    assert proxy.calls_so_far() == ["initialize"]


def test_worker_crash_is_reported_and_recovered(proxy):
    first_pid = proxy.pid()
    with pytest.raises(mt5_worker.WorkerLost):
        proxy.die()
    assert proxy.pid() != first_pid


def test_worker_exits_when_its_pipe_closes(proxy):
    proxy.pid()
    proc = proxy._proc
    proxy._proc.stdin.close()
    proc.wait(timeout=5)
    assert proc.returncode == 0


def _alive(pid: int) -> bool:
    k32 = ctypes.WinDLL("kernel32")
    k32.OpenProcess.restype = ctypes.c_void_p
    h = k32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not h:
        return False
    code = ctypes.c_ulong()
    k32.GetExitCodeProcess(ctypes.c_void_p(h), ctypes.byref(code))
    k32.CloseHandle(ctypes.c_void_p(h))
    return code.value == 259  # STILL_ACTIVE


@windows_only
def test_worker_dies_with_its_parent(fake_module, tmp_path):
    """The guardian kills a hung app.py with Stop-Process; its MT5 worker must
    not survive as an orphan holding a terminal connection."""
    service = Path(__file__).parent
    parent = subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(f'''
            import sys, time
            sys.path[:0] = [{str(service)!r}, {str(tmp_path)!r}]
            import fake_mt5
            from providers import mt5_worker
            p = mt5_worker.WorkerProxy(fake_mt5, module_name="fake_mt5")
            print(p.pid(), flush=True)
            time.sleep(60)
        ''')],
        stdout=subprocess.PIPE, text=True)
    try:
        worker_pid = int(parent.stdout.readline())
        assert _alive(worker_pid)
        parent.kill()
        parent.wait(timeout=5)
        deadline = time.time() + 5
        while _alive(worker_pid) and time.time() < deadline:
            time.sleep(0.1)
        assert not _alive(worker_pid)
    finally:
        parent.kill()


# ── MT5Client keeps its contract on top of the worker ───────────────────────

def test_fetch_turns_a_worker_timeout_into_an_mt5_error(monkeypatch):
    from providers import exness_mt5 as X
    c = X.MT5Client(symbol_suffix="m")
    c._initialized = True

    class _Hung:
        @staticmethod
        def symbol_select(sym, on):
            return True

        @staticmethod
        def copy_rates_from_pos(*a):
            raise mt5_worker.WorkerTimeout("copy_rates_from_pos timed out after 20s")

    monkeypatch.setattr(X, "mt5", _Hung, raising=False)
    monkeypatch.setattr(X, "MT5_AVAILABLE", True)
    monkeypatch.setitem(X._INTERVAL_TO_TIMEFRAME, "15min", 15)
    with pytest.raises(X.MT5Error, match="timed out"):
        c.fetch("EUR/USD", "15min")
    assert not c._lock.locked()


def test_status_reports_unavailable_when_the_worker_fails(monkeypatch):
    from providers import exness_mt5 as X
    c = X.MT5Client(symbol_suffix="m")
    c._initialized = True

    class _Lost:
        @staticmethod
        def account_info():
            raise mt5_worker.WorkerLost("worker exited")

    monkeypatch.setattr(X, "mt5", _Lost, raising=False)
    monkeypatch.setattr(X, "MT5_AVAILABLE", True)
    out = c.status()
    assert out["available"] is False and "worker exited" in out["reason"]


def test_the_service_never_calls_the_binding_in_process():
    from providers import exness_mt5 as X
    if not X.MT5_AVAILABLE:
        pytest.skip("MetaTrader5 not installed")
    assert isinstance(X.mt5, mt5_worker.WorkerProxy)


# ── faulthandler watchdog ────────────────────────────────────────────────────

@windows_only
def test_watchdog_dumps_stacks_while_a_c_call_holds_the_gil(tmp_path):
    """Reproduces the freeze: kernel32.Sleep through ctypes.PyDLL keeps the GIL
    (exactly what the MetaTrader5 binding does), so no Python thread can run.
    The faulthandler C thread must still write every stack to the dump file."""
    dump = tmp_path / "hang_dump.log"
    script = textwrap.dedent(f'''
        import ctypes, sys, time
        sys.path.insert(0, {str(Path(__file__).parent)!r})
        import hang_watchdog
        hang_watchdog.start({str(dump)!r}, timeout=1.0, beat=0.2)
        time.sleep(0.5)
        def frozen_in_mt5_call():
            ctypes.PyDLL("kernel32").Sleep(2500)
        frozen_in_mt5_call()
    ''')
    subprocess.run([sys.executable, "-c", script], timeout=30, check=True)
    text = dump.read_text(encoding="utf-8", errors="replace")
    assert "watchdog armed" in text
    assert "Timeout (0:00:01)" in text
    assert "frozen_in_mt5_call" in text


def test_watchdog_stays_quiet_while_the_interpreter_runs(tmp_path):
    dump = tmp_path / "hang_dump.log"
    script = textwrap.dedent(f'''
        import sys, time
        sys.path.insert(0, {str(Path(__file__).parent)!r})
        import hang_watchdog
        hang_watchdog.start({str(dump)!r}, timeout=1.0, beat=0.2)
        time.sleep(2.5)
    ''')
    subprocess.run([sys.executable, "-c", script], timeout=30, check=True)
    assert "Timeout" not in dump.read_text(encoding="utf-8", errors="replace")
