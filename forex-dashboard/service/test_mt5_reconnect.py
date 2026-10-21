"""MT5Client must reconnect after the terminal restarts.

Incident 2026-10-20: the MT5 terminal restarted at 17:16 UTC. The client kept
its `_initialized` flag from the old terminal, so every call for 3+ hours got
(-10001, 'IPC send failed') and the service ran on the TwelveData fallback
(no volume, still-open bars), which fired a false 13/50 cross alert.
"""
from __future__ import annotations

import pytest

from providers import exness_mt5 as X


class _Terminal:
    """Fake MetaTrader5 binding: the IPC link breaks until initialize() runs again."""

    def __init__(self):
        self.link_ok = True
        self.init_calls = 0
        self.shutdown_calls = 0
        self.connected = True

    def initialize(self):
        self.init_calls += 1
        self.link_ok = True
        return True

    def shutdown(self):
        self.shutdown_calls += 1
        self.link_ok = False

    def last_error(self):
        return (1, "Success") if self.link_ok else (-10001, "IPC send failed")

    def symbol_select(self, sym, on):
        return self.link_ok

    def copy_rates_from_pos(self, sym, tf, start, n):
        if not self.link_ok:
            return None
        return [{"time": 1_000_000 * 900, "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "tick_volume": 10}]

    def symbol_info_tick(self, sym):
        return None

    def account_info(self):
        if not self.link_ok:
            return None
        return type("A", (), {"login": 1, "server": "s", "trade_mode": 0})()

    def terminal_info(self):
        if not self.link_ok:
            return None
        return type("T", (), {"connected": self.connected})()


@pytest.fixture
def term(monkeypatch):
    t = _Terminal()
    monkeypatch.setattr(X, "mt5", t, raising=False)
    monkeypatch.setattr(X, "MT5_AVAILABLE", True)
    monkeypatch.setitem(X._INTERVAL_TO_TIMEFRAME, "15min", 15)
    return t


def _client():
    c = X.MT5Client(symbol_suffix="m")
    c.INIT_RETRY_COOLDOWN_SECS = 0
    return c


def test_ipc_failure_drops_the_connection_and_the_next_call_reconnects(term):
    c = _client()
    assert c.fetch("EUR/USD", "15min")
    assert term.init_calls == 1

    term.link_ok = False                       # the terminal restarted under us
    with pytest.raises(X.MT5Error, match="IPC send failed"):
        c.fetch("EUR/USD", "15min")
    assert c._initialized is False             # the stale connection is forgotten

    bars = c.fetch("EUR/USD", "15min")         # next call re-initializes
    assert bars and term.init_calls == 2
    assert not c._lock.locked()


def test_status_reconnects_after_a_terminal_restart(term):
    c = _client()
    assert c.status()["available"] is True
    term.link_ok = False
    c._status_cache = None
    out = c.status()
    assert out["available"] is False
    assert c._initialized is False
    c._status_cache = None
    assert c.status()["available"] is True and term.init_calls == 2


def test_a_slow_reconnect_does_not_block_other_callers(term, monkeypatch):
    """Review 2026-10-20: with the terminal really down, initialize() can take
    up to 75s while holding the lock; every other fetch waited for it, timed
    out in the scheduler and never reached the TwelveData fallback. Callers
    that find a reconnect in progress must give up at once."""
    import threading, time as _t
    c = _client()
    started, release = threading.Event(), threading.Event()

    def slow_init():
        started.set()
        release.wait(5)
        term.init_calls += 1
        return True

    monkeypatch.setattr(term, "initialize", slow_init)
    t = threading.Thread(target=lambda: c._ensure_init())
    t.start()
    assert started.wait(2)
    t0 = _t.time()
    with pytest.raises(X.MT5NotConnected):
        c.fetch("EUR/USD", "15min")           # another caller during the reconnect
    assert _t.time() - t0 < 1.0               # fell through immediately
    release.set()
    t.join(5)
    assert c._initialized is True


def test_cooldown_starts_when_the_attempt_ends(term, monkeypatch):
    """A failed 60s initialize must not be retried the moment it returns."""
    c = X.MT5Client(symbol_suffix="m")
    clock = [1000.0]

    def slow_fail():
        clock[0] += 60                                     # MT5 waits ~60s for a dead terminal
        return False

    monkeypatch.setattr(term, "initialize", slow_fail)
    monkeypatch.setattr(X.time, "time", lambda: clock[0])
    assert c._ensure_init() is False
    clock[0] += c.INIT_RETRY_COOLDOWN_SECS - 1
    calls = []
    monkeypatch.setattr(term, "initialize", lambda: calls.append(1) or False)
    assert c._ensure_init() is False and calls == []      # still cooling down


def test_a_normal_missing_symbol_does_not_drop_the_connection(term, monkeypatch):
    """Only IPC errors mean the link is dead; an unknown symbol is just an error."""
    c = _client()
    c.fetch("EUR/USD", "15min")
    monkeypatch.setattr(term, "symbol_select", lambda s, on: False)
    monkeypatch.setattr(term, "last_error", lambda: (-1, "Terminal: Call failed"))
    with pytest.raises(X.MT5Error):
        c.fetch("EUR/USD", "15min")
    assert c._initialized is True and term.init_calls == 1
