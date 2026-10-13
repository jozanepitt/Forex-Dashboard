"""Reliability regressions (2026-10-13).

1. fetcher.get_candles hit the provider on almost every call. The cache holds
   CLOSED bars only, so after a bar that opened at T the next one cannot exist
   before T + 2 intervals — but the freshness test was T + 1 interval + 60s,
   written as if the forming bar were stored. Every dashboard request therefore
   pulled 3200 bars from MT5 under its single lock for ~14 of every 15 minutes,
   starving the scheduler ("provider hung" timeouts, 5-minute refreshes).
2. scheduler.refresh_all did `from app import ...` while the service runs as
   __main__, re-importing app.py: a second Flask/SocketIO instance and a second
   RotatingFileHandler on service.log (the log died at the 2 MB rollover).
3. The web server went deaf for 26 minutes while the scheduler kept running and
   nothing restarted it. The service now probes its own /health and exits after
   repeated failures so the guardian relaunches it.
"""
from __future__ import annotations

import inspect

import pytest

import cache
import fetcher
import scheduler

M15 = 900
H1 = 3600
LAST = 1_791_000_000 - 1_791_000_000 % 900          # an arbitrary M15 bar open


class _Clock:
    def __init__(self, now):
        self._now = now

    def now(self, tz=None):
        import datetime as dt
        return dt.datetime.fromtimestamp(self._now, tz=tz)

    @staticmethod
    def fromtimestamp(ts, tz=None):
        import datetime as dt
        return dt.datetime.fromtimestamp(ts, tz=tz)


@pytest.fixture
def fetch_harness(monkeypatch):
    calls = []
    monkeypatch.setattr(cache, "read_candles", lambda sym, iv, limit=800: ["cached"])
    monkeypatch.setattr(fetcher, "_incremental", lambda sym, iv, last: (calls.append((sym, iv)), False)[1])
    monkeypatch.setattr(fetcher, "_backfill", lambda sym, iv, limit: (calls.append(("backfill", sym)), False)[1])

    def at(now, last_ts, interval="15min"):
        monkeypatch.setattr(cache, "max_ts", lambda sym, iv: last_ts)
        monkeypatch.setattr(fetcher, "datetime", _Clock(now))
        calls.clear()
        bars, stale = fetcher.get_candles("EUR/USD", interval, limit=100)
        return calls[:], bars, stale

    return at


def test_no_provider_call_while_the_next_bar_cannot_have_closed(fetch_harness):
    # Newest cached bar opened at LAST and closed at LAST+15m. The next bar
    # closes at LAST+30m, so nothing new can exist before then.
    for age in (M15 + 61, M15 + 300, 2 * M15 - 1):
        calls, bars, stale = fetch_harness(LAST + age, LAST)
        assert calls == [], f"fetched at +{age}s"
        assert bars == ["cached"] and stale is False


def test_provider_is_called_once_the_next_bar_has_closed(fetch_harness):
    calls, _, _ = fetch_harness(LAST + 2 * M15, LAST)
    assert calls == [("EUR/USD", "15min")]
    calls, _, _ = fetch_harness(LAST + 2 * M15 + 120, LAST)       # the :02 scheduler run
    assert calls == [("EUR/USD", "15min")]


def test_hourly_bars_are_not_refetched_every_quarter_hour(fetch_harness):
    last_h1 = LAST - LAST % H1
    calls, _, _ = fetch_harness(last_h1 + H1 + 17 * 60, last_h1, "1h")
    assert calls == []
    calls, _, _ = fetch_harness(last_h1 + 2 * H1 + 120, last_h1, "1h")
    assert calls == [("EUR/USD", "1h")]


def test_empty_cache_still_backfills(fetch_harness):
    calls, _, _ = fetch_harness(LAST, None)
    assert calls == [("backfill", "EUR/USD")]


# ── scheduler -> dashboard notification without importing app ────────────────

def test_scheduler_never_imports_app():
    import ast
    tree = ast.parse(inspect.getsource(scheduler))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert "app" not in imported


def test_refresh_listener_is_called_and_failures_are_contained():
    seen = []
    scheduler.set_refresh_listener(seen.append)
    try:
        scheduler._notify_refresh(27)
        assert seen == [27]

        def boom(n):
            raise RuntimeError("socket gone")
        scheduler.set_refresh_listener(boom)
        scheduler._notify_refresh(27)                 # must not raise
    finally:
        scheduler.set_refresh_listener(None)
    scheduler._notify_refresh(27)                     # no listener: no-op


# ── HTTP self-check ──────────────────────────────────────────────────────────

@pytest.fixture
def selfcheck():
    scheduler._http_fail_streak = 0
    yield
    scheduler._http_fail_streak = 0


def test_healthy_probe_never_restarts(selfcheck):
    dead = []
    for _ in range(10):
        scheduler._http_self_check(probe=lambda: True, on_dead=lambda: dead.append(1))
    assert dead == []


def test_restart_only_after_consecutive_failures(selfcheck):
    dead = []
    kill = lambda: dead.append(1)
    for _ in range(scheduler.HTTP_SELF_CHECK_FAILS - 1):
        scheduler._http_self_check(probe=lambda: False, on_dead=kill)
    assert dead == []
    scheduler._http_self_check(probe=lambda: True, on_dead=kill)       # recovery resets the streak
    for _ in range(scheduler.HTTP_SELF_CHECK_FAILS - 1):
        scheduler._http_self_check(probe=lambda: False, on_dead=kill)
    assert dead == []
    scheduler._http_self_check(probe=lambda: False, on_dead=kill)
    assert dead == [1]


def test_probe_exception_counts_as_a_failure(selfcheck):
    dead = []

    def boom():
        raise OSError("connection refused")
    for _ in range(scheduler.HTTP_SELF_CHECK_FAILS):
        scheduler._http_self_check(probe=boom, on_dead=lambda: dead.append(1))
    assert dead == [1]


def test_status_does_not_flag_pairs_stale_while_the_refresh_is_due(monkeypatch):
    """Between a quarter-hour and the scheduler's run a few minutes later the
    newest closed bar is 30+ minutes old by its open time. That is normal."""
    monkeypatch.setattr(cache, "max_ts", lambda sym, iv: LAST)
    monkeypatch.setattr(fetcher, "client", lambda: type("C", (), {"status": lambda self: []})())
    monkeypatch.setattr(fetcher.exness_mt5, "client", lambda: type("M", (), {"status": lambda self: {}})())
    monkeypatch.setattr(fetcher, "datetime", _Clock(LAST + 2 * M15 + 240))
    assert all(p["fresh"] for p in fetcher.status_summary()["pairs"])
    monkeypatch.setattr(fetcher, "datetime", _Clock(LAST + 3 * M15 + 240))
    assert not any(p["fresh"] for p in fetcher.status_summary()["pairs"])
