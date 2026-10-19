"""Regression tests for the scheduler's live-alert gate.

Incident (2026-10-11): `CAD/JPY:vwap9ema_sell` was sent on a Sunday. The MT5
link had died on Thursday, CAD/JPY's newest cached M5 bar was frozen at
Fri 09:20 UTC, and nothing between the cache and Discord checked that the data
was live or the market open — so the same cross on the same two Friday bars
was re-sent every time the hourly throttle expired.

These tests drive every `_run_*alerts` runner with a strategy stub that always
returns an alertable row, and assert the alert function is only reached when
the symbol's market is open AND its newest bar is fresh.
"""
from __future__ import annotations

import datetime as dt
import importlib

import pytest

import alerts
import cache
import scheduler


def ts(y: int, m: int, d: int, hh: int, mm: int = 0) -> int:
    return int(dt.datetime(y, m, d, hh, mm, tzinfo=dt.timezone.utc).timestamp())


SUNDAY_NOW = ts(2026, 10, 11, 10, 2)      # the incident
FRIDAY_NOW = ts(2026, 10, 9, 14, 2)       # market open, feed already frozen
WEEKDAY_NOW = ts(2026, 10, 7, 10, 3)      # Wednesday, healthy
FROZEN_BAR = ts(2026, 10, 9, 9, 20)
# The incident pair was CAD/JPY; the tests use EUR/USD because it is in every
# strategy's universe, and the gate is an allow-list keyed on that universe.
SYM = "EUR/USD"

# runner name, strategy module, analyze fn, alert fns it may call
RUNNERS = [
    ("_run_vwap9ema_alerts", "vwap9ema_strategy", "analyze_universe", ["alert_vwap9ema_setup"]),
    ("_run_vwap_mr_alerts", "vwap_mean_reversion_strategy", "analyze_universe",
     ["alert_vwap_mr_setup", "alert_vwap_mr_watch"]),
    ("_run_tdi123_alerts", "tdi_cycle_123", "analyze_universe",
     ["alert_tdi123_setup", "alert_tdi123_watch"]),
    ("_run_engulfing_alerts", "engulfing_strategy", "analyze_universe", ["alert_engulfing_setup"]),
    ("_run_id50_alerts", "btmm_id50", "analyze_universe",
     ["alert_id50_setup", "alert_id50_watch"]),
    ("_run_crt_5am_alerts", "crt_strategy", "analyze_universe_5am", ["alert_crt_5am_setup"]),
    ("_run_vwap_bounce_alerts", "vwap_bounce_strategy", "analyze_universe", ["alert_vwap_bounce_setup"]),
    ("_run_sweep_alerts", "sweep_strategy", "analyze_universe", ["alert_sweep_setup"]),
]


def _bars(last_ts: int, n: int = 300) -> list[dict]:
    """n bars ending at last_ts; spacing is irrelevant to the gate, which only
    looks at the newest bar."""
    return [{"ts_utc": last_ts - (n - 1 - i) * 300, "open": 1.0, "high": 1.0,
             "low": 1.0, "close": 1.0, "volume": 1.0} for i in range(n)]


@pytest.fixture
def harness(monkeypatch):
    """Patch the clock, the cache and every alert function; return a recorder."""
    calls: list[tuple[str, str]] = []
    # "last_bar" applies to every interval unless overridden per interval in
    # "last_bar_by_interval" (so a test can freeze just one feed).
    state = {"last_bar": FROZEN_BAR, "now": SUNDAY_NOW, "last_bar_by_interval": {}}

    monkeypatch.setattr(scheduler.time, "time", lambda: float(state["now"]))
    monkeypatch.setattr(
        cache, "read_candles",
        lambda sym, interval, limit=None: _bars(
            state["last_bar_by_interval"].get(interval, state["last_bar"])))
    scheduler._last_gate_summary.clear()
    for _, _, _, alert_fns in RUNNERS:
        for fn in alert_fns:
            monkeypatch.setattr(alerts, fn,
                                lambda pair, row, _fn=fn: calls.append((_fn, pair)))
    monkeypatch.setattr(alerts, "evaluate_pair",
                        lambda **kw: calls.append(("evaluate_pair", kw["pair_symbol"])))
    # The TDI123 runner also resolves journaled trades against the real DB.
    import trade_tracker
    monkeypatch.setattr(trade_tracker, "resolve_open_trades", lambda: None)

    def stub_strategy(module_name: str, fn_name: str):
        mod = importlib.import_module(module_name)
        monkeypatch.setattr(mod, fn_name, lambda candles_by_pair: {
            "pairs": [{"symbol": SYM, "setup": "SELL", "grade": "A",
                       "entry": 1.0, "sl": 1.1, "tp1": 0.8}]})

    state["calls"] = calls
    state["stub"] = stub_strategy
    return state


@pytest.mark.parametrize("runner,module,fn,alert_fns", RUNNERS, ids=[r[0] for r in RUNNERS])
def test_no_alert_on_a_closed_market(harness, runner, module, fn, alert_fns):
    """The incident itself: Sunday, frozen Friday bars -> nothing may be sent."""
    harness["stub"](module, fn)
    getattr(scheduler, runner)()
    assert harness["calls"] == []


@pytest.mark.parametrize("runner,module,fn,alert_fns", RUNNERS, ids=[r[0] for r in RUNNERS])
def test_no_alert_on_a_frozen_feed_while_the_market_is_open(harness, runner, module, fn, alert_fns):
    """Friday afternoon, feed dead since the morning: the weekend rule can't
    help, the freshness rule must."""
    harness["now"] = FRIDAY_NOW
    harness["stub"](module, fn)
    getattr(scheduler, runner)()
    assert harness["calls"] == []


@pytest.mark.parametrize("runner,module,fn,alert_fns", RUNNERS, ids=[r[0] for r in RUNNERS])
def test_alert_still_fires_on_live_data(harness, runner, module, fn, alert_fns):
    """Positive control — the gate must not simply block everything."""
    harness["now"] = WEEKDAY_NOW
    harness["last_bar"] = ts(2026, 10, 7, 10, 0)
    harness["stub"](module, fn)
    getattr(scheduler, runner)()
    assert {name for name, _ in harness["calls"]} == set(alert_fns)
    assert all(pair == SYM for _, pair in harness["calls"])


def _stub_btmm_core(monkeypatch):
    import btmm_core
    monkeypatch.setattr(btmm_core, "analyze", lambda bars, symbol=None: {
        "signal": "strong_buy", "score": 85, "checklist_score": 11,
        "adr_consumed": 0.4})
    monkeypatch.setattr(scheduler, "PRIORITY_PAIRS", [SYM])


def test_btmm_core_runner_is_gated_on_a_closed_market(harness, monkeypatch):
    _stub_btmm_core(monkeypatch)
    scheduler._run_alerts()
    assert harness["calls"] == []


def test_btmm_core_runner_fires_on_live_data(harness, monkeypatch):
    _stub_btmm_core(monkeypatch)
    harness["now"] = WEEKDAY_NOW
    harness["last_bar"] = ts(2026, 10, 7, 10, 0)
    scheduler._run_alerts()
    assert harness["calls"] == [("evaluate_pair", SYM)]


def test_btmm_core_runner_is_gated_on_a_frozen_weekday_feed(harness, monkeypatch):
    _stub_btmm_core(monkeypatch)
    harness["now"] = FRIDAY_NOW
    scheduler._run_alerts()
    assert harness["calls"] == []


H1_RUNNERS = [r for r in RUNNERS if r[0] in
              ("_run_tdi123_alerts", "_run_engulfing_alerts", "_run_id50_alerts")]


@pytest.mark.parametrize("runner,module,fn,alert_fns", H1_RUNNERS, ids=[r[0] for r in H1_RUNNERS])
def test_frozen_h1_blocks_even_when_m15_is_live(harness, runner, module, fn, alert_fns):
    """These strategies also trade off H1. A fallback provider can keep M15
    flowing while H1 is frozen — the same bug on the other leg."""
    harness["now"] = WEEKDAY_NOW
    harness["last_bar"] = ts(2026, 10, 7, 10, 0)
    harness["last_bar_by_interval"] = {"1h": ts(2026, 10, 5, 9, 0)}
    harness["stub"](module, fn)
    getattr(scheduler, runner)()
    assert harness["calls"] == []


@pytest.mark.parametrize("key,interval,runner", [
    ("5min", "15min", "_run_vwap9ema_alerts"),      # M5 strategy: frozen M5, live M15
    ("15min", "5min", "_run_vwap_mr_alerts"),       # M15 strategy: frozen M15, live M5
])
def test_each_runner_gates_on_its_own_timeframe(harness, key, interval, runner):
    """Guards against a runner being wired to the wrong feed."""
    row = next(r for r in RUNNERS if r[0] == runner)
    harness["now"] = WEEKDAY_NOW
    harness["last_bar"] = ts(2026, 10, 7, 10, 0)
    harness["last_bar_by_interval"] = {key: FROZEN_BAR}
    harness["stub"](row[1], row[2])
    getattr(scheduler, runner)()
    assert harness["calls"] == []


def test_one_malformed_bundle_does_not_sink_the_runner(harness, monkeypatch):
    import vwap9ema_strategy
    harness["now"] = WEEKDAY_NOW
    good = _bars(ts(2026, 10, 7, 10, 0))
    monkeypatch.setattr(cache, "read_candles", lambda sym, interval, limit=None:
                        [{"ts_utc": None}] if sym == "GBP/USD" else good)
    monkeypatch.setattr(vwap9ema_strategy, "analyze_universe", lambda c: {
        "pairs": [{"symbol": "GBP/USD", "setup": "BUY"}, {"symbol": SYM, "setup": "SELL"}]})
    scheduler._run_vwap9ema_alerts()
    assert harness["calls"] == [("alert_vwap9ema_setup", SYM)]


def test_crypto_still_alerts_on_a_weekend_with_live_data(harness, monkeypatch):
    """Crypto trades 24/7 — the weekend rule must not silence it."""
    import vwap9ema_strategy
    harness["last_bar"] = SUNDAY_NOW - 120
    monkeypatch.setattr(vwap9ema_strategy, "analyze_universe", lambda c: {
        "pairs": [{"symbol": "BTC/USD", "setup": "BUY"}, {"symbol": SYM, "setup": "SELL"}]})
    scheduler._run_vwap9ema_alerts()
    assert harness["calls"] == [("alert_vwap9ema_setup", "BTC/USD")]


def test_every_alert_runner_is_covered_by_these_tests():
    """A new `_run_*alerts` runner added without gate coverage must fail here,
    not in production on a Sunday."""
    runners = {n for n in dir(scheduler) if n.startswith("_run_") and n.endswith("alerts")}
    covered = {r[0] for r in RUNNERS} | {"_run_alerts"}
    assert runners == covered
