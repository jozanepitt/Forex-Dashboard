"""Tests for the standalone M15 13/50 EMA cross check (ema1350_cross.py).

Synthetic M15 series only. Each scenario is replayed bar by bar the way the
live scanner sees it (one new closed M15 bar per 15-minute cycle), and the
first cycle that produces a BUY/SELL is the signal under test.
"""
from __future__ import annotations

import datetime as dt

import pytest

import alerts
import ema1350_cross as ec

T0 = int(dt.datetime(2026, 10, 12, 0, 0, tzinfo=dt.timezone.utc).timestamp())  # a Monday
WICK = 0.0002


def _path(start, segments):
    """Piecewise-linear closes: segments = [(target, n_bars), ...]."""
    out, p = [], start
    for target, n in segments:
        step = (target - p) / n
        for _ in range(n):
            p += step
            out.append(p)
    return out


def _bars(closes, start=1.2000, t0=T0):
    bars, prev = [], start
    for k, c in enumerate(closes):
        o = prev
        bars.append({"ts_utc": t0 + 900 * k, "open": o, "close": c,
                     "high": max(o, c) + WICK, "low": min(o, c) - WICK})
        prev = c
    return bars


def _mirror(bars, pivot=1.15):
    return [{"ts_utc": b["ts_utc"], "open": 2 * pivot - b["open"], "close": 2 * pivot - b["close"],
             "high": 2 * pivot - b["low"], "low": 2 * pivot - b["high"]} for b in bars]


def _first_signal(bars, d1=None, symbol="EUR/USD"):
    """Replay the series one closed bar at a time; return the first BUY/SELL row."""
    for n in range(ec.MIN_BARS, len(bars) + 1):
        row = ec.analyze_pair(symbol, bars[:n], d1)
        if row["setup"] in ("BUY", "SELL"):
            return row, n
    return None, None


def _downtrend_then(segments):
    # 150 bars down from 1.2000 to 1.1000 puts the 13 EMA well under the 50.
    return _bars(_path(1.2000, [(1.1000, 150)] + segments))


W_SEGMENTS = [(1.0900, 6), (1.0960, 6), (1.0890, 6), (1.1150, 30)]


# ── confirmation candle ──────────────────────────────────────────────────────

def _flat_bar(o, c, k):
    return {"ts_utc": T0 + 900 * k, "open": o, "close": c,
            "high": max(o, c) + WICK, "low": min(o, c) - WICK}


def test_confirmation_needs_the_whole_body_past_the_50():
    e50 = [1.1000] * 6
    e13 = [1.0990, 1.0995, 1.1001, 1.1004, 1.1006, 1.1008]   # crosses up at index 2
    bars = [_flat_bar(1.0990, 1.0990, 0), _flat_bar(1.0990, 1.0995, 1),
            _flat_bar(1.0995, 1.1010, 2),        # body straddles the 50 -> not confirmation
            _flat_bar(1.1010, 1.1002, 3),        # body fully above (wick may touch) -> confirmation
            _flat_bar(1.1002, 1.1012, 4), _flat_bar(1.1012, 1.1015, 5)]
    conf = ec.find_cross_confirmation(bars, e13, e50, end_idx=3)
    assert conf == {"direction": "bullish", "cross_idx": 2, "confirm_idx": 3}
    # Only the FIRST confirming candle counts: bar 4 is not a fresh confirmation.
    assert ec.find_cross_confirmation(bars, e13, e50, end_idx=4) is None
    assert ec.find_cross_confirmation(bars, e13, e50, end_idx=2) is None


def test_the_cross_candle_itself_can_confirm():
    """User decision 2026-10-21: waiting for the candle AFTER the cross made the
    EUR/JPY alert 15 minutes late. A cross candle whose whole body is past the
    50 confirms on its own close."""
    e50 = [1.1000] * 5
    e13 = [1.0990, 1.0995, 1.1001, 1.1004, 1.1006]           # crosses up at index 2
    bars = [_flat_bar(1.0990, 1.0990, 0), _flat_bar(1.0990, 1.0995, 1),
            _flat_bar(1.1003, 1.1010, 2),                     # cross candle, body already above
            _flat_bar(1.1010, 1.1012, 3), _flat_bar(1.1012, 1.1015, 4)]
    assert ec.find_cross_confirmation(bars, e13, e50, end_idx=2) == {
        "direction": "bullish", "cross_idx": 2, "confirm_idx": 2}
    assert ec.find_cross_confirmation(bars, e13, e50, end_idx=3) is None   # fires once only


def test_monday_previous_day_is_friday_not_the_sunday_stub():
    """Review 2026-10-20: D1 bars include a ~2h Sunday bar, which became Monday's PDL."""
    monday = T0                                                  # 2026-10-12 is a Monday
    friday, saturday, sunday = monday - 3 * 86400, monday - 2 * 86400, monday - 86400
    d1 = [{"ts_utc": friday}, {"ts_utc": saturday}, {"ts_utc": sunday}, {"ts_utc": monday}]
    assert ec._previous_trading_day(d1, monday)["ts_utc"] == friday
    assert ec._previous_trading_day(d1, monday + 86400)["ts_utc"] == monday
    assert ec._previous_trading_day([], monday) is None
    # crypto trades all weekend: Monday's previous day is Sunday
    assert ec._previous_trading_day(d1, monday, skip_weekends=False)["ts_utc"] == sunday


def test_confirmation_expires_after_max_bars():
    e50 = [1.1000] * 10
    e13 = [1.0990, 1.1001] + [1.1002] * 8                      # crosses up at index 1
    bars = [_flat_bar(1.0990, 1.0990, 0)] + [_flat_bar(1.0995, 1.1005, k) for k in range(1, 9)] \
        + [_flat_bar(1.1003, 1.1010, 9)]                         # first full body at index 9
    assert ec.find_cross_confirmation(bars, e13, e50, end_idx=9) is None


# ── end-to-end scenarios ─────────────────────────────────────────────────────

def test_w_pattern_then_cross_gives_buy_with_divergence():
    row, _ = _first_signal(_downtrend_then(W_SEGMENTS))
    assert row is not None
    assert row["setup"] == "BUY"
    assert row["timeframe"] == "M15"
    assert row["pattern"]["type"] == "W"
    assert row["divergence"] is True
    assert row["sl"] < row["pattern"]["extreme"] < row["entry"] < row["tp1"]
    assert row["tp1"] - row["entry"] == pytest.approx(ec.RR * (row["entry"] - row["sl"]))
    assert row["grade"] in ("A+", "A")


def test_m_pattern_mirror_gives_sell():
    row, _ = _first_signal(_mirror(_downtrend_then(W_SEGMENTS)))
    assert row is not None
    assert row["setup"] == "SELL"
    assert row["pattern"]["type"] == "M"
    assert row["divergence"] is True
    assert row["sl"] > row["pattern"]["extreme"] > row["entry"] > row["tp1"]


def test_half_batman_anchor_gives_grade_without_divergence():
    row, _ = _first_signal(_downtrend_then([(1.0900, 6), (1.1150, 30)]))
    assert row is not None
    assert row["setup"] == "BUY"
    assert row["pattern"]["type"] == "half_batman"
    assert row["divergence"] is False


def test_no_pattern_inside_lookback_means_no_signal():
    # The low is set, then a slow grind higher: the 13/50 cross comes long
    # after the low has left the 48-bar pattern window.
    bars = _downtrend_then([(1.0900, 6), (1.0900 + 0.0001 * 150, 150)])
    row, _ = _first_signal(bars)
    assert row is None
    notes = [ec.analyze_pair("EUR/USD", bars[:m], None)["notes"] for m in range(ec.MIN_BARS, len(bars) + 1)]
    # The cross itself did confirm; it was rejected only for the missing anchor.
    assert any("no anchor" in s for s in notes)


def test_pattern_at_the_low_of_day_is_a_plus():
    row, _ = _first_signal(_downtrend_then(W_SEGMENTS))
    assert "LOD" in row["origin"]
    assert row["grade"] == "A+"


def test_pattern_away_from_key_levels_is_not_a_plus():
    # Tuesday opens with a spike low far below the W, so the W is not the LOD;
    # no D1 data, so no PDL/PWL either. Divergence alone is only grade B.
    closes = _path(1.2000, [(1.1300, 96)])                       # Monday
    closes += _path(1.1300, [(1.0600, 2), (1.1300, 2)])          # Tuesday-open spike low
    closes += _path(1.1300, [(1.1000, 50)] + W_SEGMENTS)
    row, _ = _first_signal(_bars(closes))
    assert row is not None
    assert row["origin"] == []
    assert row["divergence"] is True
    assert row["grade"] == "B"


def test_previous_day_low_counts_as_origin():
    bars = _downtrend_then(W_SEGMENTS)
    row, n = _first_signal(bars)
    day = (bars[n - 1]["ts_utc"] // 86400) * 86400
    d1 = [{"ts_utc": day - 86400, "open": 1.12, "high": 1.13, "low": row["pattern"]["extreme"] + 0.0001,
           "close": 1.11},
          {"ts_utc": day, "open": 1.11, "high": 1.12, "low": 1.08, "close": 1.10}]   # today: ignored
    row2 = ec.analyze_pair("EUR/USD", bars[:n], d1)
    assert "PDL" in row2["origin"]


def test_previous_week_low_counts_as_origin():
    bars = _downtrend_then(W_SEGMENTS)
    row, n = _first_signal(bars)
    prev_week = [{"ts_utc": T0 - 86400 * d, "open": 1.12, "high": 1.13, "low": 1.10, "close": 1.11}
                 for d in range(3, 8)]
    prev_week[0]["low"] = row["pattern"]["extreme"] - 0.0001
    row2 = ec.analyze_pair("EUR/USD", bars[:n], prev_week)
    assert "PWL" in row2["origin"]


def test_signal_does_not_repeat_on_later_bars():
    bars = _downtrend_then(W_SEGMENTS)
    _, n = _first_signal(bars)
    later = [ec.analyze_pair("EUR/USD", bars[:m], None)["setup"] for m in range(n + 1, len(bars) + 1)]
    assert set(later) == {"NO-TRADE"}


def test_daily_vwap_is_anchored_at_utc_midnight():
    day = T0 + 86400
    bars = [{"ts_utc": day - 900, "open": 9, "high": 9, "low": 9, "close": 9, "volume": 1000},   # yesterday: ignored
            {"ts_utc": day, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            {"ts_utc": day + 900, "open": 4, "high": 4, "low": 4, "close": 4, "volume": 3}]
    assert ec.daily_vwap(bars) == pytest.approx((1 * 1 + 4 * 3) / 4)


def test_grade_rules():
    # User decision 2026-10-20 after the 10-day replay (~49 alerts/day -> ~14):
    # A+ = any key level + RSI divergence; A = PDH/PDL/PWH/PWL alone.
    # Plain HOD/LOD, divergence alone and the VWAP side are confluence only.
    assert ec.grade_setup(origin=["LOD"], divergence=True, vwap_aligned=False) == "A+"
    assert ec.grade_setup(origin=["PWH"], divergence=True, vwap_aligned=False) == "A+"
    assert ec.grade_setup(origin=["LOD", "PDL"], divergence=False, vwap_aligned=False) == "A"
    assert ec.grade_setup(origin=["PWL"], divergence=False, vwap_aligned=True) == "A"
    assert ec.grade_setup(origin=["LOD"], divergence=False, vwap_aligned=True) == "B"
    assert ec.grade_setup(origin=[], divergence=True, vwap_aligned=True) == "B"
    assert ec.grade_setup(origin=[], divergence=False, vwap_aligned=False) == "B"


def test_row_reports_vwap_side():
    row, _ = _first_signal(_downtrend_then(W_SEGMENTS))
    assert row["vwap_aligned"] == (row["entry"] > row["daily_vwap"])
    srow, _ = _first_signal(_mirror(_downtrend_then(W_SEGMENTS)))
    assert srow["vwap_aligned"] == (srow["entry"] < srow["daily_vwap"])


def test_insufficient_data():
    row = ec.analyze_pair("EUR/USD", _bars([1.1] * 20), None)
    assert row["setup"] == "NO-TRADE"
    assert "Insufficient" in row["notes"]


def test_analyze_universe_counts():
    bars = _downtrend_then(W_SEGMENTS)
    _, n = _first_signal(bars)
    res = ec.analyze_universe({"EUR/USD": {"m15": bars[:n], "1d": []},
                               "GBP/USD": {"m15": bars[:20], "1d": []}})
    assert res["buys"] == 1 and res["sells"] == 0
    assert {p["symbol"] for p in res["pairs"]} == {"EUR/USD", "GBP/USD"}


# ── Discord alert path ───────────────────────────────────────────────────────

@pytest.fixture
def posted(monkeypatch):
    out = []
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda *a, **k: set())
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (out.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "_mark_sent", lambda pair, rule: None)
    monkeypatch.setattr(alerts, "EMA1350_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "EMA1350_ALERT_GRADES", ("A+", "A"))
    monkeypatch.setattr(alerts, "_ema1350_alerted_anchors", {})
    return out


def _alert_row(grade="A+", setup="BUY"):
    return {"symbol": "EUR/USD", "setup": setup, "grade": grade, "timeframe": "M15",
            "entry": 1.1000, "sl": 1.0950, "tp1": 1.1100,
            "pattern": {"type": "W", "extreme": 1.0955}, "divergence": True,
            "origin": ["LOD", "PDL"], "daily_vwap": 1.0980, "vwap_aligned": True, "notes": "test row"}


def test_alert_sends_a_plus_and_a(posted):
    alerts.alert_ema1350_setup("EUR/USD", _alert_row("A+"))
    alerts.alert_ema1350_setup("EUR/USD", _alert_row("A"))
    assert len(posted) == 2
    assert "13/50" in posted[0]["title"]
    fields = {f["name"]: f["value"] for f in posted[0]["fields"]}
    assert "A+" in fields["Grade"]
    assert "W" in fields["Pattern"]
    assert "LOD" in fields["Origin"] and "PDL" in fields["Origin"]
    assert "Yes" in fields["Divergence"]
    assert "1.098" in fields["Daily VWAP"] and "trade side" in fields["Daily VWAP"]


def test_alert_skips_grade_b(posted):
    alerts.alert_ema1350_setup("EUR/USD", _alert_row("B"))
    assert posted == []


def test_alert_skips_no_trade_and_disabled(posted, monkeypatch):
    alerts.alert_ema1350_setup("EUR/USD", {**_alert_row(), "setup": "NO-TRADE"})
    monkeypatch.setattr(alerts, "EMA1350_ALERTS_ENABLED", False)
    alerts.alert_ema1350_setup("EUR/USD", _alert_row())
    assert posted == []


def test_alert_blocked_by_news(posted, monkeypatch):
    monkeypatch.setattr(alerts, "EMA1350_NEWS_FILTER", True)
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda *a, **k: {"USD"})
    alerts.alert_ema1350_setup("EUR/USD", _alert_row())
    assert posted == []


def test_the_same_anchor_alerts_only_once(posted):
    """Review 2026-10-20: a whipsaw re-cross at 18:00 re-sent the 14:30 W as A+."""
    alerts._ema1350_alerted_anchors.clear()
    row = _alert_row()
    row["pattern"] = {"type": "W", "extreme": 1.09, "extreme_ts": 12345}
    alerts.alert_ema1350_setup("EUR/USD", row)
    alerts.alert_ema1350_setup("EUR/USD", {**row, "confirm_ts": 99999})
    assert len(posted) == 1


def test_alert_uses_its_own_throttle_rule(posted, monkeypatch):
    rules = []
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: rules.append(rule) or False)
    alerts.alert_ema1350_setup("EUR/USD", _alert_row(setup="SELL", grade="A") | {
        "entry": 1.1000, "sl": 1.1050, "tp1": 1.0900})
    assert rules == ["ema1350_sell"]


def test_quick_job_fetches_m15_then_runs_the_check(monkeypatch):
    """User request 2026-10-21: the 13/50 alert came ~5 min after the candle
    closed (refresh at :02 + 81s fetch + other scanners first). The quick job
    runs ~20s after each M15 close, fetches M15 only, then runs the check."""
    import scheduler
    order = []
    monkeypatch.setattr(scheduler, "_fetch_guarded",
                        lambda sym, iv, limit=None, **k: order.append(("fetch", sym, iv)))
    monkeypatch.setattr(scheduler, "_run_ema1350_alerts", lambda: order.append(("run",)))
    monkeypatch.setattr(scheduler, "FANOUT_DELAY_SECS", 0)
    monkeypatch.setattr(scheduler, "_mt5_ready", lambda: True)
    scheduler._quick_m15_ema1350()
    fetches = [o for o in order if o[0] == "fetch"]
    assert {o[1] for o in fetches} == set(ec.EMA1350_UNIVERSE)
    assert {o[2] for o in fetches} == {"15min"}
    assert order[-1] == ("run",)

    # MT5 down: no burst on the TwelveData fallback; refresh_all handles it
    order.clear()
    monkeypatch.setattr(scheduler, "_mt5_ready", lambda: False)
    scheduler._quick_m15_ema1350()
    assert order == []


def test_quick_job_is_scheduled_right_after_each_m15_close():
    import scheduler
    sched = scheduler.start()
    try:
        job = sched.get_job("ema1350_quick")
        assert job is not None
        fields = {f.name: str(f) for f in job.trigger.fields}
        assert fields["minute"] == "0,15,30,45" and fields["second"] == "20"
    finally:
        sched.shutdown(wait=False)


def test_scheduler_runs_the_check_on_m15_and_d1(monkeypatch):
    import cache
    import scheduler

    bars = _downtrend_then(W_SEGMENTS)
    _, n = _first_signal(bars)
    series = bars[:n]
    asked = set()

    def fake_read_candles(symbol, interval, limit=800):
        asked.add(interval)
        return series[-limit:] if interval == "15min" else []

    seen = {}
    monkeypatch.setattr(cache, "read_candles", fake_read_candles)
    monkeypatch.setattr(alerts, "alert_ema1350_setup", lambda sym, row: seen.__setitem__(sym, row))
    monkeypatch.setattr(scheduler.time, "time", lambda: float(series[-1]["ts_utc"] + 900 + 120))
    scheduler._run_ema1350_alerts()
    assert asked == {"15min", "1day"}
    assert seen and all(r["setup"] == "BUY" for r in seen.values())
