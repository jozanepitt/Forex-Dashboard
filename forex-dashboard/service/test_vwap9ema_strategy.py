"""Tests for the live VWAP+9EMA scanner — deliberately simplified per
explicit user request (2026-09-20) to just: the 9 EMA crosses the session
VWAP on the M5 chart, nothing else (no pullback-reject pattern, no
volume-profile filter, no volume-climax filter — all removed). The user
judges volume/context manually before trading.

This is a different rule than the earlier "vp-climax" variant tested by
the previous version of this file (which failed its own honest backtest,
0/48) — not a lighter/re-tuned version of it. It has zero backtesting.
Consequently the old parity test against backtest_mt5.run_day() is gone
too: live and backtest are now intentionally decoupled, since the backtest
tooling still implements the old vp-climax variant and this simplification
was explicitly scoped to the live dashboard only.

Session window is UTC 09:00-23:00 (Exness MT5 stamps bars in raw UTC,
offset=0s — see vwap9ema_strategy.py's module docstring), kept unchanged
from before. MIN_BARS=12 also kept unchanged.

Fixtures below are hand-constructed decline-then-rally (BUY) / rally-then-
decline (SELL) M5 series, engineered so the 9EMA/VWAP cross lands exactly
on the signal bar (index n-2), then verified by running the real
vwap9ema_strategy.analyze_pair() and hardcoding the observed output —
not independently re-derived by hand, since the formula itself (EMA(9) vs
session VWAP, sign change between consecutive bars) is simple enough that
mirroring the implementation in the fixture-construction script and then
asserting on its output is the accurate check here; the entry/stop/target
arithmetic (swing + buffer, RR multiple) is unchanged from before and was
already covered by this file's assertions.
"""
from __future__ import annotations
import datetime as dt

import pytest

import vwap9ema_strategy as m
from config import PRIORITY_PAIRS

UTC = dt.timezone.utc


def _bar(ts_utc, o, h, l, c, v):
    return {"ts_utc": ts_utc, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _ts(day_hour_min):
    """day_hour_min: (day, hour, minute) on 2026-01-05 (a Monday), UTC."""
    day, hour, minute = day_hour_min
    return int(dt.datetime(2026, 1, day, hour, minute, tzinfo=UTC).timestamp())


def _ts_offset(base_minute_offset):
    """Bar timestamp for the Nth M5 bar starting at 10:00 UTC on 2026-01-05,
    handling hour rollover past minute 59 (fixture spans >60 minutes)."""
    return int(dt.datetime(2026, 1, 5, 10, 0, tzinfo=UTC).timestamp()) + base_minute_offset * 60


def _buy_cross_series():
    """22 M5 bars, 10:00-11:45 UTC on 2026-01-05: a decline (9EMA settles
    below VWAP) followed by a sharp rally that crosses the 9EMA back above
    VWAP on bar index 20 -- the second-newest of the 22 closed bars."""
    candles = []
    price = 1.1000
    for i in range(15):
        price -= 0.0003
        candles.append(_bar(_ts_offset(5 * i), round(price + 0.0003, 6), round(price + 0.0004, 6),
                             round(price - 0.0001, 6), round(price, 6), 100))
    for i in range(15, 22):
        price += 0.0006
        candles.append(_bar(_ts_offset(5 * i), round(price - 0.0006, 6), round(price + 0.0002, 6),
                             round(price - 0.0007, 6), round(price, 6), 150))
    return candles


def _sell_cross_series():
    """Mirror of _buy_cross_series(): a rally followed by a sharp decline
    that crosses the 9EMA back below VWAP on the signal bar."""
    candles = []
    price = 1.1000
    for i in range(15):
        price += 0.0003
        candles.append(_bar(_ts_offset(5 * i), round(price - 0.0003, 6), round(price + 0.0001, 6),
                             round(price - 0.0004, 6), round(price, 6), 100))
    for i in range(15, 22):
        price -= 0.0006
        candles.append(_bar(_ts_offset(5 * i), round(price + 0.0006, 6), round(price + 0.0007, 6),
                             round(price - 0.0002, 6), round(price, 6), 150))
    return candles


def test_buy_signal_on_cross_above():
    row = m.analyze_pair("EURUSDm", _buy_cross_series())
    assert row["setup"] == "BUY", row.get("notes")
    candles = _buy_cross_series()
    assert row["entry"] == candles[-1]["close"]              # newest closed bar
    swing = min(c["low"] for c in candles[19:])               # bar before the cross .. newest
    stop = swing - m.STOP_BUFFER * (row["entry"] - swing)
    assert abs(row["sl"] - stop) < 1e-9
    assert abs(row["tp1"] - (row["entry"] + m.RR * (row["entry"] - stop))) < 1e-9
    assert row["cross_ts"] == candles[20]["ts_utc"]
    assert row["grade"] == "B"                    # no H1 data -> the trend check fails
    assert row["validated"] is False
    assert "crossed above" in row["notes"].lower()


def test_sell_signal_on_cross_below():
    row = m.analyze_pair("EURUSDm", _sell_cross_series())
    assert row["setup"] == "SELL", row.get("notes")
    candles = _sell_cross_series()
    assert row["entry"] == candles[-1]["close"]
    swing = max(c["high"] for c in candles[19:])
    stop = swing + m.STOP_BUFFER * (swing - row["entry"])
    assert abs(row["sl"] - stop) < 1e-9
    assert abs(row["tp1"] - (row["entry"] - m.RR * (stop - row["entry"]))) < 1e-9
    assert row["grade"] == "B"
    assert "crossed below" in row["notes"].lower()


# ── grades (user request 2026-10-19) ──────────────────────────────────────────

def _flat_crosses(n=40):
    """e9 and vwap series with no crosses: e9 above vwap throughout."""
    return [1.0] * n, [0.9] * n


def test_grade_a_needs_no_flip_tight_stop_and_the_h1_trend():
    e9, vwap = _flat_crosses()
    grade, misses = m.grade_signal(1, 35, e9, vwap, risk=0.001, rr=2.0, atr=0.001, trend="bullish")
    assert grade == "A" and misses == []


def test_opposite_cross_in_the_last_two_hours_is_grade_b():
    e9, vwap = _flat_crosses()
    e9[20], e9[21] = 0.8, 1.0                    # a bearish cross at 20 (e9 drops below vwap) ...
    grade, misses = m.grade_signal(1, 35, e9, vwap, risk=0.001, rr=2.0, atr=0.001, trend="bullish")
    assert grade == "B" and any("flip" in x for x in misses)
    # ... but one older than 24 bars no longer counts
    grade, _ = m.grade_signal(1, 50, e9 + [1.0] * 20, vwap + [0.9] * 20,
                              risk=0.001, rr=2.0, atr=0.001, trend="bullish")
    assert grade == "A"


def test_wide_stop_or_wrong_trend_is_grade_b():
    e9, vwap = _flat_crosses()
    assert m.grade_signal(1, 35, e9, vwap, risk=0.002, rr=2.0, atr=0.001, trend="bullish")[0] == "B"
    assert m.grade_signal(1, 35, e9, vwap, risk=0.001, rr=2.0, atr=0.001, trend="bearish")[0] == "B"
    assert m.grade_signal(1, 35, e9, vwap, risk=0.001, rr=2.0, atr=None, trend="bullish")[0] == "B"


def test_h1_trend_uses_the_200_ema():
    up = [{"close": 1.0 + 0.001 * k} for k in range(250)]
    assert m._h1_trend(up) == "bullish"
    assert m._h1_trend(list(reversed(up))) == "bearish"
    assert m._h1_trend(up[:150]) is None


def test_no_signal_on_flat_series():
    # No movement -- 9EMA and VWAP both sit flat, no cross ever occurs.
    candles = [_bar(_ts((5, 10, 5 * k)), 100.0, 100.1, 99.9, 100.0, 500) for k in range(12)]
    row = m.analyze_pair("AUDUSDm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "cross" in row["notes"].lower()


def test_outside_london_ny_session_is_no_trade():
    """+16h (10:00 -> 02:00 UTC next day) is outside the 09:00-23:00 window."""
    candles = _buy_cross_series()
    for c in candles:
        c["ts_utc"] += int(dt.timedelta(hours=16).total_seconds())
    row = m.analyze_pair("EURUSDm", candles)
    assert row["setup"] == "NO-TRADE"
    assert row["in_active_session"] is False


def test_insufficient_data_is_no_trade():
    row = m.analyze_pair("EURUSDm", _buy_cross_series()[:5])
    assert row["setup"] == "NO-TRADE"
    assert "insufficient" in row["notes"].lower()


def test_analyze_universe_shape():
    result = m.analyze_universe({
        "EURUSDm": {"m5": _buy_cross_series()},
        "AUDUSDm": {"m5": []},
    })
    assert result["buys"] == 1
    assert result["sells"] == 0
    assert len(result["pairs"]) == 2


def test_universe_constant():
    """Full PRIORITY_PAIRS universe (same pairs TDI123/BTMM123 scan,
    including BTC/USD and ETH/USD) per explicit user request. Comparing
    against a freshly-imported PRIORITY_PAIRS (not a hardcoded copy) checks
    the wiring stays live -- if config.py's list ever changes, this constant
    must track it, not silently diverge."""
    assert m.VWAP9EMA_UNIVERSE == list(PRIORITY_PAIRS)
    assert "EUR/USD" in m.VWAP9EMA_UNIVERSE
    assert "BTC/USD" in m.VWAP9EMA_UNIVERSE
    assert "ETH/USD" in m.VWAP9EMA_UNIVERSE


def test_m5_fetch_window_covers_full_forex_day():
    """Regression guard for the 2026-09-25 accuracy fix. The live VWAP is
    anchored at the 00:00 UTC day boundary (_forex_day_start_utc; was 22:00
    UTC until 2026-10-06) and must include that day's very first M5 bar, or the VWAP silently
    re-anchors mid-day and stops matching a real chart. A full forex
    trading day is 24h x 12 = 288 M5 bars, so the fetch window used by
    app.py and scheduler.py must always be >= 288 bars (plus slack for the
    currently-forming bar). app.py:156 and scheduler.py:112 previously
    fetched limit=100 (only 8h20m -> mis-anchored VWAP for ~16h/day)."""
    assert m.M5_FETCH_LIMIT >= 288            # one full UTC day (00:00->00:00)
    assert m.M5_FETCH_LIMIT <= 2 * 288        # sanity: no point fetching more than ~2 days
    assert m.M5_FETCH_LIMIT % 12 == 0         # whole hours of M5 bars


def test_9ema_uses_full_history_not_daily_reset():
    """Regression test for the day-anchored 9EMA bug (2026-09-25). The live
    9EMA was computed only over today's session bars, so it re-seeded at the
    day boundary each day and could contradict the M5 chart's true
    (full-history) 9EMA -- 'fake' EMA data, most obvious near the day open.
    It must now run over the ENTIRE fetched M5 history.

    Proof: prepend one full forex day (288 bars) at a level far ABOVE the
    fixture's range, ending just before the fixture's day boundary
    (2026-01-05 00:00 UTC). The current-day session bars are unchanged, so a
    session-only EMA would still emit the old BUY (_buy_cross_series is
    proven to BUY on its own -- see test_buy_signal_on_cross_above). A true
    full-history EMA carries that plateau into the signal area, stays above
    VWAP throughout and fires no cross. The fix is correct iff the outcome
    is NO-TRADE."""
    boundary = int(dt.datetime(2026, 1, 5, 0, 0, tzinfo=UTC).timestamp())
    pre = []
    ts = boundary - (288 * 300) - 300  # one full forex day, shifted back a bar so it ends strictly before the boundary
    for _ in range(288):
        ts += 300
        pre.append(_bar(ts, 1.5000, 1.5001, 1.4999, 1.5000, 100))
    assert pre[-1]["ts_utc"] < boundary  # history strictly ends before the day boundary
    candles = pre + _buy_cross_series()
    row = m.analyze_pair("EURUSDm", candles)
    assert row["setup"] == "NO-TRADE", row


# The tests below pin WHERE the VWAP day boundary is. Anchor = 00:00 UTC =
# MT5 server midnight on Exness (broker_offset_secs == 0, ts_utc is already
# true UTC), switched from 22:00 UTC (SAST midnight / TradingView's reset)
# on 2026-10-06 at the user's request. None of the fixtures above can tell
# the two apart: their bars all start at 10:00 UTC, after both boundaries.

def _utc(year, month, day, hour, minute=0):
    return int(dt.datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp())


def test_forex_day_start_is_midnight_utc():
    # 2026-10-21: the day starts at 00:00 SAST = 22:00 UTC (the user's TradingView VWAP)
    start = _utc(2026, 1, 4, 22, 0)
    for ts in (_utc(2026, 1, 4, 22, 0), _utc(2026, 1, 4, 23, 55), _utc(2026, 1, 5, 0, 0),
               _utc(2026, 1, 5, 12, 30), _utc(2026, 1, 5, 21, 55)):
        assert m._forex_day_start_utc(ts) == start, ts
    assert m._forex_day_start_utc(_utc(2026, 1, 5, 22, 0)) == _utc(2026, 1, 5, 22, 0)
    assert m._forex_day_start_utc(_utc(2026, 1, 4, 21, 55)) == _utc(2026, 1, 3, 22, 0)


def test_session_bars_start_at_midnight_not_2200():
    start = _utc(2026, 1, 4, 20, 0)
    candles = [_bar(start + 300 * k, 1.1, 1.1, 1.1, 1.1, 100) for k in range(16 * 12 + 1)]  # 20:00 -> 12:00 next day
    assert candles[-1]["ts_utc"] == _utc(2026, 1, 5, 12, 0)
    session = m._session_bars_for_today(candles)
    assert session[0]["ts_utc"] == _utc(2026, 1, 4, 22, 0)   # 00:00 SAST, not 00:00 UTC
    assert len(session) == 14 * 12 + 1


def test_reported_vwap_is_the_volume_weighted_hlc3_since_midnight_utc():
    """End-to-end: bars before 00:00 SAST (22:00 UTC) sit at a loud 2.0000;
    if the VWAP were anchored anywhere earlier it would swallow them and land
    nowhere near 1.1. (Anchor 00:00 SAST since 2026-10-21.)"""
    start = _utc(2026, 1, 4, 20, 0)
    midnight = _utc(2026, 1, 4, 22, 0)
    candles = []
    for k in range(16 * 12 + 1):                      # 2026-01-04 20:00 -> 2026-01-05 12:00
        ts = start + 300 * k
        p = 2.0000 if ts < midnight else round(1.1000 + 0.0001 * (k % 7), 6)
        candles.append(_bar(ts, p, p + 0.0002, p - 0.0002, p, 50 + k % 13))
    expected_num = expected_den = 0.0
    for c in candles:
        if c["ts_utc"] >= midnight:
            tp = (c["high"] + c["low"] + c["close"]) / 3.0
            expected_num += tp * c["volume"]
            expected_den += c["volume"]
    row = m.analyze_pair("EURUSDm", candles)
    assert row["vwap"] == pytest.approx(expected_num / expected_den, abs=1e-6), row


def test_both_vwap_scanners_share_the_same_anchor():
    """The M5 and M15 scanners keep separate copies of the day-boundary
    function. A drift between them would make the dashboard's two VWAP tabs
    disagree about when the day starts."""
    import vwap_mean_reversion_strategy as mr
    for ts in (_utc(2026, 1, 5, 0, 0), _utc(2026, 1, 5, 9, 30), _utc(2026, 1, 5, 21, 55),
               _utc(2026, 1, 5, 22, 0), _utc(2026, 1, 5, 23, 55), _utc(2026, 1, 6, 0, 0)):
        assert m._forex_day_start_utc(ts) == mr._forex_day_of(ts), ts


def test_degenerate_stop_is_no_trade():
    """No bar since the cross traded below the newest close, so the swing
    low sits on the entry price: zero risk, nothing to offer."""
    candles = _buy_cross_series()
    for c in candles[19:]:
        c["low"] = candles[-1]["close"]
    row = m.analyze_pair("EURUSDm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "wrong side" in row["notes"].lower() or "risk is zero" in row["notes"].lower()


# ── every closed bar since the last scan is checked (2026-10-13) ─────────────
# The candle cache only holds CLOSED bars, and M5 is refreshed once per
# 15-minute cycle. The scanner used to test a single bar (n-2, on the false
# assumption that the newest bar was still forming), so it saw one M5 bar in
# three and reported what it did see a bar late.

def _extend(candles, n, step):
    out = [dict(c) for c in candles]
    for _ in range(n):
        last = out[-1]
        p = round(last["close"] + step, 6)
        out.append(_bar(last["ts_utc"] + 300, last["close"], round(max(p, last["close"]) + 0.0002, 6),
                        round(min(p, last["close"]) - 0.0002, 6), p, 150))
    return out


def test_cross_on_the_newest_bar_is_reported():
    candles = _buy_cross_series()[:21]                 # the cross bar (index 20) is now the newest
    row = m.analyze_pair("EURUSDm", candles)
    assert row["setup"] == "BUY", row.get("notes")
    assert row["cross_ts"] == candles[-1]["ts_utc"]
    assert row["entry"] == candles[-1]["close"]


def test_cross_two_bars_back_is_still_reported():
    candles = _extend(_buy_cross_series(), 1, +0.0003)  # cross is now at n-3
    row = m.analyze_pair("EURUSDm", candles)
    assert row["setup"] == "BUY", row.get("notes")
    assert row["cross_ts"] == candles[20]["ts_utc"]
    assert row["entry"] == candles[-1]["close"]


def test_cross_older_than_the_scan_window_is_not_reported():
    candles = _extend(_buy_cross_series(), 2, +0.0003)  # cross is now at n-4
    row = m.analyze_pair("EURUSDm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "cross" in row["notes"].lower()


def test_lookback_covers_one_refresh_cycle():
    assert m.CROSS_LOOKBACK_BARS * 5 == 15
