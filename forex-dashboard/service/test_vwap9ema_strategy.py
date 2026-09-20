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
    VWAP exactly on the signal bar (index 20, i.e. n-2 for n=22)."""
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
    assert abs(row["entry"] - 1.0991) < 1e-6
    assert abs(row["sl"] - 1.09767) < 1e-5
    assert abs(row["tp1"] - 1.10196) < 1e-5
    assert row["grade"] == "UNVALIDATED"
    assert "crossed above" in row["notes"].lower()


def test_sell_signal_on_cross_below():
    row = m.analyze_pair("EURUSDm", _sell_cross_series())
    assert row["setup"] == "SELL", row.get("notes")
    assert abs(row["entry"] - 1.1009) < 1e-6
    assert abs(row["sl"] - 1.10233) < 1e-5
    assert abs(row["tp1"] - 1.09804) < 1e-5
    assert row["grade"] == "UNVALIDATED"
    assert "crossed below" in row["notes"].lower()


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


def test_entry_gap_past_swing_extreme_is_no_trade():
    """Entry bar gaps below the swing low (BUY case), inverting the risk
    setup -- caught by the defensive guard and skipped as NO-TRADE."""
    candles = _buy_cross_series()
    candles[-1]["open"] = candles[-2]["low"] - 0.001  # force a gap well below the swing
    row = m.analyze_pair("EURUSDm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "wrong side" in row["notes"].lower()
