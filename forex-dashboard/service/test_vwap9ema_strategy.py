"""Tests for the live VWAP+9EMA scanner (vp-climax variant, ema=9, rr=2.0).

This strategy failed its own honest backtest (0/48, see
docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md) —
it is built anyway per explicit user decision (see
docs/superpowers/specs/2026-09-14-vwap9ema-live-replaces-1am-crt-design.md),
labeled unvalidated everywhere, and (per a later explicit user request)
scanning the same full pair universe as TDI123/BTMM123. These tests check
the LIVE ADAPTATION logic (most-recent-bar signal detection, day/session
filtering, entry/stop/target wiring) — the underlying VWAP/EMA/volume-profile
math is already hand-verified in service/test_vwap9ema_volume_profile.py
and service/test_vwap9ema_backtest_math.py and is not re-derived here,
matching the convention every other live strategy's test file already
follows.

Session window is UTC 09:00-17:00 (Exness MT5 stamps bars in raw UTC,
offset=0s, confirmed live -- see vwap9ema_strategy.py's module docstring).
MIN_BARS=12 matches backtest_mt5.run_day()'s own floor (n<12 -> no trades).

The 12-bar BUY-signal fixture below is a 4-bar flat consolidation prefix
(pads bar count past MIN_BARS without altering the signal) followed by the
original 8-bar pattern from the first version of this test. It was
independently re-derived from scratch (VWAP, EMA(9), entry, stop, target,
volume-profile filter) via a standalone script that reimplements the same
formulas without importing vwap9ema_strategy, cross-checked against the
real compute_session_volume_profile/price_passes_vp_filter functions:
  entry=101.05, stop=100.775, target=101.6 (same numeric values as before,
  because the 4-bar flat prefix happens to average out to the same typical
  price as the original series' first bar -- confirmed by recomputing VWAP
  and EMA(9) across all 12 bars, not assumed from the old 8-bar fixture).
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


def _buy_signal_series():
    """12 M5 bars, 10:00-10:55 UTC on 2026-01-05 (inside the 09:00-17:00
    London window): 4 flat consolidation bars (typ=100.8 each, padding bar
    count past MIN_BARS=12 without affecting the signal window), then the
    original 8-bar pattern -- a shallow dip that pierces the 9-EMA and
    rejects up, with the entry bar (last bar) landing back inside the
    value area and carrying a volume spike. Re-verified in full (VWAP,
    EMA, entry/stop/target, vp-filter) against the real imported functions
    via a standalone script before being encoded here -- see module
    docstring."""
    opens  = [100.80,100.80,100.80,100.80, 100.8, 100.9, 101.0, 101.1, 101.05, 100.95, 100.95, 101.05]
    highs  = [100.85,100.85,100.85,100.85, 101.0, 101.1, 101.2, 101.2, 101.1, 101.0, 101.1, 101.2]
    lows   = [100.75,100.75,100.75,100.75, 100.7, 100.8, 100.9, 100.95, 100.85, 100.8, 100.85, 100.95]
    closes = [100.80,100.80,100.80,100.80, 100.9, 101.0, 101.1, 101.05, 100.95, 101.15, 101.05, 101.15]
    vols   = [400,400,400,400, 500,500,500,500,500,500,500,900]
    candles = []
    for k in range(12):
        minute = 5 * k
        candles.append(_bar(_ts((5, 10, minute)), opens[k], highs[k], lows[k], closes[k], vols[k]))
    return candles


def test_buy_signal_full_happy_path():
    row = m.analyze_pair("USTECm", _buy_signal_series())
    assert row["setup"] == "BUY", row.get("notes")
    assert abs(row["entry"] - 101.05) < 1e-9
    assert abs(row["sl"] - 100.775) < 1e-9
    assert abs(row["tp1"] - 101.6) < 1e-9
    assert row["grade"] == "UNVALIDATED"


def test_fails_volume_climax_when_no_spike():
    """Boost the pre-entry window's volume so the entry bar's own 900
    volume no longer clears the 1.3x-average climax threshold. Boosting the
    ENTRY bar's own volume down instead (as the pre-12-bar-fixture version
    of this test did) also shifts the volume profile's value area and fails
    the earlier vp-filter check instead of climax -- confirmed via a
    standalone script, hence boosting the window bars here rather than the
    entry bar. Also confirmed the signal itself still fires with these
    boosted volumes (VWAP shifts but not past the divergence needed).
    Hand-verified: avg_vol(idx 0..10) = (400*4 + 900*7)/11 = 718.18...,
    1.3*718.18 = 933.6 > vols[11]=900 -> climax check fails."""
    candles = _buy_signal_series()
    for c in candles[4:11]:
        c["volume"] = 900
    row = m.analyze_pair("USTECm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "climax" in row["notes"].lower()


def test_no_signal_on_flat_series():
    # No trend, no VWAP/EMA divergence -- up/dn both False by construction.
    candles = [_bar(_ts((5, 10, 5 * k)), 100.0, 100.1, 99.9, 100.0, 500) for k in range(12)]
    row = m.analyze_pair("AUDUSDm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "pullback" in row["notes"].lower() or "signal" in row["notes"].lower()


def test_outside_london_session_is_no_trade():
    candles = _buy_signal_series()
    for c in candles:
        c["ts_utc"] += int(dt.timedelta(hours=12).total_seconds())  # 10:00-10:55 -> 22:00-22:55 UTC
    row = m.analyze_pair("USTECm", candles)
    assert row["setup"] == "NO-TRADE"
    assert row["in_active_session"] is False


def test_insufficient_data_is_no_trade():
    row = m.analyze_pair("USTECm", _buy_signal_series()[:5])
    assert row["setup"] == "NO-TRADE"
    assert "insufficient" in row["notes"].lower()


def test_analyze_universe_shape():
    result = m.analyze_universe({
        "USTECm": {"m5": _buy_signal_series()},
        "AUDUSDm": {"m5": []},
    })
    assert result["buys"] == 1
    assert result["sells"] == 0
    assert len(result["pairs"]) == 2


def test_universe_constant():
    """Full PRIORITY_PAIRS universe (same pairs TDI123/BTMM123 scan) per
    explicit user request, not just the 2 backtested symbols. Comparing
    against a freshly-imported PRIORITY_PAIRS (not a hardcoded copy) checks
    the wiring stays live -- if config.py's list ever changes, this constant
    must track it, not silently diverge."""
    assert m.VWAP9EMA_UNIVERSE == list(PRIORITY_PAIRS)
    assert "USTEC" in m.VWAP9EMA_UNIVERSE
    assert "AUD/USD" in m.VWAP9EMA_UNIVERSE


def test_entry_gap_past_swing_extreme_is_no_trade():
    """Entry bar gaps below swing low (BUY case), inverting the risk setup.

    Constructed from the happy-path fixture by setting the entry bar's open
    to 100.75 (below sw=100.8). Signal is still detected on the prior bar,
    but entry gaps down, creating stop >= entry (100.805 >= 100.75) --
    caught by the defensive guard and skipped as NO-TRADE.

    Hand-verified (re-derived for the 12-bar fixture, same swing bars as
    before since they're unaffected by the 4-bar flat prefix):
      - sw = min(lows[10], lows[9]) = min(100.85, 100.8) = 100.8
      - entry = opens[11] = 100.75 (gap below sw)
      - stop = sw - 0.10*(entry-sw) = 100.8 - 0.10*(-0.05) = 100.805
      - Guard check: BUY needs stop < entry: 100.805 < 100.75? NO -> skip
    """
    candles = _buy_signal_series()
    candles[-1]["open"] = 100.75  # gap down entry (was 101.05)
    row = m.analyze_pair("USTECm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "wrong side" in row["notes"].lower()


def test_run_day_parity_zero_spread():
    """Spec requirement (docs/superpowers/specs/2026-09-14-vwap9ema-live-
    replaces-1am-crt-design.md line 65): a live candle sequence must produce
    the same entry/direction that backtest_mt5.run_day() would produce for
    the identical OHLCV series, allowing for the documented half-spread
    difference (run_day pays half the entry bar's spread; live doesn't --
    see Fix #5's disclosure). With spread_points=0 for every bar that
    difference vanishes, so entry/direction must match EXACTLY -- this is
    the actual imported run_day(), not a reimplementation, so it also
    exercises the exact production backtest code path against the live one."""
    import pandas as pd
    import backtest_mt5 as bt

    candles = _buy_signal_series()
    df = pd.DataFrame({
        "time": pd.to_datetime([c["ts_utc"] for c in candles], unit="s", utc=True),
        "open": [c["open"] for c in candles],
        "high": [c["high"] for c in candles],
        "low": [c["low"] for c in candles],
        "close": [c["close"] for c in candles],
        "tick_volume": [c["volume"] for c in candles],
        "spread_points": [0.0] * len(candles),
        "point": [1.0] * len(candles),
    })

    trades = bt.run_day(df, RR=2.0, buf=0.10, emaLen=9, variant="vp-climax")
    assert len(trades) == 1, f"expected exactly 1 trade, got {trades}"
    bt_trade = trades[0]

    live_row = m.analyze_pair("USTECm", candles)
    assert live_row["setup"] == "BUY"

    assert bt_trade["dir"] == 1, "run_day disagrees on direction (expected BUY/+1)"
    assert abs(bt_trade["entry"] - live_row["entry"]) < 1e-9, (
        f"entry mismatch: run_day={bt_trade['entry']} live={live_row['entry']}"
    )

    # run_day doesn't expose stop/target directly; reconstruct risk from the
    # returned r-multiple (r = dir*(exit-entry)/risk) and cross-check against
    # live's sl/tp1, which were already independently hand-verified above.
    exitpx = bt_trade["exit"]
    r = bt_trade["r"]
    assert r != 0, "degenerate r-multiple, can't cross-check risk"
    implied_risk = abs((exitpx - bt_trade["entry"]) / r)
    live_risk = abs(live_row["entry"] - live_row["sl"])
    assert abs(implied_risk - live_risk) < 1e-6, (
        f"risk mismatch: run_day-implied={implied_risk} live={live_risk}"
    )
