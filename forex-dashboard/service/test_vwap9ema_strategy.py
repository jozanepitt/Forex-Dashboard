"""Tests for the live VWAP+9EMA scanner (vp-climax variant, ema=9, rr=2.0).

This strategy failed its own honest backtest (0/48, see
docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md) —
it is built anyway per explicit user decision (see
docs/superpowers/specs/2026-09-14-vwap9ema-live-replaces-1am-crt-design.md),
scoped narrowly and labeled unvalidated everywhere. These tests check the
LIVE ADAPTATION logic (most-recent-bar signal detection, day/session
filtering, entry/stop/target wiring) — the underlying VWAP/EMA/volume-profile
math is already hand-verified in service/test_vwap9ema_volume_profile.py
and service/test_vwap9ema_backtest_math.py and is not re-derived here,
matching the convention every other live strategy's test file already
follows (e.g. test_vwap_mean_reversion_strategy.py's analyze_pair tests
check structural properties, not re-hand-computed VWAP arithmetic).

The BUY-signal fixture below was independently verified against the real
imported functions before being written into this file:
  entry=101.05, stop=100.775, target=101.6
"""
from __future__ import annotations
import datetime as dt

import vwap9ema_strategy as m

UTC = dt.timezone.utc


def _bar(ts_utc, o, h, l, c, v):
    return {"ts_utc": ts_utc, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _ts(day_hour_min):
    """day_hour_min: (day, hour, minute) on 2026-01-05 (a Monday), UTC."""
    day, hour, minute = day_hour_min
    return int(dt.datetime(2026, 1, day, hour, minute, tzinfo=UTC).timestamp())


def _buy_signal_series():
    """8 M5 bars, 08:00-08:35 UTC on 2026-01-05 (inside the 07:00-15:00 London
    window): a brief consolidation establishing a volume-profile value area,
    then a shallow dip that pierces the 9-EMA and rejects up, with the entry
    bar (last bar) landing back inside the value area and carrying a volume
    spike. Verified against the real compute_session_volume_profile/
    price_passes_vp_filter/ema functions before being encoded here."""
    opens  = [100.8, 100.9, 101.0, 101.1, 101.05, 100.95, 100.95, 101.05]
    highs  = [101.0, 101.1, 101.2, 101.2, 101.1, 101.0, 101.1, 101.2]
    lows   = [100.7, 100.8, 100.9, 100.95, 100.85, 100.8, 100.85, 100.95]
    closes = [100.9, 101.0, 101.1, 101.05, 100.95, 101.15, 101.05, 101.15]
    vols   = [500, 500, 500, 500, 500, 500, 500, 900]
    candles = []
    for k in range(8):
        minute = 5 * k
        candles.append(_bar(_ts((5, 8, minute)), opens[k], highs[k], lows[k], closes[k], vols[k]))
    return candles


def test_buy_signal_full_happy_path():
    row = m.analyze_pair("USTECm", _buy_signal_series())
    assert row["setup"] == "BUY", row.get("notes")
    assert abs(row["entry"] - 101.05) < 1e-9
    assert abs(row["sl"] - 100.775) < 1e-9
    assert abs(row["tp1"] - 101.6) < 1e-9
    assert row["grade"] == "UNVALIDATED"


def test_fails_volume_climax_when_no_spike():
    candles = _buy_signal_series()
    candles[-1]["volume"] = 500  # remove the spike (was 900)
    row = m.analyze_pair("USTECm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "climax" in row["notes"].lower()


def test_no_signal_on_flat_series():
    # No trend, no VWAP/EMA divergence -- up/dn both False by construction.
    candles = [_bar(_ts((5, 8, 5 * k)), 100.0, 100.1, 99.9, 100.0, 500) for k in range(8)]
    row = m.analyze_pair("AUDUSDm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "pullback" in row["notes"].lower() or "signal" in row["notes"].lower()


def test_outside_london_session_is_no_trade():
    candles = _buy_signal_series()
    for c in candles:
        c["ts_utc"] += int(dt.timedelta(hours=12).total_seconds())  # shift to ~20:00 UTC
    row = m.analyze_pair("USTECm", candles)
    assert row["setup"] == "NO-TRADE"
    assert row["in_active_session"] is False


def test_insufficient_data_is_no_trade():
    row = m.analyze_pair("USTECm", _buy_signal_series()[:3])
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
    assert m.VWAP9EMA_UNIVERSE == ["USTEC", "AUD/USD"]


def test_entry_gap_past_swing_extreme_is_no_trade():
    """Entry bar gaps below swing low (BUY case), inverting the risk setup.

    Constructed from the happy-path fixture by setting opens[7] to 100.75
    (below sw=100.8). Signal is still detected on bar 6, but entry on bar 7
    gaps down, creating stop >= entry (100.805 >= 100.75) -- caught by the
    defensive guard and skipped as NO-TRADE.

    Hand-verified:
      - sw = min(lows[6], lows[5]) = min(100.85, 100.8) = 100.8
      - entry = opens[7] = 100.75 (gap below sw)
      - stop = sw - 0.10*(entry-sw) = 100.8 - 0.10*(-0.05) = 100.805
      - Guard check: BUY needs stop < entry: 100.805 < 100.75? NO -> skip
    """
    candles = _buy_signal_series()
    candles[-1]["open"] = 100.75  # gap down entry (was 101.05)
    row = m.analyze_pair("USTECm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "wrong side" in row["notes"].lower()
