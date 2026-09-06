"""Tests for the BTMM 123 strategy — classic price-structure 1-2-3 reversal,
confirmed by BTMM doctrine (EMA Level cascade, stop hunt, Asian range) instead
of a TDI/oscillator dependency. Replaces the removed Malaysian SNR Emperor
slot in the dashboard.

Geometry (swings, 123 pattern, session labels, ATR) is reused directly from
tdi_cycle_123.py by import — tdi_cycle_123.py itself is untouched. This file
tests only what's new here: the EMA-Level score, stop-hunt score, Asian-range
score, grade thresholds, and the end-to-end pipeline/alert gate.
"""
from __future__ import annotations

import btmm_123 as m
from alerts import _should_alert_btmm123


# ──────────────────────────────────────────────────────────────────────
# _level_score — EMA Level I/II confirmation, direction-matched
# ──────────────────────────────────────────────────────────────────────

def test_level_score_ii_matching_direction_scores_full():
    level = {"count": 5, "level_ii": True, "level_i": False, "direction": "bullish"}
    assert m._level_score(level, "bullish") == 4


def test_level_score_i_matching_direction_scores_partial():
    level = {"count": 2, "level_ii": False, "level_i": True, "direction": "bullish"}
    assert m._level_score(level, "bullish") == 2


def test_level_score_mismatched_direction_scores_zero():
    """A Level II bullish cascade must not score for a bearish 123 pattern —
    the EMA stack is confirming the OPPOSITE direction, not this setup."""
    level = {"count": 5, "level_ii": True, "level_i": False, "direction": "bullish"}
    assert m._level_score(level, "bearish") == 0


# ──────────────────────────────────────────────────────────────────────
# _stop_hunt_score — the actual BTMM confirmation, replacing TDI divergence
# ──────────────────────────────────────────────────────────────────────

def test_stop_hunt_matching_direction_scores():
    hunt = {"active": True, "direction": "bullish"}
    assert m._stop_hunt_score(hunt, "bullish") == 4


def test_stop_hunt_wrong_direction_scores_zero():
    hunt = {"active": True, "direction": "bearish"}
    assert m._stop_hunt_score(hunt, "bullish") == 0


def test_stop_hunt_inactive_scores_zero():
    assert m._stop_hunt_score({"active": False, "direction": None}, "bullish") == 0


# ──────────────────────────────────────────────────────────────────────
# _asian_score — tight Asian box (<50 pips per detect_asian_range)
# ──────────────────────────────────────────────────────────────────────

def test_asian_score_valid_range_scores():
    assert m._asian_score({"valid": True, "range_pips": 22}) == 2


def test_asian_score_invalid_range_scores_zero():
    assert m._asian_score({"valid": False, "range_pips": 80}) == 0


def test_asian_score_missing_data_scores_zero():
    assert m._asian_score({"valid": False}) == 0


# ──────────────────────────────────────────────────────────────────────
# _grade_from_score — same 15-pt scale and thresholds as TDI123
# ──────────────────────────────────────────────────────────────────────

def test_grade_thresholds():
    assert m._grade_from_score(11) == "A"
    assert m._grade_from_score(8) == "B"
    assert m._grade_from_score(5) == "C"
    assert m._grade_from_score(4) == "NO-TRADE"
    assert m._grade_from_score(0) == "NO-TRADE"


# ──────────────────────────────────────────────────────────────────────
# analyze_pair — end-to-end pipeline
# ──────────────────────────────────────────────────────────────────────

def _bar(ts, hi, lo, cl):
    return {"ts_utc": ts, "open": cl, "high": hi, "low": lo, "close": cl}


def test_analyze_pair_insufficient_data_is_no_trade():
    bars = [_bar(i * 3600, 1.1001, 1.0999, 1.1000) for i in range(10)]
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"
    assert row["grade"] == "NO-DATA"
    assert row["score"] == 0


def test_analyze_pair_flat_series_has_no_pattern():
    """100+ bars but no real swings -> NO-TRADE, not a crash."""
    bars = [_bar(i * 3600, 1.1000 + 0.00001 * (i % 2), 1.0999, 1.10005) for i in range(150)]
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"
    assert row["score"] == 0


def _build_bullish_123_series():
    """Hand-built H1 series with a clean bullish 1-2-3: P1 low ~1.1900,
    P2 high ~1.2050 (150 pip leg), P3 a modest overshoot low ~1.1880, then a
    strong rally that both (a) flips the EMA stack bullish (Level II) and
    (b) ends on a stop-hunt-and-reclaim bar (pierces the recent 20-bar low,
    closes back above it)."""
    bars = []
    ts = 0
    step = 3600

    # Seed: 30 flat bars around 1.2000 (too small to register as a 123 leg).
    for i in range(30):
        p = 1.2000 + (0.0001 if i % 2 == 0 else -0.0001)
        bars.append(_bar(ts, p + 0.0001, p - 0.0001, p)); ts += step

    # Leg 1->2: descend to P1 (swing low ~1.1900), then rally to P2 (~1.2050).
    for i in range(10):
        p = 1.2000 - 0.0010 * i  # -> 1.1910
        bars.append(_bar(ts, p + 0.0003, p - 0.0003, p)); ts += step
    p1_price = bars[-1]["low"]
    for i in range(10):
        p = 1.1910 + 0.0014 * i  # -> 1.2050
        bars.append(_bar(ts, p + 0.0003, p - 0.0003, p)); ts += step

    # Leg 2->3: decline to P3, a modest overshoot beyond P1 (~1.1880).
    for i in range(10):
        p = 1.2050 - 0.0017 * i  # -> 1.1880
        bars.append(_bar(ts, p + 0.0003, p - 0.0003, p)); ts += step

    # Two bars of higher lows to confirm P3 as a fractal swing (right=2).
    for i in range(2):
        p = 1.1880 + 0.0005 * (i + 1)
        bars.append(_bar(ts, p + 0.0003, p - 0.0003, p)); ts += step

    # Strong rally to build a bullish EMA cascade (Level II) over many bars.
    for i in range(40):
        p = 1.1890 + 0.0025 * i  # -> ~1.2890
        bars.append(_bar(ts, p + 0.0005, p - 0.0005, p)); ts += step

    # Final bar: dip below the recent 20-bar low, then close back above it —
    # a stop hunt on the way up, independent of P3's own swing.
    recent_low = min(b["low"] for b in bars[-20:])
    hunt_low = recent_low - 0.0010
    last_close = bars[-1]["close"] + 0.0020
    bars.append(_bar(ts, last_close, hunt_low, last_close))

    return bars


def test_analyze_pair_detects_bullish_123_and_confirms():
    bars = _build_bullish_123_series()
    row = m.analyze_pair("EUR/USD", bars)

    assert row["direction"] == "bullish"
    assert row["setup"] == "BUY"
    assert row["grade"] in ("A", "B", "C")
    assert row["score"] > 0
    assert row["stop_hunt"]["active"] is True
    assert row["stop_hunt"]["direction"] == "bullish"

    plan = row["trade_plan"]
    assert plan["entry"] == row["current_price"]
    assert plan["sl"] < plan["entry"]           # stop below entry for a BUY


def test_analyze_universe_shape_matches_dashboard_convention():
    bars = _build_bullish_123_series()
    result = m.analyze_universe({"EUR/USD": {"1h": bars}})
    assert "pairs" in result and "buys" in result and "sells" in result
    row = next(p for p in result["pairs"] if p["symbol"] == "EUR/USD")
    assert row["setup"] == "BUY"


# ──────────────────────────────────────────────────────────────────────
# _should_alert_btmm123 — grade + session gate (mirrors _should_alert_tdi123)
# ──────────────────────────────────────────────────────────────────────

def test_should_alert_grade_a_always():
    assert _should_alert_btmm123({"grade": "A", "in_active_session": True}) is True


def test_should_alert_grade_c_never():
    assert _should_alert_btmm123({"grade": "C", "in_active_session": True}) is False


def test_should_alert_blocks_outside_session():
    assert _should_alert_btmm123({"grade": "A", "in_active_session": False}) is False


def test_should_alert_none_session_does_not_hard_fail():
    """Missing session data must not silently kill an otherwise-valid alert."""
    assert _should_alert_btmm123({"grade": "A", "in_active_session": None}) is True
