"""Tests for the VWAP Mean Reversion scanner.

Deviations from the source document (VWAP_Mean_Reversion_Strategy.md) are
documented in docs/superpowers/specs/2026-09-10-vwap-mean-reversion-strategy-design.md
and echoed as comments in vwap_mean_reversion_strategy.py at each point they apply.
"""
from __future__ import annotations

import pytest

import vwap_mean_reversion_strategy as m


def _bar(ts, hi, lo, cl, open_=None, vol=0):
    return {
        "ts_utc": ts, "open": open_ if open_ is not None else cl,
        "high": hi, "low": lo, "close": cl, "volume": vol,
    }


# ──────────────────────────────────────────────────────────────────────
# VWAP / sigma / z-score — hand-computed reconciliation (doc's own
# top-priority check: "compute session VWAP... reconcile it to the cent")
# ──────────────────────────────────────────────────────────────────────

def test_vwap_series_basic_reconciliation():
    candles = [
        _bar(0, 1.10000, 1.10000, 1.10000, vol=1),
        _bar(900, 1.10000, 1.10000, 1.10000, vol=1),
        _bar(1800, 1.10100, 1.10100, 1.10100, vol=1),
    ]
    vwap = m._vwap_series(candles)
    assert vwap[0] == pytest.approx(1.10000)
    assert vwap[1] == pytest.approx(1.10000)
    assert vwap[2] == pytest.approx(1.10000 + 0.00100 / 3)


def test_sigma_and_z_hand_computed():
    """Three equal-weighted bars [x, x, x+d] give sigma_2 = d*sqrt(2)/3 and
    z_2 = sqrt(2) exactly, independent of d. Derivation: vwap_2 = x + d/3;
    deviations are [-d/3, -d/3, 2d/3]; sum of squares = 6*(d/3)^2 = 2*d^2/3;
    variance = that / 3 = 2*d^2/9; sigma = d*sqrt(2)/3; z = (2d/3) / sigma
    = 2/sqrt(2) = sqrt(2). A clean, arithmetic-slip-proof case."""
    candles = [
        _bar(0, 1.10000, 1.10000, 1.10000, vol=1),
        _bar(900, 1.10000, 1.10000, 1.10000, vol=1),
        _bar(1800, 1.10100, 1.10100, 1.10100, vol=1),
    ]
    vwap = m._vwap_series(candles)
    sigma = m._sigma_series(candles, vwap)
    z = m._z_scores(candles, vwap, sigma)
    assert sigma[2] == pytest.approx(0.00100 * (2 ** 0.5) / 3)
    assert z[2] == pytest.approx(2 ** 0.5)


def test_vwap_and_sigma_reset_at_day_boundary():
    candles = [
        _bar(0, 1.10000, 1.10000, 1.10000, vol=1),        # day 1 (epoch 0 = 1970-01-01 00:00 UTC)
        _bar(86400, 1.20000, 1.20000, 1.20000, vol=1),     # day 2, 24h later
    ]
    vwap = m._vwap_series(candles)
    sigma = m._sigma_series(candles, vwap)
    assert vwap[1] == pytest.approx(1.20000)   # not blended with day 1
    assert sigma[1] == pytest.approx(0.0)      # single bar so far this day -> zero variance


def test_atr_series_stabilizes_to_constant_true_range():
    # Every bar: high-low = 0.0010, close constant -> true range = 0.0010 always.
    candles = [_bar(i * 900, 1.1005, 1.0995, 1.1000, vol=10) for i in range(20)]
    atr = m._atr_series(candles, period=14)
    assert atr[12] is None            # only 13 bars available -> not enough
    assert atr[13] == pytest.approx(0.0010)   # 14th bar -> first valid value
    assert atr[19] == pytest.approx(0.0010)


def test_d_score_uses_atr_normalisation():
    candles = [_bar(i * 900, 1.1005, 1.0995, 1.1000, vol=10) for i in range(19)]
    candles.append(_bar(19 * 900, 1.1050, 1.1000, 1.1020, vol=10))
    vwap = m._vwap_series(candles)
    atr = m._atr_series(candles, period=14)
    d = m._d_scores(candles, vwap, atr)
    assert d[12] is None       # ATR not yet available
    assert d[19] is not None
    assert d[19] == pytest.approx((candles[19]["close"] - vwap[19]) / atr[19])


# ──────────────────────────────────────────────────────────────────────
# Efficiency Ratio regime filter — hand-computed (doc's own advice:
# "unit-test ER against hand-computed values")
# ──────────────────────────────────────────────────────────────────────

def test_efficiency_ratio_trending_is_near_one():
    closes = [100.0 + i for i in range(21)]  # strictly increasing by 1 each bar
    er = m._efficiency_ratio(closes, 20, n=20)
    assert er == pytest.approx(1.0)


def test_efficiency_ratio_choppy_is_near_zero():
    closes = [100.0 if i % 2 == 0 else 101.0 for i in range(21)]  # alternating
    er = m._efficiency_ratio(closes, 20, n=20)
    assert er == pytest.approx(0.0, abs=1e-9)


def test_efficiency_ratio_none_before_lookback():
    closes = [100.0 + i for i in range(10)]
    assert m._efficiency_ratio(closes, 5, n=20) is None


# ──────────────────────────────────────────────────────────────────────
# Extension detection
# ──────────────────────────────────────────────────────────────────────

def test_find_extension_detects_short():
    candles = [_bar(i * 900, 1.1000, 1.0990, 1.0995, vol=10) for i in range(20)]
    z = [None] * 19 + [2.5]
    result = m._find_extension(candles, z)
    assert result == {"idx": 19, "direction": "short", "z": 2.5}


def test_find_extension_detects_long():
    candles = [_bar(i * 900, 1.1000, 1.0990, 1.0995, vol=10) for i in range(20)]
    z = [None] * 19 + [-2.2]
    result = m._find_extension(candles, z)
    assert result == {"idx": 19, "direction": "long", "z": -2.2}


def test_find_extension_none_when_no_bar_qualifies():
    candles = [_bar(i * 900, 1.1000, 1.0990, 1.0995, vol=10) for i in range(20)]
    z = [1.0] * 20
    assert m._find_extension(candles, z) is None


def test_find_extension_rejects_before_min_bars_into_session():
    """Only 10 bars total this session (< MIN_BARS_BEFORE_SIGNAL=15) --
    extension present but must be rejected."""
    candles = [_bar(i * 900, 1.1000, 1.0990, 1.0995, vol=10) for i in range(10)]
    z = [None] * 9 + [3.0]
    assert m._find_extension(candles, z) is None


def test_find_extension_ignores_stale_extension_outside_window():
    """Extension 9 bars before the latest bar -- outside the
    CONFIRMATION_TIMEOUT_BARS(6)+1 lookback window, must be ignored."""
    candles = [_bar(i * 900, 1.1000, 1.0990, 1.0995, vol=10) for i in range(30)]
    z = [None] * 30
    z[20] = 2.8
    assert m._find_extension(candles, z) is None


def test_bars_into_session_crosses_calendar_day_boundary():
    """Verify _bars_into_session correctly counts only bars on the same UTC
    calendar day, NOT cumulative array position. This test spans two calendar
    days to ensure the date-comparison branch is exercised and correct."""
    # Day 1: 5 bars starting at epoch 0 (1970-01-01 00:00 UTC)
    day1_bars = [_bar(i * 900, 1.1000, 1.0990, 1.0995, vol=10) for i in range(5)]
    # Day 2: 8 bars starting at ts_utc = 86400 + small offset (1970-01-02 00:15 UTC)
    day2_bars = [_bar(86400 + i * 900, 1.1000, 1.0990, 1.0995, vol=10) for i in range(8)]
    candles = day1_bars + day2_bars

    # Last bar is at index 12 (5 from day1 + 8 from day2)
    # _bars_into_session should return 8 (only the day2 count), not 13
    assert m._bars_into_session(candles, 12) == 8

    # Verify that a bar in day 1 returns only day 1 count
    assert m._bars_into_session(candles, 3) == 4  # bars 0-3 inclusive = 4 bars on day 1


# ──────────────────────────────────────────────────────────────────────
# Stall/rejection confirmation — the doc's "fiddliest logic" (§8 build
# order note), each of the 3 variants tested in isolation
# ──────────────────────────────────────────────────────────────────────

def test_confirmation_close_inside_band():
    extension = {"idx": 5, "direction": "short", "z": 2.5}
    candles = [_bar(i * 900, 1.1050, 1.1030, 1.1040, vol=100) for i in range(7)]
    z = [None] * 6 + [1.5]   # bar 6: |z|=1.5 < 2.0 -> back inside the band
    result = m._check_confirmation(candles, z, extension)
    assert result["type"] == "close_inside_band"
    assert result["idx"] == 6
    assert result["bars_since_extension"] == 1


def test_confirmation_rejection_wick():
    extension = {"idx": 5, "direction": "short", "z": 2.5}
    candles = [_bar(i * 900, 1.1050, 1.1030, 1.1040, vol=100) for i in range(6)]
    # bar 6: range 0.0045, wick (high - max(open,close)) = 0.0040 -> 88.9% of range
    candles.append(_bar(6 * 900, 1.1080, 1.1035, 1.1038, open_=1.1040, vol=80))
    z = [None] * len(candles)
    result = m._check_confirmation(candles, z, extension)
    assert result["type"] == "rejection_wick"
    assert result["idx"] == 6
    assert result["bars_since_extension"] == 1


def test_confirmation_two_bar_pattern():
    extension = {"idx": 5, "direction": "short", "z": 2.5}
    # ext bar (idx 5) high = 1.1060
    candles = [_bar(i * 900, 1.1060, 1.1030, 1.1040, vol=100) for i in range(6)]
    candles.append(_bar(6 * 900, 1.1050, 1.1020, 1.1040, open_=1.1035, vol=90))  # lower high #1
    candles.append(_bar(7 * 900, 1.1040, 1.1015, 1.1025, open_=1.1030, vol=90))  # lower high #2
    z = [None] * len(candles)
    result = m._check_confirmation(candles, z, extension)
    assert result["type"] == "two_bar_pattern"
    assert result["idx"] == 7
    assert result["bars_since_extension"] == 2


def test_confirmation_times_out():
    extension = {"idx": 5, "direction": "short", "z": 2.5}
    candles = [_bar(i * 900, 1.1060, 1.1030, 1.1040, vol=100) for i in range(6)]
    for i in range(6, 12):
        # close near the high (small wick) and high held flat (no lower-high streak)
        candles.append(_bar(i * 900, 1.1060, 1.1030, 1.1058, open_=1.1055, vol=100))
    z = [None] * len(candles)
    result = m._check_confirmation(candles, z, extension)
    assert result is None


def test_confirmation_rejection_wick_long_direction():
    """Long direction: wick on the LOW side. Bar extended down (z <= -2.0),
    now shows rejection of that downward move via a wick below open/close."""
    extension = {"idx": 5, "direction": "long", "z": -2.5}
    candles = [_bar(i * 900, 1.1050, 1.1030, 1.1040, vol=100) for i in range(6)]
    # bar 6: low wicks down; range 0.0040, wick (min(open,close) - low) = 0.0030 -> 75% of range
    candles.append(_bar(6 * 900, 1.1040, 1.1000, 1.1020, open_=1.1030, vol=80))
    z = [None] * len(candles)
    result = m._check_confirmation(candles, z, extension)
    assert result["type"] == "rejection_wick"
    assert result["idx"] == 6
    assert result["bars_since_extension"] == 1


def test_confirmation_two_bar_pattern_long_direction():
    """Long direction: two consecutive bars with HIGHER lows, mirroring the
    short direction's lower-high streak."""
    extension = {"idx": 5, "direction": "long", "z": -2.5}
    # ext bar (idx 5) low = 1.1020
    candles = [_bar(i * 900, 1.1060, 1.1020, 1.1040, vol=100) for i in range(6)]
    candles.append(_bar(6 * 900, 1.1050, 1.1030, 1.1040, open_=1.1035, vol=90))  # higher low #1
    candles.append(_bar(7 * 900, 1.1040, 1.1035, 1.1025, open_=1.1030, vol=90))  # higher low #2
    z = [None] * len(candles)
    result = m._check_confirmation(candles, z, extension)
    assert result["type"] == "two_bar_pattern"
    assert result["idx"] == 7
    assert result["bars_since_extension"] == 2


# ──────────────────────────────────────────────────────────────────────
# Exhaustion / fading volume, session tagging, scoring
# ──────────────────────────────────────────────────────────────────────

def test_exhaustion_volume_true_when_spiking():
    candles = [_bar(i * 900, 1.1005, 1.0995, 1.1000, vol=10) for i in range(20)]
    candles[19]["volume"] = 20   # > 1.5 x avg(10) = 15
    assert m._exhaustion_volume(candles, 19) is True


def test_exhaustion_volume_false_when_normal():
    candles = [_bar(i * 900, 1.1005, 1.0995, 1.1000, vol=10) for i in range(20)]
    assert m._exhaustion_volume(candles, 19) is False


def test_fading_volume():
    candles = [_bar(i * 900, 1.1, 1.09, 1.095, vol=10) for i in range(5)]
    candles[3]["volume"] = 20
    candles[4]["volume"] = 8
    assert m._fading_volume(candles, 3, 4) is True
    assert m._fading_volume(candles, 3, 3) is False


def test_session_status_buckets():
    assert m._session_status(14) == "ACTIVE"
    assert m._session_status(9) == "LONDON"
    assert m._session_status(18) == "NY-LATE"
    assert m._session_status(2) == "ASIAN"


def test_score_and_grade_max_is_ten_grade_a():
    score, grade = m._score_and_grade(True, True, "rejection_wick", "ACTIVE")
    assert score == 10
    assert grade == "A"


def test_score_and_grade_below_c_floor_is_no_trade():
    score, grade = m._score_and_grade(False, False, "unknown", "ASIAN")
    assert score == 3
    assert grade == "NO-TRADE"


def test_score_and_grade_at_c_floor_is_c():
    score, grade = m._score_and_grade(False, False, "close_inside_band", "ASIAN")
    assert score == 4   # 3 base + 0 exhaustion + 0 fading + 1 confirmation + 0 session
    assert grade == "C"


def test_score_and_grade_boundary_b():
    score, grade = m._score_and_grade(True, False, "close_inside_band", "LONDON")
    assert score == 7   # 3 base + 2 exhaustion + 0 fading + 1 confirmation + 1 session
    assert grade == "B"
