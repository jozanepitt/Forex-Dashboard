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


# ──────────────────────────────────────────────────────────────────────
# Stop and target calculation
# ──────────────────────────────────────────────────────────────────────

def test_calc_stop_and_targets_sell():
    candles = [_bar(i * 900, 1.1005, 1.0995, 1.1000, vol=10) for i in range(19)]
    candles.append(_bar(19 * 900, 1.1050, 1.1010, 1.1020, vol=10))   # extension, high=1.1050
    candles.append(_bar(20 * 900, 1.1030, 1.1015, 1.1018, vol=8))    # confirmation
    extension = {"idx": 19, "direction": "short", "z": 2.5}
    confirmation = {"idx": 20, "type": "rejection_wick", "bars_since_extension": 1}
    vwap = m._vwap_series(candles)
    sigma = m._sigma_series(candles, vwap)
    plan = m._calc_stop_and_targets(candles, extension, confirmation, vwap, sigma, "EUR/USD")
    assert plan["setup"] == "SELL"
    assert plan["entry"] == candles[-1]["close"]
    assert plan["sl"] == pytest.approx(1.1051, abs=1e-6)  # exact: structural distance dominates
    assert plan["sl"] > plan["entry"]           # stop above entry for a short
    assert plan["tp1"] == pytest.approx(vwap[-1])
    assert plan["tp2"] < plan["tp1"]            # overshoot below VWAP for a short
    assert plan["time_stop_bar_idx"] == 20 + m.TIME_STOP_BARS


def test_calc_stop_and_targets_buy():
    candles = [_bar(i * 900, 1.1005, 1.0995, 1.1000, vol=10) for i in range(19)]
    candles.append(_bar(19 * 900, 1.0990, 1.0950, 1.0970, vol=10))   # extension, low=1.0950
    candles.append(_bar(20 * 900, 1.0985, 1.0965, 1.0980, vol=8))    # confirmation
    extension = {"idx": 19, "direction": "long", "z": -2.5}
    confirmation = {"idx": 20, "type": "close_inside_band", "bars_since_extension": 1}
    vwap = m._vwap_series(candles)
    sigma = m._sigma_series(candles, vwap)
    plan = m._calc_stop_and_targets(candles, extension, confirmation, vwap, sigma, "EUR/USD")
    assert plan["setup"] == "BUY"
    assert plan["sl"] == pytest.approx(1.0949, abs=1e-6)  # exact: structural distance dominates
    assert plan["sl"] < plan["entry"]           # stop below entry for a long
    assert plan["tp1"] == pytest.approx(vwap[-1])
    assert plan["tp2"] > plan["tp1"]            # overshoot above VWAP for a long


# ──────────────────────────────────────────────────────────────────────
# analyze_pair / analyze_universe — end-to-end pipeline
# ──────────────────────────────────────────────────────────────────────

def test_analyze_pair_insufficient_data_is_no_trade():
    bars = [_bar(i * 900, 1.1001, 1.0999, 1.1000, vol=10) for i in range(10)]
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"
    assert row["grade"] == "NO-DATA"
    assert row["score"] == 0


def _build_choppy_baseline(n=28, start_ts=0, start_price=1.10000):
    """n bars of tiny back-and-forth oscillation -- keeps ER low (choppy),
    all on the same UTC day starting at ts=0 (1970-01-01 00:00 UTC)."""
    candles = []
    ts = start_ts
    price = start_price
    for i in range(n):
        delta = 0.00005 if i % 2 == 0 else -0.00005
        o = price
        c = price + delta
        hi = max(o, c) + 0.00002
        lo = min(o, c) - 0.00002
        candles.append(_bar(ts, hi, lo, c, open_=o, vol=10))
        price = c
        ts += 900
    return candles, ts, price


def _build_short_setup_series():
    """Choppy baseline, then a high-volume spike (extension) and a
    rejection-wick confirmation bar the next bar -- should produce a
    confirmed SELL. If this doesn't trip both the regime gate and the
    z>=2.0 threshold, print row['er']/row['z']/row['notes'] and widen the
    spike (currently +0.0035) or shrink the baseline oscillation. Baseline
    is 40 bars (not the 28-bar default) so the 42-bar total clears
    MIN_CANDLES_REQUIRED (40) -- with the 28-bar default, analyze_pair
    short-circuits on "insufficient candle history" before the regime/z
    logic is even reached."""
    candles, ts, price = _build_choppy_baseline(n=40)
    ext_open = price
    ext_close = price + 0.0035
    candles.append(_bar(ts, ext_close + 0.0002, ext_open - 0.0002, ext_close,
                        open_=ext_open, vol=40))
    ts += 900
    conf_high = ext_close + 0.0010
    # Pullback tuned to 0.0022 (from the brief's initial 0.0015): a 0.0015
    # pullback still leaves the confirmation bar's own |z| >= 2.0, so
    # _find_extension (which scans newest-to-oldest and returns the FIRST
    # bar over threshold) latches onto the confirmation bar itself as "the
    # extension", leaving no bars left to search for confirmation -> the
    # setup times out. See task-7-report.md for the full derivation.
    conf_close = ext_close - 0.0022
    candles.append(_bar(ts, conf_high, conf_close - 0.0002, conf_close,
                        open_=ext_close, vol=15))
    return candles


def _build_long_setup_series():
    """Mirror of _build_short_setup_series for a BUY."""
    candles, ts, price = _build_choppy_baseline(n=40)
    ext_open = price
    ext_close = price - 0.0035
    candles.append(_bar(ts, ext_open + 0.0002, ext_close - 0.0002, ext_close,
                        open_=ext_open, vol=40))
    ts += 900
    conf_low = ext_close - 0.0010
    conf_close = ext_close + 0.0022
    candles.append(_bar(ts, conf_close + 0.0002, conf_low, conf_close,
                        open_=ext_close, vol=15))
    return candles


def test_analyze_pair_produces_confirmed_sell():
    candles = _build_short_setup_series()
    row = m.analyze_pair("EUR/USD", candles)
    assert row["setup"] == "SELL", (
        f"expected SELL, got {row['setup']} -- er={row.get('er')} z={row.get('z')} "
        f"notes={row.get('notes')!r}. Tune the extension size in "
        f"_build_short_setup_series if this fails."
    )
    assert row["grade"] in ("A", "B", "C")
    assert row["entry"] is not None and row["sl"] is not None and row["tp1"] is not None
    assert row["sl"] > row["entry"]


def test_analyze_pair_produces_confirmed_buy():
    candles = _build_long_setup_series()
    row = m.analyze_pair("EUR/USD", candles)
    assert row["setup"] == "BUY", (
        f"expected BUY, got {row['setup']} -- er={row.get('er')} z={row.get('z')} "
        f"notes={row.get('notes')!r}. Tune the extension size in "
        f"_build_long_setup_series if this fails."
    )
    assert row["sl"] < row["entry"]


def test_analyze_pair_no_trade_when_confirmation_times_out():
    """Extension present but price just keeps extending (trend, not stall)
    -- confirmation never fires within 6 bars -> NO-TRADE via the timeout
    branch specifically (not the insufficient-data or regime-fail gates).

    Fix note (review round): the original version called
    _build_choppy_baseline() with no `n` (defaulting to 28), so 28 + 10 =
    38 total candles -- below MIN_CANDLES_REQUIRED=40, so analyze_pair
    short-circuited on "Insufficient candle history" and the assertion
    passed for the wrong reason (gate 1, not gate 5). It also used a
    10-bar / 0.0006-per-bar trend, which fails the regime gate (ER blows
    out on any nontrivial single-direction lookback -- see
    test_analyze_pair_no_trade_when_regime_fails), so even with enough
    candles it would have hit gate 3, not gate 5.

    This version uses _build_choppy_baseline(n=40) (>= MIN_CANDLES_REQUIRED
    once the 6 trend bars are appended) and a much smaller per-bar delta
    (0.00003 vs 0.0006) so the trailing-20-bar ER stays under the 0.35
    regime threshold while the cumulative move still clears the z>=2.0
    extension threshold every bar (this baseline's session-cumulative sigma
    is tiny, so even a few 0.00003 steps register as a large z).

    Structural note on what this test can and cannot exercise: because
    _find_extension always returns the MOST RECENT bar (scanning backward)
    that crosses the z threshold, any bar after the "true" extension that
    also stays >= 2.0 (i.e. avoids the close-inside-band confirmation type)
    is, by construction, itself a valid extension candidate and gets picked
    as THE extension instead -- so a still-extending trend like this one
    always resolves to "extension found at the newest bar, zero bars left
    to search" (_check_confirmation's loop range is empty, returns None).
    That is still gate 5 (the timeout branch) firing for real -- confirmed
    below via regime_ok=True, extension is not None, confirmation is None,
    and the "timed out" wording in notes -- it just cannot, under this
    production logic, also exercise a multi-iteration loop body in the same
    assertion; that would require crossing a UTC day boundary (resetting
    VWAP/sigma) to disqualify newer bars from extension-selection while
    still feeding them through confirmation checks, which is out of scope
    for a test-data-only fix."""
    candles, ts, price = _build_choppy_baseline(n=40)
    for i in range(6):
        price += 0.00003   # keeps extending every bar, never stalls
        candles.append(_bar(ts, price + 0.0002, price - 0.0004, price, vol=15))
        ts += 900
    row = m.analyze_pair("EUR/USD", candles)
    assert row["setup"] == "NO-TRADE"
    assert row["regime_ok"] is True, (
        f"expected regime gate to pass so the timeout branch is what's "
        f"actually under test -- er={row.get('er')} notes={row.get('notes')!r}"
    )
    assert row["extension"] is not None, (
        f"expected an extension to be detected -- z={row.get('z')} "
        f"notes={row.get('notes')!r}"
    )
    assert row["confirmation"] is None
    assert "timed out" in row["notes"], row["notes"]


def test_analyze_pair_no_trade_when_regime_fails():
    """A cleanly trending market (ER stays near 1.0 the whole way) must be
    blocked by the regime gate regardless of whether an extension also
    exists -- the doc's 'mandatory, not optional' regime filter (this
    project's hard-gate choice, see spec). One-directional, constant-delta
    closes give ER ~= 1.0 by the same derivation as test_efficiency_ratio_
    trending_is_near_one in Task 2."""
    candles = []
    ts = 0
    price = 1.10000
    for i in range(40):
        price += 0.0002   # steady one-directional trend -> ER stays high
        candles.append(_bar(ts, price + 0.0001, price - 0.0001, price, vol=10))
        ts += 900
    row = m.analyze_pair("EUR/USD", candles)
    assert row["setup"] == "NO-TRADE"
    assert row["regime_ok"] is False
    assert "Regime filter blocked" in row["notes"]


def test_analyze_universe_shape():
    candles_by_pair = {sym: {"m15": []} for sym in m.VWAP_MR_UNIVERSE}
    candles_by_pair["EUR/USD"]["m15"] = _build_short_setup_series()
    result = m.analyze_universe(candles_by_pair)
    assert result["universe"] == m.VWAP_MR_UNIVERSE
    assert len(result["pairs"]) == len(m.VWAP_MR_UNIVERSE)
    assert result["sells"] >= 1
