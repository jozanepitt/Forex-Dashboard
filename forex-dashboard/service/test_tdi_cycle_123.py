"""Tests for the TDI Cycle 123 divergence / pattern-selection gates.

Divergence doctrine (StrictlyCorrect chart notes + "TOP 5 TDI Strategies",
p.9 — "disagreement between price action and the green line"): the 1→3
trendline on price must run OPPOSITE to the 1→3 trendline on the oscillator.

  Bearish (US.OIL, GER30, NZDUSD examples): price higher high, TDI lower high.
  Bullish (AUDUSD example):                 price lower low,   TDI higher low.

Price falling SHORT of point 1's extreme while the oscillator also weakens is
both series agreeing — a shallow-miss retest, not divergence.

These tests use hand-built pattern dicts + RSI series so behaviour is
deterministic and independent of any data provider.
"""
from __future__ import annotations

import tdi_cycle_123 as t
from alerts import _should_alert_tdi123


# ──────────────────────────────────────────────────────────────────────
# _should_alert_tdi123 — must not crash on a None R:R
# ──────────────────────────────────────────────────────────────────────

def test_grade_b_with_none_rr_does_not_crash():
    """A Grade-B row whose trade plan has no TP1 (rr1=None) must return False,
    not raise. Regression: `None >= 1.0` threw TypeError in the live alert path.
    """
    row = {
        "grade": "B",
        "divergence": {"present": True},
        "htf_aligned": True,
        "trade_plan": {"rr1": None},
    }
    assert _should_alert_tdi123(row) is False


def test_grade_b_with_good_rr_alerts():
    row = {
        "grade": "B",
        "divergence": {"present": True},
        "htf_aligned": True,
        "trade_plan": {"rr1": 1.5},
    }
    assert _should_alert_tdi123(row) is True


def test_grade_a_always_alerts_even_without_plan():
    assert _should_alert_tdi123({"grade": "A"}) is True


def test_ketchup_is_informational_not_a_gate():
    """Ketchup reclaim is surfaced for the trader but must NOT suppress alerts —
    a 30-day A/B test showed gating on it worsened expectancy. A Grade-A setup
    alerts regardless of the reclaim flag."""
    assert _should_alert_tdi123({"grade": "A", "ketchup_reclaimed": False}) is True
    assert _should_alert_tdi123({"grade": "A", "ketchup_reclaimed": True}) is True


def test_ketchup_bullish_requires_price_above_ema13():
    """analyze_pair marks the reclaim only when price is on the right side."""
    # Build a bullish setup where current price is BELOW the 13 EMA -> not yet
    # reclaimed. (Direct helper check keeps this independent of full pipeline.)
    closes_below = [1.6100] * 30 + [1.6000]   # last close well below the EMA
    ema13 = t.ema_last(closes_below, 13)
    assert (closes_below[-1] > ema13) is False   # bullish reclaim would be False


def _pattern(direction, p1_price, p2_price, p3_price, p1_idx=10, p3_idx=30):
    kind = "low" if direction == "bullish" else "high"
    return {
        "direction": direction,
        "p1": {"idx": p1_idx, "price": p1_price, "type": kind},
        "p2": {"idx": 20, "price": p2_price},
        "p3": {"idx": p3_idx, "price": p3_price, "type": kind},
        "leg1_range": abs(p2_price - p1_price),
    }


def _rsi(p1_val, p3_val, n=40, p1_idx=10, p3_idx=30):
    """Flat 50 series with a single spike at each pivot index."""
    s = [50.0] * n
    s[p1_idx] = p1_val
    s[p3_idx] = p3_val
    return s


# ──────────────────────────────────────────────────────────────────────
# _check_divergence — true divergence
# ──────────────────────────────────────────────────────────────────────

def test_bullish_regular_divergence_price_lower_low_rsi_higher_low():
    """Price makes a LOWER low while RSI makes a HIGHER low -> divergence."""
    div = t._check_divergence(
        _pattern("bullish", 1.6000, 1.6043, 1.5995), _rsi(30.0, 35.0))
    assert div["present"] is True
    assert div["strong"] is True


def test_bearish_regular_divergence_price_higher_high_rsi_lower_high():
    """The US.OIL case: price higher high, TDI lower high -> divergence."""
    div = t._check_divergence(
        _pattern("bearish", 1.6100, 1.6057, 1.6108), _rsi(70.0, 63.0))
    assert div["present"] is True
    assert div["strong"] is True


def test_equal_low_within_tolerance_still_counts_but_not_strong():
    """p3 a hair above p1 is an 'equal low' -> divergence, not strong."""
    leg1 = 1.6043 - 1.6000
    p3 = 1.6000 + 0.5 * t.DIVERGENCE_EQUAL_TOLERANCE_PCT * leg1
    div = t._check_divergence(
        _pattern("bullish", 1.6000, 1.6043, p3), _rsi(30.0, 35.0))
    assert div["present"] is True
    assert div["strong"] is False


# ──────────────────────────────────────────────────────────────────────
# _check_divergence — confirmation must NOT be reported as divergence
# ──────────────────────────────────────────────────────────────────────

def test_bullish_higher_low_with_higher_rsi_is_not_divergence():
    """Both price and RSI improved -> confirmation, not divergence.

    Regression: the old ratio gate accepted price up to +15 % of leg 1 above
    p1, so this scored the full divergence points.
    """
    div = t._check_divergence(
        _pattern("bullish", 1.6000, 1.6043, 1.6005), _rsi(30.0, 35.0))
    assert div["present"] is False


def test_bearish_lower_high_with_lower_rsi_is_not_divergence():
    """Both price and RSI weakened -> confirmation, not divergence."""
    div = t._check_divergence(
        _pattern("bearish", 1.6100, 1.6057, 1.6095), _rsi(70.0, 65.0))
    assert div["present"] is False


def test_rsi_move_below_threshold_is_noise():
    """RSI barely moved -> not divergence even with a clean lower low."""
    div = t._check_divergence(
        _pattern("bullish", 1.6000, 1.6043, 1.5995), _rsi(30.0, 31.0))
    assert div["present"] is False


def test_rsi_extreme_read_from_window_not_exact_bar():
    """The oscillator trough sits a bar off the price swing and is still found."""
    series = [50.0] * 40
    series[10] = 30.0
    series[31] = 36.0          # RSI trough one bar AFTER the p3 price swing
    series[30] = 50.0
    div = t._check_divergence(_pattern("bullish", 1.6000, 1.6043, 1.5995), series)
    assert div["rsi_at_p3"] == 36.0
    assert div["present"] is True


# ──────────────────────────────────────────────────────────────────────
# _find_123_pattern — freshness
# ──────────────────────────────────────────────────────────────────────

def _swings_123(p1_price, p2_price, p3_price, base_idx=180):
    """Build exactly one low-high-low or high-low-high swing triple so
    _find_123_pattern evaluates only this pattern (no competing triples)."""
    t1 = "low" if p1_price < p2_price else "high"
    t2 = "high" if t1 == "low" else "low"
    return [
        {"type": t1, "idx": base_idx, "price": p1_price, "ts_utc": "p1"},
        {"type": t2, "idx": base_idx + 5, "price": p2_price, "ts_utc": "p2"},
        {"type": t1, "idx": base_idx + 10, "price": p3_price, "ts_utc": "p3"},
    ]


def _bars(n=200, price=1.0):
    return [{"close": price, "high": price, "low": price, "ts_utc": "x"}] * n


# ──────────────────────────────────────────────────────────────────────
# _find_123_pattern — asymmetric geometry (reference: TDI 123 Doc2)
# ──────────────────────────────────────────────────────────────────────

def test_bearish_overshoot_from_reference_gbpjpy_accepted():
    """GBP/JPY H1 (image 1): p1=217.30 p2=216.35 p3=217.90.

    Point 3 overshoots point 1 by ~63% of leg 1 (a stop-hunt). The old
    symmetric ±25% gate rejected this canonical example; the asymmetric gate
    must accept it.
    """
    swings = _swings_123(217.30, 216.35, 217.90)
    pat = t._find_123_pattern(swings, _bars(price=217.0), symbol="GBP/JPY")
    assert pat is not None
    assert pat["direction"] == "bearish"
    assert pat["p3_kind"] == "overshoot"
    assert pat["p3_overshoot_pct"] > 0.25   # would have failed the old gate


def test_bearish_overshoot_from_reference_eurjpy_accepted():
    """EUR/JPY H1 (image 6): p1=185.30 p2=184.75 p3=185.47 — ~31% overshoot."""
    swings = _swings_123(185.30, 184.75, 185.47)
    pat = t._find_123_pattern(swings, _bars(price=185.0), symbol="EUR/JPY")
    assert pat is not None
    assert pat["direction"] == "bearish"
    assert pat["p3_kind"] == "overshoot"


def test_bullish_overshoot_lower_low_accepted():
    """Mirror of the bearish case: bullish p3 makes a lower low (stop hunt)."""
    swings = _swings_123(1.6000, 1.6050, 1.5975)   # p3 30% below p1
    pat = t._find_123_pattern(swings, _bars(price=1.60), symbol="EUR/USD")
    assert pat is not None
    assert pat["direction"] == "bullish"
    assert pat["p3_kind"] == "overshoot"


def test_shortfall_beyond_tolerance_rejected():
    """A p3 that falls far SHORT of p1 (not a real retest) is still rejected."""
    # bearish: p1=1.6100 high, p2=1.6000 low, leg1=0.0100; p3 only back to
    # 1.6050 = 50% short of p1, beyond the 25% shortfall bound.
    swings = _swings_123(1.6100, 1.6000, 1.6050)
    pat = t._find_123_pattern(swings, _bars(price=1.605), symbol="EUR/USD")
    assert pat is None


def test_overshoot_beyond_generous_bound_rejected():
    """A p3 that overshoots p1 by more than the full generous bound is out."""
    # bearish: leg1=0.0100, p3 = p1 + 0.0080 = 80% overshoot (> 65%)
    swings = _swings_123(1.6100, 1.6000, 1.6180)
    pat = t._find_123_pattern(swings, _bars(price=1.618), symbol="EUR/USD")
    assert pat is None


def test_returns_freshest_pattern_not_the_oldest():
    """Two valid 123s in the window -> the recent one wins.

    Regression: the old loop returned on first match, handing back a pattern
    that had already played out ~80 bars ago.
    """
    swings = [
        {"type": "low",  "idx": 100, "price": 1.6000, "ts_utc": "t1"},
        {"type": "high", "idx": 110, "price": 1.6100, "ts_utc": "t2"},
        {"type": "low",  "idx": 120, "price": 1.6002, "ts_utc": "t3"},
        {"type": "high", "idx": 180, "price": 1.6300, "ts_utc": "t4"},
        {"type": "low",  "idx": 190, "price": 1.6200, "ts_utc": "t5"},
        {"type": "high", "idx": 199, "price": 1.6298, "ts_utc": "t6"},
    ]
    bars = [{"close": 1.6295, "high": 1.63, "low": 1.629, "ts_utc": "x"}] * 200
    pat = t._find_123_pattern(swings, bars, symbol="EUR/USD")
    assert pat is not None
    assert pat["p3"]["idx"] == 199


# ──────────────────────────────────────────────────────────────────────
# _check_signal_cross — the cross must still be in effect
# ──────────────────────────────────────────────────────────────────────

def test_signal_cross_confirmed_when_still_in_effect():
    pat = _pattern("bullish", 1.6000, 1.6043, 1.5995, p3_idx=30)
    fast = [40.0] * 32 + [55.0] * 8   # crosses up at idx 32 and stays there
    slow = [50.0] * 40
    cross = t._check_signal_cross(pat, fast, slow)
    assert cross["present"] is True


def test_signal_cross_rejected_after_crossing_back():
    """A cross that has since reversed is momentum lost, not confirmation.

    Regression: the old scan took the first cross after p3 and never checked
    what happened afterwards.
    """
    pat = _pattern("bullish", 1.6000, 1.6043, 1.5995, p3_idx=30)
    fast = [40.0] * 32 + [55.0] * 4 + [45.0] * 4   # crossed up, then back down
    slow = [50.0] * 40
    cross = t._check_signal_cross(pat, fast, slow)
    assert cross["present"] is False


def test_weekly_fib_pivots_levels():
    """Fib pivots project the prior week's range from P=(H+L+C)/3."""
    lv = t._weekly_fib_pivots((110.0, 100.0, 105.0))  # H,L,C -> P=105, range=10
    assert abs(lv["P"] - 105.0) < 1e-9
    assert abs(lv["range"] - 10.0) < 1e-9
    assert abs(lv["R61"] - (105.0 + 0.618 * 10)) < 1e-9
    assert abs(lv["S100"] - (105.0 - 1.0 * 10)) < 1e-9


def test_location_sell_at_resistance_is_good():
    """A bearish setup with price up at R61–R100 is a high-probability zone."""
    lv = t._weekly_fib_pivots((110.0, 100.0, 105.0))  # P=105, range=10
    loc = t._location(105.0 + 0.7 * 10, "bearish", lv)   # price at R70
    assert loc["quality"] in ("good", "prime")
    assert loc["ok"] is True


def test_location_buy_at_support_is_good():
    lv = t._weekly_fib_pivots((110.0, 100.0, 105.0))
    loc = t._location(105.0 - 0.7 * 10, "bullish", lv)   # price at S70
    assert loc["quality"] in ("good", "prime")
    assert loc["ok"] is True


def test_location_wrong_side_flagged():
    """A sell setup with price BELOW the pivot (at support) is the wrong side."""
    lv = t._weekly_fib_pivots((110.0, 100.0, 105.0))
    loc = t._location(105.0 - 0.7 * 10, "bearish", lv)   # sell but at support
    assert loc["quality"] == "wrongside"
    assert loc["ok"] is False


def test_location_near_pivot_is_poor():
    lv = t._weekly_fib_pivots((110.0, 100.0, 105.0))
    loc = t._location(105.2, "bearish", lv)              # basically on the pivot
    assert loc["quality"] == "poor"
    assert loc["ok"] is False


def test_prev_week_hlc_no_lookahead():
    """_prev_week_hlc must only see the fully-completed prior week."""
    # build 3 weeks of daily bars; ask for pivots as of week-3 -> gets week-2 H/L/C
    day = 86400
    base = t._MONDAY_EPOCH + 100 * 7 * day    # some Monday
    bars = []
    for wk, (hi, lo, cl) in enumerate([(10, 5, 8), (20, 15, 18), (30, 25, 28)]):
        for d in range(5):
            ts = base + wk * 7 * day + d * day
            bars.append({"ts_utc": ts, "high": hi, "low": lo, "close": cl})
    as_of = base + 2 * 7 * day + 2 * day      # mid week-3
    hlc = t._prev_week_hlc(bars, as_of)
    assert hlc == (20, 15, 18)                # week-2, not week-3


def test_atr_measures_true_range():
    """ATR of a series with a constant 10-wide range and no gaps is 10."""
    bars = [{"high": 100 + 10, "low": 100, "close": 105} for _ in range(20)]
    assert abs(t._atr(bars, period=14) - 10) < 1e-9


def test_sl_is_not_hair_tight_when_entry_sits_on_p3():
    """Regression: the old p3±5pip stop gave a ~5-pip SL for a fresh-from-p3
    entry, producing absurd R:R. The ATR floor must guarantee real room."""
    # bullish 123 with entry essentially on p3, ATR ~ 0.0020 (20 pips)
    price = 1.6000
    bars = [{"open": price, "high": price + 0.0010, "low": price - 0.0010,
             "close": price, "ts_utc": i} for i in range(300)]
    # carve out a real 123: p1 low, p2 high, p3 low ~ current price
    swings = [
        {"type": "low", "idx": 280, "price": 1.5990, "ts_utc": "p1"},
        {"type": "high", "idx": 288, "price": 1.6030, "ts_utc": "p2"},
        {"type": "low", "idx": 296, "price": 1.5998, "ts_utc": "p3"},
    ]
    # feed enough RSI extreme + cross so it grades tradeable is not needed here;
    # test the SL math directly via the helper pieces.
    atr = t._atr(bars)
    assert atr is not None and atr > 0
    entry = price
    p3 = 1.5998
    sl_struct = p3 - t.SL_STRUCT_ATR_MULT * atr
    sl_floor = entry - t.SL_MIN_ATR_MULT * atr
    sl = min(sl_struct, sl_floor)
    stop_pips = (entry - sl) / 0.0001
    assert stop_pips >= 15   # at least ~1 ATR of room, never the old ~5 pips


def test_trend_regime_detects_stacked_bear_cascade():
    """Long steady downtrend -> price < 50 < 200, strictly stacked bearish."""
    closes = [1.6600 - 0.0004 * i for i in range(400)]
    regime = t._trend_regime(closes)
    assert regime["stacked"] is True
    assert regime["direction"] == "bearish"


def test_trend_regime_not_stacked_when_ranging():
    """A flat oscillating series is not a strong trend."""
    closes = [1.6000 + (0.0020 if i % 2 else -0.0020) for i in range(400)]
    regime = t._trend_regime(closes)
    assert regime["stacked"] is False


def test_signal_cross_rejected_when_stale():
    """A cross older than SIGNAL_CROSS_MAX_AGE no longer confirms entry."""
    pat = _pattern("bullish", 1.6000, 1.6043, 1.5995, p1_idx=2, p3_idx=5)
    n = t.SIGNAL_CROSS_MAX_AGE + 30
    fast = [40.0] * 6 + [55.0] * (n - 6)   # crossed at idx 6, far in the past
    slow = [50.0] * n
    cross = t._check_signal_cross(pat, fast, slow)
    assert cross["present"] is False
    assert cross["bars_since_cross"] > t.SIGNAL_CROSS_MAX_AGE
