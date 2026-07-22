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
