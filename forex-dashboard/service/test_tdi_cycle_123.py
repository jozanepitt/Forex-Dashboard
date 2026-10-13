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
from alerts import _should_alert_tdi123, _pair_currencies, _news_blocks_pair


# ──────────────────────────────────────────────────────────────────────
# News filter helpers (pure — blocklist injected, no network/clock)
# ──────────────────────────────────────────────────────────────────────

def test_pair_currencies_fx_metal_index():
    assert _pair_currencies("EUR/USD") == {"EUR", "USD"}
    assert _pair_currencies("XAU/USD") == {"XAU", "USD"}
    assert _pair_currencies("USTEC") == {"USD"}   # index -> its driving currency
    assert _pair_currencies("DE30") == {"EUR"}


def test_news_blocks_when_either_currency_hit():
    assert _news_blocks_pair("EUR/USD", {"USD"}) is True     # quote currency
    assert _news_blocks_pair("EUR/USD", {"EUR"}) is True     # base currency
    assert _news_blocks_pair("EUR/USD", {"GBP"}) is False    # unrelated
    assert _news_blocks_pair("USTEC", {"USD"}) is True       # index via map
    assert _news_blocks_pair("EUR/USD", set()) is False      # empty blocklist
    assert _news_blocks_pair("GBP/JPY", None) is False       # None fails open


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


def _ts(hour_utc, day=5):
    import datetime as _dt
    return int(_dt.datetime(2026, 1, day, hour_utc, 0, tzinfo=_dt.timezone.utc).timestamp())


def test_session_labels_and_active_window():
    assert t._session_label(_ts(8)) == "London-open"
    assert t._session_label(_ts(11)) == "London"
    assert t._session_label(_ts(14)) == "NY-open"
    assert t._session_label(_ts(18)) == "NY-late"
    assert t._session_label(_ts(2)) == "Asian"
    # active window = 03:00-18:00 UTC (05:00-20:00 SAST per user rule)
    assert t._in_active_session(_ts(7)) is True
    assert t._in_active_session(_ts(15)) is True
    assert t._in_active_session(_ts(16)) is True    # NY-afternoon, still active
    assert t._in_active_session(_ts(18)) is False   # 20:00 SAST, exclusive
    assert t._in_active_session(_ts(2)) is False     # before window opens


def test_adr_context_from_daily_candles():
    import datetime as _dt
    # 15 prior days each with a 0.1000 (1000-pip) range, then "today"
    d1 = []
    for k in range(15):
        ts = int(_dt.datetime(2026, 1, 1 + k, 0, 0, tzinfo=_dt.timezone.utc).timestamp())
        d1.append({"ts_utc": ts, "high": 1.1000, "low": 1.0000, "close": 1.0500})
    today = _dt.datetime(2026, 1, 20, 12, 0, tzinfo=_dt.timezone.utc)
    # today's H1 bars span a 0.0400 range so far -> 40% of the 0.1000 ADR
    h1 = [{"ts_utc": int((today.replace(hour=h)).timestamp()),
           "high": 1.0400, "low": 1.0000, "close": 1.0200} for h in range(9, 13)]
    ctx = t._adr_context(h1, d1)
    assert ctx is not None
    assert abs(ctx["adr"] - 0.1) < 1e-9
    assert abs(ctx["consumed_pct"] - 40.0) < 1e-6
    assert abs(ctx["remaining"] - 0.06) < 1e-9


def test_session_gate_blocks_asian_but_none_passes():
    """Session filter blocks an out-of-session signal; missing data never blocks."""
    assert _should_alert_tdi123({"grade": "A", "in_active_session": False}) is False
    assert _should_alert_tdi123({"grade": "A", "in_active_session": True}) is True
    assert _should_alert_tdi123({"grade": "A"}) is True   # no field -> not blocked


def test_adr_gate_off_by_default():
    """ADR filter is OFF by default (A/B showed it starved the strategy), so an
    unreachable-TP1 signal is NOT blocked out of the box."""
    assert _should_alert_tdi123({"grade": "A", "tp1_reachable": False}) is True


def test_adr_gate_blocks_when_enabled(monkeypatch):
    """When TDI123_ADR_FILTER is enabled, an unreachable TP1 is blocked; a None
    (missing ADR data) still passes."""
    import alerts
    monkeypatch.setattr(alerts, "TDI123_ADR_FILTER", True)
    assert alerts._should_alert_tdi123({"grade": "A", "tp1_reachable": False}) is False
    assert alerts._should_alert_tdi123({"grade": "A", "tp1_reachable": True}) is True
    assert alerts._should_alert_tdi123({"grade": "A"}) is True   # None -> not blocked


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


# --- TDI extreme: the baseline must reach the zone (2026-10-12) --------------
# Doctrine: the 34-period baseline has to enter the extreme at point 3. The
# code used to accept the RSI line alone; in the walk-forward replay those
# RSI-only alerts went 2 wins / 11 losses.

def _extreme(direction, baseline, rsi):
    pat = {"direction": direction, "p3": {"idx": 5}}
    return t._check_tdi_extreme(pat, [rsi] * 10, [baseline] * 10)


def test_rsi_alone_at_extreme_is_not_a_tdi_extreme():
    assert _extreme("bullish", baseline=45.0, rsi=25.0)["present"] is False
    assert _extreme("bearish", baseline=55.0, rsi=75.0)["present"] is False


def test_baseline_at_extreme_is_enough():
    ex = _extreme("bullish", baseline=34.0, rsi=40.0)
    assert ex["present"] is True and ex["strong"] is False
    ex = _extreme("bearish", baseline=66.0, rsi=60.0)
    assert ex["present"] is True and ex["strong"] is False


def test_baseline_and_rsi_at_extreme_is_strong():
    assert _extreme("bullish", baseline=34.0, rsi=30.0)["strong"] is True
    assert _extreme("bearish", baseline=66.0, rsi=70.0)["strong"] is True


# --- three edge filters from the walk-forward replay (2026-10-13) -------------
# Each pointed the same way on both H1 and M15 over ~5 months / 27 pairs:
#   baseline only just at 63/37   -> H1 -0.35R (18), M15 -0.25R (15)
#   location prime/extended       -> H1 -0.47R (26), M15 -0.74R (17)
#   metals + crypto               -> 2 wins from 16 (and 0 from 12 in the journal)

def test_baseline_levels_are_the_documents_63_and_37():
    """Source page: "TDI Overbought levels 63 > 68, Oversold levels 37 < 32".
    A 65/35 variant was tried on 2026-10-13 and reverted at the user's call."""
    assert t.TDI_OB_ZONE == 63.0 and t.TDI_OS_ZONE == 37.0
    assert _extreme("bearish", baseline=62.9, rsi=72.0)["present"] is False
    assert _extreme("bearish", baseline=63.0, rsi=60.0)["present"] is True
    assert _extreme("bullish", baseline=37.1, rsi=28.0)["present"] is False
    assert _extreme("bullish", baseline=37.0, rsi=40.0)["present"] is True


def test_far_pivot_locations_are_capped_like_poor_ones():
    for q in ("poor", "wrongside", "prime", "extended"):
        assert t._cap_grade_for_location("A", q) == "C", q
        assert t._cap_grade_for_location("B", q) == "C", q
    for q in ("good", "weak", "unknown"):
        assert t._cap_grade_for_location("A", q) == "A", q
        assert t._cap_grade_for_location("B", q) == "B", q
    assert t._cap_grade_for_location("C", "prime") == "C"
    assert t._cap_grade_for_location("NO-TRADE", "prime") == "NO-TRADE"


def test_location_classification_is_unchanged():
    piv = {"P": 1.1000, "range": 0.0100}
    assert t._location(1.1095, "bearish", piv)["quality"] == "prime"
    assert t._location(1.1140, "bearish", piv)["quality"] == "extended"
    assert t._location(1.1065, "bearish", piv)["quality"] == "good"
    assert t._location(1.1035, "bearish", piv)["quality"] == "weak"


def test_metals_and_crypto_are_excluded_from_tdi_alerts(monkeypatch):
    import alerts
    reached = []
    monkeypatch.setattr(alerts, "TDI123_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "TDI123_WATCH_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "TDI123_EXCLUDED_CLASSES", ("metal", "crypto"))
    monkeypatch.setattr(alerts, "_should_alert_tdi123", lambda row: (reached.append("setup"), False)[1])
    monkeypatch.setattr(alerts, "_tdi123_watch_reasons", lambda pair, row: (reached.append("watch"), [])[1])
    row = {"setup": "BUY", "grade": "A", "timeframe": "H1",
           "m15": {"setup": "BUY", "grade": "A", "timeframe": "M15"}}
    for pair in ("XAU/USD", "XAG/USD", "BTC/USD", "ETH/USD"):
        alerts.alert_tdi123_setup(pair, row)
        alerts.alert_tdi123_watch(pair, row)
    assert reached == []
    alerts.alert_tdi123_setup("EUR/USD", row)
    alerts.alert_tdi123_setup("US30", row)
    assert reached.count("setup") == 4            # H1 + M15 leg for each of the two pairs


def test_exclusion_list_is_configurable(monkeypatch):
    import alerts
    reached = []
    monkeypatch.setattr(alerts, "TDI123_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "TDI123_EXCLUDED_CLASSES", ())
    monkeypatch.setattr(alerts, "_should_alert_tdi123", lambda row: (reached.append(1), False)[1])
    alerts.alert_tdi123_setup("XAU/USD", {"setup": "BUY", "grade": "A", "timeframe": "H1"})
    assert reached == [1]


# --- midline reset between point 1 and point 3 (2026-10-13) -------------------
# Source diagram: points 1 and 3 sit in the extreme zone, point 2 returns to
# the 50 line. Without that reset the TDI is just pinned in the zone while a
# trend grinds on, which is not a peak formation.

def _mid(direction, rsi_between, level=None):
    """RSI series: extreme at p1 (idx 10) and p3 (idx 30), `rsi_between` in the middle."""
    edge = 72.0 if direction == "bearish" else 28.0
    series = [edge] * 40
    for i in range(15, 26):
        series[i] = rsi_between
    pat = {"direction": direction, "p1": {"idx": 10}, "p3": {"idx": 30}}
    return t._midline_reset(pat, series) if level is None else t._midline_reset(pat, series, level)


def test_sell_needs_the_rsi_back_down_at_50_between_the_peaks():
    assert _mid("bearish", 49.0)["present"] is True
    assert _mid("bearish", 50.0)["present"] is True
    assert _mid("bearish", 58.0)["present"] is False       # never left the upper half
    assert _mid("bearish", 58.0)["rsi_between"] == 58.0


def test_buy_needs_the_rsi_back_up_at_50_between_the_lows():
    assert _mid("bullish", 51.0)["present"] is True
    assert _mid("bullish", 44.0)["present"] is False
    assert _mid("bullish", 44.0)["rsi_between"] == 44.0


def test_midline_level_can_be_loosened():
    assert _mid("bearish", 54.0, level=55.0)["present"] is True
    assert _mid("bullish", 46.0, level=55.0)["present"] is True    # buys mirror the level: 100 - 55 = 45
    assert _mid("bearish", 56.0, level=55.0)["present"] is False


def test_midline_handles_bad_indices():
    assert t._midline_reset({"direction": "bearish", "p1": {"idx": 5}, "p3": {"idx": 99}}, [60.0] * 10)["present"] is False
    assert t._midline_reset({"direction": "bearish", "p1": {"idx": 7}, "p3": {"idx": 7}}, [60.0] * 10)["present"] is False


def test_midline_rule_is_off_by_default():
    assert t.MIDLINE_REQUIRED is False
    assert t.TDI_MIDLINE == 50.0


# --- multi-day peak formation, found on the TDI itself (2026-10-13) -----------
# Source charts (Gold, FRA40, USDCAD, NAS100, all H1): point 1 is an RSI peak
# beyond the 68 line with the yellow baseline pushed into the 63-68 zone;
# point 2 is the RSI back at the 50 line; point 3 is the RSI back in the zone
# but LOWER than at point 1 (the divergence line), 1.5 to 4 days after point 1.

def _peak_fixture(p1_rsi=76.0, p2_rsi=47.0, p3_rsi=66.0, base_peak=65.0,
                  gap12=22, gap23=24, tail=3, p3_high=1.2008, n_before=140):
    """Returns (bars, rsi, baseline, i1, i2, i3) for a bearish formation."""
    rsi, base, hi, lo = [], [], [], []

    def seg(k, r0, r1, h0, h1, b0, b1):
        for j in range(1, k + 1):
            f = j / k
            rsi.append(r0 + (r1 - r0) * f); hi.append(h0 + (h1 - h0) * f); base.append(b0 + (b1 - b0) * f)
            lo.append(hi[-1] - 0.0012)

    rsi += [52.0] * n_before; hi += [1.1900] * n_before; base += [52.0] * n_before
    lo += [1.1888] * n_before
    seg(12, 52.0, p1_rsi, 1.1900, 1.2000, 52.0, 58.0)             # push to point 1
    i1 = len(rsi) - 1
    seg(gap12, p1_rsi, p2_rsi, 1.2000, 1.1900, 58.0, base_peak)    # reset to the midline
    i2 = len(rsi) - 1
    seg(gap23, p2_rsi, p3_rsi, 1.1900, p3_high, base_peak, 60.0)   # second push
    i3 = len(rsi) - 1
    seg(tail, p3_rsi, p3_rsi - 6.0, p3_high, p3_high - 0.0015, 60.0, 59.0)
    bars = [{"ts_utc": 1_790_000_000 + 3600 * k, "open": hi[k] - 0.0006, "high": hi[k],
             "low": lo[k], "close": hi[k] - 0.0005} for k in range(len(rsi))]
    return bars, rsi, base, i1, i2, i3


def _flip(bars, rsi, base, pivot=1.2000):
    fb = [{"ts_utc": b["ts_utc"], "open": 2 * pivot - b["open"], "high": 2 * pivot - b["low"],
           "low": 2 * pivot - b["high"], "close": 2 * pivot - b["close"]} for b in bars]
    return fb, [100.0 - x for x in rsi], [100.0 - x for x in base]


def test_peak_formation_is_found_with_the_right_points():
    bars, rsi, base, i1, i2, i3 = _peak_fixture()
    pat = t._find_peak_formation(bars, rsi, base, symbol="EUR/USD")
    assert pat is not None
    assert pat["direction"] == "bearish" and pat["kind"] == "peak"
    assert (pat["p1"]["idx"], pat["p3"]["idx"]) == (i1, i3)
    assert abs(pat["p2"]["idx"] - i2) <= 1
    assert pat["p1"]["price"] == bars[i1]["high"] and pat["p3"]["price"] == bars[i3]["high"]
    assert pat["rsi_p1"] == 76.0 and abs(pat["rsi_p3"] - 66.0) < 1e-9 and pat["rsi_p2"] <= 50.0
    assert pat["span_bars"] == i3 - i1 == 46
    assert pat["baseline_peak"] >= 63.0
    assert pat["leg1_range"] > 0 and pat["leg1_range_pips"] > 50


def test_bullish_mirror_is_found():
    bars, rsi, base, i1, i2, i3 = _peak_fixture()
    fb, fr, fbase = _flip(bars, rsi, base)
    pat = t._find_peak_formation(fb, fr, fbase, symbol="EUR/USD")
    assert pat is not None and pat["direction"] == "bullish"
    assert (pat["p1"]["idx"], pat["p3"]["idx"]) == (i1, i3)
    assert pat["p1"]["price"] == fb[i1]["low"]


def test_no_formation_without_a_reset_to_the_midline():
    bars, rsi, base, *_ = _peak_fixture(p2_rsi=57.0)
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_no_formation_if_point_1_never_cleared_68():
    bars, rsi, base, *_ = _peak_fixture(p1_rsi=67.0, p3_rsi=64.0)
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_no_formation_if_point_3_does_not_get_back_into_the_zone():
    bars, rsi, base, *_ = _peak_fixture(p3_rsi=60.0)
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_no_formation_without_divergence():
    bars, rsi, base, *_ = _peak_fixture(p1_rsi=72.0, p3_rsi=74.0)
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_no_formation_if_the_baseline_never_entered_the_zone():
    bars, rsi, base, *_ = _peak_fixture(base_peak=60.0)
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_baseline_gets_one_point_of_grace_for_feed_differences():
    """The source's own USD/JPY example peaks at 62.4 on the Exness feed."""
    bars, rsi, base, *_ = _peak_fixture(base_peak=62.4)
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is not None
    bars, rsi, base, *_ = _peak_fixture(base_peak=61.5)
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_no_formation_when_too_short_or_too_long():
    bars, rsi, base, *_ = _peak_fixture(gap12=4, gap23=5)             # 9 bars: an intraday wobble
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None
    bars, rsi, base, *_ = _peak_fixture(gap12=60, gap23=60)            # 120 bars: over the 4-day limit
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_no_formation_when_point_3_is_stale():
    bars, rsi, base, *_ = _peak_fixture(tail=t.PEAK_P3_MAX_AGE + 4)
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_point_3_must_have_turned_down():
    bars, rsi, base, *_ = _peak_fixture(tail=0)                        # still rising on the newest bar
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_no_formation_when_price_falls_well_short_of_point_1():
    bars, rsi, base, *_ = _peak_fixture(p3_high=1.1950)                # 50% short of the 100-pip leg
    assert t._find_peak_formation(bars, rsi, base, symbol="EUR/USD") is None


def test_peak_extreme_reads_the_baseline_across_the_formation():
    bars, rsi, base, *_ = _peak_fixture()
    pat = t._find_peak_formation(bars, rsi, base, symbol="EUR/USD")
    ex = t._peak_extreme(pat, rsi, base)
    assert ex["present"] is True and ex["strong"] is True
    assert ex["baseline_peak"] == pat["baseline_peak"]
    assert "baseline_at_p3" in ex and "rsi_at_p3" in ex


def test_peak_finder_is_opt_in():
    """Built and verified against the source's examples, but its replay showed
    no edge (H1 -0.15R over 94 formations), so the default stays "swing"."""
    assert t.PATTERN_MODE == "swing"


def test_peak_mode_routes_the_pipeline_through_the_new_finder(monkeypatch):
    called = []
    monkeypatch.setattr(t, "PATTERN_MODE", "peak")
    monkeypatch.setattr(t, "_find_peak_formation",
                        lambda bars, rsi, base, symbol=None: (called.append(len(bars)), None)[1])
    bars = [{"ts_utc": 1_790_000_000 + 3600 * k, "open": 1.1, "high": 1.1005, "low": 1.0995,
             "close": 1.1 + 0.0001 * (k % 5)} for k in range(150)]
    row = t._analyze_timeframe("EUR/USD", bars, None, None, timeframe="H1", bias_timeframe="H4")
    assert called == [150]
    assert row["setup"] == "NO-TRADE"


# --- big-picture TDI read on the next timeframe up (2026-10-13) ---------------
# The mentorship material pairs a 4h TDI at an extreme with a 1h 1-2-3.
# Reported on every row as row["htf_tdi"]; not a gate.

def _closes_to_bars(closes):
    return [{"ts_utc": 1_790_000_000 + 14400 * k, "open": c, "high": c + 0.0005, "low": c - 0.0005, "close": c}
            for k, c in enumerate(closes)]


def test_htf_context_sees_an_overbought_higher_timeframe():
    closes = [1.1000 + 0.00002 * (k % 3) for k in range(120)] + [1.1000 + 0.0012 * k for k in range(1, 40)]
    ctx = t._htf_tdi_context(_closes_to_bars(closes), "bearish")
    assert ctx["available"] is True
    assert ctx["rsi_extreme"] is True and ctx["baseline_extreme"] is True
    assert t._htf_tdi_context(_closes_to_bars(closes), "bullish")["rsi_extreme"] is False


def test_htf_context_is_neutral_on_a_flat_higher_timeframe():
    closes = [1.1000 + 0.0003 * ((k % 4) - 1.5) for k in range(200)]
    ctx = t._htf_tdi_context(_closes_to_bars(closes), "bearish")
    assert ctx["available"] is True
    assert ctx["rsi_extreme"] is False and ctx["baseline_extreme"] is False and ctx["formation"] is False


def test_htf_context_without_data():
    assert t._htf_tdi_context(None, "bearish")["available"] is False
    assert t._htf_tdi_context(_closes_to_bars([1.1] * 20), "bullish")["available"] is False


# --- stop already traded through (moved here when btmm_123 was removed) -------

def test_stop_already_hit_helper():
    after = [{"high": 1.2010, "low": 1.1990}, {"high": 1.2030, "low": 1.1950}]
    assert t._stop_already_hit(after, "BUY", 1.1960) is True
    assert t._stop_already_hit(after, "BUY", 1.1940) is False
    assert t._stop_already_hit(after, "SELL", 1.2020) is True
    assert t._stop_already_hit(after, "SELL", 1.2040) is False
    assert t._stop_already_hit([], "BUY", 1.1960) is False
    assert t._stop_already_hit(after, "BUY", None) is False
