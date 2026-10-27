"""Regression tests for BTMM correctness fixes (2026-07-24 audit).

Covers the two criticals and the HOD/LOD high-severity bug found by the audit:
  * calc_ema no longer flat-lines at the last close for short series
  * confidence votes are directional-only (no structural bullish bias)
  * detect_hod_lod requires a retest, not a fresh-breakout proximity
"""
from __future__ import annotations

import btmm_core as b


# ── #2  calc_ema short-series flat-line ───────────────────────────────────────

def test_calc_ema_short_series_does_not_flatline_at_last_close():
    """A rising series shorter than the period must yield an EMA that LAGS the
    last close, not equal it (the old bug returned [closes[-1]]*n, so the
    800-EMA == price and every setup was forced bearish)."""
    closes = [1.0 + 0.01 * i for i in range(300)]   # rising, < 800 bars
    e = b.ema_last(closes, 800)
    assert e != closes[-1]        # old bug: exactly closes[-1]
    assert e < closes[-1]         # EMA lags below rising price
    assert e > closes[0]          # but has advanced from the start


def test_calc_ema_full_period_unchanged():
    closes = [1.0] * 900
    assert abs(b.ema_last(closes, 800) - 1.0) < 1e-9


# ── #1  directional confidence factors ────────────────────────────────────────

def test_bearish_can_reach_high_confidence_without_exhausted_adr():
    """Five directional factors all bearish -> high confidence, independent of
    ADR/AMD/kill-zone (the old code needed ADR 80-100% consumed for bearish)."""
    factors = b._btmm_direction_factors(
        {"score": -5}, {"bearish": True, "bullish": False}, False,
        {"detected": True, "pattern": "M"}, {"detected": True, "direction": "bearish"})
    assert factors == [-1, -1, -1, -1, -1]
    assert b._classify_confidence(0, 5) == "high"


def test_confidence_factors_symmetric_no_bullish_bias():
    """Mirror-image bull and bear states must produce mirror factor sums."""
    bull = b._btmm_direction_factors(
        {"score": 5}, {"bullish": True, "bearish": False}, True,
        {"detected": True, "pattern": "W"}, {"detected": True, "direction": "bullish"})
    bear = b._btmm_direction_factors(
        {"score": -5}, {"bearish": True, "bullish": False}, False,
        {"detected": True, "pattern": "M"}, {"detected": True, "direction": "bearish"})
    assert sum(bull) == 5
    assert sum(bear) == -5


def test_confidence_excludes_nondirectional_factors():
    """A neutral-direction state stays low even in a kill zone / fresh ADR — the
    old kz/adr votes could push it up."""
    factors = b._btmm_direction_factors(
        {"score": 0}, {"bullish": False, "bearish": False}, True,   # bias +1 only
        {"detected": False}, {"detected": False})
    assert factors == [0, 0, 1, 0, 0]
    assert b._classify_confidence(1, 0) == "low"


# ── 2026-10-27 phase 1: direction bugs ────────────────────────────────────────

def _ctx(direction):
    """Every checklist input pointing `direction` ('bullish'/'bearish')."""
    bull = direction == "bullish"
    return dict(
        stack={"score": 5 if bull else -5}, tdi={"bullish": bull, "bearish": not bull}, kz="London",
        adr_c=40, amd={"phase": "Distribution"}, asian={"valid": True},
        push_hunt={"detected": True, "direction": direction}, hunt={"active": True, "direction": direction},
        tdi_leg={"leg": 2, "direction": direction}, level={"level_ii": True, "direction": direction},
        mw={"detected": True, "pattern": "W" if bull else "M", "confirmed": True},
        shark={"entry_ready": True, "direction": direction},
        hod_lod={"at_lod": bull, "at_hod": not bull}, half_bat={"detected": True, "direction": direction},
        tdi_div={"detected": True, "direction": direction})


def test_checklist_counts_only_items_pointing_the_trade_way():
    """Audit 2026-10-15: TDI 2nd leg, Shark Fin, stop hunt and Half Batman
    passed even when they pointed the OTHER way (16,294 times in the replay)."""
    assert sum(b._btmm_checklist(True, **_ctx("bullish"))) == 13
    against = _ctx("bearish")
    against.update(stack={"score": 5}, tdi={"bullish": True, "bearish": False},
                   mw={"detected": True, "pattern": "W"}, hod_lod={"at_lod": True, "at_hod": False})
    items = b._btmm_checklist(True, **against)
    names = ["ema", "tdi", "kz", "adr", "amd", "asian", "hunt", "leg2", "level", "mw", "shark", "hodlod", "div"]
    failed = {n for n, ok in zip(names, items) if not ok}
    assert {"hunt", "leg2", "level", "shark", "div"} <= failed


def test_checklist_is_symmetric_for_buys_and_sells():
    assert b._btmm_checklist(True, **_ctx("bullish")) == b._btmm_checklist(False, **_ctx("bearish"))


def test_a_plus_sell_is_reachable():
    """Audit 2026-10-15: a sell maxed out at about +30 while A+ needs 70, so
    0 A+ Sells in 35,950 sell setups."""
    assert b._composite_score(5, 13, True) == 100
    assert b._composite_score(-5, 13, False) == -100
    assert b._signal_label(-100, True, False, kz="London") == "A+ Sell"
    assert b._signal_label(100, True, True, kz="London") == "A+ Buy"


def test_signal_tiers_are_symmetric():
    for score, bull, label in ((45, True, "Strong Buy"), (-45, False, "Strong Sell"),
                               (25, True, "Buy"), (-25, False, "Sell"), (10, True, "Neutral"),
                               (-10, False, "Neutral")):
        assert b._signal_label(score, False, bull, kz="NY") == label, (score, bull)
    assert b._signal_label(-45, False, False, kz=None) == "WAIT"


# ── #4  HOD/LOD requires a retest ─────────────────────────────────────────────

def _bar(ts, hi, lo, cl):
    return {"ts_utc": ts, "open": cl, "high": hi, "low": lo, "close": cl}


def test_hod_lod_fresh_breakout_is_not_a_setup():
    """Price marching straight up to a new HOD (never pulling away) must NOT
    register at_hod — that's the breakout trap, not a retest."""
    bars = [_bar(i * 900, 1.10 + 0.001 * i + 0.0002, 1.10 + 0.001 * i, 1.10 + 0.001 * i)
            for i in range(20)]
    r = b.detect_hod_lod(bars, symbol="EUR/USD")
    assert r["at_hod"] is False
    assert r["retest_hod"] is False


def test_hod_lod_retest_after_pullback_qualifies():
    """HOD set, price pulls ~30 pips away, then returns to the high -> at_hod."""
    bars = []
    # ramp up to a HOD of ~1.1200
    for i in range(8):
        p = 1.1100 + 0.0013 * i
        bars.append(_bar(i * 900, p + 0.0002, p, p))
    hod = max(x["high"] for x in bars)
    # pull ~30 pips down
    for i in range(8, 14):
        p = hod - 0.0030
        bars.append(_bar(i * 900, p + 0.0002, p - 0.0002, p))
    # return to just under the HOD (retest)
    bars.append(_bar(14 * 900, hod, hod - 0.0004, hod - 0.0002))
    r = b.detect_hod_lod(bars, symbol="EUR/USD")
    assert r["retest_hod"] is True
    assert r["at_hod"] is True
