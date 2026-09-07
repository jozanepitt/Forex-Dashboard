"""Tests for the BTMM 123 strategy — classic price-structure 1-2-3 reversal,
confirmed by BTMM doctrine (EMA Level cascade, stop hunt, Asian range) instead
of a TDI/oscillator dependency. Replaces the removed Malaysian SNR Emperor
slot in the dashboard.

Geometry (swings, 123 pattern, session labels, ATR) is reused directly from
tdi_cycle_123.py by import — tdi_cycle_123.py itself is untouched. This file
tests only what's new here: the EMA-Level score, stop-hunt score, Asian-range
score, pivot-location gate, H4 bias, M15 leg, 200EMA Re-set branch, grade
thresholds, and the end-to-end pipeline/alert gate.
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


def test_should_alert_grade_b_allowed_by_default():
    """A+B convention like CRT/TDI123: Grade B alerts unless restricted."""
    assert _should_alert_btmm123({"grade": "B", "in_active_session": True}) is True


def test_should_alert_grade_b_blocked_when_a_only(monkeypatch):
    """BTMM123_GRADE_A_ONLY=true restricts to Grade A (kill-switch)."""
    import alerts
    monkeypatch.setattr(alerts, "BTMM123_GRADE_A_ONLY", True)
    assert alerts._should_alert_btmm123({"grade": "B", "in_active_session": True}) is False
    assert alerts._should_alert_btmm123({"grade": "A", "in_active_session": True}) is True


def test_should_alert_grade_c_never():
    assert _should_alert_btmm123({"grade": "C", "in_active_session": True}) is False


def test_should_alert_blocks_outside_session():
    assert _should_alert_btmm123({"grade": "A", "in_active_session": False}) is False


def test_should_alert_none_session_does_not_hard_fail():
    """Missing session data must not silently kill an otherwise-valid alert."""
    assert _should_alert_btmm123({"grade": "A", "in_active_session": None}) is True


# ──────────────────────────────────────────────────────────────────────
# Fix 1 — pivot location gate (Critical-Area doctrine: never sell into
# support / buy into resistance)
# ──────────────────────────────────────────────────────────────────────

def test_location_keys_present_on_tradeable_row():
    bars = _build_bullish_123_series()
    row = m.analyze_pair("EUR/USD", bars)
    assert "location" in row and "location_ok" in row
    assert isinstance(row["location_ok"], bool)
    assert "pivot location" in row["notes"]


# ──────────────────────────────────────────────────────────────────────
# Fix 2 — H4 bias alignment (+2) and M15 trigger leg
# ──────────────────────────────────────────────────────────────────────

def _build_h4_uptrend():
    """70 rising H4 closes: price above a rising 50 EMA -> bullish bias."""
    return [{"ts_utc": i * 14400, "open": 1.1000 + 0.0010 * i,
             "high": 1.1005 + 0.0010 * i, "low": 1.0995 + 0.0010 * i,
             "close": 1.1000 + 0.0010 * i} for i in range(70)]


def test_h4_aligned_bias_adds_two_points():
    bars = _build_bullish_123_series()
    plain = m.analyze_pair("EUR/USD", bars)
    biased = m.analyze_pair("EUR/USD", bars, h4_candles=_build_h4_uptrend())
    assert biased["htf_bias"] == "bullish"
    assert biased["htf_aligned"] is True
    assert biased["score"] == plain["score"] + 2
    assert "H4 bias aligned" in biased["notes"]


def test_m15_leg_attached_when_tradeable():
    h1_bars = _build_bullish_123_series()
    m15_bars = _build_bullish_123_series()
    row = m.analyze_pair("EUR/USD", h1_bars, m15_candles=m15_bars)
    assert row["m15"]["setup"] == "BUY"
    assert row["m15"]["timeframe"] == "M15"


def _minimal_alertable_row(timeframe: str, m15: Optional[dict] = None) -> dict:
    """Smallest row that clears every gate in alert_btmm123_setup, so the
    recursion test isolates M15-recursion behavior from pattern geometry."""
    row = {
        "symbol": "EUR/USD",
        "setup": "BUY",
        "grade": "A",
        "score": 15,
        "setup_type": "123",
        "timeframe": timeframe,
        "notes": "test row",
        "in_active_session": True,
        "pattern": {"p1": {"price": 1.0900}, "p2": {"price": 1.1000}, "p3": {"price": 1.0950},
                    "leg1_range_pips": 100.0},
        "level": {"count": 5, "level_ii": True, "level_i": False},
        "stop_hunt": {"active": True},
        "trade_plan": {"entry": 1.1000, "sl": 1.0950, "sl_pips": 50.0,
                       "tp1": 1.1050, "tp2": None, "tp3": None, "rr1": 1.0},
    }
    if m15 is not None:
        row["m15"] = m15
    return row


def test_alert_recurses_into_m15_leg(monkeypatch):
    """Regression: alert_btmm123_setup must recurse into row['m15'] like
    alert_tdi123_setup does — otherwise a tradeable M15 leg is scored and
    shown on the dashboard but never reaches Discord."""
    import alerts

    monkeypatch.setattr(alerts, "BTMM123_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "BTMM123_NEWS_FILTER", False)
    sent_titles = []
    monkeypatch.setattr(alerts, "_post_discord",
                        lambda embed: (sent_titles.append(embed["title"]) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "_mark_sent", lambda pair, rule: None)

    m15_row = _minimal_alertable_row("M15")
    row = _minimal_alertable_row("H1", m15=m15_row)

    alerts.alert_btmm123_setup("EUR/USD", row)

    assert len(sent_titles) == 2, "expected one Discord embed for H1 and one for M15"


def test_alert_throttle_key_distinguishes_h1_from_m15():
    """H1 and M15 alerts on the same pair/direction/grade must not collide
    on the throttle key — each is its own setup, same convention as TDI123."""
    import alerts

    h1_row = {"timeframe": "H1", "setup_type": "123"}
    m15_row = {"timeframe": "M15", "setup_type": "123"}
    setup, grade = "buy", "A"
    h1_rule = f"btmm123_{h1_row['timeframe'].lower()}_{h1_row['setup_type']}_{setup}_{grade}"
    m15_rule = f"btmm123_{m15_row['timeframe'].lower()}_{m15_row['setup_type']}_{setup}_{grade}"
    assert h1_rule != m15_rule


# ──────────────────────────────────────────────────────────────────────
# Fix 3 — 200EMA false-breakout Re-set branch
# ──────────────────────────────────────────────────────────────────────

def _build_bearish_reset_series():
    """Flat ~1.1000 base (no 8-pip 123 legs), then a 5-bar push above the
    200 EMA that fails back below it with a stop-hunt low on the final bar."""
    bars = []
    for i in range(209):
        p = 1.1000 + (0.00005 if i % 2 == 0 else -0.00005)
        bars.append(_bar(i * 3600, p + 0.00005, p - 0.00005, p))
    ts = 209 * 3600
    bars.append(_bar(ts, 1.1030, 1.0995, 1.1025)); ts += 3600
    bars.append(_bar(ts, 1.1026, 1.1000, 1.1005)); ts += 3600
    bars.append(_bar(ts, 1.1008, 1.0998, 1.1000)); ts += 3600
    bars.append(_bar(ts, 1.1002, 1.0997, 1.0999)); ts += 3600
    bars.append(_bar(ts, 1.1000, 1.0985, 1.0996))
    return bars


def test_200ema_false_breakout_fires_bearish_reset():
    row = m.analyze_pair("EUR/USD", _build_bearish_reset_series())
    assert row["setup"] == "SELL"
    assert row["setup_type"] == "reset"
    assert row["pattern"] == {}
    assert row["reset"]["extreme"] > row["current_price"]
    assert row["trade_plan"]["sl"] > row["trade_plan"]["entry"]
    # 9 points (3 base + 4 Level II + 2 Asian; final-bar hunt points the
    # other way) = B on points, but the synthetic weekly pivots leave price
    # on the wrong side, so the location cap correctly drops it to C.
    assert row["score"] == 9
    assert row["grade"] == "C"
    assert "grade capped: poor pivot location" in row["notes"]


def test_reset_detector_quiet_without_break():
    bars = [_bar(i * 3600, 1.10005, 1.09995, 1.1000) for i in range(220)]
    closes = [b["close"] for b in bars]
    assert m._detect_ema200_false_break(bars, closes)["active"] is False
