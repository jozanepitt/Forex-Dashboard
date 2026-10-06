"""Tests for the BTMM ID50 strategy — the authoritative M15 intraday 50-EMA
bounce scanner (13/50 cross → move-away → first-quality 50-EMA retest with a
trap/rejection trigger, TDI confirmed), scored 0-26 with grades A+/A/B/C.

The legacy 5-gate bounce5050 inside btmm_core.analyze() (which the dashboard
labels "50/50 Bounce == ID 50") is deliberately NOT touched — it stays parallel
as the H1-shape setup, and this file tests only what's new here: the cross/
anchor/move-away/first-retest/trap detectors, the 26-pt scoring, the grade
bands, the trade-plan math, the A+/A/B filter tile counts, and the alert gate.
"""
from __future__ import annotations

import btmm_id50 as m
from alerts import _should_alert_id50


# ──────────────────────────────────────────────────────────────────────
# _grade_from_score — 26-pt scale with A+/A/B/C/NO-TRADE thresholds
# ──────────────────────────────────────────────────────────────────────

def test_grade_thresholds():
    assert m._grade_from_score(20) == "A+"
    assert m._grade_from_score(26) == "A+"
    assert m._grade_from_score(19) == "A"
    assert m._grade_from_score(18) == "A"
    assert m._grade_from_score(17) == "B"
    assert m._grade_from_score(15) == "B"
    assert m._grade_from_score(14) == "C"
    assert m._grade_from_score(12) == "C"
    assert m._grade_from_score(11) == "NO-TRADE"
    assert m._grade_from_score(0) == "NO-TRADE"


# ──────────────────────────────────────────────────────────────────────
# _cross_state — last 13/50 EMA cross, direction + age
# ──────────────────────────────────────────────────────────────────────

def test_cross_state_finds_recent_bullish_cross():
    """Flat closes then a steady rally -> the single e13/e50 cross is near the
    end of the series and directional."""
    closes = [1.1000] * 60 + [1.1000 + 0.0006 * i for i in range(1, 13)]
    cs = m._cross_state(closes)
    assert cs["direction"] == "bullish"
    assert cs["idx"] is not None
    assert cs["bars_ago"] <= 20


def test_cross_state_flat_series_has_none():
    cs = m._cross_state([1.1000] * 80)
    assert cs["idx"] is None


# ──────────────────────────────────────────────────────────────────────
# _anchor_fractal — H2: pre-impulse swing fracture at/before the cross
# ──────────────────────────────────────────────────────────────────────

def test_anchor_fractal_picks_recent_swing_low_before_cross():
    bars = []
    ts = 0
    step = 900
    # drift up, with one distinct swing low 8 bars before the end (pre-cross)
    for i in range(20):
        p = 1.1000 + 0.0002 * i
        bars.append({"ts_utc": ts, "open": p, "high": p + 0.0001,
                     "low": p - 0.0001, "close": p})
        ts += step
    # the swing low at idx 12
    bars[12] = {"ts_utc": 12 * step, "open": 1.1015, "high": 1.1016,
                "low": 1.0999, "close": 1.1002}
    cross_idx = 17
    a = m._anchor_fractal(bars, cross_idx, "bullish")
    assert a is not None
    assert a["anchor"] == 1.0999


def test_anchor_fractal_none_without_pre_cross_low():
    bars = [{"ts_utc": i * 900, "open": 1.1000 + 0.0002 * i,
             "high": 1.1001 + 0.0002 * i, "low": 1.0999 + 0.0002 * i,
             "close": 1.1000 + 0.0002 * i} for i in range(20)]
    assert m._anchor_fractal(bars, 10, "bullish") is None


# ──────────────────────────────────────────────────────────────────────
# _move_away — H5: max extension past e50 since the cross
# ──────────────────────────────────────────────────────────────────────

def test_move_away_detects_meaningful_extension():
    closes = [1.1000] * 60 + [1.1000 + 0.0010 * i for i in range(1, 25)]
    cs = m._cross_state(closes)
    bars = [{"ts_utc": i * 900, "open": c, "high": c + 0.0005,
             "low": c - 0.0005, "close": c} for i, c in enumerate(closes)]
    atr = m._atr14(bars)
    mv = m._move_away(bars, closes, cs["idx"], "bullish", atr)
    assert mv["ok"] is True
    assert mv["magnitude_atr"] >= m.MOVE_AWAY_ATR_MULT


def test_move_away_fails_without_extension():
    closes = [1.1000] * 80
    bars = [{"ts_utc": i * 900, "open": c, "high": c + 0.0001,
             "low": c - 0.0001, "close": c} for i, c in enumerate(closes)]
    atr = m._atr14(bars)
    mv = m._move_away(bars, closes, 5, "bullish", atr)
    assert mv["ok"] is False


# ──────────────────────────────────────────────────────────────────────
# _first_retest — H6: first-quality retest, no prior close-through
# ──────────────────────────────────────────────────────────────────────

def test_first_retest_ok_on_clean_pullback():
    closes = [1.1000] * 60 + [1.1000 + 0.0008 * i for i in range(1, 25)]
    cs = m._cross_state(closes)
    bars = [{"ts_utc": i * 900, "open": c, "high": c + 0.0004,
             "low": c - 0.0004, "close": c} for i, c in enumerate(closes)]
    atr = m._atr14(bars)
    mv = m._move_away(bars, closes, cs["idx"], "bullish", atr)
    peak_cross = closes[mv["peak_idx"]]
    # genuine pullback: two shallow descend bars that never touch e50, then a
    # final bar that wicks into e50 and closes back on the trade side
    bars = bars[:(mv["peak_idx"] + 1)]
    for step_down in (0.0003, 0.0005):
        p = peak_cross - step_down
        bars.append({"ts_utc": len(bars) * 900, "open": p + 0.0001,
                     "high": p + 0.0002, "low": p - 0.0008, "close": p + 0.0001})
    e50v = m.calc_e50([b["close"] for b in bars])
    bars.append({"ts_utc": len(bars) * 900, "open": e50v + 0.0001,
                 "high": e50v + 0.0004, "low": e50v - 0.0005,
                 "close": e50v + 0.0003})
    fr = m._first_retest(bars, peak_cross, mv["peak_idx"], "bullish",
                         retrace_min_frac=m.RETRACE_MIN_FRAC)
    assert fr["ok"] is True
    assert fr["prior_close_through"] is False


def test_first_retest_fails_on_prior_close_through():
    """Regression pin: a bar after the peak that retested e50 AND closed back
    on the trade side is a SECOND retest — the first bounce already churned.
    H6 must reject it."""
    closes = [1.1000] * 60 + [1.1000 + 0.0008 * i for i in range(1, 25)]
    cs = m._cross_state(closes)
    bars = [{"ts_utc": i * 900, "open": c, "high": c + 0.0004,
             "low": c - 0.0004, "close": c} for i, c in enumerate(closes)]
    atr = m._atr14(bars)
    mv = m._move_away(bars, closes, cs["idx"], "bullish", atr)
    peak_cross = closes[mv["peak_idx"]]
    bars = bars[:(mv["peak_idx"] + 1)]
    # first retest that resolved (touched e50 and closed back on the trade side)
    e50_0 = m.calc_e50([b["close"] for b in bars])
    bars.append({"ts_utc": len(bars) * 900, "open": e50_0 + 0.0004,
                 "high": e50_0 + 0.0005, "low": e50_0 - 0.0006,
                 "close": e50_0 + 0.0002})
    # current bar — also at e50, but this is now the SECOND retest
    e50v = m.calc_e50([b["close"] for b in bars])
    bars.append({"ts_utc": len(bars) * 900, "open": e50v + 0.0001,
                 "high": e50v + 0.0003, "low": e50v - 0.0005,
                 "close": e50v + 0.0002})
    fr = m._first_retest(bars, peak_cross, mv["peak_idx"], "bullish",
                         retrace_min_frac=m.RETRACE_MIN_FRAC)
    assert fr["ok"] is False
    assert fr["prior_close_through"] is True


# ──────────────────────────────────────────────────────────────────────
# _ema_trap — H7: wick into e50, reject and close back on the trade side
# ──────────────────────────────────────────────────────────────────────

def test_ema_trap_detects_rejection():
    closes = [1.1000] * 70 + [1.1010, 1.1012, 1.1013, 1.1014, 1.1015]
    bars = [{"ts_utc": i * 900, "open": c, "high": c + 0.0002,
             "low": c - 0.0002, "close": c} for i, c in enumerate(closes)]
    # final bar dips through e50 with a long lower wick, then closes back above
    e50 = m.calc_e50(closes)
    final = bars[-1]
    final["low"] = e50 - 0.0004
    final["open"] = e50 - 0.0001
    final["close"] = e50 + 0.0002
    tr = m._ema_trap(bars, "bullish", atr=0.0004, pip=0.0001)
    assert tr["trap"] is True
    assert tr["rejection"] is True


def test_ema_trap_fails_without_wick_through():
    closes = [1.1000] * 70 + [1.1010, 1.1012, 1.1013, 1.1014, 1.1015]
    bars = [{"ts_utc": i * 900, "open": c, "high": c + 0.0002,
             "low": c - 0.0002, "close": c} for i, c in enumerate(closes)]
    tr = m._ema_trap(bars, "bullish", atr=0.0004, pip=0.0001)
    # last bar is well above e50, no overlap -> no trap
    assert tr["trap"] is False


# ──────────────────────────────────────────────────────────────────────
# _entry_candle — H8: nameable candle / outside-bar / big-body close
# ──────────────────────────────────────────────────────────────────────

def test_entry_candle_hammer_ok():
    prev = {"ts_utc": 0, "open": 1.10000, "high": 1.10002, "low": 1.09998, "close": 1.10001}
    # body 0.0002 > 15% of 0.0008 range (not a Doji); lower wick 0.0005 >= 2x body;
    # upper wick 0.0001 < body -> Hammer per detect_nameable_candle
    curr = {"ts_utc": 900, "open": 1.1000, "high": 1.1003, "low": 1.0995,
            "close": 1.1002}
    bars = [prev, curr]
    assert m._entry_candle(bars, "bullish", atr=0.0005)["ok"] is True


def test_entry_candle_doji_fails():
    prev = {"ts_utc": 0, "open": 1.1000, "high": 1.1000, "low": 1.0999, "close": 1.1000}
    curr = {"ts_utc": 900, "open": 1.1000, "high": 1.1001, "low": 1.0999,
            "close": 1.1000}
    bars = [prev, curr]
    assert m._entry_candle(bars, "bullish", atr=0.0005)["ok"] is False


def test_entry_candle_big_body_bullish_ok():
    prev = {"ts_utc": 0, "open": 1.1000, "high": 1.1000, "low": 1.0999, "close": 1.1000}
    curr = {"ts_utc": 900, "open": 1.1001, "high": 1.1009, "low": 1.1000,
            "close": 1.1008}
    bars = [prev, curr]
    assert m._entry_candle(bars, "bullish", atr=0.0003)["ok"] is True


# _detect_star + _entry_candle — H8: Morning/Evening Star (PDF entry trigger)
# ──────────────────────────────────────────────────────────────────────

MORNING_STAR_TRIPLE = [
    {"ts_utc": 0, "open": 1.1020, "high": 1.1022, "low": 1.1015, "close": 1.1016},  # long bearish leg
    {"ts_utc": 900, "open": 1.1015, "high": 1.1016, "low": 1.1014, "close": 1.1015},  # small star below
    {"ts_utc": 1800, "open": 1.1016, "high": 1.1022, "low": 1.1016, "close": 1.1021},  # long bullish into c1
]

EVENING_STAR_TRIPLE = [
    {"ts_utc": 0, "open": 1.0980, "high": 1.0986, "low": 1.0979, "close": 1.0985},  # long bullish leg
    {"ts_utc": 900, "open": 1.0985, "high": 1.0987, "low": 1.0985, "close": 1.0986},  # small star above
    {"ts_utc": 1800, "open": 1.0985, "high": 1.0986, "low": 1.0979, "close": 1.0980},  # long bearish into c1
]


def test_detect_star_morning_and_evening():
    assert m._detect_star(MORNING_STAR_TRIPLE)["name"] == "Morning Star"
    assert m._detect_star(EVENING_STAR_TRIPLE)["name"] == "Evening Star"


def test_detect_star_requires_three_bars():
    assert m._detect_star(MORNING_STAR_TRIPLE[:2])["found"] is False


def test_detect_star_rejects_when_third_candle_does_not_close_into_first_body():
    bars = [
        MORNING_STAR_TRIPLE[0],
        MORNING_STAR_TRIPLE[1],
        {"ts_utc": 1800, "open": 1.1016, "high": 1.1018, "low": 1.1016, "close": 1.1017},  # above
    ]
    # close 1.1017 < c1 mid 1.1018 -> no close INTO c1's body
    assert m._detect_star(bars)["found"] is False


def test_detect_star_rejects_oversized_star_body():
    bars = [
        MORNING_STAR_TRIPLE[0],
        {"ts_utc": 900, "open": 1.1018, "high": 1.1019, "low": 1.1014, "close": 1.1015},
        MORNING_STAR_TRIPLE[2],
    ]
    # star body 0.0003 is not small relative to the 0.0004 first leg
    assert m._detect_star(bars)["found"] is False


def test_entry_candle_morning_star_ok():
    res = m._entry_candle(MORNING_STAR_TRIPLE, "bullish", atr=0.0005)
    assert res["ok"] is True
    assert "Morning Star" in res["reason"]


def test_entry_candle_evening_star_ok():
    res = m._entry_candle(EVENING_STAR_TRIPLE, "bearish", atr=0.0005)
    assert res["ok"] is True
    assert "Evening Star" in res["reason"]


def test_entry_candle_star_wrong_direction_fails():
    assert m._entry_candle(MORNING_STAR_TRIPLE, "bearish", atr=0.0005)["ok"] is False


# ──────────────────────────────────────────────────────────────────────
# Trade plan — SL floor + 2-pip clamp, TP1 = move-away peak, room
# ──────────────────────────────────────────────────────────────────────

def test_trade_plan_sl_floor_and_tp_peak():
    plan = m._trade_plan(symbol="EUR/USD", price=1.1000, direction="bullish",
                         anchor=1.0990, retest_low=1.0993, atr=0.0004,
                         peak=1.1040, e50=1.1010)
    assert plan["entry"] == 1.1000
    # raw: min(anchor,retest_low) - 0.5*ATR = 1.0990 - 0.0002 = 1.0988
    assert abs(plan["sl"] - 1.0988) < 1e-9
    assert plan["tp1"] == 1.1040
    assert plan["sl_pips"] >= 10  # 10-pip min-sl floor on EUR/USD, never tighter
    assert plan["tp2"] > plan["tp1"]
    assert plan["tp3"] > plan["tp2"]
    assert plan["room_pips"] is not None


def test_trade_plan_room_scores_when_two_sl_risks():
    # room = peak - e50 >= 2 * (entry - sl) -> buy room_ok
    sl_dist = 0.0006
    plan = m._trade_plan(symbol="EUR/USD", price=1.1000, direction="bullish",
                         anchor=1.0992, retest_low=1.0994, atr=0.0004,
                         peak=1.1040, e50=1.1010)
    assert plan["room_ok"] is True


# ──────────────────────────────────────────────────────────────────────
# Scoring / hard-rule gate — building-block combos
# ──────────────────────────────────────────────────────────────────────

def test_cross_points_bracket():
    assert m._cross_points(5) == 2
    assert m._cross_points(15) == 1
    assert m._cross_points(30) == 0


def test_analyze_pair_insufficient_data_is_no_trade():
    bars = [{"ts_utc": i * 900, "open": 1.1000, "high": 1.1001,
             "low": 1.0999, "close": 1.1000} for i in range(10)]
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"
    assert row["grade"] == "NO-DATA"
    assert row["score"] == 0


def test_analyze_pair_flat_series_has_no_trade():
    bars = [{"ts_utc": i * 900, "open": 1.1000 + 0.00001 * (i % 2),
             "high": 1.10005, "low": 1.09995,
             "close": 1.1000 + 0.00001 * (i % 2)} for i in range(260)]
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"
    assert row["grade"] == "NO-TRADE"
    assert row["score"] == 0


# ──────────────────────────────────────────────────────────────────────
# Backtest sweep override — ID50_PARAMS must flow through analyze_pair
# ──────────────────────────────────────────────────────────────────────

def test_resolve_params_merges_sweep_overrides():
    base = m._resolve_params(None)
    for k, v in m.ID50_PARAMS.items():
        assert base[k] == v
    overridden = m._resolve_params({"MOVE_AWAY_ATR_MULT": 0.25})
    assert overridden["MOVE_AWAY_ATR_MULT"] == 0.25
    assert overridden["RETEST_TOL_ATR"] == m.ID50_PARAMS["RETEST_TOL_ATR"]
    # unknown keys pass through untouched (harmless for future knobs)
    assert m._resolve_params({"NEW_KNOB": 1.0})["NEW_KNOB"] == 1.0


def test_analyze_pair_params_override_turns_the_knob():
    """A punitively strict move-away mult must reject the known-good series,
    proving the sweep override actually reaches the H5 hard-rule gate."""
    bars = _build_bullish_id50_series()
    strict = m.analyze_pair("EUR/USD", bars, params={"MOVE_AWAY_ATR_MULT": 50.0})
    assert strict["setup"] == "NO-TRADE"
    assert "move-away" in (strict.get("reason") or "")


# ──────────────────────────────────────────────────────────────────────
# _should_alert_id50 — grade + session gate (mirrors BTMM123/TDI123)
# ──────────────────────────────────────────────────────────────────────

def test_should_alert_grade_aplus_always():
    assert _should_alert_id50({"grade": "A+", "in_active_session": True}) is True


def test_should_alert_grade_a_always():
    assert _should_alert_id50({"grade": "A", "in_active_session": True}) is True


def test_should_alert_grade_b_blocked_by_default():
    assert _should_alert_id50({"grade": "B", "in_active_session": True}) is False


def test_should_alert_grade_b_allowed_when_a_only_disabled(monkeypatch):
    import alerts
    monkeypatch.setattr(alerts, "BTMM_ID50_GRADE_A_ONLY", False)
    assert alerts._should_alert_id50({"grade": "B", "in_active_session": True}) is True
    assert alerts._should_alert_id50({"grade": "A", "in_active_session": True}) is True


def test_should_alert_grade_c_never():
    assert _should_alert_id50({"grade": "C", "in_active_session": True}) is False
    assert _should_alert_id50({"grade": "NO-TRADE", "in_active_session": True}) is False


def test_should_alert_blocks_outside_session():
    assert _should_alert_id50({"grade": "A+", "in_active_session": False}) is False


def test_should_alert_none_session_does_not_hard_fail():
    assert _should_alert_id50({"grade": "A", "in_active_session": None}) is True


# ──────────────────────────────────────────────────────────────────────
# End-to-end walk-forward smoke pin (the behavioural lock from the spec):
#   known-good series -> TRADE (BUY, tradeable grade)
#   second-retest series -> NO-TRADE
# ──────────────────────────────────────────────────────────────────────

def _build_bullish_id50_series() -> list[dict]:
    """Hand-built M15 series: flat seed -> dips, crosses 13/50, extends well
    past e50 (move-away), pulls back cleanly (no prior close-through), and the
    final bar wicks through e50, rejects, and closes on the trade side."""
    bars = []
    ts = 0
    step = 900
    # seed 210 flat bars ~1.1000 (210+10+45+3+1 = 269 >= MIN_DATA_BARS 220)
    for i in range(210):
        p = 1.1000 + (0.00003 if i % 2 == 0 else -0.00003)
        bars.append({"ts_utc": ts, "open": p, "high": p + 0.00003,
                     "low": p - 0.00003, "close": p}); ts += step
    # gentle dip (anchor zone) ~1.0992
    dip = 1.0992
    for i in range(10):
        p = 1.1000 - 0.00008 * (i + 1)
        bars.append({"ts_utc": ts, "open": p, "high": p + 0.0002,
                     "low": p - 0.0002, "close": p}); ts += step
    # rally: 45 rising bars to ~1.1085 (crosses e13/e50; big move-away)
    for i in range(45):
        p = 1.0990 + 0.00021 * i
        bars.append({"ts_utc": ts, "open": p, "high": p + 0.0004,
                     "low": p - 0.0002, "close": p}); ts += step
    peak_idx = len(bars) - 1
    peak = bars[peak_idx]["close"]
    # pullback: 3 bars trending down, closes stay high, no prior close-through
    for step_down in (0.00035, 0.00055, 0.00070):
        p = peak - step_down
        bars.append({"ts_utc": ts, "open": p + 0.0001, "high": p + 0.0002,
                     "low": p - 0.0003, "close": p + 0.0001}); ts += step
    # final trap bar: wick through e50, long lower wick, close back above
    e50 = m.calc_e50([b["close"] for b in bars])
    p = e50 - 0.00005
    bars.append({"ts_utc": ts, "open": p + 0.00005, "high": p + 0.0003,
                 "low": p - 0.0006, "close": p + 0.00025})
    return bars


def test_analyze_pair_detects_bullish_id50():
    bars = _build_bullish_id50_series()
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "BUY"
    assert row["grade"] in ("A", "A+", "B", "C")
    assert row["score"] >= 12
    id50 = row
    assert id50["anchor"] is not None
    assert id50["move_away"]["ok"] is True
    assert id50["retest"]["ok"] is True
    assert id50["trap"]["trap"] is True
    plan = row["trade_plan"]
    assert plan["entry"] == row["current_price"]
    assert plan["sl"] < plan["entry"]
    assert plan["tp1"] >= plan["entry"]


def test_analyze_pair_second_retest_is_no_trade():
    """Take the known-good series, add an extra resolved retest before the
    final pullback -> H6 must reject as a second retest, NOT trade."""
    bars = _build_bullish_id50_series()
    # rebuild: insert a resolved dip-and-close-through (touches e50, closes back
    # on the trade side) between peak and final -> the final trap bar is now a
    # SECOND retest and H6 must reject it
    ts = bars[-1]["ts_utc"] + 900
    e50now = m.calc_e50([b["close"] for b in bars])
    close_through = {"ts_utc": ts, "open": e50now + 0.0004, "high": e50now + 0.0005,
                     "low": e50now - 0.0006, "close": e50now + 0.0002}
    ts2 = ts + 900
    resolve = {"ts_utc": ts2, "open": e50now + 0.0001, "high": e50now + 0.0003,
               "low": e50now + 0.00005, "close": e50now + 0.00025}
    tail = [b for b in bars if b["ts_utc"] >= bars[-2]["ts_utc"]]
    bars = [b for b in bars if b["ts_utc"] < bars[-2]["ts_utc"]]
    bars = bars[:-3]
    bars.extend([close_through, resolve])
    bars.extend(tail)
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE", row


# ──────────────────────────────────────────────────────────────────────
# analyze_universe — shape matches the dashboard convention
# ──────────────────────────────────────────────────────────────────────

def test_analyze_universe_shape():
    bars = _build_bullish_id50_series()
    result = m.analyze_universe({"EUR/USD": {"m15": bars}})
    assert "pairs" in result and "buys" in result and "sells" in result
    assert "grade_a" in result and "grade_aplus" in result and "grade_b" in result
    row = next(p for p in result["pairs"] if p["symbol"] == "EUR/USD")
    assert row["setup"] in ("BUY", "SELL", "NO-TRADE")


# ──────────────────────────────────────────────────────────────────────
# _watch_row — precursor "Pairs to Watch" (fresh 13/50 cross + anchor)
# ──────────────────────────────────────────────────────────────────────

def _watch_bars():
    """M15 bars: 60 flat then a steady rally -> fresh bullish e13/e50 cross
    near the end (bars_ago <= 20), with a pre-cross swing low at idx 5 that
    anchors the setup (per test_anchor_fractal_picks_recent_swing_low_before_cross).

    The flat seed's LOWs rise strictly (2e-5/bar) so no trivially-equal
    flat-bar low hijacks the 5-bar fractal; the dip bar at idx 5 keeps its
    CLOSE at 1.1000 so the dip never dips e13 through e50 (a close dip would
    age the last cross out: idx -> 21, bars_ago -> 50, stale)."""
    closes = [1.1000] * 60 + [1.1000 + 0.0006 * i for i in range(1, 13)]
    bars = []
    for i, c in enumerate(closes):
        if i < 60:
            bars.append({"ts_utc": i * 900, "open": 1.1000, "high": 1.1001,
                         "low": 1.0999 + i * 0.000002, "close": 1.1000})
        else:
            bars.append({"ts_utc": i * 900, "open": c, "high": c + 0.0001,
                         "low": c - 0.0001, "close": c})
    bars[5] = {"ts_utc": 5 * 900, "open": 1.1000, "high": 1.1001,
               "low": 1.0998, "close": 1.1000}
    return bars


def test_watch_row_present_for_fresh_cross_plus_anchor():
    row = m._watch_row("EUR/USD", _watch_bars(), {"setup": "NO-TRADE"})
    assert row is not None
    assert row["symbol"] == "EUR/USD"
    assert row["direction"] == "bullish"
    assert row["cross_age"] <= m.CROSS_AGE_ACCEPT_MAX
    assert row["anchor"] == 1.0998
    assert row["price"] > 1.10


def test_watch_row_excludes_stale_cross():
    bars = _watch_bars()
    last = bars[-1]["close"]
    for i in range(1, 41):  # 40 slow continuation bars -> cross ages past 20 bars
        c = last + 0.0001 * i
        bars.append({"ts_utc": len(bars) * 900, "open": c - 0.0001,
                     "high": c + 0.0001, "low": c - 0.0001, "close": c})
    closes = [b["close"] for b in bars]
    cs = m._cross_state(closes)
    assert cs["direction"] == "bullish"               # same (last) cross, just older
    assert cs["bars_ago"] > m.CROSS_AGE_ACCEPT_MAX    # fixture is genuinely stale
    assert m._watch_row("EUR/USD", bars, {"setup": "NO-TRADE"}) is None


def test_watch_row_excludes_missing_anchor(monkeypatch):
    bars = _watch_bars()
    cs = m._cross_state([b["close"] for b in bars])
    assert cs["idx"] is not None and cs["bars_ago"] <= m.CROSS_AGE_ACCEPT_MAX
    # Real anchor-less M15 data with a FRESH cross is effectively impossible:
    # the turn that makes the cross fresh is itself a 5-bar fractal low. Pin
    # the W2 gate in isolation - `_anchor_fractal` missing is exactly what an
    # anchor-less impulse yields (verified: it returns None on, e.g., a
    # monotonic ramp, so the gate is real).
    monkeypatch.setattr(m, "_anchor_fractal", lambda *a, **k: None)
    assert m._watch_row("EUR/USD", bars, {"setup": "NO-TRADE"}) is None


def test_watch_row_dedupes_same_direction_live_signal():
    bars = _watch_bars()
    assert m._watch_row("EUR/USD", bars, {"setup": "BUY"}) is None      # bullish cross + BUY
    row = m._watch_row("EUR/USD", bars, {"setup": "SELL"})              # per-direction dedupe
    assert row is not None
    assert row["direction"] == "bullish"


def test_watch_row_none_on_short_data():
    assert m._watch_row("EUR/USD", _watch_bars()[:40], {"setup": "NO-TRADE"}) is None


def _bearish_watch_bars():
    """M15 mirror of `_watch_bars`: 60 flat then a steady decline -> fresh
    bearish e13/e50 cross near the end (bars_ago <= 20), with a pre-cross
    swing HIGH at idx 5 that anchors the setup (bearish 5-bar fractal).

    The flat seed's HIGHs fall strictly (2e-5/bar) so no trivially-equal
    flat-bar high hijacks the fractal; the spike bar at idx 5 keeps its CLOSE
    at 1.1000 so e13/e50 timing is untouched (a close spike above the seed
    would age the last bearish cross out)."""
    closes = [1.1000] * 60 + [1.1000 - 0.0006 * i for i in range(1, 13)]
    bars = []
    for i, c in enumerate(closes):
        if i < 60:
            bars.append({"ts_utc": i * 900, "open": 1.1000,
                         "high": 1.1002 - i * 0.000002, "low": 1.0999,
                         "close": 1.1000})
        else:
            bars.append({"ts_utc": i * 900, "open": c, "high": c + 0.0001,
                         "low": c - 0.0001, "close": c})
    bars[5] = {"ts_utc": 5 * 900, "open": 1.1000, "high": 1.1003,
               "low": 1.0999, "close": 1.1000}
    return bars


def test_watch_row_bearish_mirror_present_and_dedupe():
    """Bearish coverage for the untested W2/W4 legs: a real bearish cross +
    swing-high anchor yields a bearish watch row, and the per-direction dedupe
    mirrors the bullish case (SELL live signal suppresses it, BUY does not)."""
    bars = _bearish_watch_bars()
    row = m._watch_row("EUR/USD", bars, {"setup": "NO-TRADE"})
    assert row is not None
    assert row["direction"] == "bearish"
    assert row["cross_age"] <= m.CROSS_AGE_ACCEPT_MAX
    assert row["anchor"] == 1.1003
    assert row["price"] < 1.10
    assert m._watch_row("EUR/USD", bars, {"setup": "SELL"}) is None  # bearish cross + SELL
    mirror = m._watch_row("EUR/USD", bars, {"setup": "BUY"})         # per-direction dedupe
    assert mirror is not None
    assert mirror["direction"] == "bearish"


# ──────────────────────────────────────────────────────────────────────
# analyze_universe wiring — watches + watch_count (freshest cross first)
# ──────────────────────────────────────────────────────────────────────

def test_analyze_universe_watches():
    a_bars = _watch_bars()                      # fresh bullish cross + anchor
    b_bars = _watch_bars()                      # same series, cross made 8 bars older
    last = b_bars[-1]["close"]
    for i in range(1, 9):
        c = last + 0.0001 * i
        b_bars.append({"ts_utc": len(b_bars) * 900, "open": c - 0.0001,
                       "high": c + 0.0001, "low": c - 0.0001, "close": c})
    cs = m._cross_state([b["close"] for b in b_bars])
    assert cs["direction"] == "bullish"               # same (last) cross, just older
    assert cs["bars_ago"] <= m.CROSS_AGE_ACCEPT_MAX   # fixture sanity: b_bars must stay fresh
    result = m.analyze_universe({"GBP/USD": {"m15": b_bars},
                                 "EUR/USD": {"m15": a_bars}})
    syms = [w["symbol"] for w in result["watches"]]
    assert result["watch_count"] == 2
    assert set(syms) == {"EUR/USD", "GBP/USD"}
    ages = [w["cross_age"] for w in result["watches"]]
    assert ages == sorted(ages)                 # freshest cross first
    assert result["watches"][0]["symbol"] == "EUR/USD"


def _real_buy_fresh_cross_bars():
    """>=220 M15 bars that drive the FULL ID50 pipeline to a real BUY with a
    FRESH 13/50 cross (bars_ago <= CROSS_AGE_ACCEPT_MAX), unlike the stale
    (age ~40) cross the legacy `_build_bullish_id50_series` produces. Structure:
    210-bar flat seed -> 10-bar dip -> 10-bar rally (the e13/e50 cross, age 7)
    -> 3-bar pullback -> final 50-EMA trap bar. Verified empirically through
    `analyze_pair`: setup=="BUY", grade C, score 16, cross bullish age 7."""
    bars = []
    for i in range(210):                        # flat seed with tiny alternate wobble
        p = 1.1000 + (0.00003 if i % 2 == 0 else -0.00003)
        bars.append({"ts_utc": i * 900, "open": p, "high": p + 0.00003,
                     "low": p - 0.00003, "close": p})
    for i in range(10):                         # dip (pre-impulse low zone)
        p = 1.1000 - 0.0008 * (i + 1) / 10
        bars.append({"ts_utc": len(bars) * 900, "open": p, "high": p + 0.00006,
                     "low": p - 0.00006, "close": p})
    base = bars[-1]["close"]
    for i in range(10):                         # rally -> fresh e13/e50 cross
        p = base + 0.00021 * (i + 1)
        bars.append({"ts_utc": len(bars) * 900, "open": p, "high": p + 0.000105,
                     "low": p - 0.000105, "close": p})
    peak = bars[-1]["close"]
    for sd in (0.00035, 0.00055, 0.00070):      # shallow pullback (no prior close-through)
        p = peak - sd
        bars.append({"ts_utc": len(bars) * 900, "open": p + 0.0001,
                     "high": p + 0.0002, "low": p - 0.0003, "close": p + 0.0001})
    e50 = m.calc_e50([b["close"] for b in bars])
    p = e50 - 0.00005                           # final trap bar: wick through e50,
    bars.append({"ts_utc": len(bars) * 900,     # close back on the trade side
                 "open": p + 0.00005, "high": p + 0.0003,
                 "low": p - 0.0006, "close": p + 0.00025})
    return bars


def test_analyze_universe_excludes_real_buy_from_watches():
    """W4 end-to-end: a pair whose pipeline row is a REAL BUY (fresh cross,
    full ID50 sequence, score 16) must be absent from `watches`. This locks
    the dedupe contract via the real universe wiring, not `_watch_row` in
    isolation, with the gate provably W4: the same series yields a watch row
    (`_watch_row` with setup "NO-TRADE") because W1-W3 pass."""
    bars = _real_buy_fresh_cross_bars()
    assert len(bars) >= m.MIN_DATA_BARS                       # >=220 bars, full pipeline
    direct = m._watch_row("EUR/USD", bars, {"setup": "NO-TRADE"})
    assert direct is not None                                 # W1-W3 pass -> W4 must be
    assert direct["cross_age"] <= m.CROSS_AGE_ACCEPT_MAX      #   the only rejecting gate
    result = m.analyze_universe({"EUR/USD": {"m15": bars}})
    row = [p for p in result["pairs"] if p["symbol"] == "EUR/USD"][0]
    assert row["setup"] == "BUY"                              # real live signal present
    syms = [w["symbol"] for w in result["watches"]]
    assert "EUR/USD" not in syms                              # W4: BUY pair never watches
    assert result["watch_count"] == 0

# ──────────────────────────────────────────────────────────────────────
# ID50 BEFORE the 50/200 cross (user doctrine, 2026-10-01):
#   the 13/50 cross + break-away + 50-EMA rejection is the core trigger;
#   the 50/200 relationship is a GRADE discriminator, NOT a hard gate.
#   50/200 aligned in-direction -> A-eligible; not yet crossed -> cap at B.
# ──────────────────────────────────────────────────────────────────────

def _fresh_cross_below_200_series() -> list[dict]:
    """Long downtrend (keeps e200 elevated) -> bottom -> fresh bullish 13/50
    cross with price breaking above e50 while e50 is STILL below e200 (ID50
    BEFORE the 50/200 cross) -> shallow pullback -> 50-EMA trap. This is the
    case the OLD `price>e50>e200` hard gate wrongly rejected as 'EMA direction
    mismatch'; the relaxed H4 must accept it as a BUY, capped at grade B."""
    bars = []
    for i in range(70):                         # seed plateau ~1.1050 (feeds e200)
        p = 1.1050 + (0.00003 if i % 2 == 0 else -0.00003)
        bars.append({"ts_utc": len(bars) * 900, "open": p, "high": p + 0.00004,
                     "low": p - 0.00004, "close": p})
    for i in range(140):                        # GENTLE recent decline 1.1050 -> ~1.1000
        p = 1.1050 - 0.0000357 * i
        bars.append({"ts_utc": len(bars) * 900, "open": p, "high": p + 0.00006,
                     "low": p - 0.00006, "close": p})
    base = bars[-1]["close"]
    for i in range(10):                         # dip (pre-impulse low zone)
        p = base - 0.00006 * (i + 1)
        bars.append({"ts_utc": len(bars) * 900, "open": p, "high": p + 0.00006,
                     "low": p - 0.00006, "close": p})
    base2 = bars[-1]["close"]
    for i in range(12):                         # rally -> fresh e13/e50 cross up
        p = base2 + 0.00025 * (i + 1)
        bars.append({"ts_utc": len(bars) * 900, "open": p, "high": p + 0.000105,
                     "low": p - 0.000105, "close": p})
    peak = bars[-1]["close"]
    for sd in (0.00035, 0.00055, 0.00070):      # shallow pullback, no prior close-through
        p = peak - sd
        bars.append({"ts_utc": len(bars) * 900, "open": p + 0.0001,
                     "high": p + 0.0002, "low": p - 0.0003, "close": p + 0.0001})
    e50 = m.calc_e50([b["close"] for b in bars])
    p = e50 - 0.00005                           # final trap bar: wick through e50,
    bars.append({"ts_utc": len(bars) * 900,     # close back on the trade side
                 "open": p + 0.00005, "high": p + 0.0003,
                 "low": p - 0.0006, "close": p + 0.00025})
    return bars


def test_fresh_cross_before_50_200_cross_is_buy_not_mismatch():
    """Part 1 (relaxed H4): a fresh 13/50 cross + break-away + 50-EMA rejection
    with e50 STILL below e200 must produce a BUY, not an 'EMA direction
    mismatch' NO-TRADE."""
    bars = _fresh_cross_below_200_series()
    closes = [b["close"] for b in bars]
    e50 = m.calc_e50(closes)
    e200 = m.calc_ema(closes, 200)[-1]
    assert e50 < e200                           # scenario sanity: 50 still below 200
    assert closes[-1] > e50                     # price rejected back above the 50
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "BUY", row.get("reason")


def test_fresh_cross_capped_at_B_not_A():
    """Part 3 (grade cap): the pre-50/200-cross setup can never be A/A+; it is
    capped at B, and the scoring note records the un-aligned state."""
    bars = _fresh_cross_below_200_series()
    row = m.analyze_pair("EUR/USD", bars)
    assert row["grade"] in ("B", "C")           # never A/A+ before the 50/200 cross
    assert "not yet aligned" in row["notes"]


def test_aligned_setup_keeps_alignment_bonus_and_is_not_capped():
    """Part 2 (scoring): an ALIGNED setup (price>e50>e200) still earns the
    e50/e200 bonus and is NOT flagged as un-aligned / capped."""
    bars = _build_bullish_id50_series()
    closes = [b["close"] for b in bars]
    assert m.calc_e50(closes) > m.calc_ema(closes, 200)[-1]   # aligned scenario
    row = m.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "BUY"
    assert "e50/e200 aligned" in row["notes"]
    assert "not yet aligned" not in row["notes"]
