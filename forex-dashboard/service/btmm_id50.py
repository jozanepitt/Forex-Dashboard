"""BTMM ID50 — the authoritative M15 intraday 50-EMA bounce scanner.

ID50 per the trader's reference material (BTMM Patterns & Setups / Steve
Mauro): the 13 EMA crosses the 50 EMA, price makes a meaningful move AWAY from
the 50 EMA, then returns to test the 50 EMA for the FIRST quality bounce — with
a rejection/"trap" trigger and TDI confirmation. It is explicitly NOT "price
merely touching the 50 EMA".

The dashboard already ships a legacy 5-gate `detect_fifty_fifty_bounce` (labelled
"50/50 Bounce == ID 50") inside btmm_core.analyze(). That H1-shape setup is
deliberately untouched and stays parallel; this module is the authoritative M15
ID50, and bounce5050 is scheduled for retirement once ID50 shows edge in the
backtest (btmm_id50_backtest.py).

Hard rules (all must pass, else NO-TRADE):
  H1  Timeframe is M15
  H2  Anchor exists — swing fractal on the pre-impulse side of the 13/50 cross
  H3  13/50 cross happened and its direction == trade direction
  H4  price > e50 > e200 (BUY) / mirrored (SELL)
  H5  Move-away ≥ MOVE_AWAY_ATR_MULT × ATR(14) past e50 since the cross
  H6  First 50-EMA retest — no PRIOR close-through retest since the peak-away
      bar, and the retracement depth is ≥ RETRACE_MIN_FRAC of the move-away
  H7  50-EMA trap — current bar overlaps e50±tol, wicks through it, and closes
      back on the trade side with a rejection signature
  H8  Valid entry candle — nameable (Hammer/Engulfing/RRT/COW/Morning Star/
      Evening Star), outside-bar close-through, or a ≥1.2×ATR directional body.
      A bare Doji fails.

Soft factors stack an out-of-26 score (A+ 20+, A 18+, B 15+, C 12+). Everything
gestalt reuses the audited helpers from btmm_core / tdi_cycle_123 by import —
neither module is modified.
"""
from __future__ import annotations

import logging
from typing import Optional

import instruments
from btmm_core import (
    calc_ema, detect_nameable_candle, detect_rsi_signal_cross,
    detect_stop_hunt, detect_asian_range, calc_tdi, _last_ema_cross,
)
from config import PRIORITY_PAIRS
from tdi_cycle_123 import (
    _session_label, _in_active_session, _atr, _htf_bias,
    _prev_week_hlc, _weekly_fib_pivots, _location,
)

log = logging.getLogger("btmm_id50")

BTMM_ID50_UNIVERSE = list(PRIORITY_PAIRS)

# ── Tunable constants (module-level — the backtest sweeps these via ID50_PARAMS) ──
ATR_PERIOD = 14
MOVE_AWAY_ATR_MULT = 0.75
RETEST_TOL_PIPS = 2.0
RETEST_TOL_ATR = 0.35
CROSS_AGE_GOOD_MAX = 10
CROSS_AGE_ACCEPT_MAX = 20
RETRACE_MIN_FRAC = 0.25
MIN_DATA_BARS = 220
BARRIER_BARS = 96

ID50_PARAMS = {
    "MOVE_AWAY_ATR_MULT": MOVE_AWAY_ATR_MULT,
    "RETEST_TOL_PIPS": RETEST_TOL_PIPS,
    "RETEST_TOL_ATR": RETEST_TOL_ATR,
    "CROSS_AGE_GOOD_MAX": CROSS_AGE_GOOD_MAX,
    "CROSS_AGE_ACCEPT_MAX": CROSS_AGE_ACCEPT_MAX,
    "RETRACE_MIN_FRAC": RETRACE_MIN_FRAC,
}


def _resolve_params(params: Optional[dict] = None) -> dict:
    """Merge a sweep/backtest override onto the live ID50 defaults.

    The backtest's --sweep re-runs analyis under `{**ID50_PARAMS, **(params or
    {})}` without touching live defaults. Unknown keys are carried through but
    have no effect on the current pipeline."""
    return {**ID50_PARAMS, **(params or {})}


def _atr14(bars: list[dict]) -> Optional[float]:
    return _atr(bars, ATR_PERIOD)


def calc_e50(closes: list[float]) -> float:
    return calc_ema(closes, 50)[-1]


# ──────────────────────────────────────────────────────────────────────────────
# Detectors — each pure, dict-returning, unit-tested in test_btmm_id50.py
# ──────────────────────────────────────────────────────────────────────────────

def _cross_state(closes: list[float]) -> dict:
    """Last e13/e50 EMA cross: direction + bar index + age in bars."""
    if len(closes) < 60:
        return {"idx": None, "direction": None, "bars_ago": None}
    e13 = calc_ema(closes, 13)
    e50 = calc_ema(closes, 50)
    c = _last_ema_cross(e13, e50)
    if not c:
        return {"idx": None, "direction": None, "bars_ago": None}
    idx, direction = c
    return {"idx": idx, "direction": direction, "bars_ago": len(closes) - 1 - idx}


def _anchor_fractal(bars: list[dict], cross_idx: int, direction: str) -> Optional[dict]:
    """H2 — most recent swing fractal on the pre-impulse side of the cross
    (BUY: a swing LOW; SELL: a swing HIGH). Must be at least 5 bars old and
    sit at/before the cross. 5-bar fractal: bars[i±2] above (BUY)."""
    n = len(bars)
    lo_scan = min(cross_idx, n - 6)
    for i in range(lo_scan, 1, -1):
        if i < 2 or i + 2 >= n:
            continue
        if direction == "bullish":
            if (bars[i]["low"] <= bars[i - 2]["low"] and
                    bars[i]["low"] <= bars[i + 2]["low"]):
                return {"anchor": bars[i]["low"], "idx": i}
        else:
            if (bars[i]["high"] >= bars[i - 2]["high"] and
                    bars[i]["high"] >= bars[i + 2]["high"]):
                return {"anchor": bars[i]["high"], "idx": i}
    return None


def _watch_row(symbol: str, m15_bars: list[dict],
               live_row: Optional[dict] = None) -> Optional[dict]:
    """Precursor ID50 watch candidate — W1-W4 (see the 2026-09-25 design):
    W1 fresh 13/50 cross exists, W2 confirmed pre-impulse anchor, W3 cross
    not older than CROSS_AGE_ACCEPT_MAX bars, W4 no same-direction live
    signal already fired. Replays only the tested H2/H3 detectors; never
    alters the audited signal pipeline. Returns None to skip."""
    if not m15_bars or len(m15_bars) < 60:
        return None
    closes = [b["close"] for b in m15_bars]
    cross = _cross_state(closes)
    if cross["idx"] is None or cross["direction"] is None:
        return None
    if cross["bars_ago"] > CROSS_AGE_ACCEPT_MAX:
        return None
    anchor = _anchor_fractal(m15_bars, cross["idx"], cross["direction"])
    if not anchor:
        return None
    setup = (live_row or {}).get("setup")
    if (cross["direction"] == "bullish" and setup == "BUY") or \
       (cross["direction"] == "bearish" and setup == "SELL"):
        return None
    return {
        "symbol": symbol,
        "direction": cross["direction"],
        "cross_age": cross["bars_ago"],
        "anchor": anchor["anchor"],
        "price": closes[-1],
    }


def _move_away(bars: list[dict], closes: list[float], cross_idx: int,
               direction: str, atr: float,
               atr_mult: float = MOVE_AWAY_ATR_MULT) -> dict:
    """H5 — max extension past e50 on the trade side since the cross, in ATR
    units. Tracks the peak-away bar (the retracement origin + TP1 target)."""
    e50 = calc_ema(closes, 50)
    best_ext = 0.0
    peak_idx = cross_idx
    peak = bars[cross_idx]["high" if direction == "bullish" else "low"]
    for i in range(cross_idx, len(closes)):
        if direction == "bullish":
            ext = bars[i]["high"] - e50[i]
            if ext > best_ext:
                best_ext, peak, peak_idx = ext, bars[i]["high"], i
        else:
            ext = e50[i] - bars[i]["low"]
            if ext > best_ext:
                best_ext, peak, peak_idx = ext, bars[i]["low"], i
    magnitude_atr = best_ext / atr if (atr and atr > 0) else 0.0
    return {"peak": peak, "peak_idx": peak_idx, "ok": magnitude_atr >= atr_mult,
            "magnitude_atr": round(magnitude_atr, 2)}


def _first_retest(bars: list[dict], peak: float, peak_idx: int, direction: str,
                  retrace_min_frac: float = RETRACE_MIN_FRAC) -> dict:
    """H6 — the FIRST-quality retest of the 50 EMA after the peak-away bar.

    Rejects when (a) no bar reaches e50 (no real retest yet), (b) a bar before
    the current one already retested e50 AND closed back on the trade side (a
    resolved first bounce = what we're seeing now is a SECOND retest), or
    (c) the retracement from peak is shallower than `retrace_min_frac` of the
    move-away leg.
    """
    closes = [b["close"] for b in bars]
    e50 = calc_ema(closes, 50)
    n = len(closes)
    cur = n - 1
    retest_low = bars[cur]["low"] if direction == "bullish" else bars[cur]["high"]

    # (a) current bar must actually reach the 50 EMA
    if direction == "bullish" and bars[cur]["low"] > e50[cur]:
        return {"ok": False, "prior_close_through": False, "retrace_pct": 0.0,
                "reason": "price not back at e50"}
    if direction == "bearish" and bars[cur]["high"] < e50[cur]:
        return {"ok": False, "prior_close_through": False, "retrace_pct": 0.0,
                "reason": "price not back at e50"}

    # (b) no PRIOR close-through retest after the peak-away bar
    for i in range(peak_idx + 1, cur):
        if direction == "bullish":
            prior_touch = bars[i]["low"] <= e50[i]
            prior_resolved = bars[i]["close"] >= e50[i]
        else:
            prior_touch = bars[i]["high"] >= e50[i]
            prior_resolved = bars[i]["close"] <= e50[i]
        if prior_touch and prior_resolved:
            return {"ok": False, "prior_close_through": True, "retrace_pct": 0.0,
                    "reason": "second retest (prior close-through)"}

    leg = abs(peak - e50[cur])
    retrace_pct = (abs(peak - retest_low) / leg) if leg > 1e-12 else 0.0
    if retrace_pct < retrace_min_frac:
        return {"ok": False, "prior_close_through": False,
                "retrace_pct": round(retrace_pct, 3),
                "reason": "retracement too shallow"}
    return {"ok": True, "prior_close_through": False,
            "retrace_pct": round(retrace_pct, 3), "retest_low": retest_low,
            "reason": "first retest"}


def _ema_trap(bars: list[dict], direction: str, atr: float, pip: float,
              retest_tol_pips: float = RETEST_TOL_PIPS,
              retest_tol_atr: float = RETEST_TOL_ATR) -> dict:
    """H7 — 50-EMA trap: the current bar overlaps e50±tol, wicks into/through
    e50, closes back on the trade side, and shows a rejection signature."""
    closes = [b["close"] for b in bars]
    e50 = calc_e50(closes)
    tol = max(retest_tol_pips * pip, retest_tol_atr * atr)
    b = bars[-1]
    lo = min(b["open"], b["close"])
    hi = max(b["open"], b["close"])
    overlap = b["low"] <= e50 + tol and b["high"] >= e50 - tol
    if not overlap:
        return {"trap": False, "rejection": False, "tol": tol}
    wick_through = (direction == "bullish" and b["low"] <= e50) or \
                   (direction == "bearish" and b["high"] >= e50)
    close_trade_side = (direction == "bullish" and b["close"] > e50) or \
                       (direction == "bearish" and b["close"] < e50)
    body = max(abs(b["close"] - b["open"]), 1e-12)
    if direction == "bullish":
        lower_wick = min(b["open"], b["close"]) - b["low"]
        rejection = lower_wick >= 0.5 * body
    else:
        upper_wick = b["high"] - max(b["open"], b["close"])
        rejection = upper_wick >= 0.5 * body
    trap = wick_through and close_trade_side and rejection
    return {"trap": trap, "rejection": rejection, "tol": tol}


def _detect_star(bars: list[dict],
                 leg_body_frac: float = 0.55,
                 star_body_frac: float = 0.35,
                 star_range_frac: float = 0.6,
                 close_into_frac: float = 0.5) -> dict:
    """H8 — Morning/Evening Star over the last three bars (local to btmm_id50:
    btmm_core's `detect_nameable_candle` only classifies two-candle patterns).

    Morning Star (bullish): a long bearish leg, a small 'star' body forming
    below the first leg's midline, then a long bullish candle that closes back
    INTO the first candle's body (close_into_frac of that body). Evening Star
    mirrors it. Behind the --entry trigger RR/COW/morning-evening star-- in the
    ID50 manual. Returns the same shape as `detect_nameable_candle`."""
    if len(bars) < 3:
        return {"found": False, "name": "None", "direction": "neutral"}
    c1, c2, c3 = bars[-3], bars[-2], bars[-1]

    def _body(b):
        return abs(b["close"] - b["open"])

    def _rng(b):
        return b["high"] - b["low"]

    if min(_rng(c1), _rng(c2), _rng(c3)) <= 0:
        return {"found": False, "name": "None", "direction": "neutral"}
    b1, b2, b3 = _body(c1), _body(c2), _body(c3)
    r1, r2, r3 = _rng(c1), _rng(c2), _rng(c3)
    legs = b1 >= leg_body_frac * r1 and b3 >= leg_body_frac * r3
    star = b2 <= star_body_frac * max(b1, 1e-12) and b2 <= star_range_frac * r2
    if not (legs and star):
        return {"found": False, "name": "None", "direction": "neutral"}
    c1_top, c1_bot = max(c1["open"], c1["close"]), min(c1["open"], c1["close"])
    c1_mid = 0.5 * (c1_top + c1_bot)
    if c1["close"] < c1["open"] and c3["close"] > c3["open"]:
        if max(c2["open"], c2["close"]) < c1_mid and c3["close"] > c1_mid:
            return {"found": True, "name": "Morning Star", "direction": "bullish"}
    if c1["close"] > c1["open"] and c3["close"] < c3["open"]:
        if min(c2["open"], c2["close"]) > c1_mid and c3["close"] < c1_mid:
            return {"found": True, "name": "Evening Star", "direction": "bearish"}
    return {"found": False, "name": "None", "direction": "neutral"}


def _entry_candle(bars: list[dict], direction: str, atr: float) -> dict:
    """H8 — nameable entry candle (Hammer/Engulfing/RRT/COW/Morning Star/
    Evening Star matching direction), outside-bar close-through-e50, or a
    >=1.2xATR directional body. Doji fails."""
    star = _detect_star(bars)
    if star["found"]:
        if (direction == "bullish" and star["direction"] == "bullish") or \
           (direction == "bearish" and star["direction"] == "bearish"):
            return {"ok": True, "reason": f"nameable {star['name']}"}
        return {"ok": False, "reason": f"nameable {star['name']} wrong side"}

    nameable = detect_nameable_candle(bars)
    if nameable.get("found"):
        name = nameable.get("name") or "None"
        if name == "Doji":
            return {"ok": False, "reason": "doji"}
        bullish_ok = name in ("Hammer", "Bullish Engulfing", "RRT", "COW")
        bearish_ok = name in ("Shooting Star", "Bearish Engulfing", "RRT", "COW")
        if (direction == "bullish" and bullish_ok) or \
           (direction == "bearish" and bearish_ok):
            return {"ok": True, "reason": f"nameable {name}"}
        return {"ok": False, "reason": f"nameable {name} wrong side/neutral"}

    if len(bars) < 2:
        return {"ok": False, "reason": "no prev bar"}
    curr, prev = bars[-1], bars[-2]
    closes = [b["close"] for b in bars]
    e50 = calc_e50(closes)

    outside = curr["high"] > prev["high"] and curr["low"] < prev["low"]
    if outside:
        close_through = (direction == "bullish" and curr["close"] > e50) or \
                        (direction == "bearish" and curr["close"] < e50)
        if close_through:
            return {"ok": True, "reason": "outside bar close-through"}
        return {"ok": False, "reason": "outside bar no close-through"}

    body = abs(curr["close"] - curr["open"])
    if body >= 1.2 * atr:
        directional = (direction == "bullish" and curr["close"] > curr["open"]) or \
                      (direction == "bearish" and curr["close"] < curr["open"])
        if directional:
            return {"ok": True, "reason": "big directional body"}
    return {"ok": False, "reason": "no valid entry candle"}


# ──────────────────────────────────────────────────────────────────────────────
# Scoring / grades
# ──────────────────────────────────────────────────────────────────────────────

def _cross_points(bars_ago: int, good_max: int = CROSS_AGE_GOOD_MAX,
                  accept_max: int = CROSS_AGE_ACCEPT_MAX) -> int:
    if bars_ago is None:
        return 0
    if bars_ago <= good_max:
        return 2
    if bars_ago <= accept_max:
        return 1
    return 0


def _grade_from_score(score: int) -> str:
    # Thresholds recentered 2026-10-01 to the achievable 26-pt distribution:
    # aligned setups top out at 20 (p90=18), so the old A>=19 / A+>=22 were
    # near-/fully-unreachable. A>=18 = top ~14% of aligned setups, A+>=20 = top ~1%.
    if score >= 20:
        return "A+"
    if score >= 18:
        return "A"
    if score >= 15:
        return "B"
    if score >= 12:
        return "C"
    return "NO-TRADE"


def _trade_plan(symbol: str, price: float, direction: str, anchor: float,
                retest_low: float, atr: float, peak: float, e50: float,
                move_away_atr_mult: float = MOVE_AWAY_ATR_MULT) -> dict:
    """Entry at current close; SL under min(anchor, retest_low) by 0.5×ATR,
    floored at instruments.min_sl_distance then clamped to 2 pips; TP1 = the
    move-away peak; TP2/TP3 = 1.5×/2× measured-move extensions."""
    pip = instruments.pip_size(symbol, price)

    if direction == "bullish":
        raw_sl = min(anchor, retest_low) - 0.5 * atr
        sl_dist = price - raw_sl
        sl_dist = max(sl_dist, instruments.min_sl_distance(symbol, price), 2 * pip)
        sl = price - sl_dist
        tp1 = peak if peak > price else None
    else:
        raw_sl = max(anchor, retest_low) + 0.5 * atr
        sl_dist = raw_sl - price
        sl_dist = max(sl_dist, instruments.min_sl_distance(symbol, price), 2 * pip)
        sl = price + sl_dist
        tp1 = peak if peak < price else None

    measured = abs(peak - e50) if tp1 is not None else abs(peak - price)
    if tp1 is not None:
        tp2 = tp1 + 0.5 * measured if direction == "bullish" else tp1 - 0.5 * measured
        tp3 = peak + measured if direction == "bullish" else peak - measured
    else:
        tp2, tp3 = None, None

    def _pips(a, b):
        return round(abs(a - b) / pip, 1) if (a is not None and b is not None) else None

    sl_pips = _pips(price, sl)
    room_pips = _pips(peak, e50) if tp1 is not None else None
    rr1 = (_pips(price, tp1) / sl_pips) if (sl_pips and tp1 is not None and sl_pips > 0) else None
    room_ok = (room_pips is not None and sl_pips is not None and
               room_pips >= 2 * sl_pips)
    return {
        "entry": price,
        "sl": sl,
        "sl_pips": sl_pips,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr1": rr1,
        "room_pips": room_pips,
        "room_ok": room_ok,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline
# ──────────────────────────────────────────────────────────────────────────────

def _no_trade(symbol: str, reason: str, timeframe: str = "M15") -> dict:
    return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE",
            "score": 0, "reason": reason, "timeframe": timeframe}


def _analyze_id50(symbol: str, m15_candles: list[dict],
                  h1_candles: Optional[list[dict]] = None,
                  *, params: Optional[dict] = None) -> dict:
    P = _resolve_params(params)
    if not m15_candles or len(m15_candles) < MIN_DATA_BARS:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-DATA",
                "reason": f"need >= {MIN_DATA_BARS} M15 candles", "score": 0,
                "timeframe": "M15"}

    closes = [b["close"] for b in m15_candles]
    price = closes[-1]
    pip = instruments.pip_size(symbol, price)
    now_ts = m15_candles[-1]["ts_utc"]

    cross = _cross_state(closes)
    if cross["idx"] is None:
        return _no_trade(symbol, "no 13/50 EMA cross")
    direction = cross["direction"]
    setup = "BUY" if direction == "bullish" else "SELL"
    cross_age = cross["bars_ago"]

    anchor_a = _anchor_fractal(m15_candles, cross["idx"], direction)
    if not anchor_a:
        return _no_trade(symbol, "no anchor fractal on pre-impulse side")

    e50 = calc_e50(closes)
    e200 = calc_ema(closes, 200)[-1]
    # H4 — ID50 fires on the 13/50 cross + break-away + 50-EMA rejection in the
    # cross direction. The 50/200 relationship is a GRADE discriminator (A when
    # the 50/200 have aligned in-direction, B when the cross is still fresh and
    # they have not), NOT a hard gate — ID50 can set up BEFORE the 50/200 cross.
    # The only hard requirement here is that price has rejected back to the trade
    # side of the 50 EMA; the break-away (H5) and the return + rejection (H6/H7)
    # below remain mandatory and keep direction honest.
    side_ok = (direction == "bullish" and price > e50) or \
              (direction == "bearish" and price < e50)
    if not side_ok:
        return _no_trade(symbol, "price on wrong side of e50 (no rejection yet)")
    e50_e200_aligned = (direction == "bullish" and e50 > e200) or \
                       (direction == "bearish" and e50 < e200)

    atr = _atr14(m15_candles) or (abs(price - anchor_a["anchor"]) * 0.5) or (10 * pip)
    mv = _move_away(m15_candles, closes, cross["idx"], direction, atr,
                    atr_mult=P["MOVE_AWAY_ATR_MULT"])
    if not mv["ok"]:
        return _no_trade(symbol, "no meaningful move-away past e50")

    fr = _first_retest(m15_candles, closes[mv["peak_idx"]], mv["peak_idx"],
                       direction, retrace_min_frac=P["RETRACE_MIN_FRAC"])
    if not fr["ok"]:
        return _no_trade(symbol, f"first-retest fail: {fr.get('reason', '?')}")

    trap = _ema_trap(m15_candles, direction, atr, pip,
                     retest_tol_pips=P["RETEST_TOL_PIPS"],
                     retest_tol_atr=P["RETEST_TOL_ATR"])
    if not trap["trap"]:
        return _no_trade(symbol, "no 50-EMA trap/rejection on current bar")

    candle = _entry_candle(m15_candles, direction, atr)
    if not candle["ok"]:
        return _no_trade(symbol, f"entry-candle fail: {candle['reason']}")

    # ── Score out of 26 ──
    notes: list[str] = []
    score = 2                     # H2 anchor confirmed
    notes.append("anchor confirmed")

    cx_pts = _cross_points(cross_age, good_max=P["CROSS_AGE_GOOD_MAX"],
                           accept_max=P["CROSS_AGE_ACCEPT_MAX"])
    score += cx_pts
    if cx_pts == 2:
        notes.append(f"13/50 cross {cross_age} bars ago")
    elif cx_pts == 1:
        notes.append(f"13/50 cross {cross_age} bars ago (older)")
    else:
        notes.append(f"13/50 cross {cross_age} bars ago (stale)")

    if e50_e200_aligned:
        score += 2                 # H4 — 50/200 aligned in the trade direction
        notes.append("e50/e200 aligned")
    else:
        notes.append("e50/e200 not yet aligned (pre 50/200 cross, B-tier)")

    score += 2                     # H5 move-away
    notes.append(f"move-away {mv['magnitude_atr']}x ATR")

    score += 3                     # H6 first retest
    notes.append(f"first retest (retrace {fr['retrace_pct']})")

    score += 2                     # H7 trap
    notes.append("50-EMA trap/rejection")

    tdi = detect_rsi_signal_cross(calc_tdi(closes), lookback=5)
    tdi_ok = tdi.get("crossed", False) and tdi.get("direction") == direction
    if tdi_ok:
        score += 2
        notes.append("TDI RSI/signal cross confirmed")

    htf = _htf_bias(h1_candles) if h1_candles else None
    htf_aligned = (htf == direction) if htf else None
    if htf_aligned:
        score += 2
        notes.append("H1 bias aligned")
    elif htf is not None and htf != "neutral":
        score -= 2
        notes.append("H1 bias countered")

    # (the e50/e200 relationship is scored once above as the H4 factor and acts
    # as the A/B grade cap below — no second count here, which previously
    # double-weighted it.)

    asian = detect_asian_range(m15_candles, symbol)
    atr_pips = (atr / pip) if pip else 0.0
    asian_tight = asian.get("valid", False) and \
        asian.get("range_pips", 1e9) < min(50, 1.5 * atr_pips)
    if asian_tight:
        score += 1
        notes.append(f"Asian tight ({asian.get('range_pips', 0):.0f} pips)")

    if _in_active_session(now_ts):
        score += 1
        notes.append("active session")

    hunt = detect_stop_hunt(m15_candles)
    if hunt.get("active") and hunt.get("direction") == direction:
        score += 2
        notes.append("stop hunt confirms")
    elif hunt.get("active"):
        score -= 1
        notes.append("stop hunt against")

    pivots = _weekly_fib_pivots(_prev_week_hlc(m15_candles, now_ts))
    location = _location(price, direction, pivots)
    loc_ok = location.get("ok", False) or location.get("quality") in ("weak", "good", "prime")
    if loc_ok:
        score += 1
    notes.append(f"pivot location {location.get('zone', '?')}")

    plan = _trade_plan(symbol, price, direction, anchor_a["anchor"],
                       fr.get("retest_low", price), atr, mv["peak"], e50,
                       move_away_atr_mult=P["MOVE_AWAY_ATR_MULT"])
    if plan["room_ok"]:
        score += 2
        notes.append("R:R room to target")

    # Final grade + caps (50/200 alignment / cross stale / pivot / R:R room)
    grade = _grade_from_score(score)
    # User doctrine: A/A+ require the 50/200 to have aligned in the trade
    # direction. A fresh-cross ID50 taken BEFORE the 50/200 cross tops out at B.
    if not e50_e200_aligned and grade in ("A", "A+"):
        grade = "B"
        notes.append("grade capped B: 50/200 not yet aligned")
    if cross_age > CROSS_AGE_ACCEPT_MAX and grade in ("A", "A+", "B"):
        grade = "C"
        notes.append("grade capped: stale cross")
    if location.get("quality") in ("poor", "wrongside") and grade in ("A", "A+", "B"):
        grade = "C"
        notes.append("grade capped: poor pivot location")
    if plan["rr1"] is not None and plan["rr1"] < 1.0 and grade in ("A", "A+", "B"):
        grade = "C"
        notes.append("grade capped: R:R < 1.0")
    setpoint = "NO-TRADE" if score < 12 or grade == "NO-TRADE" else setup

    return {
        "symbol": symbol,
        "setup": setpoint,
        "grade": grade,
        "score": score,
        "notes": "; ".join(notes) if setpoint != "NO-TRADE" else f"{'; '.join(notes)}",
        "current_price": price,
        "direction": direction,
        "setup_type": "id50",
        "timeframe": "M15",
        "reason": None if setpoint != "NO-TRADE" else (notes[-1] if notes else "score too low"),
        "cross": cross,
        "anchor": anchor_a["anchor"],
        "move_away": mv,
        "retest": {"ok": fr["ok"], "retrace_pct": fr.get("retrace_pct", 0.0),
                   "retest_low": fr.get("retest_low")},
        "trap": trap,
        "entry_candle": {"ok": candle["ok"], "reason": candle["reason"]},
        "tdi": {"ok": tdi_ok},
        "h1_bias": htf,
        "h1_bias_timeframe": "H1",
        "h1_aligned": bool(htf_aligned),
        "e50": e50,
        "e200": e200,
        "asian_range": asian,
        "asian_tight": asian_tight,
        "stop_hunt": hunt,
        "location": location,
        "location_ok": location.get("ok", False),
        "session": _session_label(now_ts),
        "in_active_session": _in_active_session(now_ts),
        "trade_plan": plan,
    }


def analyze_pair(symbol: str, m15_candles: list[dict],
                 h1_candles: Optional[list[dict]] = None,
                 *, params: Optional[dict] = None) -> dict:
    """Full ID50 pipeline for one pair: M15 primary (biased by H1).

    `params` is the backtest's sweep override; it merges onto ID50_PARAMS and
    never mutates the live defaults."""
    return _analyze_id50(symbol, m15_candles, h1_candles, params=params)


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    """Run ID50 across the universe. `candles_by_pair[sym]` = {'m15': [...],
    '1h': [...]}. Missing H1 degrades bias to unknown, never a hard fail."""
    pairs_out: list[dict] = []
    watch_rows: list[dict] = []
    for sym in BTMM_ID50_UNIVERSE:
        bundles = candles_by_pair.get(sym, {}) or {}
        m15 = bundles.get("m15") or bundles.get("M15") or []
        h1 = bundles.get("1h") or bundles.get("h1") or []
        try:
            row = analyze_pair(sym, m15, h1 or None)
        except Exception as e:  # noqa: BLE001
            log.exception("btmm_id50 failed for %s: %s", sym, e)
            row = {"symbol": sym, "setup": "NO-TRADE", "grade": "NO-DATA",
                   "reason": f"error: {e}", "score": 0}
        pairs_out.append(row)
        try:
            watch = _watch_row(sym, m15, row)
        except Exception:  # noqa: BLE001  best-effort watch, never fail the universe
            log.debug("id50 watch failed for %s", sym)
            watch = None
        if watch:
            watch_rows.append(watch)

    buys = sum(1 for p in pairs_out if p.get("setup") == "BUY")
    sells = sum(1 for p in pairs_out if p.get("setup") == "SELL")
    grade_aplus = sum(1 for p in pairs_out if p.get("grade") == "A+")
    grade_a = sum(1 for p in pairs_out if p.get("grade") == "A")
    grade_b = sum(1 for p in pairs_out if p.get("grade") == "B")

    return {
        "universe": BTMM_ID50_UNIVERSE,
        "buys": buys,
        "sells": sells,
        "grade_a": grade_a,
        "grade_aplus": grade_aplus,
        "grade_b": grade_b,
        "pairs": pairs_out,
        "watches": sorted(watch_rows, key=lambda w: w["cross_age"]),
        "watch_count": len(watch_rows),
    }