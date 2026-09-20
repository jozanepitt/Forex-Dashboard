"""BTMM 123 — classic price-structure 1-2-3 reversal, confirmed by BTMM
doctrine instead of a TDI/oscillator dependency.

Replaces the removed Malaysian SNR Emperor slot in the dashboard. Where
TDI Cycle 123 confirms its swing pattern with TDI divergence + RSI extremes,
BTMM 123 confirms the same swing geometry with:

  * EMA Level I/II cascade alignment (btmm_core.detect_level_count) —
    "institutional-grade structure" per classic BTMM doctrine.
  * A stop hunt at/near the reversal (btmm_core.detect_stop_hunt) — BTMM's
    actual doctrinal answer to "why does point 3 reverse," replacing TDI
    divergence.
  * A tight Asian range (btmm_core.detect_asian_range, <50 pips) — the
    same pre-trade gate forex-check.md already uses for manual analysis.

Swing detection, 123 geometry, session labelling, EMA-cascade targets, and
ATR are reused directly from tdi_cycle_123.py by import — tdi_cycle_123.py
itself is not modified, so its own (already-audited, already-live) behaviour
is unaffected.

Grade is a 0-17 confluence score, same thresholds as TDI Cycle 123
for dashboard consistency (A >= 11, B >= 8, C >= 5):
  3 base (pattern found: 123 or 200EMA Re-set) + up to 4 (EMA Level)
  + up to 4 (stop hunt) + up to 2 (Asian range) + up to 2 (freshness,
  123 path only) + up to 2 (HTF bias alignment).

Session/news gating happens at alert time (see alerts._should_alert_btmm123),
mirroring TDI123's architecture — analyze_pair always returns the full
analysis regardless of session, so the dashboard can show it either way.
"""
from __future__ import annotations

import logging
from typing import Optional

import instruments
from btmm_core import ema_stack, ema_last, detect_level_count, detect_stop_hunt, detect_asian_range
from config import PRIORITY_PAIRS
from tdi_cycle_123 import (
    _find_swings, _find_123_pattern, _session_label, _in_active_session,
    _ema_targets, _atr, _htf_bias, _prev_week_hlc, _weekly_fib_pivots,
    _location,
)

log = logging.getLogger("btmm_123")

BTMM123_UNIVERSE = list(PRIORITY_PAIRS)

# ATR structure-stop sizing — same convention as TDI Cycle 123 (a hair-tight
# fixed-pip stop gets hunted; place beyond the p3 extreme with a floor).
SL_STRUCT_ATR_MULT = 0.5
SL_MIN_ATR_MULT = 1.0


def _level_score(level: dict, direction: str) -> int:
    """Score btmm_core.detect_level_count() output when it confirms the
    123 pattern's direction. A Level II cascade in the OPPOSITE direction
    is not a confirmation of this setup — it scores zero, not negative."""
    if level.get("direction") != direction:
        return 0
    if level.get("level_ii"):
        return 4
    if level.get("level_i"):
        return 2
    return 0


def _stop_hunt_score(hunt: dict, direction: str) -> int:
    """Score btmm_core.detect_stop_hunt() output. `hunt['direction']` is the
    fade direction after the hunt, which must match the pattern's direction."""
    if hunt.get("active") and hunt.get("direction") == direction:
        return 4
    return 0


def _asian_score(asian: dict) -> int:
    return 2 if asian.get("valid") else 0


def _detect_ema200_false_break(bars: list[dict], closes: list[float]) -> dict:
    """1h 200EMA false breakout — the desk's Re-set setup ("123 PATTERN &
    1h 200ema false breakout: the 2 Main Setups that we RELY ON").

    Bullish Re-set: a recent low wicked BELOW the 200 EMA but price reclaimed
    back above it (failed breakdown). Bearish mirror. Only the last 5-bar
    window is examined so the break is fresh, not historical.
    """
    if len(closes) < 210 or len(bars) < 7:
        return {"active": False, "direction": None, "extreme": None}
    e200 = ema_last(closes, 200)
    window = bars[-6:-1]
    current_close = closes[-1]
    hi = max(b["high"] for b in window)
    lo = min(b["low"] for b in window)
    if hi > e200 and current_close < e200:
        return {"active": True, "direction": "bearish", "extreme": hi}
    if lo < e200 and current_close > e200:
        return {"active": True, "direction": "bullish", "extreme": lo}
    return {"active": False, "direction": None, "extreme": None}


def _grade_from_score(score: int) -> str:
    if score >= 11:
        return "A"
    if score >= 8:
        return "B"
    if score >= 5:
        return "C"
    return "NO-TRADE"


def _analyze_btmm_timeframe(
    symbol: str,
    entry_candles: list[dict],
    bias_candles: Optional[list[dict]] = None,
    bias_timeframe: str = "H4",
    timeframe: str = "H1",
) -> dict:
    """One timeframe of the BTMM 123 pipeline: classic 123 or, failing that,
    a 200EMA false-breakout Re-set — confirmed by Level cascade, stop hunt
    and Asian range, located against the weekly pivot zone, biased by the
    next timeframe up (H4 for H1 entries, H1 for M15 entries)."""
    if not entry_candles or len(entry_candles) < 100:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-DATA",
                "reason": f"need >=100 {timeframe} candles", "score": 0,
                "timeframe": timeframe}

    closes = [b["close"] for b in entry_candles]
    price = closes[-1]

    swings = _find_swings(entry_candles)
    pattern = _find_123_pattern(swings, entry_candles, symbol=symbol) if len(swings) >= 3 else None
    reset: Optional[dict] = None
    if pattern:
        direction = pattern["direction"]
        setup_type = "123"
    else:
        reset = _detect_ema200_false_break(entry_candles, closes)
        if not reset["active"]:
            reason = ("not enough swings" if len(swings) < 3
                      else "no 123 pattern or 200ema reset")
            return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE",
                    "reason": reason, "score": 0,
                    "current_price": price, "timeframe": timeframe}
        direction = reset["direction"]
        setup_type = "reset"

    stack = ema_stack(closes)
    level = detect_level_count(entry_candles, closes)
    hunt = detect_stop_hunt(entry_candles)
    asian = detect_asian_range(entry_candles, symbol)

    score = 3
    if setup_type == "123":
        notes = [f"{timeframe} 123 geometry ok"]
    else:
        notes = [f"{timeframe} 200ema false-breakout re-set"]

    lvl_pts = _level_score(level, direction)
    score += lvl_pts
    if lvl_pts == 4:
        notes.append("Level II EMA cascade aligned")
    elif lvl_pts == 2:
        notes.append("Level I EMA alignment")

    hunt_pts = _stop_hunt_score(hunt, direction)
    score += hunt_pts
    if hunt_pts:
        notes.append("stop hunt confirms reversal")

    asian_pts = _asian_score(asian)
    score += asian_pts
    if asian_pts:
        notes.append(f"Asian range tight ({asian.get('range_pips', 0):.0f} pips)")

    p3_price = pattern["p3"]["price"] if setup_type == "123" else None
    if setup_type == "123":
        freshness = abs(price - p3_price) < 0.30 * pattern["leg1_range"]
        if freshness:
            score += 2
            notes.append("fresh from p3")

    # Bias-timeframe alignment (H4 for H1 entries, H1 for M15 entries).
    htf = _htf_bias(bias_candles) if bias_candles else None
    htf_aligned = (htf == direction)
    if htf_aligned:
        score += 2
        notes.append(f"{bias_timeframe} bias aligned")

    # Location: pattern must sit in a tradable pivot zone — selling into
    # support or buying into resistance is capped to C, mirroring TDI123.
    # Weekly H/L/C from the entry series itself (no D1 feed on this path).
    pivots = _weekly_fib_pivots(_prev_week_hlc(entry_candles, entry_candles[-1]["ts_utc"]))
    location = _location(price, direction, pivots)
    notes.append(f"pivot location {location['zone']}")

    grade = _grade_from_score(score)
    if location["quality"] in ("poor", "wrongside") and grade in ("A", "B"):
        grade = "C"
        notes.append("grade capped: poor pivot location")
    setup = "NO-TRADE" if grade == "NO-TRADE" else ("BUY" if direction == "bullish" else "SELL")

    if setup_type == "123":
        leg1_range = pattern.get("leg1_range", 0.0)
        anchor = pattern["p3"]["price"]
    else:
        leg1_range = 0.0
        anchor = reset["extreme"]
    targets = _ema_targets(closes, direction, price, leg1_range=leg1_range)

    pip = instruments.pip_size(symbol, price)
    atr = _atr(entry_candles) or (abs(price - anchor) * 0.5) or (10 * pip)
    p3p = anchor
    if setup == "BUY":
        entry = price
        sl = min(p3p - SL_STRUCT_ATR_MULT * atr, entry - SL_MIN_ATR_MULT * atr)
    elif setup == "SELL":
        entry = price
        sl = max(p3p + SL_STRUCT_ATR_MULT * atr, entry + SL_MIN_ATR_MULT * atr)
    else:
        entry, sl = None, None

    def _pips(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None or b is None:
            return None
        return round(abs(a - b) / pip, 1)

    now_ts = entry_candles[-1]["ts_utc"]

    if setup_type == "123":
        pattern_out = {
            "p1": {"idx": pattern["p1"]["idx"], "price": pattern["p1"]["price"],
                    "ts_utc": pattern["p1"]["ts_utc"]},
            "p2": {"idx": pattern["p2"]["idx"], "price": pattern["p2"]["price"],
                    "ts_utc": pattern["p2"]["ts_utc"]},
            "p3": {"idx": pattern["p3"]["idx"], "price": pattern["p3"]["price"],
                    "ts_utc": pattern["p3"]["ts_utc"]},
            "leg1_range_pips": round(pattern["leg1_range_pips"], 1),
        }
    else:
        pattern_out = {}

    return {
        "symbol": symbol,
        "setup": setup,
        "grade": grade,
        "score": score,
        "notes": "; ".join(notes),
        "current_price": price,
        "direction": direction,
        "setup_type": setup_type,
        "timeframe": timeframe,
        "pattern": pattern_out,
        "level": level,
        "stop_hunt": hunt,
        "asian_range": asian,
        "location": location,
        "location_ok": location["ok"],
        "htf_bias": htf,
        "htf_bias_timeframe": bias_timeframe,
        "htf_aligned": htf_aligned,
        "reset": ({"extreme": reset["extreme"]} if reset else None),
        "session": _session_label(now_ts),
        "in_active_session": _in_active_session(now_ts),
        "targets": {
            "L1": targets["tp1"],
            "L2": targets["tp2"],
            "L3": targets["tp3"],
            "L1_pips": _pips(entry, targets["tp1"]),
            "L2_pips": _pips(entry, targets["tp2"]),
            "L3_pips": _pips(entry, targets["tp3"]),
            "projected": targets["projected"],
        },
        "trade_plan": {
            "entry": entry,
            "sl": sl,
            "sl_pips": _pips(entry, sl),
            "tp1": targets["tp1"],
            "tp2": targets["tp2"],
            "tp3": targets["tp3"],
            "rr1": (_pips(entry, targets["tp1"]) / _pips(entry, sl))
                   if (_pips(entry, sl) and _pips(entry, targets["tp1"])) else None,
        },
    }


def analyze_pair(
    symbol: str,
    h1_candles: list[dict],
    h4_candles: Optional[list[dict]] = None,
    m15_candles: Optional[list[dict]] = None,
) -> dict:
    """Full pipeline for one pair: H1 primary (biased by H4) + optional M15
    (biased by H1), mirroring TDI123's structure. Each leg independently
    resolves to a classic 123 or a 200EMA false-breakout Re-set.

    Old single-arg calls analyze_pair(sym, h1) keep working — bias/timeframe
    legs simply degrade to unknown instead of blocking.
    """
    h1_result = _analyze_btmm_timeframe(
        symbol, h1_candles, h4_candles,
        bias_timeframe="H4", timeframe="H1",
    )

    if m15_candles:
        m15_result = _analyze_btmm_timeframe(
            symbol, m15_candles, h1_candles,
            bias_timeframe="H1", timeframe="M15",
        )
        if m15_result.get("setup") in ("BUY", "SELL"):
            h1_result["m15"] = m15_result

    return h1_result


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    """Run the BTMM 123 pipeline for every pair in the universe.

    `candles_by_pair[sym]` = {'1h': [...], '4h': [...], 'm15': [...]}.
    Missing timeframes gracefully degrade (H4 bias / M15 skipped).
    Mirrors tdi_cycle_123's analyze_universe shape so the app/scheduler/
    dashboard wiring is symmetrical.
    """
    pairs_out: list[dict] = []
    for sym in BTMM123_UNIVERSE:
        bundles = candles_by_pair.get(sym, {}) or {}
        h1 = bundles.get("1h") or bundles.get("h1") or []
        h4 = bundles.get("4h") or bundles.get("h4") or []
        m15 = bundles.get("m15") or bundles.get("M15") or []
        try:
            row = analyze_pair(sym, h1, h4_candles=h4 or None,
                               m15_candles=m15 or None)
        except Exception as e:  # noqa: BLE001
            log.exception("btmm_123 failed for %s: %s", sym, e)
            row = {"symbol": sym, "setup": "NO-TRADE", "grade": "NO-DATA",
                   "reason": f"error: {e}", "score": 0}
        pairs_out.append(row)

    buys = sum(1 for p in pairs_out if p.get("setup") == "BUY")
    sells = sum(1 for p in pairs_out if p.get("setup") == "SELL")
    grade_a = sum(1 for p in pairs_out if p.get("grade") == "A")
    grade_b = sum(1 for p in pairs_out if p.get("grade") == "B")

    return {
        "universe": BTMM123_UNIVERSE,
        "buys": buys,
        "sells": sells,
        "grade_a": grade_a,
        "grade_b": grade_b,
        "pairs": pairs_out,
    }
