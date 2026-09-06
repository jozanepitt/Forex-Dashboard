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

Grade is a 0-15 confluence score, same scale and thresholds as TDI Cycle 123
for dashboard consistency:
  3 base (pattern found) + up to 4 (EMA Level) + up to 4 (stop hunt)
  + up to 2 (Asian range) + up to 2 (freshness). Grade A >= 11, B >= 8, C >= 5.

Session/news gating happens at alert time (see alerts._should_alert_btmm123),
mirroring TDI123's architecture — analyze_pair always returns the full
analysis regardless of session, so the dashboard can show it either way.
"""
from __future__ import annotations

import logging
from typing import Optional

import instruments
from btmm_core import ema_stack, detect_level_count, detect_stop_hunt, detect_asian_range
from config import PRIORITY_PAIRS
from tdi_cycle_123 import (
    _find_swings, _find_123_pattern, _session_label, _in_active_session,
    _ema_targets, _atr,
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


def _grade_from_score(score: int) -> str:
    if score >= 11:
        return "A"
    if score >= 8:
        return "B"
    if score >= 5:
        return "C"
    return "NO-TRADE"


def analyze_pair(symbol: str, h1_candles: list[dict]) -> dict:
    """Full BTMM 123 pipeline for one pair, H1 only (no HTF bias/M15 —
    kept intentionally simpler than TDI123 until a backtest justifies more)."""
    if not h1_candles or len(h1_candles) < 100:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-DATA",
                "reason": "need >=100 H1 candles", "score": 0}

    closes = [b["close"] for b in h1_candles]
    price = closes[-1]

    swings = _find_swings(h1_candles)
    if len(swings) < 3:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE",
                "reason": "not enough swings", "score": 0, "current_price": price}

    pattern = _find_123_pattern(swings, h1_candles, symbol=symbol)
    if not pattern:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE",
                "reason": "no 123 pattern", "score": 0, "current_price": price}

    direction = pattern["direction"]

    stack = ema_stack(closes)
    level = detect_level_count(stack)
    hunt = detect_stop_hunt(h1_candles)
    asian = detect_asian_range(h1_candles, symbol)

    score = 3
    notes = ["123 geometry ok"]

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

    p3_price = pattern["p3"]["price"]
    freshness = abs(price - p3_price) < 0.30 * pattern["leg1_range"]
    if freshness:
        score += 2
        notes.append("fresh from p3")

    grade = _grade_from_score(score)
    setup = "NO-TRADE" if grade == "NO-TRADE" else ("BUY" if direction == "bullish" else "SELL")

    targets = _ema_targets(closes, direction, price, leg1_range=pattern.get("leg1_range", 0.0))

    pip = instruments.pip_size(symbol, price)
    atr = _atr(h1_candles) or (pattern["leg1_range"] * 0.5)
    p3p = pattern["p3"]["price"]
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

    now_ts = h1_candles[-1]["ts_utc"]

    return {
        "symbol": symbol,
        "setup": setup,
        "grade": grade,
        "score": score,
        "notes": "; ".join(notes),
        "current_price": price,
        "direction": direction,
        "pattern": {
            "p1": {"idx": pattern["p1"]["idx"], "price": pattern["p1"]["price"],
                    "ts_utc": pattern["p1"]["ts_utc"]},
            "p2": {"idx": pattern["p2"]["idx"], "price": pattern["p2"]["price"],
                    "ts_utc": pattern["p2"]["ts_utc"]},
            "p3": {"idx": pattern["p3"]["idx"], "price": pattern["p3"]["price"],
                    "ts_utc": pattern["p3"]["ts_utc"]},
            "leg1_range_pips": round(pattern["leg1_range_pips"], 1),
        },
        "level": level,
        "stop_hunt": hunt,
        "asian_range": asian,
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


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    """Run the BTMM 123 pipeline for every pair in the universe.

    `candles_by_pair[sym]` = {'1h': [...]}. Mirrors tdi_cycle_123's
    analyze_universe shape so the app/scheduler/dashboard wiring is symmetrical.
    """
    pairs_out: list[dict] = []
    for sym in BTMM123_UNIVERSE:
        bundles = candles_by_pair.get(sym, {}) or {}
        h1 = bundles.get("1h") or bundles.get("h1") or []
        try:
            row = analyze_pair(sym, h1)
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
