"""Malaysian SNR Emperor — M15 Fast Scanner.

Same methodology as snr_strategy.py but shifted DOWN one timeframe tier:

    H4 scanner (slow):  D1 levels → H4 breakout/engulfing → H1 MTF drop-down
    M15 scanner (fast):  H1 levels → M15 breakout/engulfing → M5/M15 MTF

This catches the same setups HOURS earlier because M15 candles close every
15 minutes vs H4 every 4 hours.  Level marking uses H1 close-to-open
junctions (refreshes every hour) instead of daily (refreshes once a day).

Pipeline per pair:
    1. Mark SNR levels from H1 close-to-open junctions.
    2. Classify 5 types: Classic, RBS, SBR, Gap, QM.
    3. Fresh/Unfresh/MISS validation.
    4. Storyline detection on M15 candles.
    5. Engulfing detection on M15 candles + PEZ/FEZ zones.
    6. Trendline detection on M15 + X-factor confluence.
    7. Entry tier classification (Setup 1-4 per Emperor SOP).
    8. 5-action SOP scoring → A/B/C/NO-TRADE grade.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import instruments
from config import PRIORITY_PAIRS

# ── Reuse all core functions from the H4 scanner ────────────────────────────
# The functions are timeframe-agnostic — they operate on generic candle lists.
# We just pass H1 where the H4 scanner passes D1, and M15 where it passes H4.
from snr_strategy import (
    _pip_size,
    _is_bullish,
    _is_bearish,
    _body_size,
    _candle_range,
    _mark_levels,
    _deduplicate_levels,
    _detect_qm_levels,
    _find_swings,
    _detect_storyline,
    _detect_engulfing,
    _multi_tf_engulfing,
    _detect_trendlines,
    _trendline_snr_confluence,
    _check_roadblocks,
    _classify_entry_tier,
    _sop_score,
    _calc_atr,
    LEVEL_TOLERANCE_PIPS,
    MAX_LEVEL_USES,
)

log = logging.getLogger("snr_m15_strategy")

NY = ZoneInfo("America/New_York")

# Same universe as the H4 scanner
SNR_M15_UNIVERSE = list(PRIORITY_PAIRS)


# ──────────────────────────────────────────────────────────────────────────────
# Trade Plan — adjusted for M15 (tighter SL, closer TP)
# ──────────────────────────────────────────────────────────────────────────────

def _build_trade_plan_m15(
    setup: str,
    entry_price: float | None,
    storyline: dict,
    all_levels: list[dict],
    h1_candles: list[dict],
    current_price: float,
    symbol: Optional[str] = None,
) -> dict:
    """Build trade plan for M15 setups — tighter stops than H4.

    Key differences from H4 trade plan:
    - ATR calculated from H1 candles (not daily) for tighter SL
    - SL buffer + ATR cap are M15-scale (per asset class, from instruments)
    - Minimum SL = the instrument's per-asset floor (so e.g. Nasdaq isn't
      given a noise-tight stop)
    """
    empty = {"entry": None, "sl": None, "tp1": None, "tp2": None,
             "rr1": None, "rr2": None, "sl_pips": None,
             "tp1_pips": None, "tp2_pips": None}

    if setup not in ("BUY", "SELL") or not entry_price:
        return empty

    pip = _pip_size(entry_price, symbol)
    slp = instruments.sl_params(symbol, "m15")
    min_sl = instruments.min_sl_distance(symbol, entry_price)
    atr = _calc_atr(h1_candles)  # H1 ATR = much tighter than daily
    if atr == 0:
        atr = min_sl * 4  # fallback when ATR unavailable (M15 scale)

    is_buy = setup == "BUY"
    entry = entry_price

    # ── SL — nearest opposing level behind entry ─────────────────────
    sl = None
    sl_candidates = []
    for lvl in all_levels:
        if is_buy and lvl["type"] == "support" and lvl["price"] < entry:
            sl_candidates.append(lvl["price"])
        elif not is_buy and lvl["type"] == "resistance" and lvl["price"] > entry:
            sl_candidates.append(lvl["price"])

    buffer = atr * slp["buffer"]  # M15-scale structure buffer (per asset class)
    if sl_candidates:
        if is_buy:
            nearest_below = max(sl_candidates)
            sl = nearest_below - buffer
        else:
            nearest_above = min(sl_candidates)
            sl = nearest_above + buffer
    else:
        sl = entry - atr * slp["atr_fallback"] if is_buy else entry + atr * slp["atr_fallback"]

    # Clamp SL: per-asset minimum floor, maximum = per-asset M15 ATR cap
    sl_dist = abs(entry - sl)
    max_sl = max(atr * slp["atr_cap"], min_sl)  # cap must never fall below the floor
    if sl_dist < min_sl:
        sl = entry - min_sl if is_buy else entry + min_sl
    elif sl_dist > max_sl:
        sl = entry - max_sl if is_buy else entry + max_sl

    # ── TP1 — storyline target or next opposing level ────────────────
    tp1 = None
    storyline_to = storyline.get("to_level")
    if storyline_to:
        tp1 = storyline_to
    else:
        tp_candidates = []
        for lvl in all_levels:
            if is_buy and lvl["type"] == "resistance" and lvl["price"] > entry:
                tp_candidates.append(lvl["price"])
            elif not is_buy and lvl["type"] == "support" and lvl["price"] < entry:
                tp_candidates.append(lvl["price"])
        if tp_candidates:
            tp1 = min(tp_candidates) if is_buy else max(tp_candidates)

    sl_dist = abs(entry - sl)
    if tp1 is None:
        tp1 = entry + sl_dist if is_buy else entry - sl_dist
    elif abs(tp1 - entry) < sl_dist:
        tp1 = entry + sl_dist if is_buy else entry - sl_dist

    # ── TP2 — next level beyond TP1, or 2× extension ────────────────
    tp2 = None
    tp2_candidates = []
    for lvl in all_levels:
        if is_buy and lvl["type"] == "resistance" and lvl["price"] > tp1 + pip:
            tp2_candidates.append(lvl["price"])
        elif not is_buy and lvl["type"] == "support" and lvl["price"] < tp1 - pip:
            tp2_candidates.append(lvl["price"])
    if tp2_candidates:
        tp2 = min(tp2_candidates) if is_buy else max(tp2_candidates)
    else:
        tp1_dist = abs(tp1 - entry)
        tp2 = entry + tp1_dist * 2 if is_buy else entry - tp1_dist * 2

    # ── R:R ──────────────────────────────────────────────────────────
    sl_dist = abs(entry - sl)
    tp1_dist = abs(tp1 - entry)
    tp2_dist = abs(tp2 - entry) if tp2 else 0
    rr1 = round(tp1_dist / sl_dist, 2) if sl_dist > 0 else 0
    rr2 = round(tp2_dist / sl_dist, 2) if sl_dist > 0 and tp2 else 0
    sl_pips = round(sl_dist / pip)
    tp1_pips = round(tp1_dist / pip)
    tp2_pips = round(tp2_dist / pip) if tp2 else 0

    return {
        "entry": round(entry, 5 if pip < 0.01 else 3),
        "sl": round(sl, 5 if pip < 0.01 else 3),
        "tp1": round(tp1, 5 if pip < 0.01 else 3),
        "tp2": round(tp2, 5 if pip < 0.01 else 3) if tp2 else None,
        "rr1": rr1,
        "rr2": rr2,
        "sl_pips": sl_pips,
        "tp1_pips": tp1_pips,
        "tp2_pips": tp2_pips,
    }


# ──────────────────────────────────────────────────────────────────────────────
# EMS confluence gate (Engulfing + MSNR + SMC)
# ──────────────────────────────────────────────────────────────────────────────
# Per "The Alchemist EMS Trinity" + MSNR ALCHEMIST notes: an M15 SNR signal is
# only tradeable when the higher timeframe (H4) bias agrees AND price has shown
# a liquidity sweep AND a market structure shift. Without these, M15 is just
# noise; with them, M15 becomes a precision refinement of the HTF intent.

import snr_strategy


def h4_direction_agrees(h4_row: Optional[dict], m15_direction: str) -> bool:
    """True when the H4 scanner's storyline is active and biased in the same
    direction as the M15 signal (BUY<->bullish, SELL<->bearish)."""
    if not h4_row:
        return False
    storyline = h4_row.get("storyline") or {}
    if not storyline.get("active"):
        return False
    want = "bullish" if m15_direction == "BUY" else "bearish"
    return storyline.get("direction") == want


def ems_gate(m15_row: dict, h4_row: Optional[dict],
             m15_candles: list[dict]) -> tuple[bool, str]:
    """Gate an M15 SNR signal through the EMS confluence checklist.

    Returns (True, "") to pass, or (False, reason) to reject. Checks run in
    order so the reject reason names the first failed confluence:
      1. H4 bias aligned (HTF agreement)
      2. Liquidity sweep present (zone validated)
      3. Market structure shift present (delivery confirmed)
    """
    direction = m15_row.get("setup")
    if direction not in ("BUY", "SELL"):
        return False, "no directional setup"
    if not h4_direction_agrees(h4_row, direction):
        return False, "H4 bias not aligned"
    if not snr_strategy.detect_liquidity_sweep(m15_candles, direction):
        return False, "no liquidity sweep"
    if not snr_strategy.detect_mss(m15_candles, direction):
        return False, "no market structure shift"
    return True, ""


# ──────────────────────────────────────────────────────────────────────────────
# Per-pair analysis — M15 version
# ──────────────────────────────────────────────────────────────────────────────

def analyze_pair(
    symbol: str,
    h1_candles: list[dict],
    m15_candles: list[dict],
    m5_candles: Optional[list[dict]] = None,
    now_ny: Optional[datetime] = None,
) -> dict:
    """Run Malaysian SNR Emperor pipeline for one pair — M15 fast scan.

    Timeframe mapping (vs H4 scanner):
        H4 scanner       →  M15 scanner
        ───────────────────────────────
        daily_candles     →  h1_candles    (level marking)
        h4_candles        →  m15_candles   (storyline / breakout / engulfing)
        h1_candles        →  m5_candles    (MTF drop-down)
    """
    if now_ny is None:
        now_ny = datetime.now(NY)

    if not h1_candles or len(h1_candles) < 15:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-DATA",
                "reason": "insufficient H1 candles", "score": 0,
                "timeframe": "M15"}

    # Use the most recent candle for current price (M15 is more recent than H1)
    current_price = (m15_candles[-1]["close"] if m15_candles
                     else h1_candles[-1]["close"])
    pip = _pip_size(current_price, symbol)

    # 1. Mark SNR levels from H1 close-to-open junctions
    #    (same function — it just sees "candles" and marks A-shape/V-shape)
    levels = _mark_levels(h1_candles, lookback=120, symbol=symbol)  # ~5 days of H1

    # 2. Detect QM patterns from M15
    qm_levels = _detect_qm_levels(m15_candles or [], current_price, symbol=symbol)
    all_levels = levels + qm_levels
    all_levels = _deduplicate_levels(all_levels, LEVEL_TOLERANCE_PIPS * pip)

    fresh_levels = [l for l in all_levels if l["fresh"]]
    unfresh_levels = [l for l in all_levels if not l["fresh"]]

    # 3. Detect storyline using M15 for breakout confirmation
    #    M15 candles cover less wall-clock time than H4, so expand lookback:
    #    rejection_lookback=30 → ~7.5h on M15 (vs ~2.5 days on H4 with 15)
    #    bo_lookback=20 → ~5h on M15 (vs ~1.7 days on H4 with 10)
    storyline = _detect_storyline(all_levels, m15_candles or [], current_price,
                                  rejection_lookback=30, bo_lookback=20, symbol=symbol)

    # 4. Detect engulfing patterns on M15
    engulfing = _detect_engulfing(m15_candles or h1_candles[-20:])

    # 4b. Multi-TF engulfing: drop to M5 (if available) or use M15 itself
    mtf_eg = _multi_tf_engulfing(engulfing, m5_candles, all_levels, current_price, symbol=symbol)

    # 5. Detect trendlines on M15
    trendlines = _detect_trendlines(m15_candles or [], current_price, symbol=symbol)
    tl_confluence = _trendline_snr_confluence(trendlines, all_levels, current_price, symbol=symbol)

    # 6. Roadblocks
    direction = (storyline.get("direction")
                 or ("bullish" if _is_bullish(h1_candles[-1]) else "bearish"))
    roadblocks = _check_roadblocks(
        all_levels, current_price, storyline.get("to_level"), direction
    )

    # 7. Entry tier classification
    entry = _classify_entry_tier(
        storyline, engulfing, all_levels, current_price,
        m15_candles or [], tl_confluence, symbol=symbol,
    )

    # 8. SOP scoring
    score, notes = _sop_score(
        all_levels, storyline, engulfing, entry, roadblocks,
        tl_confluence, qm_levels, current_price, mtf_eg, symbol=symbol,
    )

    # ── Setup determination ───────────────────────────────────────────
    if storyline.get("active") and entry["tier"] != "no_setup":
        setup = "BUY" if storyline["direction"] == "bullish" else "SELL"
    elif storyline.get("active") and storyline.get("confirmed"):
        setup = "BUY" if storyline["direction"] == "bullish" else "SELL"
    elif not storyline.get("active") and engulfing and engulfing[-1]["direction"]:
        last_eng = engulfing[-1]
        if last_eng.get("zone_status") == "PEZ":
            setup = "BUY" if last_eng["direction"] == "bullish" else "SELL"
        else:
            setup = "NO-TRADE"
    else:
        setup = "NO-TRADE"

    # ── Grade ─────────────────────────────────────────────────────────
    if score >= 10:
        grade = "A"
    elif score >= 7:
        grade = "B"
    elif score >= 4:
        grade = "C"
    else:
        grade = "NO-TRADE"
        setup = "NO-TRADE"

    # ── Nearest fresh levels ──────────────────────────────────────────
    nearest_support = None
    nearest_resistance = None
    for lvl in sorted(fresh_levels, key=lambda x: abs(x["price"] - current_price)):
        if lvl["type"] == "support" and lvl["price"] < current_price and nearest_support is None:
            nearest_support = lvl["price"]
        elif lvl["type"] == "resistance" and lvl["price"] > current_price and nearest_resistance is None:
            nearest_resistance = lvl["price"]
        if nearest_support and nearest_resistance:
            break

    # ── SNR type summary ──────────────────────────────────────────────
    type_counts = {}
    for lvl in all_levels:
        t = lvl["snr_type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    # ── PEZ/FEZ summary ──────────────────────────────────────────────
    pez_count = sum(1 for e in engulfing if e.get("zone_status") == "PEZ")
    fez_count = sum(1 for e in engulfing if e.get("zone_status") == "FEZ")

    # ── Trade Plan — M15-scaled SL / TP1 / TP2 / R:R ─────────────────
    trade_plan = _build_trade_plan_m15(
        setup=setup,
        entry_price=entry.get("entry_price"),
        storyline=storyline,
        all_levels=all_levels,
        h1_candles=h1_candles,
        current_price=current_price,
        symbol=symbol,
    )

    return {
        "symbol": symbol,
        "timeframe": "M15",
        "setup": setup,
        "grade": grade,
        "score": score,
        "notes": "; ".join(notes) if notes else "No confluence",
        "current_price": current_price,
        "fresh_levels_count": len(fresh_levels),
        "unfresh_levels_count": len(unfresh_levels),
        "miss_validated": sum(1 for l in all_levels if l["miss_count"] >= 2),
        "nearest_support": nearest_support,
        "nearest_resistance": nearest_resistance,
        "snr_types": type_counts,
        "storyline": {
            "active": storyline.get("active", False),
            "direction": storyline.get("direction"),
            "from_level": storyline.get("from_level"),
            "from_snr_type": storyline.get("from_snr_type"),
            "to_level": storyline.get("to_level"),
            "confirmed": storyline.get("confirmed", False),
        },
        "entry_tier": {
            "tier": entry["tier"],
            "setup_num": entry.get("setup_num", 0),
            "label": entry["label"],
            "confidence": entry["confidence"],
            "entry_price": entry.get("entry_price"),
        },
        "engulfing_patterns": [
            {"type": e["type"], "direction": e["direction"],
             "zone_status": e.get("zone_status", "?")}
            for e in engulfing
        ],
        "pez_count": pez_count,
        "fez_count": fez_count,
        "trendlines": [
            {"direction": t["direction"], "type": t["type"],
             "near_price": t["near_price"],
             "projected": round(t["projected_price"], 5)}
            for t in trendlines[:3]
        ],
        "tl_confluence": {
            "active": tl_confluence.get("active", False),
            "tl_type": tl_confluence.get("tl_type"),
            "snr_type": tl_confluence.get("snr_type"),
        },
        "mtf_engulfing": {
            "ltf_levels_in_zone": mtf_eg.get("ltf_levels_in_zone", []),
            "eg_flow": mtf_eg.get("eg_flow"),
            "eg_flow_label": mtf_eg.get("eg_flow_label"),
        },
        "trade_plan": trade_plan,
        "roadblocks": roadblocks,
        "qm_levels": [
            {"price": q["price"], "type": q["type"], "snr_type": q["snr_type"]}
            for q in qm_levels
        ],
        "levels": [
            {"price": l["price"], "type": l["type"], "snr_type": l["snr_type"],
             "fresh": l["fresh"], "uses": l["uses"], "miss_count": l["miss_count"]}
            for l in all_levels[:25]
        ],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Universe-level entry point
# ──────────────────────────────────────────────────────────────────────────────

def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    """Run Malaysian SNR Emperor M15 pipeline for every pair.

    `candles_by_pair[sym]` = {'1h': [...], 'm15': [...]}.
    """
    now_ny = datetime.now(NY)

    pairs_out = []
    for sym in SNR_M15_UNIVERSE:
        bundles = candles_by_pair.get(sym, {})
        h1 = bundles.get("1h", bundles.get("h1", []))
        m15 = bundles.get("m15", [])
        m5 = bundles.get("m5", [])

        pairs_out.append(
            analyze_pair(
                symbol=sym,
                h1_candles=h1,
                m15_candles=m15,
                m5_candles=m5 or None,
                now_ny=now_ny,
            )
        )

    buys = sum(1 for p in pairs_out if p["setup"] == "BUY")
    sells = sum(1 for p in pairs_out if p["setup"] == "SELL")
    grade_a = sum(1 for p in pairs_out if p["grade"] == "A")
    grade_b = sum(1 for p in pairs_out if p["grade"] == "B")
    storylines = sum(1 for p in pairs_out if p.get("storyline", {}).get("active"))

    return {
        "universe": SNR_M15_UNIVERSE,
        "timeframe": "M15",
        "now_ny": now_ny.strftime("%Y-%m-%d %H:%M %Z"),
        "buys": buys,
        "sells": sells,
        "grade_a": grade_a,
        "grade_b": grade_b,
        "active_storylines": storylines,
        "pairs": pairs_out,
    }
