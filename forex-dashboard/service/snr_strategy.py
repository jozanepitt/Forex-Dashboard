"""Malaysian SNR Emperor strategy analyzer.

Sources:
  - Malaysian SNR Emperor (Abdiwahab / KororFX): Close-to-open junction levels,
    5 SNR types, fresh/unfresh/MISS, 4-rule storyline, 4 setup tiers,
    3 engulfing types + PEZ/FEZ, trendlines, 5-action SOP.
  - Malaysian SNR Institutional (KenneDynespot): QMR/QMC/QMM patterns,
    gap levels (Double Maru).

Pipeline per pair:
    1. Mark SNR levels from close-to-open junctions (A-shape / V-shape).
    2. Classify 5 types: Classic, RBS, SBR, Gap, QM.
    3. Fresh/Unfresh/MISS validation.
    4. Storyline detection (4 rules: same-TF, rejection, BO confirmation, roadblocks).
    5. Engulfing detection (Perfect, Quasimodo, Hidden) + PEZ/FEZ zones.
    6. Trendline detection (Regular, Breakout, QM) + X-factor confluence.
    7. Entry tier classification (Setup 1-4 per Emperor SOP).
    8. 5-action SOP scoring → A/B/C/NO-TRADE grade.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from config import PRIORITY_PAIRS

log = logging.getLogger("snr_strategy")

NY = ZoneInfo("America/New_York")

# Use the same universe as BTMM and 1AM CRT dashboards
SNR_UNIVERSE = list(PRIORITY_PAIRS)

MAX_LEVEL_USES = 2
LEVEL_TOLERANCE_PIPS = 5


def _pip_size(price: float) -> float:
    """Return pip size based on instrument price range."""
    if price > 5000:  # Indices: DAX (~25000), US30 (~40000)
        return 1.0
    if price > 500:   # Gold (~2500 but < 5000 handled above)
        return 0.10
    if price > 10:    # JPY, Silver
        return 0.01
    return 0.0001


def _is_bullish(candle: dict) -> bool:
    return candle["close"] > candle["open"]


def _is_bearish(candle: dict) -> bool:
    return candle["close"] < candle["open"]


def _body_size(candle: dict) -> float:
    return abs(candle["close"] - candle["open"])


def _candle_range(candle: dict) -> float:
    return candle["high"] - candle["low"]


# ──────────────────────────────────────────────────────────────────────────────
# 1. Level Marking — Close-to-Open Junction (Malaysian Emperor Method)
# ──────────────────────────────────────────────────────────────────────────────

def _mark_levels(daily_candles: list[dict], lookback: int = 60) -> list[dict]:
    """Mark SNR levels using the Malaysian close-to-open junction method.

    Resistance ("A-shape"): Bullish candle close → Bearish candle open.
    The level price = bullish candle's CLOSE.

    Support ("V-shape"): Bearish candle close → Bullish candle open.
    The level price = bearish candle's CLOSE.

    Also detects:
    - Gap levels: when open differs significantly from previous close
    - RBS/SBR: role reversals tracked during fresh/unfresh processing

    Returns list of {price, type, snr_type, fresh, uses, formed_idx,
                     last_touch_idx, miss_count, gap_reaction}.
    """
    candles = daily_candles[-lookback:]
    if len(candles) < 5:
        return []

    pip = _pip_size(candles[-1]["close"])
    tolerance = LEVEL_TOLERANCE_PIPS * pip
    levels: list[dict] = []

    for i in range(1, len(candles)):
        prev = candles[i - 1]
        curr = candles[i]

        # Classic A-shape: Bullish → Bearish = Resistance
        if _is_bullish(prev) and _is_bearish(curr):
            level_price = prev["close"]
            levels.append({
                "price": level_price,
                "type": "resistance",
                "snr_type": "classic_a",
                "fresh": True,
                "uses": 0,
                "formed_idx": i - 1,
                "last_touch_idx": i - 1,
                "miss_count": 0,
                "gap_reaction": False,
            })

        # Classic V-shape: Bearish → Bullish = Support
        elif _is_bearish(prev) and _is_bullish(curr):
            level_price = prev["close"]
            levels.append({
                "price": level_price,
                "type": "support",
                "snr_type": "classic_v",
                "fresh": True,
                "uses": 0,
                "formed_idx": i - 1,
                "last_touch_idx": i - 1,
                "miss_count": 0,
                "gap_reaction": False,
            })

        # Gap levels: significant gap between close and next open
        gap = curr["open"] - prev["close"]
        if abs(gap) > tolerance * 3:
            if gap > 0:
                # Bullish gap — the gap zone acts as support
                levels.append({
                    "price": prev["close"],  # bottom of gap
                    "type": "support",
                    "snr_type": "gap_bullish",
                    "fresh": True,
                    "uses": 0,
                    "formed_idx": i,
                    "last_touch_idx": i,
                    "miss_count": 0,
                    "gap_reaction": True,
                })
            else:
                # Bearish gap — the gap zone acts as resistance
                levels.append({
                    "price": prev["close"],  # top of gap
                    "type": "resistance",
                    "snr_type": "gap_bearish",
                    "fresh": True,
                    "uses": 0,
                    "formed_idx": i,
                    "last_touch_idx": i,
                    "miss_count": 0,
                    "gap_reaction": True,
                })

    # Deduplicate levels that are within tolerance of each other
    levels = _deduplicate_levels(levels, tolerance)

    # Process fresh/unfresh + MISS logic through subsequent candles
    for i in range(len(candles)):
        candle = candles[i]
        for lvl in levels:
            if lvl["formed_idx"] >= i:
                continue

            wick_touch = False
            body_break = False

            if lvl["type"] == "resistance":
                # Wick touch: high reaches level but close stays below
                wick_touch = (
                    abs(candle["high"] - lvl["price"]) < tolerance
                    and candle["close"] < lvl["price"] + tolerance
                )
                # Body break: close above level
                body_break = candle["close"] > lvl["price"] + tolerance
            else:  # support
                wick_touch = (
                    abs(candle["low"] - lvl["price"]) < tolerance
                    and candle["close"] > lvl["price"] - tolerance
                )
                body_break = candle["close"] < lvl["price"] - tolerance

            if body_break:
                # Body break = level re-freshens at the BREAK POINT (role reversal).
                # Emperor: the new S/R level is where price broke structure,
                # NOT where the candle happened to close (which may overshoot).
                break_price = lvl["price"]  # keep original level as the break point
                if lvl["type"] == "resistance":
                    lvl["type"] = "support"
                    lvl["snr_type"] = "rbs"  # Resistance Broken → Support
                else:
                    lvl["type"] = "resistance"
                    lvl["snr_type"] = "sbr"  # Support Broken → Resistance
                lvl["fresh"] = True
                lvl["price"] = break_price
                lvl["uses"] += 1
                lvl["last_touch_idx"] = i
                lvl["miss_count"] = 0
            elif wick_touch:
                # Wick touch = unfresh
                lvl["fresh"] = False
                lvl["uses"] += 1
                lvl["last_touch_idx"] = i
                lvl["miss_count"] = 0
            else:
                # MISS: candle approached the level but did NOT touch it →
                # validates the level.  Emperor: the candle must get close
                # (within ~10 pips for majors) but fail to reach.
                # tolerance*2 ≈ 10 pips for majors, ~$1 for Gold.
                candle_near = False
                if lvl["type"] == "resistance":
                    candle_near = candle["high"] > lvl["price"] - tolerance * 2
                else:
                    candle_near = candle["low"] < lvl["price"] + tolerance * 2
                if candle_near and i > lvl["last_touch_idx"]:
                    lvl["miss_count"] += 1

    # Filter: max 2 uses (gap levels with reaction get 3)
    max_uses = lambda l: 3 if l["gap_reaction"] else MAX_LEVEL_USES
    active_levels = [l for l in levels if l["uses"] < max_uses(l)]
    return active_levels


def _deduplicate_levels(levels: list[dict], tolerance: float) -> list[dict]:
    """Merge levels within tolerance — keep the freshest / most recent."""
    if not levels:
        return []
    levels.sort(key=lambda l: l["price"])
    deduped = [levels[0]]
    for lvl in levels[1:]:
        if abs(lvl["price"] - deduped[-1]["price"]) < tolerance:
            # Keep the more recent one
            if lvl["formed_idx"] > deduped[-1]["formed_idx"]:
                deduped[-1] = lvl
        else:
            deduped.append(lvl)
    return deduped


# ──────────────────────────────────────────────────────────────────────────────
# 2. QM Pattern Detection (from Institutional PDF)
# ──────────────────────────────────────────────────────────────────────────────

def _detect_qm_levels(h4_candles: list[dict], current_price: float) -> list[dict]:
    """Detect Quasimodo Reversal (QMR) and Continuation (QMC) patterns on H4.

    QMR: Head-and-shoulders structure where the "shoulder" level is the entry.
    Needs "3D" — three distinct directional legs.

    QMC: Quasimodo that continues existing trend direction.
    """
    if len(h4_candles) < 10:
        return []

    pip = _pip_size(current_price)
    tolerance = LEVEL_TOLERANCE_PIPS * pip
    qm_levels: list[dict] = []

    # Find swing highs and lows
    swings = _find_swings(h4_candles[-30:])
    if len(swings) < 5:
        return []

    # Look for QMR pattern: HH → HL → HH → LL (bearish QMR)
    # or LL → LH → LL → HH (bullish QMR)
    for i in range(4, len(swings)):
        s1, s2, s3, s4, s5 = swings[i-4], swings[i-3], swings[i-2], swings[i-1], swings[i]

        # Bearish QMR: swing-high, swing-low, higher-high, lower-low
        if (s1["type"] == "high" and s2["type"] == "low"
                and s3["type"] == "high" and s4["type"] == "low"
                and s3["price"] > s1["price"]  # higher high
                and s4["price"] < s2["price"]  # lower low
                and s5["type"] == "high"):
            # Entry = left shoulder (s1) level — resistance
            qm_levels.append({
                "price": s1["price"],
                "type": "resistance",
                "snr_type": "qmr",
                "fresh": True,
                "uses": 0,
                "formed_idx": s5["idx"],
                "last_touch_idx": s5["idx"],
                "miss_count": 0,
                "gap_reaction": False,
            })

        # Bullish QMR: swing-low, swing-high, lower-low, higher-high
        if (s1["type"] == "low" and s2["type"] == "high"
                and s3["type"] == "low" and s4["type"] == "high"
                and s3["price"] < s1["price"]  # lower low
                and s4["price"] > s2["price"]  # higher high
                and s5["type"] == "low"):
            # Entry = left shoulder (s1) level — support
            qm_levels.append({
                "price": s1["price"],
                "type": "support",
                "snr_type": "qmr",
                "fresh": True,
                "uses": 0,
                "formed_idx": s5["idx"],
                "last_touch_idx": s5["idx"],
                "miss_count": 0,
                "gap_reaction": False,
            })

    return qm_levels[:4]  # Max 4 QM levels


def _find_swings(candles: list[dict], left: int = 2, right: int = 2,
                 use_body: bool = False) -> list[dict]:
    """Find swing highs and lows using left/right bar comparison.

    use_body=True → Emperor's "hooking method" for trendlines: uses
    max(open,close) for highs and min(open,close) for lows instead of
    wicks.  This matches the PDF: "Body Bullish / Body Bearish".
    """
    # Define accessor functions once, outside the loop
    if use_body:
        def _hi(c): return max(c["open"], c["close"])
        def _lo(c): return min(c["open"], c["close"])
    else:
        def _hi(c): return c["high"]
        def _lo(c): return c["low"]

    swings = []
    for i in range(left, len(candles) - right):
        hi = _hi(candles[i])
        lo = _lo(candles[i])

        is_high = all(hi >= _hi(candles[i-j]) for j in range(1, left+1)) \
                  and all(hi >= _hi(candles[i+j]) for j in range(1, right+1))
        is_low = all(lo <= _lo(candles[i-j]) for j in range(1, left+1)) \
                 and all(lo <= _lo(candles[i+j]) for j in range(1, right+1))

        if is_high:
            swings.append({"type": "high", "price": hi, "idx": i})
        if is_low:
            swings.append({"type": "low", "price": lo, "idx": i})

    return swings


# ──────────────────────────────────────────────────────────────────────────────
# 3. Storyline Detection (4 Rules from Emperor)
# ──────────────────────────────────────────────────────────────────────────────

def _detect_storyline(levels: list[dict], h4_candles: list[dict],
                      current_price: float,
                      rejection_lookback: int = 15,
                      bo_lookback: int = 10) -> dict:
    """Detect storyline using the Emperor's 4 engagement rules.

    Rule 1: Same-TF levels — price travels from one level to another on same TF.
    Rule 2: Rejection starts/ends storyline — candle wick rejects from level.
    Rule 3: Confirmation — 1-2 TF lower breakout (H4 breakout for daily levels).
    Rule 4: Roadblocks — 1 TF lower opposing fresh levels in the path.

    rejection_lookback: how many recent candles to scan for rejection (default 15).
    bo_lookback: how many candles to look back for swing high/low in breakout check.
    When called from M15 scanner, pass larger values (e.g. 30, 20) since M15 candles
    cover less wall-clock time than H4 candles.
    """
    if not levels or not h4_candles or len(h4_candles) < 5:
        return {"active": False, "direction": None, "from_level": None,
                "to_level": None, "confirmed": False, "rule_status": {}}

    pip = _pip_size(current_price)
    tolerance = LEVEL_TOLERANCE_PIPS * pip

    # Separate fresh levels by type
    fresh_supports = sorted(
        [l for l in levels if l["fresh"] and l["type"] == "support"
         and l["price"] < current_price],
        key=lambda x: x["price"], reverse=True,
    )
    fresh_resistances = sorted(
        [l for l in levels if l["fresh"] and l["type"] == "resistance"
         and l["price"] > current_price],
        key=lambda x: x["price"],
    )

    # Rule 2: Find rejection in recent H4 candles
    # Emperor SOP 2: "Price rejection candle — Engulfing or Pin Bar."
    # Accept: (a) pin bar — wick > 0.5× body, or
    #         (b) engulfing — current candle body engulfs prior candle body.
    recent = h4_candles[-rejection_lookback:]
    rejection_level = None
    rejection_dir = None
    rejection_candle = None

    for ci, candle in enumerate(reversed(recent)):
        real_idx = len(recent) - 1 - ci
        prev_candle = recent[real_idx - 1] if real_idx > 0 else None

        # Bullish rejection: wick dips to support, closes above
        for sup in fresh_supports[:5]:
            if (candle["low"] <= sup["price"] + tolerance
                    and candle["close"] > sup["price"] + tolerance):
                wick_down = min(candle["open"], candle["close"]) - candle["low"]
                body = _body_size(candle)
                is_pin = wick_down > body * 0.5
                is_engulf = (prev_candle is not None
                             and _is_bullish(candle)
                             and candle["close"] >= max(prev_candle["open"], prev_candle["close"])
                             and candle["open"] <= min(prev_candle["open"], prev_candle["close"]))
                if is_pin or is_engulf:
                    rejection_level = sup
                    rejection_dir = "bullish"
                    rejection_candle = candle
                    break
        if rejection_level:
            break

        # Bearish rejection: wick pokes resistance, closes below
        for res in fresh_resistances[:5]:
            if (candle["high"] >= res["price"] - tolerance
                    and candle["close"] < res["price"] - tolerance):
                wick_up = candle["high"] - max(candle["open"], candle["close"])
                body = _body_size(candle)
                is_pin = wick_up > body * 0.5
                is_engulf = (prev_candle is not None
                             and _is_bearish(candle)
                             and candle["close"] <= min(prev_candle["open"], prev_candle["close"])
                             and candle["open"] >= max(prev_candle["open"], prev_candle["close"]))
                if is_pin or is_engulf:
                    rejection_level = res
                    rejection_dir = "bearish"
                    rejection_candle = candle
                    break
        if rejection_level:
            break

    if not rejection_level:
        return {"active": False, "direction": None, "from_level": None,
                "to_level": None, "confirmed": False, "rule_status": {}}

    # Rule 1: Target = next fresh level on same timeframe in direction
    target = None
    if rejection_dir == "bullish" and fresh_resistances:
        target = fresh_resistances[0]
    elif rejection_dir == "bearish" and fresh_supports:
        target = fresh_supports[0]

    # Rule 3: Check for breakout confirmation (1 TF lower BO)
    confirmed = False
    if len(h4_candles) >= 3:
        last3 = h4_candles[-3:]
        if rejection_dir == "bullish":
            # Close above recent swing high = breakout
            recent_highs = [c["high"] for c in h4_candles[-bo_lookback:-1]]
            if recent_highs:
                swing_high = max(recent_highs)
                confirmed = last3[-1]["close"] > swing_high
        else:
            recent_lows = [c["low"] for c in h4_candles[-bo_lookback:-1]]
            if recent_lows:
                swing_low = min(recent_lows)
                confirmed = last3[-1]["close"] < swing_low

    return {
        "active": True,
        "direction": rejection_dir,
        "from_level": rejection_level["price"],
        "from_type": rejection_level["type"],
        "from_snr_type": rejection_level["snr_type"],
        "to_level": target["price"] if target else None,
        "confirmed": confirmed,
        "rule_status": {
            "r1_same_tf": True,
            "r2_rejection": True,
            "r3_bo_confirmed": confirmed,
            "r4_roadblocks": "pending",
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# 4. Engulfing Detection (Emperor: Perfect, QM, Hidden) + PEZ/FEZ
# ──────────────────────────────────────────────────────────────────────────────

def _detect_engulfing(candles: list[dict], lookback: int = 15) -> list[dict]:
    """Detect 3 engulfing types from the Emperor methodology.

    1. Perfect Engulfing: full body engulf of prior candle
    2. QM Engulfing: engulf with left-shoulder (Quasimodo) structure
    3. Hidden Engulfing: engulfs wick area, not necessarily the full body

    Also classifies zones as PEZ (intact) or FEZ (broken).
    """
    if len(candles) < 3:
        return []

    recent = candles[-lookback:]
    patterns: list[dict] = []

    for i in range(1, len(recent)):
        curr = recent[i]
        prev = recent[i - 1]
        curr_body = curr["close"] - curr["open"]
        prev_body = prev["close"] - prev["open"]

        if _candle_range(curr) == 0 or _candle_range(prev) == 0:
            continue

        direction = "bullish" if curr_body > 0 else "bearish"

        # ── Perfect Engulfing ────────────────────────────────────────────
        # Emperor: full BODY engulf of prior candle's BODY (not wicks)
        prev_body_lo = min(prev["open"], prev["close"])
        prev_body_hi = max(prev["open"], prev["close"])
        is_perfect = False
        if direction == "bullish":
            is_perfect = (curr["open"] <= prev_body_lo
                          and curr["close"] >= prev_body_hi)
        elif direction == "bearish":
            is_perfect = (curr["open"] >= prev_body_hi
                          and curr["close"] <= prev_body_lo)

        if is_perfect:
            # Check if zone is still intact (PEZ) or broken (FEZ)
            zone_high = max(curr["high"], prev["high"])
            zone_low = min(curr["low"], prev["low"])
            zone_broken = False
            for j in range(i + 1, len(recent)):
                if direction == "bullish" and recent[j]["close"] < zone_low:
                    zone_broken = True
                    break
                elif direction == "bearish" and recent[j]["close"] > zone_high:
                    zone_broken = True
                    break

            # Emperor: "If Bullish Engulfing is broken it becomes Engulf
            # Failed sell zone" — FEZ flips the direction.
            fez_dir = direction
            if zone_broken:
                fez_dir = "bearish" if direction == "bullish" else "bullish"

            patterns.append({
                "type": "perfect",
                "direction": fez_dir if zone_broken else direction,
                "idx": i,
                "candle": curr,
                "zone_high": zone_high,
                "zone_low": zone_low,
                "zone_status": "FEZ" if zone_broken else "PEZ",
            })
            continue

        # ── QM Engulfing (needs 3+ candles: left shoulder structure) ────
        if i >= 2:
            prev_prev = recent[i - 2]
            is_qm = False
            if direction == "bullish":
                # Prev makes lower low than prev_prev, curr engulfs above prev_prev high
                is_qm = (prev["low"] < prev_prev["low"]
                          and curr["close"] > prev_prev["high"])
            else:
                is_qm = (prev["high"] > prev_prev["high"]
                          and curr["close"] < prev_prev["low"])

            if is_qm:
                zone_high = max(curr["high"], prev["high"], prev_prev["high"])
                zone_low = min(curr["low"], prev["low"], prev_prev["low"])
                zone_broken = False
                for j in range(i + 1, len(recent)):
                    if direction == "bullish" and recent[j]["close"] < zone_low:
                        zone_broken = True
                        break
                    elif direction == "bearish" and recent[j]["close"] > zone_high:
                        zone_broken = True
                        break

                # FEZ flips direction (same Emperor rule as perfect engulfing)
                fez_dir = direction
                if zone_broken:
                    fez_dir = "bearish" if direction == "bullish" else "bullish"

                patterns.append({
                    "type": "quasimodo",
                    "direction": fez_dir if zone_broken else direction,
                    "idx": i,
                    "candle": curr,
                    "zone_high": zone_high,
                    "zone_low": zone_low,
                    "zone_status": "FEZ" if zone_broken else "PEZ",
                })
                continue

        # ── Hidden Engulfing (engulfs wick zone beyond the body) ─────────
        # Emperor: current candle's body engulfs the prior candle's wick
        # area (the region between body edge and high/low), not the body itself.
        is_hidden = False
        if direction == "bullish":
            # Bullish hidden: current close exceeds prev high (engulfs upper wick)
            # AND current open is above prev body top (doesn't need to engulf body)
            is_hidden = (curr["close"] > prev["high"]
                         and curr["open"] >= prev_body_hi
                         and prev["high"] > prev_body_hi + _candle_range(prev) * 0.05)
        elif direction == "bearish":
            # Bearish hidden: current close below prev low (engulfs lower wick)
            # AND current open is below prev body bottom
            is_hidden = (curr["close"] < prev["low"]
                         and curr["open"] <= prev_body_lo
                         and prev["low"] < prev_body_lo - _candle_range(prev) * 0.05)

        if is_hidden:
            zone_high = max(curr["high"], prev["high"])
            zone_low = min(curr["low"], prev["low"])
            zone_broken = False
            for j in range(i + 1, len(recent)):
                if direction == "bullish" and recent[j]["close"] < zone_low:
                    zone_broken = True
                    break
                elif direction == "bearish" and recent[j]["close"] > zone_high:
                    zone_broken = True
                    break

            # FEZ flips direction (same Emperor rule as perfect engulfing)
            fez_dir = direction
            if zone_broken:
                fez_dir = "bearish" if direction == "bullish" else "bullish"

            patterns.append({
                "type": "hidden",
                "direction": fez_dir if zone_broken else direction,
                "idx": i,
                "candle": curr,
                "zone_high": zone_high,
                "zone_low": zone_low,
                "zone_status": "FEZ" if zone_broken else "PEZ",
            })

    return patterns[-6:]  # Most recent 6


def _multi_tf_engulfing(h4_engulfing: list[dict], h1_candles: list[dict] | None,
                        levels: list[dict], current_price: float) -> dict:
    """Emperor's 2-TF engulfing zone analysis (PDF Pages 46-47).

    SOP:
    1. Identify engulfing zone on TF1 (H4).
    2. Drop to TF2 (H1) and find Fresh A/V/Gap levels inside the zone.
    3. Track EG-to-EG / EG-to-EF / EF-to-EG flow between zones.

    Returns dict with ltf_levels (levels inside zone) and eg_flow type.
    """
    result = {"ltf_levels_in_zone": [], "eg_flow": None, "eg_flow_label": None}

    if not h4_engulfing:
        return result

    # Find the most relevant H4 engulfing zone (nearest to current price)
    pez_zones = [e for e in h4_engulfing if e.get("zone_status") == "PEZ"]
    fez_zones = [e for e in h4_engulfing if e.get("zone_status") == "FEZ"]
    # Prefer PEZ, fall back to FEZ
    active_zone = None
    for zone in pez_zones + fez_zones:
        if zone["zone_low"] <= current_price <= zone["zone_high"]:
            active_zone = zone
            break
    if not active_zone:
        # Price not inside any zone — find nearest
        by_dist = sorted(h4_engulfing,
                         key=lambda z: min(abs(current_price - z["zone_high"]),
                                           abs(current_price - z["zone_low"])))
        if by_dist:
            active_zone = by_dist[0]

    if not active_zone:
        return result

    # Find SNR levels that fall inside the H4 engulfing zone
    zone_hi = active_zone["zone_high"]
    zone_lo = active_zone["zone_low"]
    pip = _pip_size(current_price)
    tol = LEVEL_TOLERANCE_PIPS * pip

    for lvl in levels:
        if lvl["fresh"] and zone_lo - tol <= lvl["price"] <= zone_hi + tol:
            result["ltf_levels_in_zone"].append({
                "price": lvl["price"],
                "type": lvl["type"],
                "snr_type": lvl["snr_type"],
            })

    # If H1 candles available, look for H1 engulfing inside the zone
    if h1_candles and len(h1_candles) >= 3:
        h1_eng = _detect_engulfing(h1_candles, lookback=20)
        for eng in h1_eng:
            # H1 engulfing whose zone overlaps with the H4 zone
            if (eng["zone_low"] >= zone_lo - tol
                    and eng["zone_high"] <= zone_hi + tol):
                result["ltf_levels_in_zone"].append({
                    "price": (eng["zone_high"] + eng["zone_low"]) / 2,
                    "type": "support" if eng["direction"] == "bullish" else "resistance",
                    "snr_type": f"h1_{eng['type']}",
                })

    # ── EG-to-EG / EG-to-EF / EF-to-EG flow tracking ────────────────
    # Emperor: "Setups will always be based on EG TO EG, EG TO EF, EF TO EG"
    if len(h4_engulfing) >= 2:
        latest = h4_engulfing[-1]
        prev_eg = h4_engulfing[-2]
        lat_status = latest.get("zone_status", "PEZ")
        prev_status = prev_eg.get("zone_status", "PEZ")

        if prev_status == "PEZ" and lat_status == "PEZ":
            result["eg_flow"] = "EG_TO_EG"
            result["eg_flow_label"] = "EG to EG (zone to zone)"
        elif prev_status == "PEZ" and lat_status == "FEZ":
            result["eg_flow"] = "EG_TO_EF"
            result["eg_flow_label"] = "EG to EF (zone to failed)"
        elif prev_status == "FEZ" and lat_status == "PEZ":
            result["eg_flow"] = "EF_TO_EG"
            result["eg_flow_label"] = "EF to EG (failed to zone)"
        elif prev_status == "FEZ" and lat_status == "FEZ":
            result["eg_flow"] = "EF_TO_EF"
            result["eg_flow_label"] = "EF to EF (both failed)"

    return result


# ──────────────────────────────────────────────────────────────────────────────
# 5. Trendline Detection (Emperor: Regular, Breakout, QM)
# ──────────────────────────────────────────────────────────────────────────────

def _detect_trendlines(h4_candles: list[dict], current_price: float) -> list[dict]:
    """Detect trendlines algorithmically by connecting swing points.

    Emperor's trendlines are "angled SNR" that map trend direction.
    Hooking method: connect body closes (bullish for support TL, bearish for resistance TL).

    Types detected:
    - Regular: standard 2-point trendline along swing lows (uptrend) or highs (downtrend)
    - Breakout: trendline that has been broken → becomes opposite bias
    - QM: trendline connecting QM shoulder points

    Returns confluence data: whether price is near a trendline + SNR convergence.
    """
    if len(h4_candles) < 10:
        return []

    pip = _pip_size(current_price)
    tolerance = LEVEL_TOLERANCE_PIPS * pip * 3  # wider zone for trendlines
    recent = h4_candles[-40:]
    # Emperor hooking method: trendlines use candle BODIES, not wicks
    swings = _find_swings(recent, left=2, right=2, use_body=True)
    trendlines: list[dict] = []

    # ── Uptrend trendlines: connect ascending swing lows ─────────────
    swing_lows = [s for s in swings if s["type"] == "low"]
    for i in range(len(swing_lows) - 1):
        for j in range(i + 1, min(i + 5, len(swing_lows))):
            s1, s2 = swing_lows[i], swing_lows[j]
            if s2["price"] <= s1["price"]:
                continue  # need ascending lows
            if s2["idx"] <= s1["idx"]:
                continue

            slope = (s2["price"] - s1["price"]) / (s2["idx"] - s1["idx"])
            # Project trendline to current bar
            bars_ahead = len(recent) - 1 - s2["idx"]
            projected = s2["price"] + slope * bars_ahead

            # Check if price is near the projected trendline
            dist = current_price - projected
            near = abs(dist) < tolerance

            # Check for breakout (price closed below)
            broken = current_price < projected - tolerance
            tl_type = "breakout" if broken else "regular"

            trendlines.append({
                "direction": "bullish",
                "type": tl_type,
                "slope": slope,
                "projected_price": projected,
                "distance": dist,
                "near_price": near,
                "p1": s1["price"],
                "p2": s2["price"],
            })

    # ── Downtrend trendlines: connect descending swing highs ─────────
    swing_highs = [s for s in swings if s["type"] == "high"]
    for i in range(len(swing_highs) - 1):
        for j in range(i + 1, min(i + 5, len(swing_highs))):
            s1, s2 = swing_highs[i], swing_highs[j]
            if s2["price"] >= s1["price"]:
                continue  # need descending highs
            if s2["idx"] <= s1["idx"]:
                continue

            slope = (s2["price"] - s1["price"]) / (s2["idx"] - s1["idx"])
            bars_ahead = len(recent) - 1 - s2["idx"]
            projected = s2["price"] + slope * bars_ahead

            dist = projected - current_price
            near = abs(dist) < tolerance

            broken = current_price > projected + tolerance
            tl_type = "breakout" if broken else "regular"

            trendlines.append({
                "direction": "bearish",
                "type": tl_type,
                "slope": slope,
                "projected_price": projected,
                "distance": dist,
                "near_price": near,
                "p1": s1["price"],
                "p2": s2["price"],
            })

    # Sort by proximity to current price
    trendlines.sort(key=lambda t: abs(t["distance"]))
    return trendlines[:5]


def _trendline_snr_confluence(trendlines: list[dict], levels: list[dict],
                              current_price: float) -> dict:
    """Check for Emperor's 'X-factor' — trendline + SNR convergence.

    When a trendline and an SNR level meet at the same price zone, it's
    a high-probability decision point.

    Emperor rule: "Trendline doesn't apply a GAP SNR (except when refined
    into LTF Engulfing)." — so gap levels are excluded from confluence.
    """
    pip = _pip_size(current_price)
    tolerance = LEVEL_TOLERANCE_PIPS * pip * 5  # generous zone for convergence

    for tl in trendlines:
        if not tl["near_price"]:
            continue
        for lvl in levels:
            if not lvl["fresh"]:
                continue
            # Emperor: trendline does NOT apply to GAP SNR levels
            if lvl["snr_type"].startswith("gap"):
                continue
            if abs(tl["projected_price"] - lvl["price"]) < tolerance:
                return {
                    "active": True,
                    "tl_direction": tl["direction"],
                    "tl_type": tl["type"],
                    "snr_price": lvl["price"],
                    "snr_type": lvl["snr_type"],
                    "convergence_price": (tl["projected_price"] + lvl["price"]) / 2,
                }

    return {"active": False}


# ──────────────────────────────────────────────────────────────────────────────
# 6. Roadblock Check (Emperor Rule 4)
# ──────────────────────────────────────────────────────────────────────────────

def _check_roadblocks(levels: list[dict], current_price: float,
                      target: Optional[float], direction: str) -> list[dict]:
    """Check for opposing fresh levels between price and target.

    Per Emperor: roadblocks are 1-TF-lower opposing fresh levels in the path.
    Price will bounce from these before completing the storyline.
    """
    if target is None:
        return []

    roadblocks = []
    for lvl in levels:
        if not lvl["fresh"]:
            continue
        if direction == "bullish" and lvl["type"] == "resistance":
            if current_price < lvl["price"] < target:
                roadblocks.append({
                    "price": lvl["price"],
                    "type": "resistance",
                    "snr_type": lvl["snr_type"],
                })
        elif direction == "bearish" and lvl["type"] == "support":
            if target < lvl["price"] < current_price:
                roadblocks.append({
                    "price": lvl["price"],
                    "type": "support",
                    "snr_type": lvl["snr_type"],
                })

    roadblocks.sort(key=lambda x: abs(x["price"] - current_price))
    return roadblocks


# ──────────────────────────────────────────────────────────────────────────────
# 7. Entry Tier Classification (Emperor Setups 1-4)
# ──────────────────────────────────────────────────────────────────────────────

def _classify_entry_tier(
    storyline: dict,
    engulfing: list[dict],
    levels: list[dict],
    current_price: float,
    h4_candles: list[dict],
    tl_confluence: dict,
) -> dict:
    """Classify entry tier per the Emperor's 4 setup types.

    Setup 1 (High Risk): Price at fresh HTF level, no lower-TF confirmation.
    Setup 2A (Medium Risk): Same-level rejection + H4 fresh level refinement.
    Setup 2B (Medium Risk): 2 TF lower breakout confirmation.
    Setup 3 (Low Risk): 1 TF BO confirmed + pullback to QM/Apex.
    Setup 4 (Continuation): Last H4 fresh aligned with H1 fresh in storyline dir.
    """
    if not storyline.get("active"):
        return {"tier": "no_setup", "setup_num": 0,
                "label": "No active storyline", "confidence": "none"}

    pip = _pip_size(current_price)
    tolerance = LEVEL_TOLERANCE_PIPS * pip
    direction = storyline["direction"]
    confirmed = storyline.get("confirmed", False)

    # Check for engulfing confirmation in storyline direction
    dir_engulfing = [e for e in engulfing[-4:]
                     if e["direction"] == direction]
    has_engulfing = len(dir_engulfing) > 0
    has_pez = any(e.get("zone_status") == "PEZ" for e in dir_engulfing)
    has_fez_opposite = any(
        e.get("zone_status") == "FEZ" and e["direction"] != direction
        for e in engulfing[-4:]
    )

    # Check proximity to fresh level
    at_fresh_level = False
    target_level = None
    for lvl in levels:
        if not lvl["fresh"]:
            continue
        if direction == "bullish" and lvl["type"] == "support":
            if abs(current_price - lvl["price"]) < tolerance * 5:
                at_fresh_level = True
                target_level = lvl["price"]
                break
        elif direction == "bearish" and lvl["type"] == "resistance":
            if abs(current_price - lvl["price"]) < tolerance * 5:
                at_fresh_level = True
                target_level = lvl["price"]
                break

    # Check for H4 breakout (proxy for 1-TF lower BO)
    h4_breakout = False
    h4_pullback = False
    if h4_candles and len(h4_candles) >= 5:
        last5 = h4_candles[-5:]
        if direction == "bullish":
            swing_high = max(c["high"] for c in last5[:-2])
            h4_breakout = last5[-1]["close"] > swing_high
            # Pullback: price came back after BO
            if h4_breakout and last5[-1]["low"] < last5[-2]["high"]:
                h4_pullback = True
        else:
            swing_low = min(c["low"] for c in last5[:-2])
            h4_breakout = last5[-1]["close"] < swing_low
            if h4_breakout and last5[-1]["high"] > last5[-2]["low"]:
                h4_pullback = True

    # Check for QM pullback (from engulfing patterns)
    qm_pullback = any(e["type"] == "quasimodo" and e["direction"] == direction
                      for e in dir_engulfing)

    # Trendline confluence bonus
    has_tl_confluence = tl_confluence.get("active", False)

    # ── Setup 3: Low Risk — 1 TF BO + pullback to QM/Apex ────────────
    if confirmed and h4_breakout and (h4_pullback or qm_pullback):
        # Confidence: both engulfing + trendline → high, one → medium, neither → low
        if has_engulfing and has_tl_confluence:
            conf = "high"
        elif has_engulfing or has_tl_confluence:
            conf = "medium"
        else:
            conf = "low"
        return {
            "tier": "low",
            "setup_num": 3,
            "label": "Low Risk — 1TF BO + pullback confirmed",
            "confidence": conf,
            "entry_price": target_level or storyline.get("to_level"),
        }

    # ── Setup 4: Low Risk — Continuation (H4 fresh aligned with direction) ──
    if confirmed and h4_breakout and not h4_pullback:
        # Look for last H4 fresh level aligned with storyline
        aligned_fresh = None
        for lvl in levels:
            if not lvl["fresh"]:
                continue
            if direction == "bullish" and lvl["type"] == "support":
                if current_price - lvl["price"] < tolerance * 10:
                    aligned_fresh = lvl
                    break
            elif direction == "bearish" and lvl["type"] == "resistance":
                if lvl["price"] - current_price < tolerance * 10:
                    aligned_fresh = lvl
                    break

        if aligned_fresh:
            return {
                "tier": "low",
                "setup_num": 4,
                "label": "Low Risk — Continuation (aligned fresh)",
                "confidence": "high" if has_engulfing else "medium",
                "entry_price": aligned_fresh["price"],
            }

    # ── Setup 2B: Medium Risk — 2 TF lower BO without pullback ───────
    if confirmed and h4_breakout and not h4_pullback:
        return {
            "tier": "medium",
            "setup_num": 2,
            "label": "Medium Risk — H4 BO confirmed, await pullback",
            "confidence": "medium",
            "entry_price": target_level or storyline.get("to_level"),
        }

    # ── Setup 2A: Medium Risk — Rejection + fresh level refinement ────
    if at_fresh_level and has_engulfing:
        return {
            "tier": "medium",
            "setup_num": 2,
            "label": "Medium Risk — Rejection + engulfing at fresh level",
            "confidence": "high" if has_pez else "medium",
            "entry_price": target_level,
        }

    # ── Setup 1: High Risk — at fresh level, no confirmation ──────────
    if at_fresh_level and not confirmed:
        return {
            "tier": "high",
            "setup_num": 1,
            "label": "High Risk — at fresh level (50/50)",
            "confidence": "low",
            "entry_price": target_level,
        }

    # ── Storyline active but no clear entry trigger ───────────────────
    if confirmed:
        return {
            "tier": "medium",
            "setup_num": 2,
            "label": "Medium Risk — BO confirmed, target in sight",
            "confidence": "medium",
            "entry_price": storyline.get("to_level"),
        }

    return {
        "tier": "no_setup",
        "setup_num": 0,
        "label": "Storyline active but no entry trigger",
        "confidence": "low",
        "entry_price": None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 8. 5-Action SOP Scoring (Emperor's Complete Flow)
# ──────────────────────────────────────────────────────────────────────────────

def _sop_score(
    levels: list[dict],
    storyline: dict,
    engulfing: list[dict],
    entry: dict,
    roadblocks: list[dict],
    tl_confluence: dict,
    qm_levels: list[dict],
    current_price: float,
    mtf_eg: dict | None = None,
) -> tuple[int, list[str]]:
    """Score using the Emperor's 5-action SOP flow.

    1. Zone marked (HTF engulfing / fresh level) → +2
    2. Price rejection (storyline rejection) → +3
    3. Price breakout (confirmed BO on lower TF) → +3
    4. Confluence (trendline + SNR X-factor) → +2
    5. Entry confirmation (engulfing + zone retest) → +2
    Bonus: MISS-validated levels → +1, clear path → +1,
           multi-TF EG zone → +1, valid EG flow → +1

    Max score: 16 (12 base + 4 bonus), capped at 15.
    Grades: A ≥ 10, B ≥ 7, C ≥ 4.
    """
    if mtf_eg is None:
        mtf_eg = {}
    pip = _pip_size(current_price)
    score = 0
    notes: list[str] = []

    # ── SOP 1: Zone Marked ────────────────────────────────────────────
    fresh_count = sum(1 for l in levels if l["fresh"])
    has_zone = fresh_count >= 2
    has_qm_zone = len(qm_levels) > 0

    if has_qm_zone:
        score += 2
        notes.append(f"QM zone ({qm_levels[0]['snr_type'].upper()})")
    elif has_zone:
        score += 2
        notes.append(f"Zone marked ({fresh_count} fresh)")
    else:
        if fresh_count == 1:
            score += 1
            notes.append(f"Weak zone ({fresh_count} fresh)")

    # ── SOP 2: Price Rejection ────────────────────────────────────────
    if storyline.get("active"):
        score += 3
        stl_type = storyline.get("from_snr_type", "classic")
        notes.append(f"Storyline {storyline['direction']} ({stl_type})")

    # ── SOP 3: Breakout Confirmation ──────────────────────────────────
    if storyline.get("confirmed"):
        score += 3
        notes.append("Breakout confirmed")
    elif storyline.get("active"):
        # No free point for unconfirmed — Emperor requires BO for full credit
        notes.append("Awaiting BO confirmation")

    # ── SOP 4: Confluence (X-Factor) ──────────────────────────────────
    if tl_confluence.get("active"):
        score += 2
        notes.append(f"TL+SNR confluence ({tl_confluence['tl_type']})")
    elif engulfing:
        # Engulfing at level = partial confluence
        dir_eng = [e for e in engulfing if storyline.get("direction")
                   and e["direction"] == storyline["direction"]]
        if dir_eng:
            pez_count = sum(1 for e in dir_eng if e.get("zone_status") == "PEZ")
            if pez_count:
                score += 2
                notes.append(f"PEZ zone ({dir_eng[-1]['type']})")
            else:
                score += 1
                notes.append(f"Engulfing: {dir_eng[-1]['type']} ({dir_eng[-1]['direction']})")

    # ── SOP 5: Entry Confirmation ─────────────────────────────────────
    if entry.get("tier") in ("low", "medium") and entry.get("confidence") in ("high", "medium"):
        tier = entry["tier"]
        if tier == "low":
            score += 2
            notes.append(f"Entry: Setup {entry.get('setup_num', '?')} (low risk)")
        elif tier == "medium":
            score += 1
            notes.append(f"Entry: Setup {entry.get('setup_num', '?')} (medium risk)")
    elif entry.get("tier") == "high":
        notes.append("Entry: Setup 1 (high risk — 50/50)")

    # ── Bonus: MISS validated levels ──────────────────────────────────
    miss_levels = [l for l in levels if l["miss_count"] >= 2 and l["fresh"]]
    if miss_levels:
        score += 1
        notes.append(f"{len(miss_levels)} MISS-validated levels")

    # ── Bonus: No roadblocks ──────────────────────────────────────────
    if not roadblocks:
        score += 1
        notes.append("Clear path (no roadblocks)")

    # ── Bonus: Multi-TF engulfing zone confirmation (Emperor p46-47) ──
    ltf_in_zone = mtf_eg.get("ltf_levels_in_zone", [])
    if ltf_in_zone:
        score += 1
        notes.append(f"MTF: {len(ltf_in_zone)} level(s) inside EG zone")

    # ── Bonus: Valid EG flow (EG→EG, EG→EF, EF→EG) ──────────────────
    eg_flow = mtf_eg.get("eg_flow")
    if eg_flow in ("EG_TO_EG", "EG_TO_EF", "EF_TO_EG"):
        score += 1
        notes.append(f"EG flow: {mtf_eg.get('eg_flow_label', eg_flow)}")

    # Cap at 15 (theoretical max can exceed 12 with bonuses)
    score = min(score, 15)

    return score, notes


# ──────────────────────────────────────────────────────────────────────────────
# Trade Plan Builder — SL / TP1 / TP2 / R:R
# ──────────────────────────────────────────────────────────────────────────────

def _calc_atr(candles: list[dict], period: int = 14) -> float:
    """Average True Range from daily candles."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        c = candles[i]
        prev_close = candles[i - 1]["close"]
        tr = max(c["high"] - c["low"],
                 abs(c["high"] - prev_close),
                 abs(c["low"] - prev_close))
        trs.append(tr)
    return sum(trs[-period:]) / min(len(trs), period)


def _build_trade_plan(
    setup: str,
    entry_price: float | None,
    storyline: dict,
    all_levels: list[dict],
    daily_candles: list[dict],
    current_price: float,
) -> dict:
    """Build concrete trade plan: Entry, SL, TP1, TP2, R:R.

    Emperor SL rules:
      - BUY: SL below the entry zone (next lower support or ATR-based)
      - SELL: SL above the entry zone (next higher resistance or ATR-based)

    Emperor TP rules:
      - TP1: storyline target (next opposing fresh level) — minimum 1:1 R:R
      - TP2: next level beyond TP1, or 2× the TP1 distance
    """
    empty = {"entry": None, "sl": None, "tp1": None, "tp2": None,
             "rr1": None, "rr2": None, "sl_pips": None,
             "tp1_pips": None, "tp2_pips": None}

    if setup not in ("BUY", "SELL") or not entry_price:
        return empty

    pip = _pip_size(entry_price)
    atr = _calc_atr(daily_candles)
    if atr == 0:
        atr = pip * 50  # fallback: 50 pips

    is_buy = setup == "BUY"

    # ── Entry ──────────────────────────────────────────────────────────
    entry = entry_price

    # ── SL — find nearest level BEHIND the entry (opposing side) ───────
    sl = None
    # For BUY: find fresh support levels BELOW entry, SL just below the nearest
    # For SELL: find fresh resistance levels ABOVE entry, SL just above the nearest
    sl_candidates = []
    for lvl in all_levels:
        if is_buy and lvl["type"] == "support" and lvl["price"] < entry:
            sl_candidates.append(lvl["price"])
        elif not is_buy and lvl["type"] == "resistance" and lvl["price"] > entry:
            sl_candidates.append(lvl["price"])

    if sl_candidates:
        if is_buy:
            # Nearest support below entry → SL below it
            nearest_below = max(sl_candidates)
            sl = nearest_below - atr * 0.15  # small buffer below zone
        else:
            nearest_above = min(sl_candidates)
            sl = nearest_above + atr * 0.15
    else:
        # Fallback: ATR-based SL
        sl = entry - atr * 0.5 if is_buy else entry + atr * 0.5

    # Clamp SL: minimum 10 pips, maximum ATR distance
    sl_dist = abs(entry - sl)
    min_sl = pip * 10
    max_sl = atr * 0.8
    if sl_dist < min_sl:
        sl = entry - min_sl if is_buy else entry + min_sl
    elif sl_dist > max_sl:
        sl = entry - max_sl if is_buy else entry + max_sl

    # ── TP1 — storyline target or next opposing fresh level ──────────
    tp1 = None
    storyline_to = storyline.get("to_level")

    if storyline_to:
        tp1 = storyline_to
    else:
        # Find next opposing level in trade direction
        tp_candidates = []
        for lvl in all_levels:
            if is_buy and lvl["type"] == "resistance" and lvl["price"] > entry:
                tp_candidates.append(lvl["price"])
            elif not is_buy and lvl["type"] == "support" and lvl["price"] < entry:
                tp_candidates.append(lvl["price"])
        if tp_candidates:
            tp1 = min(tp_candidates) if is_buy else max(tp_candidates)

    # Fallback / minimum: at least 1:1 R:R
    sl_dist = abs(entry - sl)
    if tp1 is None:
        tp1 = entry + sl_dist if is_buy else entry - sl_dist
    elif abs(tp1 - entry) < sl_dist:
        # TP1 closer than SL → use 1:1 R:R minimum
        tp1 = entry + sl_dist if is_buy else entry - sl_dist

    # ── TP2 — next level beyond TP1, or 2× extension ─────────────────
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
        # Fallback: 2× TP1 distance
        tp1_dist = abs(tp1 - entry)
        tp2 = entry + tp1_dist * 2 if is_buy else entry - tp1_dist * 2

    # ── R:R calculation ───────────────────────────────────────────────
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
# Per-pair analysis
# ──────────────────────────────────────────────────────────────────────────────

def analyze_pair(
    symbol: str,
    daily_candles: list[dict],
    h4_candles: list[dict],
    h1_candles: Optional[list[dict]] = None,
    m15_candles: Optional[list[dict]] = None,
    now_ny: Optional[datetime] = None,
) -> dict:
    """Run the Malaysian SNR Emperor pipeline for one pair."""
    if now_ny is None:
        now_ny = datetime.now(NY)

    if not daily_candles or len(daily_candles) < 15:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-DATA",
                "reason": "insufficient daily candles", "score": 0}

    current_price = daily_candles[-1]["close"]
    pip = _pip_size(current_price)

    # 1. Mark SNR levels from daily close-to-open junctions
    levels = _mark_levels(daily_candles)

    # 2. Detect QM patterns from H4
    qm_levels = _detect_qm_levels(h4_candles or [], current_price)
    # Merge QM levels into main level list
    all_levels = levels + qm_levels
    all_levels = _deduplicate_levels(all_levels, LEVEL_TOLERANCE_PIPS * pip)

    fresh_levels = [l for l in all_levels if l["fresh"]]
    unfresh_levels = [l for l in all_levels if not l["fresh"]]

    # 3. Detect storyline (4 rules)
    storyline = _detect_storyline(all_levels, h4_candles or [], current_price)

    # 4. Detect engulfing patterns (Perfect, QM, Hidden) + PEZ/FEZ
    engulfing = _detect_engulfing(h4_candles or daily_candles[-20:])

    # 4b. Multi-TF engulfing: drop to H1 to find levels inside H4 zone
    #     + EG-to-EG / EG-to-EF / EF-to-EG flow (Emperor Pages 46-47)
    mtf_eg = _multi_tf_engulfing(engulfing, h1_candles, all_levels, current_price)

    # 5. Detect trendlines
    trendlines = _detect_trendlines(h4_candles or [], current_price)
    tl_confluence = _trendline_snr_confluence(trendlines, all_levels, current_price)

    # 6. Roadblocks
    direction = (storyline.get("direction")
                 or ("bullish" if _is_bullish(daily_candles[-1]) else "bearish"))
    roadblocks = _check_roadblocks(
        all_levels, current_price, storyline.get("to_level"), direction
    )

    # 7. Entry tier classification (Emperor setups 1-4)
    entry = _classify_entry_tier(
        storyline, engulfing, all_levels, current_price,
        h4_candles or [], tl_confluence,
    )

    # 8. 5-Action SOP scoring
    score, notes = _sop_score(
        all_levels, storyline, engulfing, entry, roadblocks,
        tl_confluence, qm_levels, current_price, mtf_eg,
    )

    # ── Setup determination ───────────────────────────────────────────
    # Priority 1: Active storyline with a concrete entry tier
    if storyline.get("active") and entry["tier"] != "no_setup":
        setup = "BUY" if storyline["direction"] == "bullish" else "SELL"
    # Priority 2: Active storyline without entry trigger — show direction but
    # keep it as NO-TRADE unless confirmation is strong.  NEVER let engulfing
    # override the storyline direction — that produced contradictory signals
    # (e.g., storyline bullish + bearish engulfing → SELL).
    elif storyline.get("active") and storyline.get("confirmed"):
        setup = "BUY" if storyline["direction"] == "bullish" else "SELL"
    # Priority 3: No storyline — use PEZ engulfing as a standalone signal
    elif not storyline.get("active") and engulfing and engulfing[-1]["direction"]:
        last_eng = engulfing[-1]
        if last_eng.get("zone_status") == "PEZ":
            setup = "BUY" if last_eng["direction"] == "bullish" else "SELL"
        else:
            setup = "NO-TRADE"
    else:
        setup = "NO-TRADE"

    # ── Grade ─────────────────────────────────────────────────────────
    # Raised thresholds: A≥10 B≥7 C≥4 to avoid grade inflation.
    # Emperor methodology is selective — Grade A should be rare.
    if score >= 10:
        grade = "A"
    elif score >= 7:
        grade = "B"
    elif score >= 4:
        grade = "C"
    else:
        grade = "NO-TRADE"
        setup = "NO-TRADE"  # Emperor: no valid SOP = no signal

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

    # ── Trade Plan — SL / TP1 / TP2 / R:R ────────────────────────────
    trade_plan = _build_trade_plan(
        setup=setup,
        entry_price=entry.get("entry_price"),
        storyline=storyline,
        all_levels=all_levels,
        daily_candles=daily_candles,
        current_price=current_price,
    )

    return {
        "symbol": symbol,
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
    """Run Malaysian SNR Emperor pipeline for every pair.

    `candles_by_pair[sym]` = {'1d': [...], 'm15': [...]}.
    H4 synthesized from M15 if not directly available.
    """
    now_ny = datetime.now(NY)

    pairs_out = []
    for sym in SNR_UNIVERSE:
        bundles = candles_by_pair.get(sym, {})
        daily = bundles.get("1d", [])
        h4 = bundles.get("4h", bundles.get("h4", []))

        # If no H4, synthesize from M15
        if not h4 and bundles.get("m15"):
            try:
                from crt_utils import build_h4_buckets
                buckets = build_h4_buckets(bundles["m15"])
                h4 = sorted(
                    [{"open": b["open"], "high": b["high"], "low": b["low"],
                      "close": b["close"],
                      "ts_utc": int(b["start_ny"].timestamp())}
                     for b in buckets.values()],
                    key=lambda x: x["ts_utc"],
                )
            except Exception as e:
                log.warning("H4 synthesis failed for %s: %s", sym, e)
                h4 = []

        h1 = bundles.get("1h", bundles.get("h1", []))
        m15 = bundles.get("m15", [])
        pairs_out.append(
            analyze_pair(
                symbol=sym,
                daily_candles=daily,
                h4_candles=h4,
                h1_candles=h1 or None,
                m15_candles=m15 or None,
                now_ny=now_ny,
            )
        )

    buys = sum(1 for p in pairs_out if p["setup"] == "BUY")
    sells = sum(1 for p in pairs_out if p["setup"] == "SELL")
    grade_a = sum(1 for p in pairs_out if p["grade"] == "A")
    grade_b = sum(1 for p in pairs_out if p["grade"] == "B")
    storylines = sum(1 for p in pairs_out if p.get("storyline", {}).get("active"))

    return {
        "universe": SNR_UNIVERSE,
        "now_ny": now_ny.strftime("%Y-%m-%d %H:%M %Z"),
        "buys": buys,
        "sells": sells,
        "grade_a": grade_a,
        "grade_b": grade_b,
        "active_storylines": storylines,
        "pairs": pairs_out,
    }
