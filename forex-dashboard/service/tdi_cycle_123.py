"""TDI Cycle 123 Reversal — improvements on classic BTMM.

Detects the "Peak Formation" 123 reversal pattern taught in the Inducement
Cycles / StrictlyCorrect material:

  * Point 1 = initial swing extreme
  * Point 2 = counter-move (must pull back through midline)
  * Point 3 = failed retest of point 1's zone (equal, marginal, or shallow miss)
  * Divergence between price 1→3 slope and TDI RSI 1→3 slope is REQUIRED
  * TDI baseline (RSI SMA-34, "yellow line") must be at an extreme when point 3 forms
    - Long: baseline ≤ 37,  RSI < 32 (oversold)
    - Short: baseline ≥ 63,  RSI > 68 (overbought)
  * Setup zone should sit near a fresh liquidity level (session HL, PDH/PDL)

Targets follow the BTMM EMA cascade:
  * L1 = 50 EMA
  * L2 = 200 EMA
  * L3 = 800 EMA

Grade is a 0-15 confluence score:
  * 5 core conditions (each up to 3 pts)
  * Extra pts for HTF alignment (H4 direction agrees)
  * Grade A ≥ 11, B ≥ 8, C ≥ 5

Public API mirrors snr_strategy so the scheduler / alerts / dashboard wiring is
symmetrical:

  analyze_pair(symbol, h1_candles, h4_candles=None, d1_candles=None) -> dict
  analyze_universe(candles_by_pair) -> {"pairs": [...], "buys": int, ...}
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import instruments
from btmm_core import calc_ema, calc_tdi, ema_last
from config import PRIORITY_PAIRS

log = logging.getLogger("tdi_cycle_123")

NY = ZoneInfo("America/New_York")

# Universe = same watched universe as BTMM / SNR
TDI123_UNIVERSE = list(PRIORITY_PAIRS)

# ── Detection thresholds ─────────────────────────────────────────────────────
# TDI RSI: standard 63/68 overbought, 32/37 oversold (StrictlyCorrect spec)
TDI_OB_ZONE = 63.0        # yellow-baseline extreme (bearish setup)
TDI_OB_STRICT = 68.0      # RSI overbought
TDI_OS_ZONE = 37.0        # yellow-baseline extreme (bullish setup)
TDI_OS_STRICT = 32.0      # RSI oversold

# Swing detection — fractal window
SWING_LEFT = 2
SWING_RIGHT = 2

# 123 geometry (in pips relative to point-1 → point-2 range)
# Point 3 must land within ±POINT3_TOLERANCE of point 1 for a valid retest.
POINT3_TOLERANCE_PCT = 0.25   # 25 % of the 1→2 leg
# Point 2 must retrace at least this fraction of leg 1
POINT2_MIN_RETRACE_PCT = 0.30

# Divergence slope threshold — how much the two 1→3 slopes must differ.
# 0 = any divergence; higher = require stronger divergence.
DIVERGENCE_MIN_SLOPE_RATIO = 0.15


def _pip_size(price: float, symbol: Optional[str] = None) -> float:
    return instruments.pip_size(symbol, price)


# ──────────────────────────────────────────────────────────────────────────────
# 1. Swing point detection
# ──────────────────────────────────────────────────────────────────────────────

def _find_swings(bars: list[dict], left: int = SWING_LEFT,
                 right: int = SWING_RIGHT) -> list[dict]:
    """Return chronologically-ordered fractal swing points.

    A swing high at i needs `left` bars on the left with lower highs, and `right`
    bars on the right with lower highs. Symmetric definition for swing lows.
    Only "confirmed" swings are returned — the last `right` bars can't produce one.
    """
    if len(bars) < left + right + 1:
        return []

    swings: list[dict] = []
    for i in range(left, len(bars) - right):
        h_center = bars[i]["high"]
        l_center = bars[i]["low"]
        is_high = all(bars[j]["high"] <= h_center for j in range(i - left, i)) and \
                  all(bars[j]["high"] < h_center for j in range(i + 1, i + right + 1))
        is_low = all(bars[j]["low"] >= l_center for j in range(i - left, i)) and \
                 all(bars[j]["low"] > l_center for j in range(i + 1, i + right + 1))
        if is_high:
            swings.append({"type": "high", "idx": i, "price": h_center,
                           "ts_utc": bars[i]["ts_utc"]})
        if is_low:
            swings.append({"type": "low", "idx": i, "price": l_center,
                           "ts_utc": bars[i]["ts_utc"]})
    return swings


# ──────────────────────────────────────────────────────────────────────────────
# 2. 123 pattern geometry
# ──────────────────────────────────────────────────────────────────────────────

def _find_123_pattern(swings: list[dict], bars: list[dict],
                      symbol: Optional[str] = None) -> Optional[dict]:
    """Look for a valid 123 setup among the most recent swings.

    Bullish setup: 1 = swing low, 2 = swing high, 3 = swing low retesting point 1.
    Bearish setup: 1 = swing high, 2 = swing low, 3 = swing high retesting point 1.

    Returns None if no valid pattern is found.
    """
    if len(swings) < 3:
        return None

    price = bars[-1]["close"]
    pip = _pip_size(price, symbol)

    # Look at the last 6 swings — pattern must sit in the recent structure.
    recent = swings[-6:]

    # Try every (a, b, c) triple in chronological order.
    for a in range(len(recent) - 2):
        for b in range(a + 1, len(recent) - 1):
            for c in range(b + 1, len(recent)):
                p1, p2, p3 = recent[a], recent[b], recent[c]

                # Alternating types required (low-high-low or high-low-high).
                if p1["type"] == p2["type"] or p2["type"] == p3["type"]:
                    continue
                if p1["type"] != p3["type"]:
                    continue

                is_bullish = (p1["type"] == "low")
                leg1_range = abs(p2["price"] - p1["price"])
                if leg1_range == 0:
                    continue

                # Retrace check: p2 must move at least POINT2_MIN_RETRACE_PCT
                # (already true by fractal definition, but be explicit)
                retrace_ok = leg1_range > POINT2_MIN_RETRACE_PCT * pip
                if not retrace_ok:
                    continue

                # Geometry: point 3 must be within tolerance of point 1
                # (equal, marginal break, or shallow miss).
                tolerance = POINT3_TOLERANCE_PCT * leg1_range
                dist_p1_p3 = abs(p3["price"] - p1["price"])
                if dist_p1_p3 > tolerance:
                    continue

                # Direction correctness — point 3 in the same "extreme" territory as p1
                if is_bullish:
                    # Bullish: p3 is a low, should be near or slightly above/below p1's low
                    valid = p3["price"] <= p1["price"] + tolerance and p3["price"] >= p1["price"] - tolerance
                else:
                    valid = p3["price"] >= p1["price"] - tolerance and p3["price"] <= p1["price"] + tolerance
                if not valid:
                    continue

                return {
                    "direction": "bullish" if is_bullish else "bearish",
                    "p1": p1,
                    "p2": p2,
                    "p3": p3,
                    "leg1_range": leg1_range,
                    "leg1_range_pips": leg1_range / pip,
                    "tolerance_pips": tolerance / pip,
                }
    return None


# ──────────────────────────────────────────────────────────────────────────────
# 3. Divergence check — price 1→3 vs TDI RSI 1→3
# ──────────────────────────────────────────────────────────────────────────────

def _check_divergence(pattern: dict, rsi_series: list[float]) -> dict:
    """Compare price direction (p1→p3) with TDI RSI direction (p1→p3).

    The two series live on different scales (price ~ 1e-5 for FX, RSI 0-100),
    so we compare DIRECTIONS + normalised magnitudes, not raw slopes.

    Bullish divergence: price makes a lower / equal low at p3 while RSI makes a
    HIGHER low at p3.
    Bearish divergence: price makes a higher / equal high at p3 while RSI makes
    a LOWER high at p3.
    """
    p1, p3 = pattern["p1"], pattern["p3"]
    if p1["idx"] >= len(rsi_series) or p3["idx"] >= len(rsi_series):
        return {"present": False, "reason": "index out of RSI series"}

    rsi_p1 = rsi_series[p1["idx"]]
    rsi_p3 = rsi_series[p3["idx"]]
    price_delta = p3["price"] - p1["price"]
    rsi_delta = rsi_p3 - rsi_p1

    # Normalise price delta to the leg-1 range so we can measure "flat vs moved"
    # without mixing scales.
    leg1 = pattern.get("leg1_range", 0) or 1e-12
    price_move_ratio = price_delta / leg1  # ~0 = flat, negative = new low, positive = new high

    is_bullish = (pattern["direction"] == "bullish")
    # Direction must be opposite (or price flat + RSI meaningfully moved)
    if is_bullish:
        direction_ok = rsi_delta > 0 and price_move_ratio <= DIVERGENCE_MIN_SLOPE_RATIO
        # Require RSI to have travelled at least ~2 pts to filter noise
        magnitude_ok = rsi_delta >= 2.0
    else:
        direction_ok = rsi_delta < 0 and price_move_ratio >= -DIVERGENCE_MIN_SLOPE_RATIO
        magnitude_ok = rsi_delta <= -2.0

    divergence = bool(direction_ok and magnitude_ok)

    return {
        "present": divergence,
        "price_move_ratio": price_move_ratio,
        "rsi_delta": rsi_delta,
        "rsi_at_p1": rsi_p1,
        "rsi_at_p3": rsi_p3,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 4. TDI extreme check
# ──────────────────────────────────────────────────────────────────────────────

def _baseline_series(rsi_series: list[float], period: int = 34) -> list[float]:
    """SMA(period) of RSI — the yellow baseline."""
    out = []
    for i in range(len(rsi_series)):
        start = max(0, i - period + 1)
        window = rsi_series[start:i + 1]
        out.append(sum(window) / len(window))
    return out


def _check_tdi_extreme(pattern: dict, rsi_series: list[float],
                        baseline_series: list[float]) -> dict:
    """Check whether TDI is at an extreme at point 3 formation."""
    p3_idx = pattern["p3"]["idx"]
    if p3_idx >= len(rsi_series) or p3_idx >= len(baseline_series):
        return {"present": False, "reason": "index out of TDI series"}

    rsi_at_p3 = rsi_series[p3_idx]
    baseline_at_p3 = baseline_series[p3_idx]
    is_bullish = (pattern["direction"] == "bullish")

    if is_bullish:
        # For a long, we want oversold TDI at point 3
        baseline_extreme = baseline_at_p3 <= TDI_OS_ZONE
        rsi_extreme = rsi_at_p3 <= TDI_OS_STRICT
    else:
        baseline_extreme = baseline_at_p3 >= TDI_OB_ZONE
        rsi_extreme = rsi_at_p3 >= TDI_OB_STRICT

    return {
        "present": bool(baseline_extreme or rsi_extreme),
        "strong": bool(baseline_extreme and rsi_extreme),
        "baseline_at_p3": baseline_at_p3,
        "rsi_at_p3": rsi_at_p3,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 5. Signal cross confirmation — TDI fast crosses signal after point 3
# ──────────────────────────────────────────────────────────────────────────────

def _check_signal_cross(pattern: dict, fast_arr: list[float],
                         slow_arr: list[float]) -> dict:
    """Look for a fast/slow cross in the confirmation direction after point 3.

    Bullish setup wants fast > slow (green over red).
    Bearish setup wants fast < slow.
    """
    p3_idx = pattern["p3"]["idx"]
    if p3_idx >= len(fast_arr) - 1:
        return {"present": False, "reason": "no post-p3 bars"}

    is_bullish = (pattern["direction"] == "bullish")
    post = list(zip(fast_arr[p3_idx:], slow_arr[p3_idx:]))
    for i in range(1, len(post)):
        prev_f, prev_s = post[i - 1]
        cur_f, cur_s = post[i]
        if is_bullish and prev_f <= prev_s and cur_f > cur_s:
            return {"present": True, "cross_offset": i}
        if not is_bullish and prev_f >= prev_s and cur_f < cur_s:
            return {"present": True, "cross_offset": i}
    return {"present": False}


# ──────────────────────────────────────────────────────────────────────────────
# 6. EMA cascade → targets L1 / L2 / L3
# ──────────────────────────────────────────────────────────────────────────────

def _ema_targets(closes: list[float], direction: str,
                 current_price: float, leg1_range: float = 0.0) -> dict:
    """Return TP1/TP2/TP3 based on BTMM 50/200/800 EMA cascade in the
    appropriate direction. When an EMA is on the wrong side of price it's
    skipped, and we fall back to leg-1-multiple targets so a valid setup
    against a strongly trending EMA structure still gets a plan."""
    if len(closes) < 50:
        return {"tp1": None, "tp2": None, "tp3": None,
                "ema50": None, "ema200": None, "ema800": None}

    e50 = ema_last(closes, 50)
    e200 = ema_last(closes, 200)
    e800 = ema_last(closes, 800)

    is_bullish = (direction == "bullish")
    candidates = [e50, e200, e800]

    if is_bullish:
        targets = sorted([t for t in candidates if t > current_price])
    else:
        targets = sorted([t for t in candidates if t < current_price], reverse=True)

    # Fallback: if no valid EMA targets, project 1×/2×/3× the leg-1 range
    # (StrictlyCorrect chart notes show measured moves off the p3 pivot when
    # EMAs are far away or invalid). Only when leg1_range is meaningful.
    if not targets and leg1_range > 0:
        sign = 1 if is_bullish else -1
        targets = [current_price + sign * leg1_range * m for m in (1.0, 2.0, 3.0)]

    tp1 = targets[0] if len(targets) >= 1 else None
    tp2 = targets[1] if len(targets) >= 2 else None
    tp3 = targets[2] if len(targets) >= 3 else None
    return {"tp1": tp1, "tp2": tp2, "tp3": tp3,
            "ema50": e50, "ema200": e200, "ema800": e800}


# ──────────────────────────────────────────────────────────────────────────────
# 7. HTF direction — is H4 in the same direction as the H1 setup?
# ──────────────────────────────────────────────────────────────────────────────

def _htf_bias(h4_bars: Optional[list[dict]]) -> Optional[str]:
    """Simple H4 bias from 50-EMA slope + price position."""
    if not h4_bars or len(h4_bars) < 60:
        return None
    closes = [b["close"] for b in h4_bars]
    e50 = ema_last(closes, 50)
    e50_prev = calc_ema(closes[:-5], 50)[-1] if len(closes) > 55 else e50
    price = closes[-1]
    if price > e50 and e50 > e50_prev:
        return "bullish"
    if price < e50 and e50 < e50_prev:
        return "bearish"
    return "neutral"


# ──────────────────────────────────────────────────────────────────────────────
# 8. Per-pair pipeline
# ──────────────────────────────────────────────────────────────────────────────

def analyze_pair(
    symbol: str,
    h1_candles: list[dict],
    h4_candles: Optional[list[dict]] = None,
    d1_candles: Optional[list[dict]] = None,
) -> dict:
    """Full pipeline for one pair on H1."""
    if not h1_candles or len(h1_candles) < 100:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-DATA",
                "reason": "need ≥100 H1 candles", "score": 0}

    closes = [b["close"] for b in h1_candles]
    price = closes[-1]

    # Compute TDI + baseline
    tdi = calc_tdi(closes)
    # calc_tdi only returns last-value scalars; build the RSI series ourselves
    # so we can query it at swing indices.
    from btmm_core import _rsi, _sma  # internal but stable
    rsi_series = _rsi(closes, 13)
    fast_arr = _sma(rsi_series, 2)
    slow_arr = _sma(rsi_series, 7)
    baseline_series = _baseline_series(rsi_series, 34)

    # 1. Swings
    swings = _find_swings(h1_candles)
    if len(swings) < 3:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE",
                "reason": "not enough swings",
                "score": 0, "current_price": price}

    # 2. 123 geometry
    pattern = _find_123_pattern(swings, h1_candles, symbol=symbol)
    if not pattern:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE",
                "reason": "no 123 pattern",
                "score": 0, "current_price": price,
                "tdi": {"rsi": tdi["rsi"], "baseline": baseline_series[-1] if baseline_series else 50}}

    direction = pattern["direction"]

    # 3. Divergence
    div = _check_divergence(pattern, rsi_series)

    # 4. TDI extreme at point 3
    extreme = _check_tdi_extreme(pattern, rsi_series, baseline_series)

    # 5. Signal cross confirmation
    cross = _check_signal_cross(pattern, fast_arr, slow_arr)

    # 6. Targets
    targets = _ema_targets(closes, direction, price,
                            leg1_range=pattern.get("leg1_range", 0.0))

    # 7. HTF bias
    htf = _htf_bias(h4_candles)
    htf_aligned = (htf == direction)

    # 8. Scoring — 15 pts max
    #    3 base: pattern present
    #    3: divergence
    #    3: TDI extreme (strong = 3, present = 2)
    #    2: signal cross confirmation
    #    2: HTF (H4) alignment
    #    2: current price still near p3 (fresh, not stale)
    score = 3
    notes = ["123 geometry ok"]
    if div["present"]:
        score += 3
        notes.append("divergence present")
    if extreme.get("strong"):
        score += 3
        notes.append("TDI baseline + RSI both extreme")
    elif extreme["present"]:
        score += 2
        notes.append("TDI extreme (partial)")
    if cross["present"]:
        score += 2
        notes.append("TDI signal cross confirmed")
    if htf_aligned:
        score += 2
        notes.append("H4 bias aligned")

    # Freshness: price hasn't moved more than 30 % of leg-1 away from p3
    p3_price = pattern["p3"]["price"]
    freshness = abs(price - p3_price) < 0.30 * pattern["leg1_range"]
    if freshness:
        score += 2
        notes.append("fresh from p3")

    # Grade
    if score >= 11:
        grade = "A"
    elif score >= 8:
        grade = "B"
    elif score >= 5:
        grade = "C"
    else:
        grade = "NO-TRADE"

    if grade == "NO-TRADE":
        setup = "NO-TRADE"
    else:
        setup = "BUY" if direction == "bullish" else "SELL"

    # Entry / SL / TPs
    pip = _pip_size(price, symbol)
    if setup == "BUY":
        entry = price
        sl = pattern["p3"]["price"] - 5 * pip
    elif setup == "SELL":
        entry = price
        sl = pattern["p3"]["price"] + 5 * pip
    else:
        entry, sl = None, None

    def _pips(a, b):
        if a is None or b is None:
            return None
        return round(abs(a - b) / pip, 1)

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
        "divergence": {
            "present": div["present"],
            "rsi_at_p1": round(div.get("rsi_at_p1", 0), 1),
            "rsi_at_p3": round(div.get("rsi_at_p3", 0), 1),
        },
        "tdi_extreme": {
            "present": extreme["present"],
            "strong": extreme.get("strong", False),
            "baseline_at_p3": round(extreme.get("baseline_at_p3", 50), 1),
            "rsi_at_p3": round(extreme.get("rsi_at_p3", 50), 1),
        },
        "signal_cross": cross,
        "htf_bias": htf,
        "htf_aligned": htf_aligned,
        "tdi_now": {
            "rsi": round(tdi["rsi"], 1),
            "baseline": round(baseline_series[-1] if baseline_series else 50, 1),
            "bb_upper": round(tdi["bb_upper"], 1),
            "bb_lower": round(tdi["bb_lower"], 1),
        },
        "targets": {
            "L1": targets["tp1"],
            "L2": targets["tp2"],
            "L3": targets["tp3"],
            "L1_pips": _pips(entry, targets["tp1"]),
            "L2_pips": _pips(entry, targets["tp2"]),
            "L3_pips": _pips(entry, targets["tp3"]),
            "ema50": targets["ema50"],
            "ema200": targets["ema200"],
            "ema800": targets["ema800"],
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
    """Run the TDI-cycle 123 pipeline for every pair in the universe.

    `candles_by_pair[sym]` = {'1h': [...], '4h': [...], '1d': [...]}
    Missing timeframes gracefully degrade (H4 bias / HTF alignment skipped).
    """
    now_ny = datetime.now(NY)

    pairs_out: list[dict] = []
    for sym in TDI123_UNIVERSE:
        bundles = candles_by_pair.get(sym, {}) or {}
        h1 = bundles.get("1h") or bundles.get("h1") or []
        h4 = bundles.get("4h") or bundles.get("h4") or []
        d1 = bundles.get("1d") or bundles.get("d1") or []
        try:
            row = analyze_pair(sym, h1, h4_candles=h4, d1_candles=d1)
        except Exception as e:  # noqa: BLE001
            log.exception("tdi_cycle_123 failed for %s: %s", sym, e)
            row = {"symbol": sym, "setup": "NO-TRADE", "grade": "NO-DATA",
                   "reason": f"error: {e}", "score": 0}
        pairs_out.append(row)

    buys = sum(1 for p in pairs_out if p.get("setup") == "BUY")
    sells = sum(1 for p in pairs_out if p.get("setup") == "SELL")
    grade_a = sum(1 for p in pairs_out if p.get("grade") == "A")
    grade_b = sum(1 for p in pairs_out if p.get("grade") == "B")

    return {
        "universe": TDI123_UNIVERSE,
        "now_ny": now_ny.strftime("%Y-%m-%d %H:%M %Z"),
        "buys": buys,
        "sells": sells,
        "grade_a": grade_a,
        "grade_b": grade_b,
        "pairs": pairs_out,
    }
