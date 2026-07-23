"""TDI Cycle 123 Reversal — improvements on classic BTMM.

Detects the "Peak Formation" 123 reversal pattern taught in the Inducement
Cycles / StrictlyCorrect material:

  * Point 1 = initial swing extreme
  * Point 2 = counter-move (must pull back through midline)
  * Point 3 = failed retest of point 1's zone (equal, marginal, or shallow miss)
  * Regular divergence 1→3: price reaches an equal-or-more-extreme level while
    the TDI RSI reaches a less-extreme one. Scored, not mandatory — a shallow
    -miss retest is still valid geometry, it just earns no divergence points.
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

# 123 geometry — point 3's position relative to point 1, as a fraction of the
# 1→2 leg. The tolerance is ASYMMETRIC, matching the reference charts (TDI 123
# Doc2): the ideal setup is a stop-hunt where point 3 pushes a marginal-to-
# moderate new extreme BEYOND point 1 (liquidity grab) while the TDI diverges.
# Measured on the H1 examples, that overshoot runs ~15–65 % of leg 1
# (EUR/JPY ≈31 %, GBP/JPY ≈63 %). A point 3 that falls SHORT of point 1 is a
# shallow-miss retest and must stay much closer to count.
POINT3_OVERSHOOT_PCT = 0.65   # p3 beyond p1 (stop hunt) — generous
POINT3_SHORTFALL_PCT = 0.25   # p3 short of p1 (shallow miss) — tight
# Minimum size of the 1→2 leg, in pips — filters out noise-sized "swings"
# that would otherwise qualify as a valid pattern on a quiet H1 candle run.
MIN_LEG1_PIPS = 8.0

# ── Divergence gates ─────────────────────────────────────────────────────────
# Regular divergence requires price to reach an EQUAL-or-MORE-extreme level at
# p3 while the oscillator reaches a LESS-extreme one. If price falls short of
# p1's extreme by more than this fraction of leg 1, the two series are moving
# the SAME way (a shallow-miss retest) — that is confirmation, not divergence.
DIVERGENCE_EQUAL_TOLERANCE_PCT = 0.05   # 5 % of leg 1 still counts as "equal"

# Minimum RSI travel (points) between p1 and p3 before it's called divergence.
DIVERGENCE_MIN_RSI_DELTA = 2.0

# The oscillator's own peak rarely lands on the exact bar of the price swing,
# so read the RSI extreme from a small window centred on the swing index —
# this mirrors how the divergence line is drawn across TDI peaks/troughs.
DIVERGENCE_RSI_WINDOW = 2

# A signal cross older than this many bars is treated as stale.
SIGNAL_CROSS_MAX_AGE = 20

# ── Stop-loss sizing (ATR-based structure stop) ──────────────────────────────
# The old SL of "p3 ± 5 pips" produced hair-tight stops that get hunted (Davit,
# BTMM: "20-30 pip hard SL = sucker move, too tight, will be hunted") and absurd
# R:R when entry sat right on p3. Replace with an ATR structure stop: placed
# beyond the p3 stop-hunt extreme, but never closer to entry than a floor so a
# fresh-from-p3 entry still gets breathing room.
ATR_PERIOD = 14
SL_STRUCT_ATR_MULT = 0.5   # buffer beyond p3's wick
SL_MIN_ATR_MULT = 1.0      # minimum stop distance from entry (anti-hunt floor)

# Minimum EMA-to-EMA spread (excluding spot price), as a fraction of price,
# before the cascade counts as a "strong trend" for the divergence-ambiguity check.
# Ranges have all EMAs converged <0.05%; real trends show 0.08%+ separation.
TREND_STACK_MIN_SPAN_PCT = 0.0005   # 0.05 % — ~8 pips on EUR/AUD at 1.64


def _pip_size(price: float, symbol: Optional[str] = None) -> float:
    return instruments.pip_size(symbol, price)


def _atr(bars: list[dict], period: int = ATR_PERIOD) -> Optional[float]:
    """Average True Range over the last `period` bars (SMA of true range).

    Returns None if there aren't enough bars to measure.
    """
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(len(bars) - period, len(bars)):
        h, l = bars[i]["high"], bars[i]["low"]
        prev_close = bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
    return sum(trs) / len(trs) if trs else None


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

    # Collect every valid triple, then pick the freshest. Returning the first
    # match would hand back the OLDEST pattern in the window and hide a live
    # setup behind one that already played out.
    candidates: list[dict] = []

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

                # Minimum leg-1 size — filters out noise-sized swings that
                # would otherwise pass the fractal definition on a quiet run.
                if leg1_range < MIN_LEG1_PIPS * pip:
                    continue

                # Geometry: point 3 relative to point 1, ASYMMETRIC by design.
                # `overshoot` > 0 means p3 pushed a new extreme BEYOND p1 (the
                # stop-hunt / liquidity grab the reference charts favour); < 0
                # means p3 fell SHORT of p1 (a shallow-miss retest). Overshoots
                # get a generous bound; shortfalls must stay close.
                if is_bullish:
                    # bullish extreme is a LOW: beyond = lower = p3 < p1
                    overshoot = p1["price"] - p3["price"]
                else:
                    # bearish extreme is a HIGH: beyond = higher = p3 > p1
                    overshoot = p3["price"] - p1["price"]

                if overshoot >= 0:
                    if overshoot > POINT3_OVERSHOOT_PCT * leg1_range:
                        continue
                    p3_kind = "overshoot"
                else:
                    if -overshoot > POINT3_SHORTFALL_PCT * leg1_range:
                        continue
                    p3_kind = "shortfall"

                candidates.append({
                    "direction": "bullish" if is_bullish else "bearish",
                    "p1": p1,
                    "p2": p2,
                    "p3": p3,
                    "leg1_range": leg1_range,
                    "leg1_range_pips": leg1_range / pip,
                    "p3_kind": p3_kind,
                    "p3_overshoot_pct": round(overshoot / leg1_range, 3),
                })

    if not candidates:
        return None

    # Freshest p3 wins; on a tie prefer the larger leg 1 (the more meaningful
    # structure of the two).
    candidates.sort(key=lambda c: (c["p3"]["idx"], c["leg1_range"]), reverse=True)
    return candidates[0]


# ──────────────────────────────────────────────────────────────────────────────
# 3. Divergence check — price 1→3 vs TDI RSI 1→3
# ──────────────────────────────────────────────────────────────────────────────

def _rsi_extreme_at(rsi_series: list[float], idx: int, want_low: bool,
                    window: int = DIVERGENCE_RSI_WINDOW) -> float:
    """Read the RSI local extreme in a small window centred on `idx`.

    The TDI peak that the divergence line is drawn from is often a bar or two
    off the price swing, so anchoring on the exact index understates the
    oscillator's actual high/low.
    """
    lo = max(0, idx - window)
    hi = min(len(rsi_series), idx + window + 1)
    seg = rsi_series[lo:hi]
    if not seg:
        return rsi_series[idx]
    return min(seg) if want_low else max(seg)


def _check_divergence(pattern: dict, rsi_series: list[float]) -> dict:
    """Compare price extreme (p1→p3) with the TDI RSI extreme (p1→p3).

    Regular divergence — the only kind the 123 cycle trades — needs the two
    series to DISAGREE at point 3:

      Bullish: price makes an equal-or-LOWER low at p3 while RSI makes a
               HIGHER low.
      Bearish: price makes an equal-or-HIGHER high at p3 while RSI makes a
               LOWER high.

    If price falls short of p1's extreme by more than the equal-tolerance, both
    series are moving the same way. That is a shallow-miss retest — valid 123
    geometry, but it is confirmation, not divergence, and scores no points here.

    `strong` marks the textbook case where price genuinely exceeded p1's
    extreme (the "1 → 3" trendline in the StrictlyCorrect chart notes).
    """
    p1, p3 = pattern["p1"], pattern["p3"]
    if p1["idx"] >= len(rsi_series) or p3["idx"] >= len(rsi_series):
        return {"present": False, "strong": False,
                "reason": "index out of RSI series"}

    is_bullish = (pattern["direction"] == "bullish")

    # Bullish setup pivots on lows, so compare RSI troughs; bearish on peaks.
    rsi_p1 = _rsi_extreme_at(rsi_series, p1["idx"], want_low=is_bullish)
    rsi_p3 = _rsi_extreme_at(rsi_series, p3["idx"], want_low=is_bullish)

    price_delta = p3["price"] - p1["price"]
    rsi_delta = rsi_p3 - rsi_p1

    # Normalise price delta to the leg-1 range so "how far past p1" is
    # comparable across pairs without mixing price and RSI scales.
    leg1 = pattern.get("leg1_range", 0) or 1e-12
    price_move_ratio = price_delta / leg1
    tol = DIVERGENCE_EQUAL_TOLERANCE_PCT

    if is_bullish:
        # price at or below p1 (allowing a small "equal low" tolerance)
        price_ok = price_move_ratio <= tol
        # RSI clearly higher
        rsi_ok = rsi_delta >= DIVERGENCE_MIN_RSI_DELTA
        strong = price_move_ratio < 0 and rsi_ok
    else:
        price_ok = price_move_ratio >= -tol
        rsi_ok = rsi_delta <= -DIVERGENCE_MIN_RSI_DELTA
        strong = price_move_ratio > 0 and rsi_ok

    divergence = bool(price_ok and rsi_ok)

    if divergence:
        reason = "regular divergence" if strong else "equal-level divergence"
    elif rsi_ok:
        reason = "price failed to reach p1 extreme — confirmation, not divergence"
    else:
        reason = "RSI did not diverge"

    return {
        "present": divergence,
        "strong": bool(divergence and strong),
        "reason": reason,
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

    The cross must still be IN EFFECT on the latest bar. Taking the first cross
    after p3 regardless of what happened since would confirm a setup whose
    momentum has already crossed back — the PDF's own exit condition.
    """
    p3_idx = pattern["p3"]["idx"]
    if p3_idx >= len(fast_arr) - 1:
        return {"present": False, "reason": "no post-p3 bars"}

    is_bullish = (pattern["direction"] == "bullish")

    # Momentum must currently sit on the confirmation side.
    held = (fast_arr[-1] > slow_arr[-1]) if is_bullish else (fast_arr[-1] < slow_arr[-1])
    if not held:
        return {"present": False, "reason": "signal crossed back — momentum lost"}

    # Walk backwards to the most recent cross so the age is the live one.
    last_bar = len(fast_arr) - 1
    for i in range(last_bar, p3_idx, -1):
        prev_f, prev_s = fast_arr[i - 1], slow_arr[i - 1]
        cur_f, cur_s = fast_arr[i], slow_arr[i]
        crossed = (prev_f <= prev_s and cur_f > cur_s) if is_bullish \
            else (prev_f >= prev_s and cur_f < cur_s)
        if crossed:
            age = last_bar - i
            if age > SIGNAL_CROSS_MAX_AGE:
                return {"present": False, "bars_since_cross": age,
                        "reason": f"cross is {age} bars stale"}
            return {"present": True, "bars_since_cross": age,
                    "cross_offset": i - p3_idx}

    return {"present": False, "reason": "no cross after p3"}


# ──────────────────────────────────────────────────────────────────────────────
# 6. EMA cascade → targets L1 / L2 / L3
# ──────────────────────────────────────────────────────────────────────────────

def _ema_targets(closes: list[float], direction: str,
                 current_price: float, leg1_range: float = 0.0) -> dict:
    """Return TP1/TP2/TP3 based on BTMM 50/200/800 EMA cascade in the
    appropriate direction. When an EMA is on the wrong side of price it's
    skipped, and we fall back to leg-1-multiple targets so a valid setup
    against a strongly trending EMA structure still gets a plan.

    `projected` is True only when ALL THREE EMAs failed the directional
    filter and every target came from the leg-1 fallback instead — callers
    (and the UI) must not label those values "EMA" when this is True."""
    if len(closes) < 50:
        return {"tp1": None, "tp2": None, "tp3": None,
                "ema50": None, "ema200": None, "ema800": None,
                "projected": False}

    # Convergence thresholds (seed influence < 1%) — same standard already
    # used for BTMM's own EMA-200/800 "warm" flags elsewhere in index.html.
    # Below these, calc_ema() is either flat-lined at current_price (below
    # its period) or too seed-biased to trust as a real cascade target.
    e50 = ema_last(closes, 50)
    e200 = ema_last(closes, 200) if len(closes) >= 300 else None
    e800 = ema_last(closes, 800) if len(closes) >= 2400 else None

    is_bullish = (direction == "bullish")
    candidates = [v for v in (e50, e200, e800) if v is not None]

    if is_bullish:
        targets = sorted([t for t in candidates if t > current_price])
    else:
        targets = sorted([t for t in candidates if t < current_price], reverse=True)

    # Fallback: if no valid EMA targets, project 1×/2×/3× the leg-1 range
    # (StrictlyCorrect chart notes show measured moves off the p3 pivot when
    # EMAs are far away or invalid). Only when leg1_range is meaningful.
    projected = False
    if not targets and leg1_range > 0:
        sign = 1 if is_bullish else -1
        targets = [current_price + sign * leg1_range * m for m in (1.0, 2.0, 3.0)]
        projected = True

    tp1 = targets[0] if len(targets) >= 1 else None
    tp2 = targets[1] if len(targets) >= 2 else None
    tp3 = targets[2] if len(targets) >= 3 else None
    return {"tp1": tp1, "tp2": tp2, "tp3": tp3,
            "ema50": e50, "ema200": e200, "ema800": e800,
            "projected": projected}


# ──────────────────────────────────────────────────────────────────────────────
# 7. HTF direction — is H4 in the same direction as the H1 setup?
# ──────────────────────────────────────────────────────────────────────────────

def _trend_regime(closes: list[float]) -> dict:
    """Classify H1 trend strength from the 50/200/800 EMA cascade.

    "Stacked" = price and every available EMA in strict cascade order, which is
    the objective read of Davit's "strong trend" caveat on divergence.
    """
    if len(closes) < 300:
        return {"stacked": False, "direction": None}

    e50 = ema_last(closes, 50)
    e200 = ema_last(closes, 200)
    e800 = ema_last(closes, 800) if len(closes) >= 2400 else None
    price = closes[-1]

    chain = [price, e50, e200] + ([e800] if e800 is not None else [])

    # Ordering alone is not enough: in a range the EMAs converge and separate
    # only by noise, which would read as a strong cascade. Measure the spread
    # across the EMAs THEMSELVES — spot price can sit far from a converged
    # cluster at the top of a range and fake a wide span.
    emas = chain[1:]
    span = abs(emas[0] - emas[-1])
    if price <= 0 or span / price < TREND_STACK_MIN_SPAN_PCT:
        return {"stacked": False, "direction": None, "span_pct": span / price if price else 0.0}

    bull = all(a > b for a, b in zip(chain, chain[1:]))
    bear = all(a < b for a, b in zip(chain, chain[1:]))

    if bull:
        return {"stacked": True, "direction": "bullish", "span_pct": span / price}
    if bear:
        return {"stacked": True, "direction": "bearish", "span_pct": span / price}
    return {"stacked": False, "direction": None, "span_pct": span / price}


# ──────────────────────────────────────────────────────────────────────────────
# 7b. Weekly Fibonacci pivots + location filter (Davit, "Pivot Trading with TDI")
# ──────────────────────────────────────────────────────────────────────────────
# Davit is emphatic that LOCATION is a prerequisite, not a bonus: "TDI signals
# are only valid when price is in a high-probability ZONE (61-100). A TDI cross
# or bullish candle at 38 is still a sucker move." The levels are Fibonacci
# projections of the PRIOR week's range from the weekly pivot P=(H+L+C)/3.
# (ratio, doctrine tag) — tags follow Davit's 38/61/78/100/138 naming, not the
# rounded percent (0.618 → "61", 0.786 → "78").
FIB_PIVOT_LEVELS = ((0.382, 38), (0.618, 61), (0.786, 78), (1.0, 100), (1.382, 138))
_WEEK_SECONDS = 7 * 86400
_MONDAY_EPOCH = 4 * 86400   # 1970-01-05 00:00 UTC was a Monday


def _prev_week_hlc(bars: list[dict], as_of_ts: int):
    """High/Low/Close of the fully-completed week before `as_of_ts`.

    Week = Monday 00:00 UTC → next Monday. Uses only bars strictly inside the
    prior week, so there is no look-ahead into the current (forming) week.
    """
    cur_week_start = _MONDAY_EPOCH + \
        ((as_of_ts - _MONDAY_EPOCH) // _WEEK_SECONDS) * _WEEK_SECONDS
    prev_start = cur_week_start - _WEEK_SECONDS
    seg = [b for b in bars if prev_start <= b["ts_utc"] < cur_week_start]
    if not seg:
        return None
    return (max(b["high"] for b in seg),
            min(b["low"] for b in seg),
            seg[-1]["close"])


def _weekly_fib_pivots(hlc) -> Optional[dict]:
    """Fib pivot levels from a (High, Low, Close) tuple."""
    if not hlc:
        return None
    H, L, C = hlc
    rng = H - L
    if rng <= 0:
        return None
    P = (H + L + C) / 3.0
    lv = {"P": P, "range": rng}
    for r, tag in FIB_PIVOT_LEVELS:
        lv[f"R{tag}"] = P + r * rng
        lv[f"S{tag}"] = P - r * rng
    return lv


def _location(price: float, direction: str, pivots: Optional[dict]) -> dict:
    """Classify where price sits vs the weekly pivot, from the setup's view.

    loc_ratio > 0 means price is in the REVERSAL zone for this setup — at
    resistance for a bearish (sell) setup, at support for a bullish (buy) one.
    A negative ratio means the setup is firing on the wrong side of the pivot.
    `ok` follows Davit's rule: only the 61–100 band counts as high-probability.
    """
    if not pivots:
        return {"quality": "unknown", "ok": False, "zone": None, "loc_ratio": None}
    P, rng = pivots["P"], pivots["range"]
    if direction == "bearish":      # sell at resistance → above pivot is good
        loc, side = (price - P) / rng, "R"
    else:                           # buy at support → below pivot is good
        loc, side = (P - price) / rng, "S"

    if loc >= 1.30:
        q, zone = "extended", f"{side}138+"
    elif loc >= 0.90:
        q, zone = "prime", f"{side}100"
    elif loc >= 0.55:
        q, zone = "good", f"{side}61-78"
    elif loc >= 0.30:
        q, zone = "weak", f"{side}38"
    elif loc >= -0.30:
        q, zone = "poor", "mid-pivot"
    else:
        q, zone = "wrongside", "wrong-side"
    return {"quality": q, "ok": q in ("good", "prime"),
            "zone": zone, "loc_ratio": round(loc, 3)}


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

    # 7b. Weekly-pivot location (Davit). Prefer daily candles for the weekly
    # H/L/C; fall back to H1 when daily isn't supplied. No look-ahead — only the
    # fully-completed prior week is used.
    week_src = d1_candles if (d1_candles and len(d1_candles) >= 5) else h1_candles
    pivots = _weekly_fib_pivots(_prev_week_hlc(week_src, h1_candles[-1]["ts_utc"]))
    location = _location(price, direction, pivots)

    # 7c. Ketchup (13 EMA) reclaim — the ENTRY TRIGGER. BTMM: "once price closes
    # above/below the 13 EMA it tends to stay there." Backtesting showed ~55 % of
    # signals stopped out before TP1 because entry fired while price was still on
    # the wrong side of the ketchup. Require price to have reclaimed it in the
    # setup direction before the setup is treated as a live entry.
    ema13 = ema_last(closes, 13)
    ketchup_reclaimed = (price > ema13) if direction == "bullish" else (price < ema13)

    # 8. Scoring — 15 pts max
    #    3 base: pattern present
    #    3: divergence (regular = 3, equal-level = 2)
    #    3: TDI extreme (strong = 3, present = 2)
    #    2: signal cross confirmation
    #    2: HTF (H4) alignment
    #    2: current price still near p3 (fresh, not stale)
    # Davit, "Pivot Trading with TDI" p.9: "When the market is in a strong trend
    # in either direction, oscillators do not function well ... Any signs of
    # divergence during a strong trend would be ambiguous at best." A 123 fired
    # against a fully-stacked EMA cascade is exactly that case, so the
    # divergence still counts but no longer carries a setup on its own.
    trend = _trend_regime(closes)
    div_ambiguous = bool(
        div["present"] and trend["stacked"] and trend["direction"] != direction
    )

    score = 3
    notes = ["123 geometry ok"]
    if div_ambiguous:
        score += 1
        notes.append(f"divergence vs stacked {trend['direction']} EMAs — ambiguous")
    elif div.get("strong"):
        score += 3
        notes.append("regular divergence (price beyond p1)")
    elif div["present"]:
        score += 2
        notes.append("equal-level divergence")
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

    # Note the location for information, but do NOT add it to the score. A 30-day
    # walk-forward found the 61–100 zone did not separate winners from losers on
    # this data (only the extreme 100-zone hinted positive, n too small), so it
    # is not treated as an edge — just displayed, plus the wrong-side gate below.
    notes.append(f"pivot location {location['zone']}")
    notes.append("ketchup reclaimed" if ketchup_reclaimed
                 else "awaiting 13-EMA reclaim")

    # Grade (max 15)
    if score >= 11:
        grade = "A"
    elif score >= 8:
        grade = "B"
    elif score >= 5:
        grade = "C"
    else:
        grade = "NO-TRADE"

    # Davit's location gate (kept as a filter, not a score): a clean TDI setup on
    # the WRONG side of the weekly pivot — selling into support or buying into
    # resistance — is a sucker move. Cap those to C so they can't present as A/B.
    if location["quality"] in ("poor", "wrongside") and grade in ("A", "B"):
        grade = "C"
        notes.append("grade capped: poor pivot location")

    if grade == "NO-TRADE":
        setup = "NO-TRADE"
    else:
        setup = "BUY" if direction == "bullish" else "SELL"

    # Entry / SL / TPs — ATR structure stop (see SL_* constants).
    pip = _pip_size(price, symbol)
    atr = _atr(h1_candles) or (pattern["leg1_range"] * 0.5)  # fallback if short
    p3p = pattern["p3"]["price"]
    if setup == "BUY":
        entry = price
        # beyond p3's low, but never closer than the anti-hunt floor from entry
        sl_struct = p3p - SL_STRUCT_ATR_MULT * atr
        sl_floor = entry - SL_MIN_ATR_MULT * atr
        sl = min(sl_struct, sl_floor)
    elif setup == "SELL":
        entry = price
        sl_struct = p3p + SL_STRUCT_ATR_MULT * atr
        sl_floor = entry + SL_MIN_ATR_MULT * atr
        sl = max(sl_struct, sl_floor)
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
            "strong": div.get("strong", False) and not div_ambiguous,
            "ambiguous": div_ambiguous,
            "reason": ("counter-trend divergence against a stacked "
                       f"{trend['direction']} EMA cascade — unreliable per Davit p.9"
                       if div_ambiguous else div.get("reason", "")),
            "rsi_at_p1": round(div.get("rsi_at_p1", 0), 1),
            "rsi_at_p3": round(div.get("rsi_at_p3", 0), 1),
        },
        "location": location,
        "location_ok": location["ok"],
        "ema13": ema13,
        "ketchup_reclaimed": ketchup_reclaimed,
        "trend_regime": trend,
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
