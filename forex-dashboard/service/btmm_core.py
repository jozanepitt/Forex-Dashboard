"""
BTMM core indicators ported from index.html JS.
All functions take a list of bar dicts: {open, high, low, close, ts_utc}.
"""
from __future__ import annotations
from typing import Optional

import instruments


# ── EMA ───────────────────────────────────────────────────────────────────────

def calc_ema(closes: list[float], period: int) -> list[float]:
    if len(closes) < period:
        return [closes[-1]] * len(closes)
    k = 2.0 / (period + 1)
    out = [sum(closes[:period]) / period]
    for v in closes[period:]:
        out.append(v * k + out[-1] * (1 - k))
    # Pad front so indices align with closes
    return [out[0]] * period + out[1:]


def ema_last(closes: list[float], period: int) -> float:
    return calc_ema(closes, period)[-1]


def _sma(values: list[float], period: int) -> list[float]:
    """Simple moving average; partial averages fill the warmup window."""
    out = []
    for i in range(len(values)):
        start = max(0, i - period + 1)
        out.append(sum(values[start:i + 1]) / (i - start + 1))
    return out


# ── TDI ───────────────────────────────────────────────────────────────────────

def _rsi(closes: list[float], period: int = 13) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rsi_vals = []
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l else 100
        rsi_vals.append(100 - 100 / (1 + rs))
    return [50.0] * (len(closes) - len(rsi_vals)) + rsi_vals


def calc_tdi(closes: list[float]) -> dict:
    rsi      = _rsi(closes, 13)
    fast_arr = _sma(rsi, 2)   # SMA(2) fast signal line
    slow_arr = _sma(rsi, 7)   # SMA(7) slow signal line

    # Bollinger Bands on RSI: 34-period SMA ± 1.6185σ (golden ratio — exact BTMM spec)
    bb_upper, bb_lower, bb_mid = 68.0, 32.0, 50.0
    if len(rsi) >= 34:
        window   = rsi[-34:]
        mu       = sum(window) / 34
        std      = (sum((v - mu) ** 2 for v in window) / 34) ** 0.5
        bb_mid   = mu
        bb_upper = mu + 1.6185 * std
        bb_lower = mu - 1.6185 * std

    return {
        "rsi":      rsi[-1],
        "fast":     fast_arr[-1],
        "slow":     slow_arr[-1],
        "fast_arr": fast_arr,       # full array — used by detect_tdi_leg
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "bb_mid":   bb_mid,
        "bullish":  fast_arr[-1] > slow_arr[-1],
        "bearish":  fast_arr[-1] < slow_arr[-1],
        "overbought": rsi[-1] > bb_upper,
        "oversold":   rsi[-1] < bb_lower,
    }


# ── ADR ───────────────────────────────────────────────────────────────────────

def calc_adr(bars: list[dict], period: int = 14) -> float:
    """Average Daily Range using last N days (96 × 15-min bars per day)."""
    bars_per_day = 96
    daily = []
    for i in range(period):
        start = max(0, len(bars) - (i + 1) * bars_per_day)
        end   = max(0, len(bars) - i * bars_per_day)
        chunk = bars[start:end]
        if chunk:
            daily.append(max(b["high"] for b in chunk) - min(b["low"] for b in chunk))
    return sum(daily) / len(daily) if daily else 0.001


def adr_consumed(bars: list[dict], adr: float) -> float:
    today = bars[-96:] if len(bars) >= 96 else bars
    rng = max(b["high"] for b in today) - min(b["low"] for b in today)
    return min(100.0, rng / adr * 100) if adr else 0


# ── EMA stack ─────────────────────────────────────────────────────────────────

def ema_stack(closes: list[float]) -> dict:
    e5   = ema_last(closes, 5)
    e13  = ema_last(closes, 13)
    e50  = ema_last(closes, 50)
    e200 = ema_last(closes, 200)
    e800 = ema_last(closes, 800)   # always use true period; calc_ema handles short datasets gracefully
    price = closes[-1]
    bullish = price > e13 > e50 > e200
    bearish = price < e13 < e50 < e200
    score = (
        (1 if price > e5   else -1) +
        (1 if price > e13  else -1) +
        (1 if price > e50  else -1) +
        (1 if price > e200 else -1) +
        (1 if price > e800 else -1)
    )
    return {
        "e5": e5, "e13": e13, "e50": e50, "e200": e200, "e800": e800,
        "price": price,
        "bullish": bullish, "bearish": bearish,
        "score": score,
        "setup_bullish": price > e800,
    }


# ── AMD phase ─────────────────────────────────────────────────────────────────

def detect_amd(bars: list[dict]) -> dict:
    """
    Session-based AMD — matches Steve Mauro's definition:
      Accumulation  = Asian session  (00:00–07:00 UTC) — tight range builds
      Manipulation  = London session (07:00–13:00 UTC) — false moves, stop hunts
      Distribution  = NY session     (13:00–17:00 UTC) — real directional move
    """
    if len(bars) < 10:
        return {"phase": "Unknown"}
    from datetime import datetime, timezone
    dt       = datetime.fromtimestamp(bars[-1]["ts_utc"], tz=timezone.utc)
    utc_hour = dt.hour

    if 13 <= utc_hour < 17:
        phase = "Distribution"
    elif 7 <= utc_hour < 13:
        phase = "Manipulation"
    else:                        # 00:00–07:00 and 17:00–00:00
        phase = "Accumulation"

    day = bars[-96:] if len(bars) >= 96 else bars
    day_h = max(b["high"] for b in day)
    day_l = min(b["low"]  for b in day)
    return {"phase": phase, "day_high": day_h, "day_low": day_l}


# ── Kill zone ─────────────────────────────────────────────────────────────────

KILL_ZONES = [
    {"name": "London",  "start":  7.0,  "end": 10.0},   # 07:00–10:00 GMT
    {"name": "Overlap", "start": 12.0,  "end": 13.0},   # 12:00–13:00 GMT
    {"name": "NY",      "start": 13.0,  "end": 16.0},   # 13:00–16:00 GMT
    {"name": "Asian",   "start": 19.0,  "end": 22.0},   # 19:00–22:00 GMT (22 Trade)
]


def active_kill_zone(ts_utc: int) -> Optional[str]:
    from datetime import datetime, timezone
    dt  = datetime.fromtimestamp(ts_utc, tz=timezone.utc)
    gmt = dt.hour + dt.minute / 60
    for kz in KILL_ZONES:
        if kz["start"] <= gmt < kz["end"]:
            return kz["name"]
    return None


# ── Stop hunt ─────────────────────────────────────────────────────────────────

def detect_stop_hunt(bars: list[dict], lookback: int = 20) -> dict:
    if len(bars) < lookback + 2:
        return {"active": False}
    window  = bars[-(lookback + 1):-1]
    current = bars[-1]
    prev_h  = max(b["high"] for b in window)
    prev_l  = min(b["low"]  for b in window)
    pierced_high = current["high"] > prev_h and current["close"] < prev_h
    pierced_low  = current["low"]  < prev_l  and current["close"] > prev_l
    return {
        "active":      pierced_high or pierced_low,
        "direction":   "bearish" if pierced_high else "bullish" if pierced_low else None,
        "prev_high":   prev_h,
        "prev_low":    prev_l,
    }


# ── Asian range ───────────────────────────────────────────────────────────────

def detect_asian_range(bars: list[dict], symbol: Optional[str] = None) -> dict:
    """Asian session = 00:00–07:00 GMT. Use last occurrence."""
    from datetime import datetime, timezone
    asian = []
    for b in reversed(bars[-200:]):
        dt  = datetime.fromtimestamp(b["ts_utc"], tz=timezone.utc)
        gmt = dt.hour + dt.minute / 60
        if 0 <= gmt < 7:
            asian.append(b)
        elif asian:
            break  # past session
    if len(asian) < 4:
        return {"valid": False}
    hi = max(b["high"] for b in asian)
    lo = min(b["low"]  for b in asian)
    pip       = instruments.pip_size(symbol, hi)
    rng_pips  = (hi - lo) / pip
    # "Tight consolidation" threshold scales per asset class: 50 pips for FX/JPY,
    # but indices/metals/crypto need a wider band (a 50-point Nasdaq range is
    # unrealistically tight). Floor at the instrument's min-SL ×2.
    cls = instruments.asset_class(symbol)
    if cls in ("fx", "jpy"):
        tight_max = 50
    else:
        floor_pips = (instruments.spec(symbol) or {}).get("min_sl_pips", 50)
        tight_max = max(50, floor_pips * 2)
    return {
        "valid":   rng_pips < tight_max,
        "high":    hi,
        "low":     lo,
        "mid":     (hi + lo) / 2,
        "range_pips": rng_pips,
    }


# ── TDI leg ───────────────────────────────────────────────────────────────────

def detect_tdi_leg(tdi: dict) -> dict:
    """
    BB-band method — exact port of JS detectTDILeg().
    Leg 1 = fast line currently outside BB bands  (Shark Fin forming).
    Leg 2 = fast line returned inside bands after a spike  (entry trigger).
    """
    fast_arr = tdi.get("fast_arr", [])
    upper    = tdi.get("bb_upper", 68.0)
    lower    = tdi.get("bb_lower", 32.0)

    if len(fast_arr) < 10:
        return {"leg": 0, "confidence": "low", "direction": "neutral", "fast_last": 50.0}

    lookback = min(len(fast_arr), 20)
    recent   = fast_arr[-lookback:]
    current  = recent[-1]
    prior    = recent[:-3] if len(recent) > 3 else []

    was_outside_bear = any(v > upper for v in prior)
    was_outside_bull = any(v < lower for v in prior)

    # Leg 1 — currently outside band (Shark Fin phase 1)
    if current > upper:
        return {"leg": 1, "direction": "bearish", "confidence": "high",   "fast_last": current}
    if current < lower:
        return {"leg": 1, "direction": "bullish", "confidence": "high",   "fast_last": current}

    # Leg 2 — returned inside bands after a prior spike (entry trigger)
    if was_outside_bear and current < upper:
        m_pattern = len(recent) >= 3 and current < recent[-3]
        return {"leg": 2, "direction": "bearish",
                "confidence": "high" if m_pattern else "medium", "fast_last": current}
    if was_outside_bull and current > lower:
        w_pattern = len(recent) >= 3 and current > recent[-3]
        return {"leg": 2, "direction": "bullish",
                "confidence": "high" if w_pattern else "medium", "fast_last": current}

    # Mid-range — no active leg setup
    direction = "bullish" if current > (upper + lower) / 2 else "bearish"
    return {"leg": 0, "direction": direction, "confidence": "low", "fast_last": current}


# ── 5/13 EMA cross ───────────────────────────────────────────────────────────

def detect_513_cross(closes: list[float]) -> dict:
    """Return whether EMA(5) crossed EMA(13) on the most recent bar."""
    if len(closes) < 14:
        return {"crossed": False, "direction": None}
    e5_curr  = ema_last(closes,      5)
    e13_curr = ema_last(closes,     13)
    e5_prev  = ema_last(closes[:-1], 5)
    e13_prev = ema_last(closes[:-1], 13)
    crossed_up   = e5_prev <= e13_prev and e5_curr > e13_curr
    crossed_down = e5_prev >= e13_prev and e5_curr < e13_curr
    direction = "bullish" if crossed_up else "bearish" if crossed_down else None
    return {"crossed": crossed_up or crossed_down, "direction": direction}


# ── Level count ───────────────────────────────────────────────────────────────

def detect_level_count(stack: dict) -> dict:
    """
    Count how many of the 5 EMAs price is on the correct side of.
    Level I  = 1–3 aligned (weak-to-moderate directional structure)
    Level II = 4–5 aligned (strong institutional-grade structure — A+ requirement)
    """
    price  = stack["price"]
    levels = [stack["e5"], stack["e13"], stack["e50"], stack["e200"], stack["e800"]]
    above  = sum(1 for e in levels if price > e)
    below  = sum(1 for e in levels if price < e)
    direction = "bullish" if above >= below else "bearish"
    count     = above if direction == "bullish" else below
    return {
        "count":     count,
        "level_ii":  count >= 4,
        "level_i":   1 <= count <= 3,
        "direction": direction,
    }


# ── M/W pattern ───────────────────────────────────────────────────────────────

def detect_mw_pattern(bars: list[dict], lookback: int = 40) -> dict:
    """
    M (bearish double-top) and W (bullish double-bottom) detection.
    Searches for swing-high-low-high or swing-low-high-low structure.
    """
    if len(bars) < lookback:
        return {"detected": False, "pattern": None, "quality": 0.0}

    window = bars[-lookback:]
    highs  = [b["high"] for b in window]
    lows   = [b["low"]  for b in window]

    swing_highs: list[tuple[int, float]] = []
    swing_lows:  list[tuple[int, float]] = []
    for i in range(2, len(window) - 2):
        if highs[i] >= max(highs[i-2:i]) and highs[i] >= max(highs[i+1:i+3]):
            swing_highs.append((i, highs[i]))
        if lows[i] <= min(lows[i-2:i]) and lows[i] <= min(lows[i+1:i+3]):
            swing_lows.append((i, lows[i]))

    pattern, quality = None, 0.0

    # M pattern: two swing highs separated by a swing low; second high ≤ first (double-top / lower high)
    if len(swing_highs) >= 2:
        (i1, h1), (i2, h2) = swing_highs[-2], swing_highs[-1]
        mid_lows = [sl for sl in swing_lows if i1 < sl[0] < i2]
        if mid_lows and h2 <= h1 * 1.002:
            pattern = "M"
            quality = 0.9 if h2 < h1 else 0.65   # lower high = stronger M

    # W pattern: only set if M was not already found (avoids silent overwrite in choppy ranges)
    if pattern is None and len(swing_lows) >= 2:
        (i1, l1), (i2, l2) = swing_lows[-2], swing_lows[-1]
        mid_highs = [sh for sh in swing_highs if i1 < sh[0] < i2]
        if mid_highs and l2 >= l1 * 0.998:
            pattern = "W"
            quality = 0.9 if l2 > l1 else 0.65   # higher low = stronger W

    return {"detected": pattern is not None, "pattern": pattern, "quality": quality}


# ── Shark Fin ─────────────────────────────────────────────────────────────────

def detect_shark_fin(tdi: dict) -> dict:
    """
    Shark Fin: TDI fast line spikes outside BB bands (leg 1), curves back inside (leg 2).
    Leg 2 + high/medium confidence = entry trigger.
    """
    leg      = detect_tdi_leg(tdi)
    fast_arr = tdi.get("fast_arr", [])
    upper    = tdi.get("bb_upper", 68.0)
    lower    = tdi.get("bb_lower", 32.0)
    band_w   = max(upper - lower, 1.0)

    spike_depth = 0.0
    if leg["leg"] == 2 and len(fast_arr) >= 10:
        recent = fast_arr[-20:]
        prior  = recent[:-3] if len(recent) > 3 else recent
        if leg["direction"] == "bearish":
            spike_depth = max(0.0, max(prior) - upper) / band_w * 100
        else:
            spike_depth = max(0.0, lower - min(prior)) / band_w * 100

    return {
        "detected":    leg["leg"] in (1, 2),
        "direction":   leg["direction"],
        "leg":         leg["leg"],
        "confidence":  leg["confidence"],
        "spike_depth": round(spike_depth, 1),
        "entry_ready": leg["leg"] == 2 and leg["confidence"] in ("high", "medium"),
    }


# ── Straightaway ──────────────────────────────────────────────────────────────

def detect_straightaway(bars: list[dict], lookback: int = 6, min_run: int = 3) -> dict:
    """
    3+ consecutive same-direction candles — momentum continuation / exhaustion signal.
    """
    if len(bars) < lookback:
        return {"detected": False, "direction": None, "count": 0}

    recent = bars[-lookback:]
    bull_run = bear_run = 0
    for b in reversed(recent):
        if b["close"] > b["open"]:
            if bear_run:
                break
            bull_run += 1
        elif b["close"] < b["open"]:
            if bull_run:
                break
            bear_run += 1
        else:
            break

    if bull_run >= min_run:
        return {"detected": True, "direction": "bullish", "count": bull_run}
    if bear_run >= min_run:
        return {"detected": True, "direction": "bearish", "count": bear_run}
    return {"detected": False, "direction": None, "count": max(bull_run, bear_run)}


# ── HOD / LOD proximity ───────────────────────────────────────────────────────

def detect_hod_lod(bars: list[dict], threshold_pips: int = 15,
                   symbol: Optional[str] = None) -> dict:
    """
    Detect proximity to the High of Day or Low of Day.
    HOD/LOD breaks in Distribution phase are BTMM continuation signals.
    """
    if len(bars) < 10:
        return {"at_hod": False, "at_lod": False, "hod": 0.0, "lod": 0.0,
                "pips_from_hod": 999, "pips_from_lod": 999}

    day = bars[-96:] if len(bars) >= 96 else bars
    hod = max(b["high"] for b in day)
    lod = min(b["low"]  for b in day)
    cur = bars[-1]["close"]
    pip = instruments.pip_size(symbol, cur)
    thr = threshold_pips * pip

    return {
        "at_hod":        abs(cur - hod) <= thr,
        "at_lod":        abs(cur - lod) <= thr,
        "hod":           hod,
        "lod":           lod,
        "pips_from_hod": round(abs(cur - hod) / pip),
        "pips_from_lod": round(abs(cur - lod) / pip),
    }


# ── Half Batman ───────────────────────────────────────────────────────────────

def detect_half_batman(bars: list[dict], asian: dict, symbol: Optional[str] = None) -> dict:
    """
    Half Batman: price breaks Asian range, extends 0.5× the range beyond the
    Asian boundary, then retraces back to the boundary — BTMM continuation.
    Full Batman = 1.0× extension. Both are high-probability re-entry setups.
    """
    if not asian.get("valid") or len(bars) < 10:
        return {"detected": False, "pattern": None, "direction": None}

    rng    = asian["high"] - asian["low"]
    cur    = bars[-1]["close"]
    pip    = instruments.pip_size(symbol, cur)
    tol    = 5 * pip

    half_h = asian["high"] + 0.5 * rng
    half_l = asian["low"]  - 0.5 * rng

    lookback = bars[-20:]
    rec_hi   = max(b["high"] for b in lookback)
    rec_lo   = min(b["low"]  for b in lookback)

    near_ah = abs(cur - asian["high"]) <= tol
    near_al = abs(cur - asian["low"])  <= tol

    # Half Batman retrace complete:
    # Bullish: price swept BELOW Asian low (stop hunt), recovered back to Asian low = BUY
    # Bearish: price swept ABOVE Asian high (stop hunt), sold off back to Asian high = SELL
    if near_al and rec_lo <= half_l:
        return {"detected": True, "pattern": "half_batman", "direction": "bullish"}
    if near_ah and rec_hi >= half_h:
        return {"detected": True, "pattern": "half_batman", "direction": "bearish"}

    # At the half-extension level (approaching reversal / re-entry zone)
    if abs(cur - half_h) <= tol:
        return {"detected": True, "pattern": "half_bat_resistance", "direction": "bearish"}
    if abs(cur - half_l) <= tol:
        return {"detected": True, "pattern": "half_bat_support",    "direction": "bullish"}

    return {"detected": False, "pattern": None, "direction": None}


# ── TDI divergence ────────────────────────────────────────────────────────────

def detect_tdi_divergence(bars: list[dict], lookback: int = 24) -> dict:
    """
    Regular divergence between price and TDI RSI.
    Bearish: price higher high, RSI lower high  → exhaustion signal.
    Bullish: price lower low,  RSI higher low   → exhaustion signal.
    """
    if len(bars) < lookback:
        return {"detected": False, "direction": None}

    closes  = [b["close"] for b in bars]
    rsi_all = _rsi(closes, 13)

    if len(rsi_all) < lookback:
        return {"detected": False, "direction": None}

    half = lookback // 2
    pc   = closes[-lookback:]
    pr   = rsi_all[-lookback:]

    first_hi  = max(pc[:half]);  sec_hi  = max(pc[half:])
    first_lo  = min(pc[:half]);  sec_lo  = min(pc[half:])
    first_rhi = max(pr[:half]);  sec_rhi = max(pr[half:])
    first_rlo = min(pr[:half]);  sec_rlo = min(pr[half:])

    # Use absolute RSI offsets (3 points) so bearish and bullish divergence are equally sensitive
    bearish = sec_hi > first_hi * 1.0001 and sec_rhi < first_rhi - 3
    bullish = sec_lo < first_lo * 0.9999 and sec_rlo > first_rlo + 3

    if bearish:
        return {"detected": True, "direction": "bearish", "type": "regular"}
    if bullish:
        return {"detected": True, "direction": "bullish", "type": "regular"}
    return {"detected": False, "direction": None}


# ── 3-Push stop hunt ──────────────────────────────────────────────────────────

def detect_3push_stop_hunt(bars: list[dict], lookback: int = 30) -> dict:
    """
    Three-drive exhaustion pattern ending with a stop hunt pierce-and-close-back.
    Three ascending highs (bearish) or descending lows (bullish) followed by
    a stop hunt = highest-conviction BTMM reversal signal.
    """
    if len(bars) < lookback + 2:
        return {"detected": False, "direction": None}

    window = bars[-lookback:]
    highs  = [b["high"] for b in window]
    lows   = [b["low"]  for b in window]

    sh: list[tuple[int, float]] = []
    sl: list[tuple[int, float]] = []
    for i in range(2, len(window) - 2):
        if highs[i] >= max(highs[i-2:i]) and highs[i] >= max(highs[i+1:i+3]):
            sh.append((i, highs[i]))
        if lows[i] <= min(lows[i-2:i]) and lows[i] <= min(lows[i+1:i+3]):
            sl.append((i, lows[i]))

    cur = bars[-1]

    # Bearish 3-push: 3 ascending swing highs WITH intervening lows between each (true 3-drive)
    if len(sh) >= 3:
        (i1, h1), (i2, h2), (i3, h3) = sh[-3], sh[-2], sh[-1]
        lows_12 = [s for s in sl if i1 < s[0] < i2]
        lows_23 = [s for s in sl if i2 < s[0] < i3]
        if (h1 < h2 < h3 and lows_12 and lows_23
                and cur["high"] > h3 and cur["close"] < h3):
            return {"detected": True, "direction": "bearish",
                    "push_level": h3, "type": "3push_bear"}

    # Bullish 3-push: 3 descending swing lows WITH intervening highs between each
    if len(sl) >= 3:
        (i1, l1), (i2, l2), (i3, l3) = sl[-3], sl[-2], sl[-1]
        highs_12 = [s for s in sh if i1 < s[0] < i2]
        highs_23 = [s for s in sh if i2 < s[0] < i3]
        if (l1 > l2 > l3 and highs_12 and highs_23
                and cur["low"] < l3 and cur["close"] > l3):
            return {"detected": True, "direction": "bullish",
                    "push_level": l3, "type": "3push_bull"}

    return {"detected": False, "direction": None}


# ── Full pair analysis ────────────────────────────────────────────────────────

def analyze(bars: list[dict], symbol: Optional[str] = None) -> dict:
    """
    Run all 13 BTMM indicators on a bar slice.
    Returns composite score (−100 to +100), 13-item checklist, A+ setup detection,
    and a full trade plan when gates pass.
    """
    if len(bars) < 50:
        return {"signal": "insufficient_data"}

    closes = [b["close"] for b in bars]
    stack  = ema_stack(closes)
    tdi    = calc_tdi(closes)
    adr    = calc_adr(bars)
    adr_c  = adr_consumed(bars, adr)
    amd    = detect_amd(bars)
    kz     = active_kill_zone(bars[-1]["ts_utc"])
    hunt   = detect_stop_hunt(bars)
    asian  = detect_asian_range(bars, symbol)
    tdi_leg = detect_tdi_leg(tdi)

    # New detectors
    level     = detect_level_count(stack)
    mw        = detect_mw_pattern(bars)
    shark     = detect_shark_fin(tdi)
    straight  = detect_straightaway(bars)
    hod_lod   = detect_hod_lod(bars, symbol=symbol)
    half_bat  = detect_half_batman(bars, asian, symbol)
    tdi_div   = detect_tdi_divergence(bars)
    push_hunt = detect_3push_stop_hunt(bars)

    setup_bullish = stack["setup_bullish"]   # price > e800

    # ── 13-item BTMM checklist ────────────────────────────────────────────────
    checklist = [
        # 1. EMA stack aligned (≥ 2 of 5 EMAs in setup direction)
        stack["score"] >= 2 if setup_bullish else stack["score"] <= -2,
        # 2. TDI direction confirms
        tdi["bullish"] if setup_bullish else tdi["bearish"],
        # 3. Kill zone active
        bool(kz),
        # 4. ADR headroom (< 75% consumed)
        adr_c < 75,
        # 5. AMD phase correct for trade direction
        amd["phase"] in ("Manipulation", "Distribution"),
        # 6. Asian range valid and tight (< 50 pips)
        asian["valid"],
        # 7. 3-push stop hunt OR basic stop hunt present
        push_hunt["detected"] or hunt["active"],
        # 8. TDI 2nd leg formed (Shark Fin entry trigger)
        tdi_leg["leg"] == 2,
        # 9. Level II — 4+ of 5 EMAs aligned (institutional-grade stack)
        level["level_ii"],
        # 10. M/W pattern matching setup direction
        mw["detected"] and (
            (mw["pattern"] == "W" and setup_bullish) or
            (mw["pattern"] == "M" and not setup_bullish)
        ),
        # 11. Shark Fin entry ready (leg 2 + medium/high confidence)
        shark["entry_ready"],
        # 12. HOD/LOD proximity OR Half Batman zone
        (hod_lod["at_hod"] and not setup_bullish) or
        (hod_lod["at_lod"] and setup_bullish) or
        half_bat["detected"],
        # 13. TDI divergence confirms exhaustion in opposing direction
        tdi_div["detected"] and (
            (tdi_div["direction"] == "bearish" and not setup_bullish) or
            (tdi_div["direction"] == "bullish" and setup_bullish)
        ),
    ]
    checklist_score = sum(checklist)

    # ── Composite score: EMA stack (−50→+50) + checklist (−50→+50) = −100→+100
    ema_contribution = stack["score"] * 10                       # −50 to +50
    cl_contribution  = round(checklist_score / 13 * 100 - 50)   # −50 to +50
    score = max(-100, min(100, ema_contribution + cl_contribution))

    # ── A+ gate check (must precede signal label so label is accurate) ──────────
    # All four gates must pass; score ≥ 70 alone is not sufficient for A+ label
    is_aplus = (
        score >= 70 and
        checklist_score >= 10 and
        level["level_ii"] and
        bool(kz)
    )

    # ── Signal tier ───────────────────────────────────────────────────────────
    if not kz:
        signal = "WAIT"
    elif is_aplus:
        signal = "A+ Buy" if setup_bullish else "A+ Sell"   # only when ALL gates pass
    elif score >= 40:
        signal = "Strong Buy" if setup_bullish else "Strong Sell"
    elif score >= 20:
        signal = "Buy" if setup_bullish else "Sell"
    else:
        signal = "Neutral"

    # ── Trade plan (SL/TP from Asian range or fallback) ───────────────────────
    pip       = instruments.pip_size(symbol, closes[-1])
    direction = "bullish" if setup_bullish else "bearish"

    # For Half Batman setups the canonical entry is at the Asian boundary (return
    # level), NOT the sweep-low/high close.  Using closes[-1] when price is still
    # below the Asian low on a bullish setup would place the SL above the entry.
    if half_bat.get("detected") and asian["valid"]:
        if setup_bullish and half_bat.get("direction") == "bullish":
            entry = asian["low"]       # limit entry at the return level
        elif not setup_bullish and half_bat.get("direction") == "bearish":
            entry = asian["high"]      # limit entry at the return level
        else:
            entry = closes[-1]
    else:
        entry = closes[-1]

    if asian["valid"]:
        asian_rng = asian["high"] - asian["low"]
        if setup_bullish:
            sl  = asian["low"]  - 2 * pip
            tp1 = asian["high"] + 0.5 * asian_rng
            tp2 = asian["high"] + asian_rng
        else:
            sl  = asian["high"] + 2 * pip
            tp1 = asian["low"]  - 0.5 * asian_rng
            tp2 = asian["low"]  - asian_rng
    else:
        # No valid Asian range → size the stop from volatility (ADR), floored at
        # the instrument's per-asset minimum so indices/metals/crypto don't get a
        # forex-sized (e.g. 20-pip) stop. Targets at 1:1.5 / 1:3 R:R.
        risk = max(instruments.min_sl_distance(symbol, entry), adr * 0.15)
        sl  = entry - risk       if setup_bullish else entry + risk
        tp1 = entry + risk * 1.5 if setup_bullish else entry - risk * 1.5
        tp2 = entry + risk * 3.0 if setup_bullish else entry - risk * 3.0

    # Safety clamp: SL must always be on the correct side of entry regardless of
    # how the Asian range and current price interact (guards against future regressions).
    if setup_bullish:
        sl = min(sl, entry - 2 * pip)
    else:
        sl = max(sl, entry + 2 * pip)

    # ── TP sanity: if price already moved past Asian-range TPs, recalculate ──
    # This happens when price breaks the Asian high/low before the alert fires.
    # Without this fix, TP1 can end up behind entry → garbage R:R (e.g. 1:0.03).
    sl_dist = abs(entry - sl)
    if setup_bullish:
        # TP1 must be above entry; TP2 must be above TP1
        if tp1 <= entry + 2 * pip:
            tp1 = entry + sl_dist          # 1:1 R:R minimum
            tp2 = entry + sl_dist * 2      # 1:2 R:R
        elif tp2 <= tp1 + 2 * pip:
            tp2 = tp1 + sl_dist            # extend TP2 to at least SL-distance past TP1
    else:
        # TP1 must be below entry; TP2 must be below TP1
        if tp1 >= entry - 2 * pip:
            tp1 = entry - sl_dist          # 1:1 R:R minimum
            tp2 = entry - sl_dist * 2      # 1:2 R:R
        elif tp2 >= tp1 - 2 * pip:
            tp2 = tp1 - sl_dist

    # ── A+ setup detection ────────────────────────────────────────────────────
    active_setup = None
    if is_aplus:
        active_setup = {
            "key":         "aplus",
            "direction":   direction,
            "gatesPassed": checklist_score,
            "gatesTotal":  13,
            "entry":       entry,
            "sl":          sl,
            "tp1":         tp1,
            "tp2":         tp2,
            "confidence":  "high",
            "tier":        "A+",
        }
    else:
        # Safety Trade proxy (legacy — score ≥ 40 region with 5/7 gates)
        near_e50     = abs(closes[-1] - stack["e50"]) / closes[-1] < 0.0008
        safety_gates = sum([
            bool(kz),
            asian["valid"],
            hunt["active"],
            tdi_leg["leg"] == 2 and tdi_leg["confidence"] != "low",
            abs(closes[-1] - stack["e200"]) / closes[-1] < 0.001,
            near_e50,
            checklist_score >= 7,
        ])
        if safety_gates >= 5 and kz:
            active_setup = {
                "key":         "safety",
                "direction":   direction,
                "gatesPassed": safety_gates,
                "gatesTotal":  7,
                "entry":       entry,
                "sl":          sl,
                "tp1":         tp1,
                "tp2":         tp2,
                "confidence":  "high" if safety_gates >= 7 else "medium",
            }

    cross_513 = detect_513_cross(closes)

    # ── Confidence classification (mirrors frontend logic) ───────────────────
    # 5 BTMM factors: EMA stack, TDI, ADR headroom, AMD phase, Kill zone
    # Count how many align bullish vs bearish. 4+ aligned = High confidence.
    ema_factor  = 1 if stack["score"] >= 2 else (-1 if stack["score"] <= -2 else 0)
    tdi_factor  = 1 if tdi["bullish"] else (-1 if tdi["bearish"] else 0)
    adr_factor  = 1 if adr_c < 50 else (-1 if adr_c > 80 else 0)
    amd_factor  = 1 if amd["phase"] in ("Manipulation",) else (-1 if amd["phase"] == "Distribution" else 0)
    kz_factor   = 1 if kz else 0
    btmm_factors = [ema_factor, tdi_factor, adr_factor, amd_factor, kz_factor]
    bullish_count = sum(1 for f in btmm_factors if f > 0)
    bearish_count = sum(1 for f in btmm_factors if f < 0)
    confidence = "high" if (bullish_count >= 4 or bearish_count >= 4) else \
                 "medium" if (bullish_count >= 2 or bearish_count >= 2) else "low"

    return {
        "signal":          signal,
        "score":           score,
        "checklist_score": checklist_score,
        "confidence":      confidence,
        "setup_bullish":   setup_bullish,
        "kz":              kz,
        "adr_consumed":    adr_c,
        "adr":             adr,
        "tdi":             tdi,
        "stack":           stack,
        "amd":             amd,
        "stop_hunt":       hunt,
        "asian_range":     asian,
        "tdi_leg":         tdi_leg,
        "active_setup":    active_setup,
        "cross_513":       cross_513,
        # Extended BTMM pattern results
        "level_count":     level,
        "mw_pattern":      mw,
        "shark_fin":       shark,
        "straightaway":    straight,
        "hod_lod":         hod_lod,
        "half_batman":     half_bat,
        "tdi_divergence":  tdi_div,
        "push_hunt":       push_hunt,
    }
