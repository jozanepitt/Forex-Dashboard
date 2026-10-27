"""
BTMM core indicators ported from index.html JS.
All functions take a list of bar dicts: {open, high, low, close, ts_utc}.
"""
from __future__ import annotations
import math
from typing import Optional

import instruments


# ── EMA ───────────────────────────────────────────────────────────────────────

def calc_ema(closes: list[float], period: int) -> list[float]:
    if not closes:
        return []
    k = 2.0 / (period + 1)
    if len(closes) < period:
        # Fewer bars than the period: seed the recursion at the first close and
        # smooth forward. Must NOT flat-line at closes[-1] — doing so made the
        # 800-EMA exactly equal the current price, so `price > e800` was always
        # False and every setup was forced BEARISH whenever <800 bars were
        # cached (with the confidence bug, that zeroed out A+ alerting).
        out = [closes[0]]
        for v in closes[1:]:
            out.append(v * k + out[-1] * (1 - k))
        return out
    out = [sum(closes[:period]) / period]
    for v in closes[period:]:
        out.append(v * k + out[-1] * (1 - k))
    # Pad front so indices align with closes
    return [out[0]] * period + out[1:]


def ema_last(closes: list[float], period: int) -> float:
    return calc_ema(closes, period)[-1]


# ── BTMM confidence (directional factors only) ────────────────────────────────

def _btmm_direction_factors(stack: dict, tdi: dict, setup_bullish: bool,
                            mw: dict, tdi_div: dict) -> list[int]:
    """Five DIRECTIONAL BTMM votes, each -1 / 0 / +1.

    Deliberately excludes ADR consumption, AMD phase and kill-zone: those are
    timing/quality gates (applied elsewhere) and are NOT bull/bear signals.
    Including them — especially kz, which could only ever vote bullish — made
    the confidence count structurally biased toward buys.
    """
    ema_f = 1 if stack.get("score", 0) >= 2 else (-1 if stack.get("score", 0) <= -2 else 0)
    tdi_f = 1 if tdi.get("bullish") else (-1 if tdi.get("bearish") else 0)
    bias_f = 1 if setup_bullish else -1                       # price vs 800 EMA
    mw_f = 1 if (mw.get("detected") and mw.get("pattern") == "W") else \
           (-1 if (mw.get("detected") and mw.get("pattern") == "M") else 0)
    div_f = 1 if (tdi_div.get("detected") and tdi_div.get("direction") == "bullish") else \
            (-1 if (tdi_div.get("detected") and tdi_div.get("direction") == "bearish") else 0)
    return [ema_f, tdi_f, bias_f, mw_f, div_f]


def _classify_confidence(bullish_count: int, bearish_count: int) -> str:
    if bullish_count >= 4 or bearish_count >= 4:
        return "high"
    if bullish_count >= 2 or bearish_count >= 2:
        return "medium"
    return "low"


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
        "slow_arr": slow_arr,       # full array — used by detect_rsi_signal_cross
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
    Session-based phase on the course's sessions (btmm_rules, New York time):
      Accumulation  = Asian session and the gaps between sessions
      Manipulation  = London session (03:30-09:00 ET) -- the stop hunt
      Distribution  = NY session     (09:30-17:00 ET) -- the trend move
    `cycle` gives the course's own words (Accumulation / Stop Hunt / Trend
    Move / End of Day Reversal); `phase` keeps the names the checklist uses.
    """
    import btmm_rules
    if len(bars) < 10:
        return {"phase": "Unknown"}
    ts = bars[-1]["ts_utc"]
    s = btmm_rules.session(ts)
    phase = {"London": "Manipulation", "NY": "Distribution"}.get(s, "Accumulation")
    day_key = btmm_rules.trading_day(ts)
    day = [b for b in bars[-200:] if btmm_rules.trading_day(b["ts_utc"]) == day_key] or bars[-96:]
    return {"phase": phase, "cycle": btmm_rules.cycle_phase(ts), "session": s,
            "day_high": max(b["high"] for b in day), "day_low": min(b["low"] for b in day)}


# ── Kill zone ─────────────────────────────────────────────────────────────────

def active_kill_zone(ts_utc: int) -> Optional[str]:
    """'London' / 'NY' during the course's trading sessions, else None
    (2026-10-27: was four short GMT windows covering 10h, one of them an
    'Asian' window that actually fell inside the course's NY session)."""
    import btmm_rules
    return btmm_rules.kill_zone(ts_utc)


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
    """The most recent Asian session (course: 20:30-03:00 New York time)."""
    import btmm_rules
    asian = []
    for b in reversed(bars[-200:]):
        if btmm_rules.session(b["ts_utc"]) == "Asian":
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

def _last_ema_cross(fast: list[float], slow: list[float], start: int = 0) -> Optional[tuple[int, str]]:
    """Most recent bar index >= start+1 where `fast` crosses `slow`, plus
    direction. None if no cross occurs in the scanned range."""
    result = None
    for i in range(max(1, start + 1), min(len(fast), len(slow))):
        prev_diff = fast[i - 1] - slow[i - 1]
        diff = fast[i] - slow[i]
        if prev_diff <= 0 < diff:
            result = (i, "bullish")
        elif prev_diff >= 0 > diff:
            result = (i, "bearish")
    return result


def detect_level_count(bars: list[dict], closes: list[float]) -> dict:
    """
    BTMM 'Levels' doctrine: the Market Maker Cycle traced as a sequence of EMA
    crossovers unfolding over multiple days —
      Level 1 = 13/50 EMA cross
      Level 2 = 50/200 EMA cross, same direction, after Level 1
      Level 3 = 50/800 or 200/800 EMA cross (same direction, after Level 2),
                or the EMA stack already fully fanned out in that direction

    Replaces a prior implementation that counted how many of 5 EMAs price was
    CURRENTLY on the correct side of on a single bar — that measured
    instantaneous alignment strength, not the actual multi-day crossover
    sequence the doctrine describes, and could report "Level II"
    (institutional-grade) with no crossover ever having occurred.
    """
    if len(closes) < 60:
        return {"level": 0, "direction": None, "level_i": False, "level_ii": False,
                "days_since_level1": None, "typical_window": False}

    e13 = calc_ema(closes, 13)
    e50 = calc_ema(closes, 50)
    e200 = calc_ema(closes, 200)
    e800 = calc_ema(closes, 800)

    l1 = _last_ema_cross(e13, e50)
    if not l1:
        return {"level": 0, "direction": None, "level_i": False, "level_ii": False,
                "days_since_level1": None, "typical_window": False}
    l1_idx, direction = l1

    level = 1
    l2 = _last_ema_cross(e50, e200, start=l1_idx)
    if l2 and l2[1] == direction:
        level = 2
        l3a = _last_ema_cross(e50, e800, start=l2[0])
        l3b = _last_ema_cross(e200, e800, start=l2[0])
        l3_candidates = [c for c in (l3a, l3b) if c and c[1] == direction]
        if l3_candidates:
            level = 3
        elif direction == "bullish" and closes[-1] > e13[-1] > e50[-1] > e200[-1] > e800[-1]:
            level = 3
        elif direction == "bearish" and closes[-1] < e13[-1] < e50[-1] < e200[-1] < e800[-1]:
            level = 3

    days_since = (bars[-1]["ts_utc"] - bars[l1_idx]["ts_utc"]) / 86400
    return {
        "level": level,
        "direction": direction,
        "level_i": level >= 1,
        "level_ii": level >= 2,
        "days_since_level1": round(days_since, 1),
        "typical_window": 2.0 <= days_since <= 6.0,
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

    # 2026-10-27: M used to be checked first and always won, and "second high
    # <= first" allowed +0.2% (~22 EUR/USD pips) -- 26 of 27 pairs read "M".
    # Now both are judged, the most recent 2nd leg wins, and the 2nd leg may
    # overshoot the 1st by at most 0.02% (~2 pips): an M is a lower/equal high.
    tol = 0.0002
    found = []
    if len(swing_highs) >= 2:
        (i1, h1), (i2, h2) = swing_highs[-2], swing_highs[-1]
        if any(i1 < sl[0] < i2 for sl in swing_lows) and h2 <= h1 * (1 + tol):
            found.append((i2, "M", 0.9 if h2 < h1 else 0.65))   # lower high = stronger M
    if len(swing_lows) >= 2:
        (i1, l1), (i2, l2) = swing_lows[-2], swing_lows[-1]
        if any(i1 < sh[0] < i2 for sh in swing_highs) and l2 >= l1 * (1 - tol):
            found.append((i2, "W", 0.9 if l2 > l1 else 0.65))   # higher low = stronger W
    if not found:
        return {"detected": False, "pattern": None, "quality": 0.0}
    _, pattern, quality = max(found)
    return {"detected": True, "pattern": pattern, "quality": quality}


# ── Nameable candle ────────────────────────────────────────────────────────────

def detect_nameable_candle(bars: list[dict]) -> dict:
    """Port of the frontend's detectNameableCandle: classifies the last candle
    against the previous one as Doji / Hammer / Shooting Star / (Bullish or
    Bearish) Engulfing / RRT (Railroad Track) / COW (Candle On Wick)."""
    if len(bars) < 2:
        return {"found": False, "name": "None", "direction": "neutral"}
    curr, prev = bars[-1], bars[-2]
    curr_body  = abs(curr["close"] - curr["open"])
    curr_range = curr["high"] - curr["low"]
    prev_body  = abs(prev["close"] - prev["open"])
    prev_range = prev["high"] - prev["low"]
    if curr_range == 0 or prev_range == 0:
        return {"found": False, "name": "None", "direction": "neutral"}

    curr_bullish = curr["close"] > curr["open"]
    prev_bullish = prev["close"] > prev["open"]
    curr_upper_wick = curr["high"] - max(curr["open"], curr["close"])
    curr_lower_wick = min(curr["open"], curr["close"]) - curr["low"]

    if curr_body / curr_range < 0.15:
        return {"found": True, "name": "Doji", "direction": "neutral"}
    if curr_lower_wick >= 2 * curr_body and curr_upper_wick < curr_body:
        return {"found": True, "name": "Hammer", "direction": "bullish"}
    if curr_upper_wick >= 2 * curr_body and curr_lower_wick < curr_body:
        return {"found": True, "name": "Shooting Star", "direction": "bearish"}
    if (curr_bullish != prev_bullish and curr_body > prev_body * 1.1 and
            min(curr["open"], curr["close"]) <= min(prev["open"], prev["close"]) and
            max(curr["open"], curr["close"]) >= max(prev["open"], prev["close"])):
        name = "Bullish Engulfing" if curr_bullish else "Bearish Engulfing"
        return {"found": True, "name": name, "direction": "bullish" if curr_bullish else "bearish"}
    if curr_bullish != prev_bullish and prev_body * 0.7 <= curr_body <= prev_body * 1.3:
        return {"found": True, "name": "RRT", "direction": "bullish" if curr_bullish else "bearish"}

    prev_body_hi, prev_body_lo = max(prev["open"], prev["close"]), min(prev["open"], prev["close"])
    curr_body_hi, curr_body_lo = max(curr["open"], curr["close"]), min(curr["open"], curr["close"])
    if curr_body_hi <= prev_body_lo or curr_body_lo >= prev_body_hi:
        in_upper_wick = curr_body_lo >= prev_body_hi and curr_body_hi <= prev["high"]
        in_lower_wick = curr_body_hi <= prev_body_lo and curr_body_lo >= prev["low"]
        if in_upper_wick or in_lower_wick:
            return {"found": True, "name": "COW", "direction": "bullish" if in_lower_wick else "bearish"}

    return {"found": False, "name": "None", "direction": "neutral"}


# Shark Fin / Blood in the Water: see btmm_rules.detect_shark_fin (course p.45-46).

def detect_rsi_signal_cross(tdi: dict, lookback: int = 5) -> dict:
    """
    Plain RSI(fast)/Signal(slow) line cross — no Bollinger-Band extreme
    precondition. Distinct from detect_tdi_leg/detect_shark_fin, which both
    require the fast line to have first spiked outside the bands (Shark Fin
    doctrine, used by setups that actually call for that).

    Some setups' own reference rules explicitly do NOT want a band/MBL
    extreme requirement — just the fast-crosses-slow event itself (e.g. the
    ID50 / 50-50 Bounce checklist: "TDI confirmation - RSI/Signal line
    cross," explicitly not gated on RSI being above/below the MBL like other
    setups are). Using detect_tdi_leg's leg==1 (currently outside the band)
    for those setups was requiring the *opposite* of the wrong thing.
    """
    fast_arr = tdi.get("fast_arr", [])
    slow_arr = tdi.get("slow_arr", [])
    n = min(len(fast_arr), len(slow_arr))
    if n < 2:
        return {"crossed": False, "direction": None}
    fast_arr, slow_arr = fast_arr[-n:], slow_arr[-n:]
    start = max(1, n - lookback)
    for i in range(start, n):
        if fast_arr[i - 1] <= slow_arr[i - 1] and fast_arr[i] > slow_arr[i]:
            return {"crossed": True, "direction": "bullish"}
        if fast_arr[i - 1] >= slow_arr[i - 1] and fast_arr[i] < slow_arr[i]:
            return {"crossed": True, "direction": "bearish"}
    return {"crossed": False, "direction": None}


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
                   symbol: Optional[str] = None, away_pips: int = 20) -> dict:
    """
    Detect a valid RETEST of the High/Low of Day (a BTMM sell/buy location).

    Two corrections over the naive version:
      * The day is the current trading day (from the 17:00 ET reset), not a
        rolling 96-bar window (which straddled two sessions).
      * `at_hod`/`at_lod` require a RETEST: price must have pulled away from the
        extreme by `away_pips` earlier in the day and now returned to it. A fresh
        breakout push straight to a new HOD/LOD is the Manipulation trap
        (doctrine §2/§15: "do NOT trade the breakout"), not a setup — proximity
        alone must not qualify.
    """
    if len(bars) < 10:
        return {"at_hod": False, "at_lod": False, "hod": 0.0, "lod": 0.0,
                "retest_hod": False, "retest_lod": False,
                "pips_from_hod": 999, "pips_from_lod": 999}

    # Trading day = from the 17:00 ET daily high/low reset (course p.24)
    import btmm_rules
    last_day = btmm_rules.trading_day(bars[-1]["ts_utc"])
    day = [b for b in bars[-200:] if btmm_rules.trading_day(b["ts_utc"]) == last_day]
    if len(day) < 3:                       # too early in the day — fall back
        day = bars[-96:] if len(bars) >= 96 else bars

    hod_idx = max(range(len(day)), key=lambda i: day[i]["high"])
    lod_idx = min(range(len(day)), key=lambda i: day[i]["low"])
    hod = day[hod_idx]["high"]
    lod = day[lod_idx]["low"]
    cur = bars[-1]["close"]
    pip = instruments.pip_size(symbol, cur)
    thr = threshold_pips * pip
    away = away_pips * pip

    # A retest requires price to have pulled at least `away` off the extreme
    # AFTER it was set, then returned. Checking bars only after the HOD/LOD bar
    # is what distinguishes a genuine retest from a fresh breakout push (in a
    # straight run-up, earlier bars are trivially far below the new HOD).
    after_hod = day[hod_idx + 1:]
    after_lod = day[lod_idx + 1:]
    pulled_from_hod = any((hod - x["low"]) >= away for x in after_hod)
    pulled_from_lod = any((x["high"] - lod) >= away for x in after_lod)

    return {
        "at_hod":        abs(cur - hod) <= thr and pulled_from_hod,
        "at_lod":        abs(cur - lod) <= thr and pulled_from_lod,
        "retest_hod":    pulled_from_hod,
        "retest_lod":    pulled_from_lod,
        "hod":           hod,
        "lod":           lod,
        "pips_from_hod": round(abs(cur - hod) / pip),
        "pips_from_lod": round(abs(cur - lod) / pip),
    }


# Half Batman Type 1 / Type 2: see btmm_rules.detect_half_batman (course
# "Half a Batman"; the old Asian-range-extension version was a different pattern).


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


# ── H1 resample (for 50/50 Bounce's higher-timeframe gate) ────────────────────

def resample_to_h1(bars: list[dict]) -> dict:
    """
    Aggregate M15 bars into H1 closes and derive a trend from the EMA50/EMA200
    stack. Exact port of the frontend's resampleToH1().
    """
    if not bars or len(bars) < 8:
        return {"valid": False, "trend": "neutral", "ema50": 0.0, "ema200": 0.0, "candle_count": 0}

    groups: dict[int, list[dict]] = {}
    for b in bars:
        groups.setdefault(b["ts_utc"] // 3600, []).append(b)

    h1_closes = [groups[key][-1]["close"] for key in sorted(groups.keys())]
    if len(h1_closes) < 10:
        return {"valid": False, "trend": "neutral", "ema50": 0.0, "ema200": 0.0,
                "candle_count": len(h1_closes)}

    h1_ema50  = ema_last(h1_closes, 50)
    h1_ema200 = ema_last(h1_closes, 200)
    h1_price  = h1_closes[-1]

    if h1_price > h1_ema50 > h1_ema200:
        trend = "bullish"
    elif h1_price < h1_ema50 < h1_ema200:
        trend = "bearish"
    else:
        trend = "neutral"

    return {"valid": True, "trend": trend, "ema50": h1_ema50, "ema200": h1_ema200,
            "current_price": h1_price, "candle_count": len(h1_closes)}


# ── Named BTMM setups (Safety Trade / 22 Trade / 50-50 Bounce / Three-Drive) ──
# Ported from the frontend's detectSafetyTrade/detect22Trade/
# detectFiftyFiftyBounce/detectThreeDrivePattern so these setups can actually
# fire on Discord and be backtested — previously only "aplus" and a "safety"
# proxy existed on the backend, so selecting the other three in the Backtest
# tab always returned zero trades and they could never alert.

def _gate_result(gates: list[dict], direction: str,
                  entry: float, sl: float, tp1: float, tp2: float) -> dict:
    """Shared confidence/active scoring — exact port of the frontend's _gateResult()."""
    passed   = sum(1 for g in gates if g["pass"])
    total    = len(gates)
    required = math.ceil(total * 0.7)
    active   = passed >= required and direction != "neutral"
    if passed == total:
        confidence = "high"
    elif passed >= required + 1:
        confidence = "medium"
    elif passed >= required:
        confidence = "low"
    else:
        confidence = "none"
    return {
        "active": active, "direction": direction, "gates": gates,
        "gatesPassed": passed, "gatesTotal": total,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "confidence": confidence,
    }


def detect_safety_trade(bars: list[dict], stack: dict, asian: dict, tdi_leg: dict,
                         level: dict, mw: dict, nameable: dict, h1mtf: dict,
                         kz: Optional[str], symbol: Optional[str] = None) -> dict:
    """
    Safety Trade — the canonical BTMM setup (dashboard's default for alerting
    and backtesting). Real port of the frontend's detectSafetyTrade, replacing
    the legacy "safety" proxy that derived direction from EMA800 bias alone
    and had no candlestick, Level, or M/W confirmation of any kind.
    """
    current_price = bars[-1]["close"]
    pip = instruments.pip_size(symbol, current_price)

    # Direction: TDI 2nd leg drives bias; nameable candle must agree.
    direction = "neutral"
    if tdi_leg.get("leg") == 2:
        direction = tdi_leg["direction"]

    asian_valid = bool(asian.get("valid"))
    in_kz = kz in ("London", "NY", "Overlap")   # excludes Asian, matching the frontend
    pierced_asian = asian_valid and (
        current_price <= asian["low"] + 5 * pip or current_price >= asian["high"] - 5 * pip
    )
    tdi_2nd = tdi_leg.get("leg") == 2 and tdi_leg.get("confidence") != "low"

    dist50_pips = abs(current_price - stack["e50"]) / pip if stack.get("e50") else 999
    dist200_pips = abs(current_price - stack["e200"]) / pip if stack.get("e200") else 999
    ema_bounce = (6 <= dist50_pips <= 8) or dist200_pips < 25

    candle_agrees = (nameable.get("found") and nameable.get("name") in ("RRT", "Hammer", "Doji")
                      and nameable.get("direction") == direction)

    # Checklist wants Level 1 specifically (the early, freshest level), not a
    # matured II/III cascade, and — same as btmm_123's _level_score — the
    # cascade's own direction must match this setup's direction.
    fresh_level = level.get("level") == 1 and level.get("direction") == direction

    # Checklist rule 4's trailing "...forming M or W" clause; the Shark-Fin/
    # band half is covered by tdi_2nd above.
    mw_confirms = mw.get("detected") and (
        (mw.get("pattern") == "W" and direction == "bullish") or
        (mw.get("pattern") == "M" and direction == "bearish")
    )

    # PDF pairs "pullback to 50 EMA" with "and the 50% level of the Asian
    # range" as a joint confluence (pp.227-231).
    near_50_level = asian_valid and abs(current_price - asian["mid"]) / pip <= 10

    h1_aligned = (not h1mtf.get("valid") or h1mtf.get("trend") == direction
                  or h1mtf.get("trend") == "neutral")

    gates = [
        {"name": "Kill Zone Active",       "pass": in_kz},
        {"name": "Asian Range Valid",      "pass": asian_valid},
        {"name": "Asian Box Pierce/Touch", "pass": pierced_asian},
        {"name": "TDI 2nd Leg Confirmed",  "pass": tdi_2nd},
        {"name": "50/200 EMA Bounce Zone", "pass": ema_bounce},
        {"name": "Nameable Candle Agrees", "pass": candle_agrees},
        {"name": "Level 1 (Fresh)",        "pass": fresh_level},
        {"name": "M/W Pattern Confirms",   "pass": mw_confirms},
        {"name": "Near Asian 50% Level",   "pass": near_50_level},
        {"name": "H1 MTF Aligned",         "pass": h1_aligned},
    ]

    entry = sl = tp1 = tp2 = 0.0
    if direction != "neutral" and asian_valid:
        entry = current_price
        if direction == "bullish":
            sl  = asian["low"] - 5 * pip
            tp1 = asian["high"]
            tp2 = asian["high"] + (asian["high"] - asian["low"])
        else:
            sl  = asian["high"] + 5 * pip
            tp1 = asian["low"]
            tp2 = asian["low"] - (asian["high"] - asian["low"])

    return _gate_result(gates, direction, entry, sl, tp1, tp2)


def detect_22_trade(bars: list[dict], asian: dict, hunt: dict, hod_lod: dict,
                     tdi: dict, symbol: Optional[str] = None) -> dict:
    """
    22 Trade — fires at 02:00 / 22:00 GMT (London prep + Asian open).
    Fade the manipulation push that hunts stops at the session boundary.
    """
    from datetime import datetime, timezone
    current_price = bars[-1]["close"]
    pip = instruments.pip_size(symbol, current_price)
    gmt_hour = datetime.fromtimestamp(bars[-1]["ts_utc"], tz=timezone.utc).hour

    in_window = (2 <= gmt_hour < 4) or (22 <= gmt_hour < 24)
    asian_range_ok = bool(asian.get("valid")) and asian.get("range_pips", 0) >= 15
    stop_hunt_active = bool(hunt.get("active"))

    # detect_stop_hunt() already stores the FADE direction (bearish after an
    # upside pierce, bullish after a downside pierce) — no inversion needed,
    # unlike the frontend's raw 'up'/'down' hunt.direction.
    direction = "neutral"
    if stop_hunt_active:
        direction = hunt.get("direction") or "neutral"
    elif hod_lod.get("at_hod"):
        direction = "bearish"
    elif hod_lod.get("at_lod"):
        direction = "bullish"

    tdi_agrees = bool(tdi) and (
        (direction == "bullish" and tdi["fast"] > tdi["slow"] and tdi["fast"] < 70) or
        (direction == "bearish" and tdi["fast"] < tdi["slow"] and tdi["fast"] > 30)
    )
    hod_lod_fade = (
        (direction == "bearish" and hod_lod.get("at_hod")) or
        (direction == "bullish" and hod_lod.get("at_lod"))
    )

    gates = [
        {"name": "22:00 or 02:00 GMT Window", "pass": in_window},
        {"name": "Asian Range >= 15 pips",    "pass": asian_range_ok},
        {"name": "Stop Hunt Detected",        "pass": stop_hunt_active},
        {"name": "HOD/LOD Retest Fade",       "pass": hod_lod_fade},
        {"name": "TDI Not At Extreme",        "pass": tdi_agrees},
    ]

    entry = sl = tp1 = tp2 = 0.0
    if direction != "neutral" and asian_range_ok:
        entry = current_price
        if direction == "bearish":
            hunt_extreme = bars[-1]["high"] if stop_hunt_active else asian["high"]
            sl  = hunt_extreme + 5 * pip
            tp1 = (asian["high"] + asian["low"]) / 2
            tp2 = asian["low"]
        else:
            hunt_extreme = bars[-1]["low"] if stop_hunt_active else asian["low"]
            sl  = hunt_extreme - 5 * pip
            tp1 = (asian["high"] + asian["low"]) / 2
            tp2 = asian["high"]

    return _gate_result(gates, direction, entry, sl, tp1, tp2)


def detect_fifty_fifty_bounce(bars: list[dict], stack: dict, level: dict, tdi_leg: dict,
                               h1mtf: dict, tdi: dict, symbol: Optional[str] = None) -> dict:
    """
    50/50 Bounce (== ID 50) — mid-day reversal off the 50 EMA in the lull
    between London close and NY open.

    TDI confirmation is a plain RSI/Signal-line cross (detect_rsi_signal_cross),
    NOT a Bollinger-Band-extreme requirement. Per the trader's own reference
    checklist for this exact setup: "TDI confirmation - RSI/Signal line cross"
    — explicitly NOT gated on RSI being above/below the MBL/band the way other
    BTMM setups are. Previously gated on tdi_leg.leg==1 (detect_tdi_leg), which
    requires the fast line to be CURRENTLY OUTSIDE the band — the opposite of
    what this setup's own rules ask for.
    """
    from datetime import datetime, timezone
    current_price = bars[-1]["close"]
    pip = instruments.pip_size(symbol, current_price)
    e50 = stack["e50"]

    direction = "neutral"
    if h1mtf.get("valid") and h1mtf.get("trend") != "neutral":
        direction = h1mtf["trend"]
    elif tdi_leg.get("leg") == 1:
        direction = tdi_leg["direction"]

    dist50_pips  = abs(current_price - e50) / pip if e50 else 999
    close_to_50  = dist50_pips <= 8
    fresh_cross  = bool(level.get("level_i"))
    rsi_sig_cross = detect_rsi_signal_cross(tdi)
    tdi_confirmed = rsi_sig_cross["crossed"] and rsi_sig_cross["direction"] == direction
    h1_aligned    = bool(h1mtf.get("valid")) and h1mtf.get("trend") == direction

    gmt_hour = datetime.fromtimestamp(bars[-1]["ts_utc"], tz=timezone.utc).hour
    in_lull  = (11 <= gmt_hour < 14) or (16 <= gmt_hour < 22)

    gates = [
        {"name": "Price <= 8 pips from EMA(50)",       "pass": close_to_50},
        {"name": "13/50 Cross Level I (fresh)",        "pass": fresh_cross},
        {"name": "RSI/Signal Line Cross (no MBL req)", "pass": tdi_confirmed},
        {"name": "H1 MTF Aligned",                     "pass": h1_aligned},
        {"name": "Mid-Day Lull Window",                "pass": in_lull},
    ]

    entry = sl = tp1 = tp2 = 0.0
    if direction != "neutral" and e50:
        entry = current_price
        risk  = instruments.min_sl_distance(symbol, entry)
        if direction == "bullish":
            sl  = e50 - risk
            d   = max(abs(entry - sl), risk)
            tp1 = entry + d * 1.5
            tp2 = entry + d * 3
        else:
            sl  = e50 + risk
            d   = max(abs(entry - sl), risk)
            tp1 = entry - d * 1.5
            tp2 = entry - d * 3

    return _gate_result(gates, direction, entry, sl, tp1, tp2)


def detect_three_drive_pattern(bars: list[dict], shark: dict, tdi_div: dict, mw: dict,
                                symbol: Optional[str] = None) -> dict:
    """
    Three-Drive — refinement of Shark Fin: three pushes into a TDI extreme with
    diverging price. Reversal entry on 4th-push failure.

    shark['direction'] is already the TRADE direction (a fin into overbought
    = 'bearish' = sell). 2026-10-27: this used to flip it, so an overbought
    fin produced a BUY.
    """
    current_price = bars[-1]["close"]

    mw_direction = ("bullish" if mw.get("pattern") == "W"
                    else "bearish" if mw.get("pattern") == "M" else None)

    direction = "neutral"
    if mw.get("detected") and mw_direction:
        direction = mw_direction
    elif shark.get("detected") and shark.get("direction"):
        direction = shark["direction"]

    gates = [
        {"name": "Shark Fin Active",         "pass": bool(shark.get("detected"))},
        {"name": "TDI Divergence Confirmed", "pass": bool(tdi_div.get("detected"))},
        {"name": "M/W Pattern Aligned",      "pass": bool(mw.get("detected")) and mw_direction == direction},
    ]

    entry = sl = tp1 = tp2 = 0.0
    if direction != "neutral":
        entry = current_price
        risk  = instruments.min_sl_distance(symbol, entry)
        if direction == "bullish":
            sl  = entry - risk
            tp1 = entry + risk * 2
            tp2 = entry + risk * 4
        else:
            sl  = entry + risk
            tp1 = entry - risk * 2
            tp2 = entry - risk * 4

    return _gate_result(gates, direction, entry, sl, tp1, tp2)


# ── Checklist / score / label (phase 1 fixes, 2026-10-27) ─────────────────────

def _btmm_checklist(bull: bool, *, stack, tdi, kz, adr_c, amd, asian, push_hunt, hunt,
                    tdi_leg, level, mw, shark, hod_lod, half_bat, tdi_div) -> list[bool]:
    """The 13-item BTMM checklist for a trade in direction `bull`. Every
    directional item must point the trade's way (audit 2026-10-15: the TDI 2nd
    leg, Shark Fin, stop hunt, Level II and Half Batman items used to pass when
    they pointed the other way)."""
    d = "bullish" if bull else "bearish"
    return [
        stack["score"] >= 2 if bull else stack["score"] <= -2,                       # 1 EMA stack
        bool(tdi["bullish"] if bull else tdi["bearish"]),                            # 2 TDI direction
        bool(kz),                                                                    # 3 kill zone
        adr_c < 75,                                                                  # 4 ADR headroom
        amd["phase"] in ("Manipulation", "Distribution"),                            # 5 AMD phase
        bool(asian.get("valid")),                                                    # 6 Asian range
        bool((push_hunt.get("detected") and push_hunt.get("direction") == d) or
             (hunt.get("active") and hunt.get("direction") == d)),                   # 7 stop hunt
        tdi_leg.get("leg") == 2 and tdi_leg.get("direction") == d,                   # 8 TDI 2nd leg
        bool(level.get("level_ii")) and level.get("direction") == d,                 # 9 Level II
        bool(mw.get("detected") and mw.get("pattern") == ("W" if bull else "M")
             and mw.get("confirmed")),                                               # 10 confirmed M/W
        bool(shark.get("entry_ready")) and shark.get("direction") == d,              # 11 Shark Fin
        bool((hod_lod.get("at_lod") if bull else hod_lod.get("at_hod")) or
             (half_bat.get("detected") and half_bat.get("direction") == d)),         # 12 HOD/LOD / Half Batman
        bool(tdi_div.get("detected")) and tdi_div.get("direction") == d,             # 13 divergence
    ]


# Score weight per checklist item. Confirmed M/W (item 10) counts half: it
# fires on most bars and the 2026-10-27 replay found no edge in it
# (-0.03R gross over 8,091 trades). User decision 2026-10-27.
CHECKLIST_WEIGHTS = (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.5, 1, 1, 1)


def _weighted_checklist(items: list[bool]) -> float:
    """Weighted checklist points, rescaled to the 13-point range."""
    return sum(w for w, ok in zip(CHECKLIST_WEIGHTS, items) if ok) * 13 / sum(CHECKLIST_WEIGHTS)


def _composite_score(stack_score: int, checklist_score: float, bull: bool) -> int:
    """-100..+100, signed by the trade direction: EMA stack (-50..+50) plus the
    checklist (0..50 toward the trade). Audit 2026-10-15: the checklist part
    used to always count upward, so a sell topped out at about +30 and an A+
    Sell (score <= -70) was impossible."""
    sgn = 1 if bull else -1
    return max(-100, min(100, stack_score * 10 + sgn * round(checklist_score / 13 * 50)))


APLUS_MIN_SCORE = 40


def _is_aplus(shark: dict, bull: bool, kz, score: int) -> bool:
    """A+ needs a TRIGGER with a replay edge: a Shark Fin entry (it already
    requires the 800 EMA bias) in the trade direction, in a session, with the
    score at least APLUS_MIN_SCORE toward the trade. M/W, Half Batman and the
    rest of the checklist are confluence only and can never make A+ on their
    own (user decision 2026-10-27; replay: Shark Fin +0.10R gross, M/W and
    Half Batman negative). Was: score >= 70 + 10/13 checklist + Level II."""
    toward = score if bull else -score
    return bool(kz) and toward >= APLUS_MIN_SCORE and bool(shark.get("entry_ready")) \
        and shark.get("direction") == ("bullish" if bull else "bearish")


def _signal_label(score: int, is_aplus: bool, bull: bool, kz) -> str:
    """Symmetric tiers: A+ / Strong / plain Buy-Sell / Neutral, WAIT outside a kill zone."""
    if not kz:
        return "WAIT"
    side = "Buy" if bull else "Sell"
    toward = score if bull else -score
    if is_aplus:
        return f"A+ {side}"
    if toward >= 40:
        return f"Strong {side}"
    if toward >= 20:
        return side
    return "Neutral"


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
    import btmm_rules
    e13_arr, e50_arr = calc_ema(closes, 13), calc_ema(closes, 50)
    unit      = btmm_rules.pip_unit(symbol, closes[-1], adr)
    tdi_s     = btmm_rules.tdi_series(closes)
    level     = detect_level_count(bars, closes)
    mm_levels = btmm_rules.detect_mm_levels(bars, e13_arr, e50_arr, calc_ema(closes, 200),
                                            calc_ema(closes, 800))
    mw        = btmm_rules.confirm_mw(bars, detect_mw_pattern(bars), e13_arr, tdi_s)
    shark     = btmm_rules.detect_shark_fin(bars, tdi_s, asian, unit,
                                            bias_bullish=closes[-1] > stack["e800"])
    straight  = detect_straightaway(bars)
    hod_lod   = detect_hod_lod(bars, symbol=symbol)
    half_bat  = btmm_rules.detect_half_batman(bars, e13_arr, e50_arr, tdi_s, unit)
    tdi_div   = detect_tdi_divergence(bars)
    push_hunt = detect_3push_stop_hunt(bars)

    # Trade direction = the 5/13 EMA relationship, the same rule the dashboard
    # tab uses (audit 2026-10-15: this used price vs the 800 EMA, so the tab
    # and Discord could disagree on the same pair). Price vs 800 is checklist
    # item 1's job and the confidence 'bias' vote.
    setup_bullish = stack["e5"] > stack["e13"]

    checklist = _btmm_checklist(
        setup_bullish, stack=stack, tdi=tdi, kz=kz, adr_c=adr_c, amd=amd, asian=asian,
        push_hunt=push_hunt, hunt=hunt, tdi_leg=tdi_leg, level=level, mw=mw, shark=shark,
        hod_lod=hod_lod, half_bat=half_bat, tdi_div=tdi_div)
    checklist_score = sum(checklist)
    score = _composite_score(stack["score"], _weighted_checklist(checklist), setup_bullish)

    is_aplus = _is_aplus(shark, setup_bullish, kz, score)
    signal = _signal_label(score, is_aplus, setup_bullish, kz)

    # ── Trade plan (course: stop 3-5 pips beyond the shift candle / HOD-LOD,
    # R:R >= 1:1, break-even at 1:1, exit on a 13 EMA close) ──────────────────
    direction = "bullish" if setup_bullish else "bearish"
    entry = closes[-1]
    if half_bat.get("detected") and half_bat.get("direction") == direction:
        anchor, anchor_kind = half_bat["shift_extreme"], f"Half Batman Type {half_bat['type']} shift candle"
    elif shark.get("entry_ready") and shark.get("direction") == direction:
        anchor, anchor_kind = shark["spike_extreme"], "Shark Fin spike (stop hunt)"
    elif hod_lod.get("at_lod" if setup_bullish else "at_hod"):
        anchor, anchor_kind = hod_lod["lod" if setup_bullish else "hod"], "LOD" if setup_bullish else "HOD"
    else:
        last3 = bars[-3:]
        anchor = min(b["low"] for b in last3) if setup_bullish else max(b["high"] for b in last3)
        anchor_kind = "shift candle (last 3 bars)"
    plan = btmm_rules.trade_plan(entry, direction, anchor, unit, adr, stack["e200"],
                                 exit_13=not anchor_kind.startswith("Shark Fin"))
    plan["anchor"] = anchor_kind
    sl, tp1, tp2 = plan["sl"], plan["tp1"], plan["tp2"]

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
        candidates = []

        # Safety Trade / 22 Trade / 50-50 Bounce / Three-Drive — real detector ports (see above).
        h1mtf = resample_to_h1(bars)
        nameable = detect_nameable_candle(bars)
        for setup_key, res in (
            ("safety",     detect_safety_trade(bars, stack, asian, tdi_leg, level, mw, nameable, h1mtf, kz, symbol)),
            ("trade22",    detect_22_trade(bars, asian, hunt, hod_lod, tdi, symbol)),
            ("bounce5050", detect_fifty_fifty_bounce(bars, stack, level, tdi_leg, h1mtf, tdi, symbol)),
            ("threeDrive", detect_three_drive_pattern(bars, shark, tdi_div, mw, symbol)),
        ):
            if res["active"]:
                candidates.append({
                    "key":         setup_key,
                    "direction":   res["direction"],
                    "gatesPassed": res["gatesPassed"],
                    "gatesTotal":  res["gatesTotal"],
                    "entry":       res["entry"],
                    "sl":          res["sl"],
                    "tp1":         res["tp1"],
                    "tp2":         res["tp2"],
                    "confidence":  res["confidence"],
                })

        # Strongest active candidate wins — same priority rule as the frontend's
        # sort-by-gatesPassed across the four named setups.
        if candidates:
            active_setup = max(candidates, key=lambda c: c["gatesPassed"])

    cross_513 = detect_513_cross(closes)

    # ── Confidence classification ────────────────────────────────────────────
    # Count how many DIRECTIONAL signals agree. Only genuinely directional
    # factors vote — ADR consumption, AMD phase and kill-zone are timing/quality
    # gates, not bull/bear signals, and previously biased the count: kz_factor
    # could only ever vote bullish, so a healthy bearish setup could never reach
    # "high" confidence (it needed ADR 80-100% consumed — the worst entry).
    btmm_factors = _btmm_direction_factors(stack, tdi, setup_bullish, mw, tdi_div)
    bullish_count = sum(1 for f in btmm_factors if f > 0)
    bearish_count = sum(1 for f in btmm_factors if f < 0)
    confidence = _classify_confidence(bullish_count, bearish_count)

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
        # Course rules (phase 2/3, btmm_rules)
        "mm_levels":       mm_levels,
        "trade_plan":      plan,
        "session":         btmm_rules.session(bars[-1]["ts_utc"]),
        "cycle":           btmm_rules.cycle_phase(bars[-1]["ts_utc"]),
    }
