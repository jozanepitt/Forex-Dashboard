"""Shared utilities for CRT (Candle Range Theory) strategies.

Provides NY-aligned H4 bucket synthesis, ATR calculation, SMT divergence,
DOL (Draw on Liquidity) detection, Order Block detection, and intraday profiles.
Used by both 1AM and 5AM CRT strategies.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

ATR_PERIOD = 20
EXPANSION_MULT = 1.5
CONSOLIDATION_MULT = 0.8


# ──────────────────────────────────────────────────────────────────────────────
# H4 synthesis from M15
# ──────────────────────────────────────────────────────────────────────────────

def ny_dt(ts_utc: int) -> datetime:
    return datetime.fromtimestamp(ts_utc, tz=timezone.utc).astimezone(NY)


def bucket_start_ny(dt_ny: datetime) -> datetime:
    """H4 bucket start in NY time, aligned at hours {1, 5, 9, 13, 17, 21}."""
    h = dt_ny.hour
    if h == 0:
        start = dt_ny.replace(hour=21, minute=0, second=0, microsecond=0) - timedelta(days=1)
    else:
        h0 = ((h - 1) // 4) * 4 + 1
        start = dt_ny.replace(hour=h0, minute=0, second=0, microsecond=0)
    return start


def build_h4_buckets(m15_candles: list[dict]) -> dict[datetime, dict]:
    """Group M15 candles into NY-aligned H4 buckets.

    Returns dict keyed by bucket-start NY datetime → {open, high, low, close, m15s}.
    """
    buckets: dict[datetime, dict] = {}
    for c in m15_candles:
        ts = c.get("ts_utc")
        if ts is None:
            continue
        dt = ny_dt(int(ts))
        start = bucket_start_ny(dt)
        b = buckets.get(start)
        if b is None:
            buckets[start] = {
                "start_ny": start,
                "open":  c["open"],
                "high":  c["high"],
                "low":   c["low"],
                "close": c["close"],
                "m15s":  [c],
            }
        else:
            if c["high"] > b["high"]: b["high"] = c["high"]
            if c["low"]  < b["low"]:  b["low"]  = c["low"]
            b["close"] = c["close"]
            b["m15s"].append(c)
    for b in buckets.values():
        b["m15s"].sort(key=lambda x: x["ts_utc"])
    return buckets


# ──────────────────────────────────────────────────────────────────────────────
# ATR
# ──────────────────────────────────────────────────────────────────────────────

def atr_h4(buckets_by_start: dict[datetime, dict], reference_start: datetime, period: int = ATR_PERIOD) -> Optional[float]:
    """Simple ATR over `period` H4 buckets strictly before `reference_start`."""
    prior = [b for s, b in buckets_by_start.items() if s < reference_start]
    prior.sort(key=lambda b: b["start_ny"])
    prior = prior[-period:]
    if len(prior) < max(5, period // 2):
        return None
    ranges = [(b["high"] - b["low"]) for b in prior]
    return sum(ranges) / len(ranges)


# ──────────────────────────────────────────────────────────────────────────────
# DOL (Draw on Liquidity)
# ──────────────────────────────────────────────────────────────────────────────

def _find_swing_highs_lows(buckets_by_start: dict[datetime, dict], reference_start: datetime, lookback: int = 30) -> dict:
    """Identify swing highs/lows and liquidity pools from H4 buckets."""
    prior = sorted(
        [(s, b) for s, b in buckets_by_start.items() if s < reference_start],
        key=lambda x: x[0],
    )
    prior = prior[-lookback:]
    if len(prior) < 5:
        return {"swing_highs": [], "swing_lows": [], "equal_highs": [], "equal_lows": []}

    swing_highs = []
    swing_lows = []
    for i in range(2, len(prior) - 2):
        _, b = prior[i]
        _, bl = prior[i - 1]
        _, br = prior[i + 1]
        _, bll = prior[i - 2]
        _, brr = prior[i + 2]
        if b["high"] > bl["high"] and b["high"] > br["high"] and b["high"] > bll["high"] and b["high"] > brr["high"]:
            swing_highs.append({"price": b["high"], "ts": prior[i][0], "raided": False})
        if b["low"] < bl["low"] and b["low"] < br["low"] and b["low"] < bll["low"] and b["low"] < brr["low"]:
            swing_lows.append({"price": b["low"], "ts": prior[i][0], "raided": False})

    # Check for equal highs/lows (liquidity pools) — within 0.1× ATR tolerance
    atr = atr_h4(buckets_by_start, reference_start)
    tolerance = (atr or 0.0005) * 0.1
    equal_highs = []
    equal_lows = []
    for i in range(len(swing_highs)):
        for j in range(i + 1, len(swing_highs)):
            if abs(swing_highs[i]["price"] - swing_highs[j]["price"]) < tolerance:
                equal_highs.append(max(swing_highs[i]["price"], swing_highs[j]["price"]))
                break
    for i in range(len(swing_lows)):
        for j in range(i + 1, len(swing_lows)):
            if abs(swing_lows[i]["price"] - swing_lows[j]["price"]) < tolerance:
                equal_lows.append(min(swing_lows[i]["price"], swing_lows[j]["price"]))
                break

    return {
        "swing_highs": swing_highs,
        "swing_lows": swing_lows,
        "equal_highs": equal_highs,
        "equal_lows": equal_lows,
    }


def detect_dol(
    buckets_by_start: dict[datetime, dict],
    reference_start: datetime,
    current_price: float,
    session_high: Optional[float] = None,
    session_low: Optional[float] = None,
) -> dict:
    """Detect Draw on Liquidity — nearest unraided liquidity pool.

    Returns: {bias, target_price, dol_type, confidence}
    """
    swings = _find_swing_highs_lows(buckets_by_start, reference_start)

    # Mark raided levels — any swing that has already been taken by recent price action
    recent = sorted(
        [(s, b) for s, b in buckets_by_start.items() if s >= reference_start - timedelta(hours=8) and s < reference_start],
        key=lambda x: x[0],
    )
    recent_high = max((b["high"] for _, b in recent), default=current_price)
    recent_low = min((b["low"] for _, b in recent), default=current_price)

    unraided_highs = []
    for sh in swings["swing_highs"]:
        if sh["price"] > recent_high and sh["price"] > current_price:
            unraided_highs.append(sh["price"])
    for eh in swings["equal_highs"]:
        if eh > recent_high and eh > current_price:
            unraided_highs.append(eh)

    unraided_lows = []
    for sl in swings["swing_lows"]:
        if sl["price"] < recent_low and sl["price"] < current_price:
            unraided_lows.append(sl["price"])
    for el in swings["equal_lows"]:
        if el < recent_low and el < current_price:
            unraided_lows.append(el)

    # Also consider previous session highs/lows as DOL targets
    if session_high is not None and session_high > current_price:
        unraided_highs.append(session_high)
    if session_low is not None and session_low < current_price:
        unraided_lows.append(session_low)

    # Nearest unraided pool determines bias
    nearest_high = min(unraided_highs, default=None)
    nearest_low = max(unraided_lows, default=None)

    if nearest_high is None and nearest_low is None:
        return {"bias": "NEUTRAL", "target": None, "dol_type": "none", "confidence": "low"}

    dist_high = (nearest_high - current_price) if nearest_high else float("inf")
    dist_low = (current_price - nearest_low) if nearest_low else float("inf")

    if dist_high <= dist_low:
        dol_type = "equal_highs" if nearest_high in swings.get("equal_highs", []) else "swing_high"
        confidence = "high" if nearest_high in swings.get("equal_highs", []) else "medium"
        return {"bias": "BULLISH", "target": nearest_high, "dol_type": dol_type, "confidence": confidence}
    else:
        dol_type = "equal_lows" if nearest_low in swings.get("equal_lows", []) else "swing_low"
        confidence = "high" if nearest_low in swings.get("equal_lows", []) else "medium"
        return {"bias": "BEARISH", "target": nearest_low, "dol_type": dol_type, "confidence": confidence}


# ──────────────────────────────────────────────────────────────────────────────
# SMT (Smart Money Technique) Divergence
# ──────────────────────────────────────────────────────────────────────────────

def detect_smt(
    self_1am: Optional[dict],
    self_asia: Optional[dict],
    partner_1am: Optional[dict],
    partner_asia: Optional[dict],
) -> str:
    """Geometric SMT divergence: one pair sweeps prior Asia high/low while the other fails."""
    if not (self_1am and self_asia and partner_1am and partner_asia):
        return "NONE"
    self_swept_high = self_1am["high"] > self_asia["high"]
    partner_swept_high = partner_1am["high"] > partner_asia["high"]
    self_swept_low = self_1am["low"] < self_asia["low"]
    partner_swept_low = partner_1am["low"] < partner_asia["low"]
    if self_swept_high != partner_swept_high:
        # A high sweep signals a bearish reversal regardless of which pair swept.
        return "BEARISH-DIVERGENCE" if self_swept_high else "BEARISH-DIVERGENCE-PARTNER"
    elif self_swept_low != partner_swept_low:
        # A low sweep signals a bullish reversal regardless of which pair swept.
        return "BULLISH-DIVERGENCE" if self_swept_low else "BULLISH-DIVERGENCE-PARTNER"
    return "NONE"


# ──────────────────────────────────────────────────────────────────────────────
# Candle Classification
# ──────────────────────────────────────────────────────────────────────────────

def classify_candle(bucket: dict, prior_bucket: Optional[dict], atr: Optional[float]) -> str:
    """Return 'consolidation' | 'manipulation' | 'expansion' | 'neutral'."""
    if not bucket:
        return "missing"
    rng = bucket["high"] - bucket["low"]
    body = abs(bucket["close"] - bucket["open"])

    swept_inside_close = False
    if prior_bucket:
        swept_high = bucket["high"] > prior_bucket["high"]
        swept_low = bucket["low"] < prior_bucket["low"]
        closes_inside = (prior_bucket["low"] <= bucket["close"] <= prior_bucket["high"])
        swept_inside_close = (swept_high or swept_low) and closes_inside

    if atr is None or atr <= 0:
        if rng == 0:
            return "consolidation"
        if swept_inside_close:
            return "manipulation"
        if body / rng >= 0.6:
            return "expansion"
        if body / rng <= 0.25:
            return "consolidation"
        return "neutral"

    if rng >= EXPANSION_MULT * atr and body / max(rng, 1e-12) >= 0.5:
        return "expansion"
    if swept_inside_close:
        return "manipulation"
    if rng <= CONSOLIDATION_MULT * atr:
        return "consolidation"
    return "neutral"


# ──────────────────────────────────────────────────────────────────────────────
# OHLC Pattern
# ──────────────────────────────────────────────────────────────────────────────

def ohlc_pattern(bucket: dict) -> str:
    """Determine OHLC vs OLHC from M15 sub-candles.

    OHLC = high before low (bearish reversal → sell).
    OLHC = low before high (bullish reversal → buy).
    """
    m15s = bucket.get("m15s") or []
    if not m15s:
        return "unknown"
    high = bucket["high"]
    low = bucket["low"]
    ts_high = ts_low = None
    for c in m15s:
        if c["high"] >= high and ts_high is None:
            ts_high = c["ts_utc"]
        if c["low"] <= low and ts_low is None:
            ts_low = c["ts_utc"]
    if ts_high is None or ts_low is None:
        return "unknown"
    if ts_high == ts_low:
        return "unknown"
    return "OHLC" if ts_high < ts_low else "OLHC"


# ──────────────────────────────────────────────────────────────────────────────
# Order Block Detection (M15)
# ──────────────────────────────────────────────────────────────────────────────

def detect_order_blocks(m15_candles: list[dict], atr: Optional[float] = None, max_obs: int = 3) -> list[dict]:
    """Find M15 Order Blocks — last opposing candle before a displacement move.

    Displacement = move > 1.5× average M15 range.
    Returns list of {type, high, low, ts_utc} sorted by recency (newest first).
    """
    if not m15_candles or len(m15_candles) < 5:
        return []

    # Average M15 range for displacement threshold
    ranges = [c["high"] - c["low"] for c in m15_candles[-40:] if c["high"] - c["low"] > 0]
    if not ranges:
        return []
    avg_range = sum(ranges) / len(ranges)
    disp_threshold = avg_range * 1.5

    obs = []
    for i in range(2, len(m15_candles)):
        curr = m15_candles[i]
        prev = m15_candles[i - 1]

        # Bullish displacement: current candle body significantly up
        curr_body = curr["close"] - curr["open"]
        if curr_body > disp_threshold:
            # Bullish OB = last down-close candle before this move
            if prev["close"] < prev["open"]:
                obs.append({
                    "type": "bullish",
                    "high": prev["high"],
                    "low": prev["low"],
                    "mid": (prev["high"] + prev["low"]) / 2,
                    "ts_utc": prev["ts_utc"],
                })
        # Bearish displacement: current candle body significantly down
        elif -curr_body > disp_threshold:
            if prev["close"] > prev["open"]:
                obs.append({
                    "type": "bearish",
                    "high": prev["high"],
                    "low": prev["low"],
                    "mid": (prev["high"] + prev["low"]) / 2,
                    "ts_utc": prev["ts_utc"],
                })

    # Return most recent OBs
    obs.reverse()
    return obs[:max_obs]


# ──────────────────────────────────────────────────────────────────────────────
# Intraday Profile Detection
# ──────────────────────────────────────────────────────────────────────────────

def detect_intraday_profile(
    buckets: dict[datetime, dict],
    session_anchor: datetime,
    crt_high: Optional[float],
    crt_low: Optional[float],
) -> dict:
    """Detect intraday profile type for the current session.

    Profiles (MADO spec):
    1. Normal Protraction — London makes high/low of day
    2. London Lunch Reversal — reversal during 5-7 AM NY
    3. NY Continuation — NY session continues London direction

    session_anchor = 1AM bucket start (for 1AM CRT) or 5AM bucket start (for 5AM CRT).
    """
    # For 1AM CRT: London = 1AM-5AM bucket, London Lunch = 5AM-9AM, NY = 9AM-13PM
    # For 5AM CRT: London = 5AM bucket, London Lunch = 9AM, NY = 9AM-13PM
    # We'll detect based on what data exists

    b_london = buckets.get(session_anchor)
    b_lunch = buckets.get(session_anchor + timedelta(hours=4))
    b_ny = buckets.get(session_anchor + timedelta(hours=8))

    if not b_london:
        return {"profile": "unknown", "label": "Insufficient data"}

    london_dir = "bullish" if b_london["close"] > b_london["open"] else "bearish"

    # Check if London swept CRT range (manipulation)
    london_swept_high = crt_high is not None and b_london["high"] > crt_high
    london_swept_low = crt_low is not None and b_london["low"] < crt_low

    if not b_lunch and not b_ny:
        # Only London data — can't fully classify yet
        if london_swept_high or london_swept_low:
            return {"profile": "normal_protraction", "label": "Normal Protraction (forming)", "direction": london_dir}
        return {"profile": "pending", "label": "Profile pending (London only)"}

    if b_lunch:
        lunch_dir = "bullish" if b_lunch["close"] > b_lunch["open"] else "bearish"
        # Reversal during lunch
        if lunch_dir != london_dir and (london_swept_high or london_swept_low):
            return {"profile": "london_lunch_reversal", "label": "London Lunch Reversal", "direction": lunch_dir}

    if b_ny:
        ny_dir = "bullish" if b_ny["close"] > b_ny["open"] else "bearish"
        if ny_dir == london_dir:
            return {"profile": "ny_continuation", "label": "NY Continuation", "direction": ny_dir}
        else:
            return {"profile": "ny_reversal", "label": "NY Reversal", "direction": ny_dir}

    if london_swept_high or london_swept_low:
        return {"profile": "normal_protraction", "label": "Normal Protraction", "direction": london_dir}

    return {"profile": "neutral", "label": f"London {london_dir}, awaiting confirmation"}


# ──────────────────────────────────────────────────────────────────────────────
# Key Time Status
# ──────────────────────────────────────────────────────────────────────────────

def key_time_status_split(now_ny: datetime, crt_anchor: datetime, windows: list[dict]) -> dict:
    """Multi-window key time status.

    `windows` = [{"name": "London Open", "start_h": 1, "end_h": 2}, ...]
    where start_h/end_h are offsets from crt_anchor in hours.
    """
    sast = ZoneInfo("Africa/Johannesburg")
    active_window = None
    all_windows = []

    for w in windows:
        w_start = crt_anchor + timedelta(hours=w["start_h"])
        w_end = crt_anchor + timedelta(hours=w["end_h"])
        sast_start = w_start.astimezone(sast).strftime("%H:%M")
        sast_end = w_end.astimezone(sast).strftime("%H:%M")
        status = "MISSED"
        if now_ny < w_start:
            status = "WAITING"
        elif w_start <= now_ny < w_end:
            status = "ACTIVE"
            active_window = w["name"]
        all_windows.append({
            "name": w["name"],
            "status": status,
            "sast": f"{sast_start}–{sast_end} SAST",
        })

    # Overall status
    if active_window:
        overall = "ACTIVE"
    elif any(w["status"] == "WAITING" for w in all_windows):
        overall = "WAITING"
    else:
        overall = "MISSED"

    first_start = crt_anchor + timedelta(hours=windows[0]["start_h"])
    last_end = crt_anchor + timedelta(hours=windows[-1]["end_h"])
    sast_str = f"{first_start.astimezone(sast).strftime('%H:%M')}–{last_end.astimezone(sast).strftime('%H:%M')} SAST"

    return {
        "status": overall,
        "active_window": active_window,
        "windows": all_windows,
        "sast_display": sast_str,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Bucket Helpers
# ──────────────────────────────────────────────────────────────────────────────

def bucket_brief(b: Optional[dict], cls: str) -> Optional[dict]:
    if not b:
        return None
    return {
        "open": b["open"], "high": b["high"], "low": b["low"], "close": b["close"],
        "type": cls,
    }
