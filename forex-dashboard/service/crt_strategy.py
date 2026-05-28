"""1AM CRT (Candle Range Theory) strategy analyzer.

Source: MADO / @Im-speculator — "How to Trade the 1AM CRT".

Pipeline per pair (NY-time anchored, DST-aware):
    1. DOL bias from nearest unraided liquidity pool (swing highs/lows, equal H/L).
    2. CRT range = max/min of the 5PM (CBDR) and 9PM (Asia) NY-aligned H4 candles.
    3. 1AM NY-aligned H4 candle facts (open/high/low/close, direction).
    4. OHLC pattern of the 1AM candle, derived from its M15 sub-candles:
         - OLHC (low first, then high) ⇒ bullish reversal ⇒ BUY below 1AM open.
         - OHLC (high first, then low) ⇒ bearish reversal ⇒ SELL above 1AM open.
    5. Market Profile classification (consolidation / manipulation / expansion):
       Type 1 = 5PM+9PM both consolidate, 1AM expands (5AM continues expansion).
       Type 2 = 5PM consol, 9PM manipulation, 1AM expansion.
    6. SMT (EUR/USD ↔ GBP/USD only): divergence on the 1AM candle highs/lows.
    7. Key-time status (London Open 02-03, Silver Bullet 03-04 NY).
    8. Intraday Profile detection (Normal Protraction, London Lunch Reversal, NY Cont).
    9. M15 Order Block detection for entry refinement.
   10. Confluence score → A/B/C/NO-TRADE grade.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from crt_utils import (
    NY,
    atr_h4,
    bucket_brief,
    bucket_start_ny,
    build_h4_buckets,
    classify_candle,
    detect_dol,
    detect_intraday_profile,
    detect_order_blocks,
    detect_smt,
    key_time_status_split,
    ny_dt,
    ohlc_pattern,
)

CRT_UNIVERSE = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD",
    "USD/CAD", "NZD/USD", "EUR/GBP", "EUR/JPY", "EUR/CHF",
    "EUR/AUD", "GBP/JPY", "GBP/CHF", "AUD/JPY", "CAD/JPY",
    "XAU/USD", "XAG/USD",
]

NY_HOUR_CBDR = 17
NY_HOUR_ASIA = 21
NY_HOUR_1AM = 1
NY_HOUR_5AM = 5

# Key time windows for 1AM CRT (offsets from 1AM anchor in hours)
KEY_TIME_WINDOWS_1AM = [
    {"name": "London Open", "start_h": 1, "end_h": 2},      # 2-3 AM NY
    {"name": "Silver Bullet", "start_h": 2, "end_h": 3},    # 3-4 AM NY
]

M15_PER_H4 = 16


def session_anchors_now(now_ny: Optional[datetime] = None) -> dict:
    if now_ny is None:
        now_ny = datetime.now(NY)
    target_1am = now_ny.replace(hour=NY_HOUR_1AM, minute=0, second=0, microsecond=0)
    if now_ny < target_1am:
        target_1am -= timedelta(days=1)
    return {
        "cbdr": target_1am - timedelta(hours=8),
        "asia": target_1am - timedelta(hours=4),
        "crt_1am": target_1am,
    }


def latest_populated_session(
    buckets: dict[datetime, dict],
    now_ny: Optional[datetime] = None,
    max_lookback_days: int = 7,
) -> dict:
    if now_ny is None:
        now_ny = datetime.now(NY)
    target_1am = now_ny.replace(hour=NY_HOUR_1AM, minute=0, second=0, microsecond=0)
    if now_ny < target_1am:
        target_1am -= timedelta(days=1)
    wallclock_1am = target_1am
    for _ in range(max_lookback_days + 1):
        if target_1am in buckets:
            return {
                "cbdr": target_1am - timedelta(hours=8),
                "asia": target_1am - timedelta(hours=4),
                "crt_1am": target_1am,
                "is_live": target_1am == wallclock_1am,
            }
        target_1am -= timedelta(days=1)
    return {
        "cbdr": wallclock_1am - timedelta(hours=8),
        "asia": wallclock_1am - timedelta(hours=4),
        "crt_1am": wallclock_1am,
        "is_live": True,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Per-pair analysis
# ──────────────────────────────────────────────────────────────────────────────

def analyze_pair(
    symbol: str,
    daily_candles: list[dict],
    m15_candles: list[dict],
    smt_partner_buckets: Optional[dict] = None,
    now_ny: Optional[datetime] = None,
) -> dict:
    """Run the 1AM CRT pipeline for one pair."""
    if now_ny is None:
        now_ny = datetime.now(NY)

    if not m15_candles or len(m15_candles) < 32:
        return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-DATA",
                "reason": "insufficient M15 candles", "score": 0,
                "dol_bias": None, "candle_1am": None,
                "provisional": False, "m15_count": len(m15_candles) if m15_candles else 0}

    buckets = build_h4_buckets(m15_candles)
    anchors = latest_populated_session(buckets, now_ny)
    b_cbdr = buckets.get(anchors["cbdr"])
    b_asia = buckets.get(anchors["asia"])
    b_1am = buckets.get(anchors["crt_1am"])
    b_5am = buckets.get(anchors["crt_1am"] + timedelta(hours=4))

    # CRT range
    if b_cbdr and b_asia:
        crt_high = max(b_cbdr["high"], b_asia["high"])
        crt_low = min(b_cbdr["low"], b_asia["low"])
    elif b_cbdr:
        crt_high, crt_low = b_cbdr["high"], b_cbdr["low"]
    elif b_asia:
        crt_high, crt_low = b_asia["high"], b_asia["low"]
    else:
        crt_high = crt_low = None

    # DOL — true liquidity draw (nearest unraided swing/equal H-L)
    current_price = b_1am["close"] if b_1am else (m15_candles[-1]["close"] if m15_candles else 0)
    dol = detect_dol(
        buckets, anchors["crt_1am"], current_price,
        session_high=crt_high, session_low=crt_low,
    )
    dol_bias = dol["bias"]

    # ATR
    atr_ref = anchors["crt_1am"] if b_1am else anchors["cbdr"]
    atr = atr_h4(buckets, atr_ref)

    # Profile classification per candle
    cbdr_type = classify_candle(b_cbdr, None, atr) if b_cbdr else "missing"
    asia_type = classify_candle(b_asia, b_cbdr, atr) if b_asia else "missing"
    am1_type = classify_candle(b_1am, b_asia, atr) if b_1am else "missing"
    am5_type = classify_candle(b_5am, b_1am, atr) if b_5am else "missing"

    # Market Profile type — Type 1 now considers 5AM expansion continuation
    if cbdr_type == "consolidation" and asia_type == "consolidation" and am1_type == "expansion":
        profile_type = "TYPE_1"
        if am5_type == "expansion":
            profile_label = "5PM & 9PM consol → 1AM expansion → 5AM continuation"
        else:
            profile_label = "5PM & 9PM consol → 1AM expansion"
    elif cbdr_type == "consolidation" and asia_type == "manipulation" and am1_type == "expansion":
        profile_type = "TYPE_2"
        profile_label = "5PM consol / 9PM manip / 1AM expansion"
    elif am1_type == "expansion":
        profile_type = "EXPANSION-OTHER"
        profile_label = f"5PM {cbdr_type} / 9PM {asia_type} / 1AM expansion"
    else:
        profile_type = "NO-EXPANSION"
        profile_label = f"5PM {cbdr_type} / 9PM {asia_type} / 1AM {am1_type}"

    # 1AM candle facts + OHLC pattern
    candle_1am = None
    ohlc = "unknown"
    setup = "NO-TRADE"
    entry_zone = None
    m15_count = len(b_1am["m15s"]) if b_1am else 0
    provisional = anchors["is_live"] and b_1am is not None and m15_count < M15_PER_H4

    if b_1am:
        c = b_1am
        candle_1am = {
            "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
            "direction": "bullish" if c["close"] > c["open"] else "bearish" if c["close"] < c["open"] else "doji",
        }
        ohlc = ohlc_pattern(b_1am)
        if ohlc == "OLHC":
            setup = "BUY"
            entry_zone = {"side": "below_open", "level": c["open"]}
        elif ohlc == "OHLC":
            setup = "SELL"
            entry_zone = {"side": "above_open", "level": c["open"]}

    # SMT
    smt = "NONE"
    if b_1am and b_asia and smt_partner_buckets:
        smt = detect_smt(
            b_1am, b_asia,
            smt_partner_buckets.get("1am"),
            smt_partner_buckets.get("asia"),
        )

    # Key-time status (split into London Open + Silver Bullet)
    kt = key_time_status_split(now_ny, anchors["crt_1am"], KEY_TIME_WINDOWS_1AM)
    key_time_status = kt["status"]
    key_time_window_sast = kt["sast_display"]
    active_kt_window = kt["active_window"]

    # Intraday Profile
    intraday = detect_intraday_profile(buckets, anchors["crt_1am"], crt_high, crt_low)

    # Order Blocks (M15 within the 1AM bucket range)
    ob_candles = b_1am["m15s"] if b_1am else m15_candles[-32:]
    order_blocks = detect_order_blocks(ob_candles, atr)

    # Relevant OB for entry refinement
    entry_ob = None
    if order_blocks and entry_zone:
        for ob in order_blocks:
            if setup == "BUY" and ob["type"] == "bullish":
                entry_ob = ob
                break
            elif setup == "SELL" and ob["type"] == "bearish":
                entry_ob = ob
                break

    # Confluence scoring (max 12)
    score = 0
    notes = []
    if profile_type in ("TYPE_1", "TYPE_2"):
        score += 3
        notes.append(f"Profile {profile_type}")
    elif profile_type == "EXPANSION-OTHER":
        score += 1
        notes.append("1AM expansion (non-textbook profile)")
    if setup == "BUY" and dol_bias == "BULLISH":
        score += 2
        notes.append("OLHC aligns with DOL bullish")
    if setup == "SELL" and dol_bias == "BEARISH":
        score += 2
        notes.append("OHLC aligns with DOL bearish")
    if setup == "BUY" and crt_low is not None and b_1am and b_1am["low"] < crt_low:
        score += 1
        notes.append("Swept CRT low")
    if setup == "SELL" and crt_high is not None and b_1am and b_1am["high"] > crt_high:
        score += 1
        notes.append("Swept CRT high")
    if smt in ("BULLISH-DIVERGENCE", "BEARISH-DIVERGENCE"):
        score += 2
        notes.append(f"SMT {smt}")
    elif smt.endswith("-PARTNER"):
        score += 1
        notes.append(f"SMT (partner-led) {smt}")
    if key_time_status == "ACTIVE":
        score += 2
        notes.append(f"In key-time: {active_kt_window}")
    elif key_time_status == "WAITING":
        score += 1
    if entry_ob:
        score += 1
        notes.append(f"M15 OB ({entry_ob['type']})")
    if intraday["profile"] in ("normal_protraction", "london_lunch_reversal"):
        score += 1
        notes.append(f"Intraday: {intraday['label']}")

    if score >= 9:
        grade = "A"
    elif score >= 6:
        grade = "B"
    elif score >= 3:
        grade = "C"
    else:
        grade = "NO-TRADE"

    sast = ZoneInfo("Africa/Johannesburg")
    return {
        "symbol": symbol,
        "dol_bias": dol_bias,
        "dol_target": dol.get("target"),
        "dol_type": dol.get("dol_type"),
        "crt_high": crt_high,
        "crt_low": crt_low,
        "candle_5pm": bucket_brief(b_cbdr, cbdr_type),
        "candle_9pm": bucket_brief(b_asia, asia_type),
        "candle_1am": candle_1am and {**candle_1am, "type": am1_type},
        "candle_5am": bucket_brief(b_5am, am5_type),
        "profile_type": profile_type,
        "profile_label": profile_label,
        "intraday_profile": intraday,
        "ohlc_pattern": ohlc,
        "setup": setup,
        "entry_zone": entry_zone,
        "order_blocks": order_blocks[:2],
        "entry_ob": entry_ob,
        "smt": smt,
        "key_time_status": key_time_status,
        "key_time_window_sast": key_time_window_sast,
        "key_time_active_window": active_kt_window,
        "key_time_windows": kt["windows"],
        "session_1am_sast": anchors["crt_1am"].astimezone(sast).strftime("%Y-%m-%d %H:%M"),
        "session_is_live": anchors["is_live"],
        "provisional": provisional,
        "m15_count": m15_count,
        "score": score,
        "grade": grade,
        "notes": "; ".join(notes) if notes else "No confluence",
    }


# ──────────────────────────────────────────────────────────────────────────────
# Universe-level entry point
# ──────────────────────────────────────────────────────────────────────────────

def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    """Run analyze_pair for every pair. `candles_by_pair[sym]` = {'1d', 'm15'}."""
    now_ny = datetime.now(NY)

    smt_partners = {"EUR/USD": "GBP/USD", "GBP/USD": "EUR/USD"}
    partner_buckets_by_sym: dict[str, Optional[dict]] = {}
    session_used = None
    for sym in ("EUR/USD", "GBP/USD"):
        m15 = candles_by_pair.get(sym, {}).get("m15", [])
        if m15:
            buckets = build_h4_buckets(m15)
            sess = latest_populated_session(buckets, now_ny)
            partner_buckets_by_sym[sym] = {
                "1am": buckets.get(sess["crt_1am"]),
                "asia": buckets.get(sess["asia"]),
            }
            if session_used is None:
                session_used = sess
        else:
            partner_buckets_by_sym[sym] = None
    if session_used is None:
        session_used = session_anchors_now(now_ny) | {"is_live": True}
    anchors = session_used

    pairs_out = []
    for sym in CRT_UNIVERSE:
        bundles = candles_by_pair.get(sym, {})
        partner = smt_partners.get(sym)
        partner_bk = partner_buckets_by_sym.get(partner) if partner else None
        pairs_out.append(
            analyze_pair(
                symbol=sym,
                daily_candles=bundles.get("1d", []),
                m15_candles=bundles.get("m15", []),
                smt_partner_buckets=partner_bk,
                now_ny=now_ny,
            )
        )

    buys = sum(1 for p in pairs_out if p["setup"] == "BUY")
    sells = sum(1 for p in pairs_out if p["setup"] == "SELL")
    grade_a = sum(1 for p in pairs_out if p["grade"] == "A")
    grade_b = sum(1 for p in pairs_out if p["grade"] == "B")

    sast = ZoneInfo("Africa/Johannesburg")
    return {
        "universe": CRT_UNIVERSE,
        "now_ny": now_ny.strftime("%Y-%m-%d %H:%M %Z"),
        "anchors_ny": {
            "cbdr": anchors["cbdr"].strftime("%Y-%m-%d %H:%M %Z"),
            "asia": anchors["asia"].strftime("%Y-%m-%d %H:%M %Z"),
            "crt_1am": anchors["crt_1am"].strftime("%Y-%m-%d %H:%M %Z"),
        },
        "anchors_sast": {
            "cbdr": anchors["cbdr"].astimezone(sast).strftime("%Y-%m-%d %H:%M"),
            "asia": anchors["asia"].astimezone(sast).strftime("%Y-%m-%d %H:%M"),
            "crt_1am": anchors["crt_1am"].astimezone(sast).strftime("%Y-%m-%d %H:%M"),
        },
        "buys": buys,
        "sells": sells,
        "grade_a": grade_a,
        "grade_b": grade_b,
        "pairs": pairs_out,
    }
