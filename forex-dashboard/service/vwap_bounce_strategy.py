"""VWAP-bounce scanner (M15) — trend-continuation entries off the session VWAP.

The user's discretionary method, written down as rules (2026-10):

  Alignment (all three, or there is no trade)
    1. the 50-bar MVWAP (moving average of the VWAP) is sloping, not flat —
       slope is measured in ATRs per 10 bars so it means the same on every pair
    2. the session VWAP sits on the trend side of the MVWAP
    3. price is trading on the trend side of the VWAP

  Trigger (either)
    * reject  — price pulls back, tags the VWAP and closes back on the trend
                side with a candle in the trend direction
    * reclaim — price closes through the VWAP, HOLDS the MVWAP, then closes
                back across the VWAP in the trend direction

  Confirmation
    * the trigger candle has relative volume >= 1.5 and a body >= 55% of its
      range (the Volume Strength Compass "good volume" candle). Without it
      the setup can never be grade A.

This is the mirror image of vwap_mean_reversion_strategy.py, which FADES
moves away from the VWAP; this module trades WITH the trend from it.

UNVALIDATED: these thresholds come from chart review, not from a backtest.
Every row carries `"validated": False`, and Discord alerts are off by default
(VWAP_BOUNCE_ALERTS_ENABLED). Forex "volume" here is MT5 tick volume.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import market_hours
from config import PRIORITY_PAIRS
from tdi_cycle_123 import _atr

VWAP_BOUNCE_UNIVERSE = list(PRIORITY_PAIRS)

# Four UTC days of M15 bars: covers the 50-bar MVWAP, the 20-bar volume
# baseline and the 40-bar swing lookback right after a weekend. Single source
# of truth for app.py and scheduler.py.
M15_FETCH_LIMIT = 400

MVWAP_LEN = 50               # "50 MVWAP" — SMA of the VWAP line
SLOPE_LOOKBACK = 10          # bars over which the MVWAP slope is measured
SLOPE_FLAT_ATR = 0.2         # |slope| below this (ATRs per 10 bars) = flat
SLOPE_STRONG_ATR = 0.5       # at or above this = strong
TOUCH_TOL_ATR = 0.10         # how close a wick must come to count as tagging the VWAP
MVWAP_HOLD_TOL_ATR = 0.25    # how far a wick may poke through the MVWAP and still "hold"
RECLAIM_MAX_BARS = 12        # a break older than this is a failed trend, not a flush
RVOL_LEN = 20
RVOL_CONFIRM = 1.5
BODY_CONFIRM = 0.55
RVOL_QUIET = 1.0             # pullback bars averaging below this are "quiet"
WATCH_DIST_ATR = 0.5         # aligned and within this of the VWAP = WATCH
SL_BUFFER_ATR = 0.25
SL_MIN_ATR = 0.5
SWING_LOOKBACK = 40
TP_FALLBACK_RR = 1.5
MIN_BARS = MVWAP_LEN + SLOPE_LOOKBACK + 5
STALE_AFTER_SECS = 2 * 900 + 600   # newest bar opened more than 40 minutes ago
SESSION_START_UTC = 7        # London open to the end of the NY afternoon
SESSION_END_UTC = 20

GRADE_A_SCORE = 8
GRADE_B_SCORE = 5


# ── indicators ───────────────────────────────────────────────────────────────

def vwap_series(bars: list[dict]) -> list[float]:
    """Session VWAP for every bar, re-anchored at each 00:00 SAST (22:00 UTC,
    session_clock: the user's TradingView session VWAP; was 00:00 UTC until
    2026-10-21). Typical price (H+L+C)/3, weighted by tick volume; a bar
    with no volume counts as 1 so a volumeless feed still gives a line."""
    import session_clock
    out: list[float] = []
    day = None
    cum_pv = cum_v = 0.0
    for b in bars:
        d = session_clock.vwap_day_start(b["ts_utc"])
        if d != day:
            day, cum_pv, cum_v = d, 0.0, 0.0
        vol = b.get("volume") or 0.0
        if vol <= 0:
            vol = 1.0
        cum_pv += (b["high"] + b["low"] + b["close"]) / 3.0 * vol
        cum_v += vol
        out.append(cum_pv / cum_v)
    return out


def sma_series(values: list[float], n: int) -> list[Optional[float]]:
    out: list[Optional[float]] = []
    run = 0.0
    for i, v in enumerate(values):
        run += v
        if i >= n:
            run -= values[i - n]
        out.append(run / n if i >= n - 1 else None)
    return out


def slope_label(slope_atr: float) -> str:
    mag = abs(slope_atr)
    if mag < SLOPE_FLAT_ATR:
        return "flat"
    side = "rising" if slope_atr > 0 else "falling"
    return f"strong_{side}" if mag >= SLOPE_STRONG_ATR else side


def rvol(volumes: list[float], i: int, n: int = RVOL_LEN) -> Optional[float]:
    """Volume of bar i relative to the average of the n bars before it.
    None when the feed carries no volume."""
    if i < n:
        return None
    base = sum(volumes[i - n:i]) / n
    if base <= 0:
        return None
    return volumes[i] / base


def body_pct(bar: dict) -> float:
    rng = bar["high"] - bar["low"]
    if rng <= 0:
        return 0.0
    return abs(bar["close"] - bar["open"]) / rng


def _in_session(ts_utc: int) -> bool:
    hour = datetime.fromtimestamp(ts_utc, tz=timezone.utc).hour
    return SESSION_START_UTC <= hour < SESSION_END_UTC


# ── scanner ──────────────────────────────────────────────────────────────────

def analyze_pair(symbol: str, m15_candles: list[dict]) -> dict:
    """Evaluate the newest bar. The candle cache only holds CLOSED bars (the
    providers drop the still-forming one), so the newest bar is the candle
    that just closed and its close is the price a trader can act on."""
    out: dict = {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE", "score": 0,
                 "state": "NO-DATA", "trigger": None, "validated": False}
    bars = m15_candles or []
    n = len(bars)
    if n < MIN_BARS:
        out["notes"] = f"Insufficient data ({n} M15 bars, need >= {MIN_BARS})."
        return out

    i = n - 1
    vw = vwap_series(bars)
    mv = sma_series(vw, MVWAP_LEN)
    atr = _atr(bars)
    if not atr or mv[i] is None or mv[i - SLOPE_LOOKBACK] is None:
        out["notes"] = "Indicators not ready."
        return out

    slope = (mv[i] - mv[i - SLOPE_LOOKBACK]) / atr
    label = slope_label(slope)
    sig = bars[i]
    close = sig["close"]
    in_session = _in_session(sig["ts_utc"] + 900)      # the moment the bar closed
    out.update({
        "vwap": round(vw[i], 6), "mvwap": round(mv[i], 6),
        "mvwap_slope_atr": round(slope, 2), "slope_label": label,
        "current_price": close, "in_active_session": in_session,
        "signal_ts": sig["ts_utc"],
        "atr": atr,
    })

    if label != "flat" and slope > 0 and vw[i] > mv[i]:
        s = 1
    elif label != "flat" and slope < 0 and vw[i] < mv[i]:
        s = -1
    else:
        out["state"] = "NOT-ALIGNED"
        out["notes"] = ("MVWAP is flat." if label == "flat"
                        else "VWAP is on the wrong side of the MVWAP for this slope.")
        return out
    out["direction"] = "bullish" if s == 1 else "bearish"

    def side(price: float, ref: float) -> float:
        """Positive when `price` is on the trend side of `ref`."""
        return s * (price - ref)

    def wick(b: dict) -> float:
        """The extreme a pullback makes: the low in an uptrend, the high in a downtrend."""
        return b["low"] if s == 1 else b["high"]

    volumes = [b.get("volume") or 0.0 for b in bars]
    rv = rvol(volumes, i)
    body = body_pct(sig)
    vol_ok = rv is not None and rv >= RVOL_CONFIRM and body >= BODY_CONFIRM
    out.update({"rvol": round(rv, 2) if rv is not None else None,
                "body_pct": round(body, 2), "vol_confirmed": vol_ok})

    closed_on_side = side(close, vw[i]) > 0
    trend_candle = side(close, sig["open"]) > 0
    prev_on_side = side(bars[i - 1]["close"], vw[i - 1]) > 0

    trigger = None
    pull_start = i - 3
    if closed_on_side and trend_candle and side(close, mv[i]) > 0:
        if prev_on_side:
            if side(wick(sig), vw[i]) <= TOUCH_TOL_ATR * atr:
                trigger = "reject"
        else:
            # Walk back to the last bar that closed on the trend side: the
            # flush started on the bar after it.
            k = i - 1
            while k >= i - RECLAIM_MAX_BARS and side(bars[k]["close"], vw[k]) <= 0:
                k -= 1
            if k >= i - RECLAIM_MAX_BARS:
                held = all(mv[b] is not None
                           and side(bars[b]["close"], mv[b]) > 0
                           and side(wick(bars[b]), mv[b]) > -MVWAP_HOLD_TOL_ATR * atr
                           for b in range(k + 1, i + 1))
                if held:
                    trigger = "reclaim"
                    pull_start = k + 1

    if trigger is None:
        near = closed_on_side and side(close, vw[i]) <= WATCH_DIST_ATR * atr
        out["state"] = "WATCH" if near else "ALIGNED"
        if near:
            out["notes"] = "Aligned and sitting on the VWAP - waiting for a rejection candle."
        elif closed_on_side:
            out["notes"] = "Aligned; price is away from the VWAP - wait for a pullback."
        else:
            out["notes"] = "VWAP/MVWAP aligned but price is through the VWAP - wait for a reclaim."
        return out

    pull = range(max(0, pull_start), i)
    pull_rvols = [r for r in (rvol(volumes, b) for b in pull) if r is not None]
    quiet = bool(pull_rvols) and sum(pull_rvols) / len(pull_rvols) < RVOL_QUIET

    strong = label.startswith("strong")
    score = (2 if strong else 1) + 3 + (3 if vol_ok else 0) + (1 if quiet else 0) + (1 if in_session else 0)
    if score >= GRADE_A_SCORE and vol_ok:
        grade = "A"
    elif score >= GRADE_B_SCORE:
        grade = "B"
    else:
        grade = "C"

    entry = close
    ext = wick(sig)
    for b in pull:
        ext = min(ext, wick(bars[b])) if s == 1 else max(ext, wick(bars[b]))
    sl = ext - s * SL_BUFFER_ATR * atr
    if side(entry, sl) < SL_MIN_ATR * atr:
        sl = entry - s * SL_MIN_ATR * atr
    risk = abs(entry - sl)
    if risk <= 0 or side(entry, sl) <= 0:
        out["state"] = "ALIGNED"
        out["notes"] = "Trigger seen but the stop is on the wrong side of entry - skipped."
        return out

    prior = bars[max(0, i - SWING_LOOKBACK):i]
    swing = max(b["high"] for b in prior) if s == 1 else min(b["low"] for b in prior)
    if side(swing, entry) >= risk:
        tp1, tp_basis = swing, "prior swing"
    else:
        tp1, tp_basis = entry + s * TP_FALLBACK_RR * risk, f"{TP_FALLBACK_RR}R"

    notes = [f"{'Rejection at' if trigger == 'reject' else 'Reclaim of'} the VWAP",
             f"MVWAP {label.replace('_', ' ')} ({slope:+.2f} ATR/10 bars)"]
    if vol_ok:
        notes.append(f"volume confirmed (RVOL {rv:.2f}, body {body:.0%})")
    elif rv is None:
        notes.append("no volume data - cannot confirm")
    else:
        notes.append(f"NO volume confirmation (RVOL {rv:.2f}, body {body:.0%})")
    if quiet:
        notes.append("quiet pullback")
    if not in_session:
        notes.append("outside London/NY hours")
    notes.append(f"target: {tp_basis}")
    notes.append("UNVALIDATED - not backtested")

    out.update({
        "setup": "BUY" if s == 1 else "SELL", "grade": grade, "score": score,
        "state": "SIGNAL", "trigger": trigger, "quiet_pullback": quiet,
        "entry": entry, "sl": sl, "tp1": tp1, "rr1": round(abs(tp1 - entry) / risk, 2),
        "notes": "; ".join(notes),
    })
    return out


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    pairs = []
    for symbol, tf in candles_by_pair.items():
        try:
            pairs.append(analyze_pair(symbol, tf.get("m15") or []))
        except Exception as e:  # noqa: BLE001 — one bad series must not sink the scan
            pairs.append({"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE", "score": 0,
                          "state": "ERROR", "trigger": None, "validated": False,
                          "notes": f"analysis failed: {e}"})
    return {
        "pairs": pairs,
        "buys": sum(1 for p in pairs if p["setup"] == "BUY"),
        "sells": sum(1 for p in pairs if p["setup"] == "SELL"),
        "watch": sum(1 for p in pairs if p.get("state") == "WATCH"),
    }


def scan_cached(read_candles, now_ts: float) -> dict:
    """Run the scanner on cached M15 bars — no provider calls, so the dashboard
    tab loads in well under a second. `read_candles(symbol, interval, limit)`
    is cache.read_candles; the scheduler keeps that cache fresh every 15
    minutes. Pairs whose newest bar is too old are listed in `stale_pairs`."""
    candles = {sym: {"m15": read_candles(sym, "15min", limit=M15_FETCH_LIMIT)}
               for sym in VWAP_BOUNCE_UNIVERSE}
    result = analyze_universe(candles)
    stale = []
    for sym, tf in candles.items():
        bars = tf["m15"]
        if not market_hours.is_market_open(sym, now_ts):
            continue
        # The cache holds closed bars and is refreshed a few minutes after each
        # quarter-hour, so the newest bar's OPEN is normally 15-34 minutes old.
        if not bars or now_ts - bars[-1]["ts_utc"] > STALE_AFTER_SECS:
            stale.append(sym)
    result["stale_pairs"] = sorted(stale)
    return result
