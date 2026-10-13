"""Trend-following Engulfing scanner (H1 entries on the H4 trend, H4 entries
on the daily trend; every dashboard pair plus oil).

Rules, from the user's rule cards (2026-10-13):

  Pattern  two candles of opposite colour; the second's body completely covers
           the first's — a bigger candle swallowing a smaller one.
  Rule 1   follow the trend: buy bullish engulfings in an uptrend, sell
           bearish engulfings in a downtrend.
  Rule 2   take the trend from the higher timeframe and the entry from a
           lower one: H1 entries against the H4 trend, H4 entries against the
           daily trend (M15 was dropped at the user's request, 2026-10-13).
  Rule 3   stop at the wick of the engulfing candle.
  Rule 4   entry at the open of the next candle, or 25% / 50% back into the
           engulfing candle (measured on the whole candle, wick to wick). All
           three are reported; `entry` is the first.
  Rule 5   an engulfing AGAINST the trend is not traded until a second
           engulfing in the same direction confirms it.

The take-profit is not defined by the rules ("according to your plan"), so it
is a fixed multiple of the risk (TARGET_RR).

Replaced BTMM 123 on the dashboard (2026-10-13). NO EDGE SHOWN: the
walk-forward test over 27 pairs, gross of spread, gave for with-trend signals
entered at the next open with a 2R target: H1 11,328 signals, 32% wins,
-0.03R (M15, since dropped: 13,550 signals, 33% wins, -0.01R). No target multiple, grade,
session or candle size changed that. Every row carries `"validated": False`
and Discord alerts are on for grade A only (user decision, 2026-10-13).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import instruments
from config import PRIORITY_PAIRS
from tdi_cycle_123 import _atr, _htf_bias

# Every dashboard pair (forex, gold, silver, indices, the dollar index, crypto)
# plus the two oil contracts (user requests 2026-10-13). The oil symbols are
# not in PRIORITY_PAIRS, so the scheduler fetches their candles separately
# (EXTRA_SYMBOLS).
EXTRA_SYMBOLS = ["USOIL", "UKOIL"]
ENGULF_UNIVERSE = list(PRIORITY_PAIRS) + EXTRA_SYMBOLS

# Candles read per timeframe. Entries are H1 (trend from H4) and H4 (trend
# from D1); the trend read needs at least 60 bars of its timeframe.
H1_LIMIT = 300
H4_LIMIT = 300
D1_LIMIT = 120
MIN_BARS = 20
MIN_BODY_ATR = 0.25          # the engulfing candle's body must be at least this many ATRs
MIN_PREV_BODY_PCT = 0.10     # the engulfed candle needs a real body (not a doji)
OPEN_GAP_TOL_ATR = 0.05      # FX candles open on the previous close; allow a hair of gap
CONFIRM_LOOKBACK = 8         # Rule 5: the first counter-trend engulfing must be this recent
SL_BUFFER_ATR = 0.10         # room beyond the wick for spread
TARGET_RR = 2.0
SESSION_START_UTC = 7
SESSION_END_UTC = 20


def _body(bar: dict) -> float:
    return abs(bar["close"] - bar["open"])


def engulfing_kind(prev: dict, cur: dict, atr: Optional[float] = None) -> Optional[str]:
    """'bullish', 'bearish' or None for the candle pair (prev, cur)."""
    prev_rng = prev["high"] - prev["low"]
    if prev_rng <= 0 or _body(prev) < MIN_PREV_BODY_PCT * prev_rng:
        return None
    if _body(cur) <= _body(prev):
        return None
    tol = OPEN_GAP_TOL_ATR * atr if atr else 0.0
    prev_bull = prev["close"] > prev["open"]
    cur_bull = cur["close"] > cur["open"]
    if prev_bull == cur_bull:
        return None
    if cur_bull:
        # red then green: green opens at/below the red close and closes above the red open
        if cur["open"] <= prev["close"] + tol and cur["close"] > prev["open"]:
            return "bullish"
    else:
        if cur["open"] >= prev["close"] - tol and cur["close"] < prev["open"]:
            return "bearish"
    return None


def covers_wicks(prev: dict, cur: dict) -> bool:
    """True when the engulfing candle's whole range covers the first candle's
    whole range, wicks included (the strict textbook engulfing)."""
    return cur["high"] >= prev["high"] and cur["low"] <= prev["low"]


HTF_ENGULF_LOOKBACK = 3      # H4 candles (12 hours) in which the higher-timeframe engulfing must sit


def recent_htf_engulfing(h4_candles: Optional[list[dict]], kind: str) -> bool:
    """Rule 2, first half: did the higher timeframe print an engulfing of this
    kind within its last HTF_ENGULF_LOOKBACK closed candles?"""
    if not h4_candles or len(h4_candles) < MIN_BARS:
        return False
    atr = _atr(h4_candles) or 0.0
    n = len(h4_candles)
    for i in range(n - 1, n - 1 - HTF_ENGULF_LOOKBACK, -1):
        if engulfing_kind(h4_candles[i - 1], h4_candles[i], atr) == kind:
            return True
    return False


def _in_session(ts_utc: int) -> bool:
    hour = datetime.fromtimestamp(ts_utc, tz=timezone.utc).hour
    return SESSION_START_UTC <= hour < SESSION_END_UTC


def _earlier_engulfing(bars: list[dict], kind: str, atr: float) -> bool:
    """Rule 5, as the rule card draws it: the newest candle is the SECOND
    engulfing of this kind, close to the first.

    The first must sit within the last CONFIRM_LOOKBACK bars with no opposite
    engulfing since; price must not have traded back beyond the first one's
    wick (that would void it); and the new candle must still overlap the
    first one's range (otherwise the move has already run and this is a chase,
    not a confirmation)."""
    n = len(bars)
    cur = bars[-1]
    for i in range(n - 2, max(1, n - 2 - CONFIRM_LOOKBACK), -1):
        k = engulfing_kind(bars[i - 1], bars[i], atr)
        if k is None:
            continue
        if k != kind:
            return False
        first = bars[i]
        between = bars[i + 1:]
        if kind == "bearish":
            voided = any(b["high"] > first["high"] for b in between)
            still_there = cur["high"] >= first["low"]
        else:
            voided = any(b["low"] < first["low"] for b in between)
            still_there = cur["low"] <= first["high"]
        return (not voided) and still_there
    return False


def analyze_timeframe(symbol: str, bars: list[dict], h4_candles: Optional[list[dict]],
                      timeframe: str, htf_label: str = "H4") -> dict:
    """Judge the newest closed candle of `bars` against the trend of the next
    timeframe up (`h4_candles`, named by `htf_label`)."""
    out: dict = {"symbol": symbol, "timeframe": timeframe, "setup": "NO-TRADE", "grade": "NO-TRADE",
                 "score": 0, "state": "NO-DATA", "pattern": None, "trend": None, "validated": False}
    if not bars or len(bars) < MIN_BARS:
        out["notes"] = f"Insufficient data ({len(bars or [])} bars, need >= {MIN_BARS})."
        return out

    prev, cur = bars[-2], bars[-1]
    atr = _atr(bars) or (cur["high"] - cur["low"]) or 0.0
    trend = _htf_bias(h4_candles)
    out.update({"trend": trend, "current_price": cur["close"], "signal_ts": cur["ts_utc"],
                "in_active_session": _in_session(cur["ts_utc"] + (bars[-1]["ts_utc"] - bars[-2]["ts_utc"]))})

    kind = engulfing_kind(prev, cur, atr)
    if kind and atr and _body(cur) < MIN_BODY_ATR * atr:
        kind = None
    if kind is None:
        out["state"] = "NO-PATTERN"
        out["notes"] = "No engulfing on the last closed candle."
        return out
    out["pattern"] = kind

    if trend not in ("bullish", "bearish"):
        out["state"] = "NO-TREND"
        out["notes"] = f"{kind.capitalize()} engulfing, but the {htf_label} has no clear trend."
        return out

    with_trend = kind == trend
    confirmed_counter = False
    if not with_trend:
        confirmed_counter = _earlier_engulfing(bars, kind, atr)
        if not confirmed_counter:
            out["state"] = "WAIT-CONFIRM"
            out["with_trend"] = False
            out["notes"] = (f"{kind.capitalize()} engulfing against the {htf_label} {trend} trend - "
                            f"wait for a second one to confirm.")
            return out

    s = 1 if kind == "bullish" else -1
    full = covers_wicks(prev, cur)
    body = _body(cur)
    entry = cur["close"]
    wick = cur["low"] if s == 1 else cur["high"]
    pip = instruments.pip_size(symbol, entry)
    sl = wick - s * max(SL_BUFFER_ATR * atr, pip)
    risk = abs(entry - sl)
    if risk <= 0:
        out["state"] = "NO-PATTERN"
        out["notes"] = "Engulfing closed on its own wick - no room for a stop."
        return out
    tp1 = entry + s * TARGET_RR * risk

    # Rule 4 options 2 and 3: 25% / 50% of the whole engulfing candle (wick to
    # wick, as the rule card draws it), measured back from the end the trade
    # is heading towards. Never beyond the close: a limit there is a market order.
    top = cur["high"] if s == 1 else cur["low"]
    rng = cur["high"] - cur["low"]

    def retrace(frac: float) -> float:
        level = top - s * frac * rng
        return min(level, entry) if s == 1 else max(level, entry)

    strong_body = bool(atr) and body >= 0.6 * atr
    in_session = out["in_active_session"]
    score = (4 if with_trend else 2) + (3 if full else 0) + (2 if strong_body else 0) + (1 if in_session else 0)
    if with_trend and full:
        grade = "A"
    elif with_trend or confirmed_counter:
        grade = "B"
    else:
        grade = "C"

    notes = [f"{kind.capitalize()} engulfing on {timeframe}",
             f"with the {htf_label} {trend} trend" if with_trend
             else f"against the {htf_label} {trend} trend, confirmed by a second engulfing",
             "covers the previous candle's wicks" if full else "body only (previous wicks stick out)"]
    htf_eng = recent_htf_engulfing(h4_candles, kind)
    if htf_eng:
        notes.append(f"the {htf_label} printed an engulfing the same way in its last 3 candles")
    if not in_session:
        notes.append("outside London/NY hours")
    notes.append("UNVALIDATED - no edge in the walk-forward test")

    out.update({
        "setup": "BUY" if s == 1 else "SELL", "grade": grade, "score": score, "state": "SIGNAL",
        "with_trend": with_trend, "confirmed_counter": confirmed_counter, "full_engulf": full,
        "htf_engulfing": htf_eng,
        "entry": entry, "entry_25": retrace(0.25), "entry_50": retrace(0.50),
        "sl": sl, "tp1": tp1, "rr1": TARGET_RR,
        "sl_pips": round(risk / pip, 1), "body_atr": round(body / atr, 2) if atr else None,
        "notes": "; ".join(notes),
    })
    return out


def analyze_pair(symbol: str, h1_candles: list[dict], h4_candles: Optional[list[dict]] = None,
                 d1_candles: Optional[list[dict]] = None) -> dict:
    """Both entry legs for one pair: H1 judged against the H4 trend, H4 judged
    against the daily trend. The top level mirrors whichever leg has a signal
    (H1 first); `legs` has both."""
    h1 = analyze_timeframe(symbol, h1_candles or [], h4_candles, "H1", htf_label="H4")
    h4 = analyze_timeframe(symbol, h4_candles or [], d1_candles, "H4", htf_label="D1")
    lead = h1 if h1["setup"] != "NO-TRADE" or h4["setup"] == "NO-TRADE" else h4
    row = dict(lead)
    row["legs"] = {"H1": h1, "H4": h4}
    return row


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    pairs = []
    for sym in ENGULF_UNIVERSE:
        b = candles_by_pair.get(sym, {}) or {}
        try:
            pairs.append(analyze_pair(sym, b.get("1h") or b.get("h1") or [],
                                      b.get("4h") or b.get("h4") or [],
                                      b.get("1d") or b.get("d1") or []))
        except Exception as ex:  # noqa: BLE001 — one bad series must not sink the scan
            pairs.append({"symbol": sym, "setup": "NO-TRADE", "grade": "NO-TRADE", "score": 0,
                          "state": "ERROR", "pattern": None, "validated": False, "legs": {},
                          "notes": f"analysis failed: {ex}"})
    legs = [l for p in pairs for l in (p.get("legs") or {}).values()]
    return {
        "pairs": pairs,
        "buys": sum(1 for l in legs if l.get("setup") == "BUY"),
        "sells": sum(1 for l in legs if l.get("setup") == "SELL"),
        "waiting": sum(1 for l in legs if l.get("state") == "WAIT-CONFIRM"),
    }


def read_bundle(read_candles) -> dict[str, dict]:
    """Candles for every pair from the cache: `read_candles(symbol, interval, limit)`."""
    return {sym: {"1h": read_candles(sym, "1h", H1_LIMIT), "4h": read_candles(sym, "4h", H4_LIMIT),
                  "1d": read_candles(sym, "1day", D1_LIMIT)} for sym in ENGULF_UNIVERSE}
