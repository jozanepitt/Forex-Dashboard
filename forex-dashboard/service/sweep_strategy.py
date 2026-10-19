"""Sweep scanner (M15): the LuxAlgo "Order Flow VWAP Deviation" stop-run dots,
turned into trade setups.

What it recreates from the TradingView indicator (user's settings, 2026-10-19):
  * Session VWAP, re-anchored at 17:00 New York (the TradingView forex session
    open, 23:00 SAST in summer), with bands at +-2 volume-weighted std devs.
  * Stop Zones: pivot highs/lows confirmed by PIVOT_LEN bars each side, kept
    until price trades through them (MAX_LINES per side).
  * Stop-run dots: a bar that trades through a Stop Zone on a volume surge
    (volume >= VOL_MULT x the average of the previous VOL_LEN bars).
    "Stops Triggered High" = dot above the bar, "Stops Triggered Low" = below.
  * Volume-profile POC from the 18:00 New York anchor, and IFVGs (fair value
    gaps that price closed through), used as obstacles on the way to VWAP.
The indicator's source was not available; these are re-implementations from
its published description, so dots can differ slightly from the chart.

The setup (agreed with the user): a dot is only a warning. The trade is the
NEXT candle closing back inside the swept level -- sell after a swept high,
buy after a swept low. Entry = that close, stop = beyond the sweep wick,
target 1 = session VWAP or the nearest POC / IFVG on the way, target 2 =
the opposite band.

Grades:
  A  = sweep reached the outer band + H1 trend (200 EMA) agrees + target 1 is
       at least 1.5x the stop.
  B  = valid, target 1 at least 1x the stop, missing one of the A conditions.
       FX pairs only carry tick volume, so for them B still needs the band.
  C  = everything else (shown, never alerted).
UNVALIDATED: not backtested at the time of writing.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import instruments
import market_hours
from config import PRIORITY_PAIRS

EXTRA_SYMBOLS = ["USOIL", "UKOIL", "EUR/CAD"]
SWEEP_UNIVERSE = list(PRIORITY_PAIRS) + EXTRA_SYMBOLS
M15_LIMIT = 400
H1_LIMIT = 300

PIVOT_LEN = 20          # "Pivot Lookback (Stop Zones)"
MAX_LINES = 6           # "Max Active Lines" (per side here)
VOL_LEN = 20
VOL_MULT = 2.0          # "Stop Zone Volume Multiplier"
BAND_MULT = 2.0         # "Std. Dev Multiplier"
PROFILE_ROWS = 70
IFVG_FILTER = 1.2       # "IFVG Volatility Filter": gap must be >= this x ATR(14)
MAX_IFVGS = 3
ATR_LEN = 14
CONFIRM_BARS = 2        # the close back inside must come within this many bars of the dot
SIGNAL_MAX_AGE = 1      # a confirmed setup stays a SIGNAL for this many bars after confirmation
STOP_BUFFER_ATR = 0.1
EMA_TREND = 200
RR_A = 1.5
RR_B = 1.0
STALE_AFTER_SECS = 2 * 900 + 600

_NY = ZoneInfo("America/New_York")
SESSION_HOUR_NY = 17    # VWAP anchor
PROFILE_HOUR_NY = 18    # "Anchor Time Range 1800-1801, GMT-4"


def _anchor_start(ts_utc: int, hour_ny: int) -> int:
    """Most recent `hour_ny`:00 New York time at or before ts_utc (DST-aware)."""
    local = datetime.fromtimestamp(ts_utc, tz=timezone.utc).astimezone(_NY)
    start = local.replace(hour=hour_ny, minute=0, second=0, microsecond=0)
    if start > local:
        start -= timedelta(days=1)
    return int(start.timestamp())


def _vol(b: dict) -> float:
    v = b.get("volume") or 0.0
    return v if v > 0 else 1.0


def session_vwap(bars: list[dict]) -> tuple[list[float], list[float]]:
    """VWAP and volume-weighted std dev for every bar, re-anchored at each
    17:00 New York session open. Typical price (H+L+C)/3."""
    vwap, sd = [], []
    anchor = None
    pv = v = pv2 = 0.0
    for b in bars:
        a = _anchor_start(b["ts_utc"], SESSION_HOUR_NY)
        if a != anchor:
            anchor, pv, v, pv2 = a, 0.0, 0.0, 0.0
        tp = (b["high"] + b["low"] + b["close"]) / 3.0
        w = _vol(b)
        pv += tp * w
        v += w
        pv2 += tp * tp * w
        m = pv / v
        vwap.append(m)
        sd.append(max(0.0, pv2 / v - m * m) ** 0.5)
    return vwap, sd


def atr_series(bars: list[dict], n: int = ATR_LEN) -> list[float]:
    out, prev_close, run = [], None, None
    for b in bars:
        tr = b["high"] - b["low"] if prev_close is None else max(
            b["high"] - b["low"], abs(b["high"] - prev_close), abs(b["low"] - prev_close))
        run = tr if run is None else (run * (n - 1) + tr) / n
        out.append(run)
        prev_close = b["close"]
    return out


def ema_last(values: list[float], n: int) -> Optional[float]:
    if len(values) < n:
        return None
    e = sum(values[:n]) / n
    k = 2.0 / (n + 1)
    for x in values[n:]:
        e = x * k + e * (1 - k)
    return e


def find_dots(bars: list[dict]) -> list[dict]:
    """Every stop-run dot in the series. A pivot at p is known only at
    p + PIVOT_LEN, so a line is live from then until price trades through it."""
    n = len(bars)
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    vols = [_vol(b) for b in bars]
    lines_hi: list[tuple[int, float]] = []   # (pivot index, level)
    lines_lo: list[tuple[int, float]] = []
    dots: list[dict] = []
    L = PIVOT_LEN
    for i in range(n):
        p = i - L
        if p - L >= 0:
            # strict on both sides: a flat run of equal highs is not a swing
            # point, and counting every bar of it would push real swing levels
            # out of the MAX_LINES window
            if highs[p] > max(highs[p - L:p]) and highs[p] > max(highs[p + 1:p + L + 1]):
                lines_hi.append((p, highs[p]))
                lines_hi = lines_hi[-MAX_LINES:]
            if lows[p] < min(lows[p - L:p]) and lows[p] < min(lows[p + 1:p + L + 1]):
                lines_lo.append((p, lows[p]))
                lines_lo = lines_lo[-MAX_LINES:]
        base = sum(vols[i - VOL_LEN:i]) / VOL_LEN if i >= VOL_LEN else None
        ratio = vols[i] / base if base else None
        surge = ratio is not None and ratio >= VOL_MULT
        crossed_hi = [ln for ln in lines_hi if ln[0] + L < i and highs[i] > ln[1]]
        crossed_lo = [ln for ln in lines_lo if ln[0] + L < i and lows[i] < ln[1]]
        if crossed_hi:
            lines_hi = [ln for ln in lines_hi if ln not in crossed_hi]
            if surge:
                dots.append({"i": i, "side": "high", "level": max(l for _, l in crossed_hi), "vol_ratio": ratio})
        if crossed_lo:
            lines_lo = [ln for ln in lines_lo if ln not in crossed_lo]
            if surge:
                dots.append({"i": i, "side": "low", "level": min(l for _, l in crossed_lo), "vol_ratio": ratio})
    return dots


def volume_poc(bars: list[dict], start: int, end: int) -> Optional[float]:
    """Point of control of bars[start:end+1]: each bar's volume spread evenly
    over the price rows its range covers."""
    seg = bars[start:end + 1]
    if len(seg) < 4:
        return None
    lo = min(b["low"] for b in seg)
    hi = max(b["high"] for b in seg)
    if hi <= lo:
        return None
    step = (hi - lo) / PROFILE_ROWS
    rows = [0.0] * PROFILE_ROWS
    for b in seg:
        a = min(PROFILE_ROWS - 1, int((b["low"] - lo) / step))
        z = min(PROFILE_ROWS - 1, int((b["high"] - lo) / step))
        share = _vol(b) / (z - a + 1)
        for r in range(a, z + 1):
            rows[r] += share
    k = max(range(PROFILE_ROWS), key=lambda r: rows[r])
    return lo + (k + 0.5) * step


def active_ifvgs(bars: list[dict], atr: list[float], upto: int) -> list[dict]:
    """Inversion FVGs alive at bar `upto`: a 3-candle gap of at least
    IFVG_FILTER x ATR that price later CLOSED through (it flips from support
    to resistance or back). Dropped once price closes back through the far side."""
    fvgs: list[dict] = []
    for i in range(2, upto + 1):
        c = bars[i]["close"]
        for f in fvgs:
            if not f["inverted"]:
                if (f["kind"] == "bull" and c < f["bottom"]) or (f["kind"] == "bear" and c > f["top"]):
                    f["inverted"], f["inv_i"] = True, i
            elif (f["kind"] == "bull" and c > f["top"]) or (f["kind"] == "bear" and c < f["bottom"]):
                f["dead"] = True
        fvgs = [f for f in fvgs if not f.get("dead")]
        a, b = bars[i - 2], bars[i]
        if b["low"] > a["high"] and b["low"] - a["high"] >= IFVG_FILTER * atr[i]:
            fvgs.append({"kind": "bull", "bottom": a["high"], "top": b["low"], "inverted": False})
        elif b["high"] < a["low"] and a["low"] - b["high"] >= IFVG_FILTER * atr[i]:
            fvgs.append({"kind": "bear", "bottom": b["high"], "top": a["low"], "inverted": False})
    inv = [f for f in fvgs if f["inverted"]]
    inv.sort(key=lambda f: f["inv_i"])
    return [{"bottom": f["bottom"], "top": f["top"]} for f in inv[-MAX_IFVGS:]]


def h1_trend(h1: list[dict]) -> Optional[str]:
    closes = [b["close"] for b in h1]
    e = ema_last(closes, EMA_TREND)
    if e is None:
        return None
    return "bullish" if closes[-1] > e else "bearish"


def _empty(symbol: str, state: str, notes: str) -> dict:
    return {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE", "state": state,
            "notes": notes, "validated": False, "timeframe": "M15"}


def analyze_pair(symbol: str, m15: list[dict], h1: Optional[list[dict]] = None) -> dict:
    """Judge the newest closed M15 bar. The cache holds closed bars only."""
    need = 2 * PIVOT_LEN + VOL_LEN + 5
    if len(m15) < need:
        return _empty(symbol, "NO-DATA", f"Need {need} M15 bars, have {len(m15)}.")

    last = len(m15) - 1
    vwap, sd = session_vwap(m15)
    atr = atr_series(m15)
    pip = instruments.pip_size(symbol, m15[-1]["close"])
    cls = instruments.asset_class(symbol)
    tick_volume = cls in ("fx", "jpy")
    trend = h1_trend(h1) if h1 else None

    base = {
        "vwap": vwap[last], "upper": vwap[last] + BAND_MULT * sd[last],
        "lower": vwap[last] - BAND_MULT * sd[last], "h1_trend": trend,
        "tick_volume": tick_volume, "timeframe": "M15", "validated": False,
    }

    dots = [d for d in find_dots(m15) if d["i"] >= last - CONFIRM_BARS - SIGNAL_MAX_AGE]
    if not dots:
        row = _empty(symbol, "NO-SWEEP", "No stop-run dot in the last few candles.")
        row.update(base)
        return row
    dot = dots[-1]
    i, side, level = dot["i"], dot["side"], dot["level"]
    sell = side == "high"
    row = {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE", **base,
           "sweep_side": side, "level": level, "dot_ts": m15[i]["ts_utc"],
           "vol_ratio": round(dot["vol_ratio"], 2) if dot["vol_ratio"] else None}

    if i == last:
        row.update(state="WAIT-CLOSE", notes=(f"Stops {'above the high' if sell else 'below the low'} "
                                              f"{_fmt(level, symbol)} just taken. Waiting for the next "
                                              f"candle to close back {'below' if sell else 'above'} it."))
        return row

    # the confirmation: first later candle that closes back inside the level
    conf = None
    for c in range(i + 1, min(last, i + CONFIRM_BARS) + 1):
        if (sell and m15[c]["close"] < level) or (not sell and m15[c]["close"] > level):
            conf = c
            break
    if conf is None:
        if last - i < CONFIRM_BARS:
            row.update(state="WAIT-CLOSE", notes="Sweep still closing beyond the level. Waiting.")
        else:
            row.update(state="BREAKOUT", notes=(f"Price kept closing {'above' if sell else 'below'} the "
                                                f"swept level: a breakout, not a trap. No trade."))
        return row
    if last - conf > SIGNAL_MAX_AGE:
        row.update(state="EXPIRED", notes="Setup confirmed more than a candle ago; entry has passed.")
        return row

    entry = m15[conf]["close"]
    extreme = max(b["high"] for b in m15[i:conf + 1]) if sell else min(b["low"] for b in m15[i:conf + 1])
    buf = max(STOP_BUFFER_ATR * atr[conf], pip)
    sl = extreme + buf if sell else extreme - buf
    risk = abs(entry - sl)
    sgn = -1 if sell else 1

    v = vwap[conf]
    candidates: list[tuple[float, str]] = []
    if sgn * (v - entry) > 0:
        candidates.append((v, "VWAP"))
    poc = volume_poc(m15, _profile_start_index(m15, conf), conf)
    if poc is not None and sgn * (poc - entry) > 0 and (not candidates or sgn * (poc - v) < 0):
        candidates.append((poc, "POC"))
    for z in active_ifvgs(m15, atr, conf):
        edge = z["top"] if sell else z["bottom"]          # the side price meets first
        if sgn * (edge - entry) > 0 and (not candidates or sgn * (edge - v) < 0):
            candidates.append((edge, "IFVG"))
    at_band = (m15[i]["high"] >= v + BAND_MULT * sd[i]) if sell else (m15[i]["low"] <= v - BAND_MULT * sd[i])
    with_trend = trend == ("bearish" if sell else "bullish")
    row.update(entry=entry, sl=sl, signal_ts=m15[conf]["ts_utc"], at_band=at_band,
               with_trend=with_trend, sl_pips=round(risk / pip, 1), setup="SELL" if sell else "BUY")

    if not candidates or risk <= 0:
        row.update(grade="C", state="SIGNAL", tp1=None, rr1=None,
                   notes="Swept and closed back inside, but VWAP is not ahead of entry. No target.")
        return row
    tp1, tp1_kind = min(candidates, key=lambda t: abs(t[0] - entry))
    rr1 = abs(tp1 - entry) / risk
    tp2 = v - BAND_MULT * sd[conf] if sell else v + BAND_MULT * sd[conf]
    if sgn * (tp2 - tp1) <= 0:
        tp2 = None

    misses = [not at_band, not with_trend, rr1 < RR_A]
    if not any(misses):
        grade = "A"
    elif rr1 >= RR_B and sum(misses) == 1 and (at_band or not tick_volume):
        grade = "B"
    else:
        grade = "C"

    why = []
    why.append("swept beyond the outer band" if at_band else "sweep inside the bands")
    why.append(f"H1 trend {trend or 'unknown'} ({'with' if with_trend else 'against'} the trade)")
    why.append(f"target 1 = {tp1_kind}, 1:{rr1:.1f}")
    if tick_volume:
        why.append("tick volume only (FX)")
    row.update(grade=grade, state="SIGNAL", tp1=tp1, tp1_kind=tp1_kind, tp2=tp2, rr1=round(rr1, 2),
               notes=f"Stops {'above' if sell else 'below'} {_fmt(level, symbol)} taken, closed back inside. "
                     + "; ".join(why) + ". UNVALIDATED - not backtested.")
    return row


def _profile_start_index(bars: list[dict], upto: int) -> int:
    start_ts = _anchor_start(bars[upto]["ts_utc"], PROFILE_HOUR_NY)
    for k in range(upto, -1, -1):
        if bars[k]["ts_utc"] < start_ts:
            return k + 1
    return 0


def _fmt(x: float, symbol: str) -> str:
    return instruments.fmt_price(symbol, x)


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    pairs = []
    for symbol, tf in candles_by_pair.items():
        try:
            pairs.append(analyze_pair(symbol, tf.get("m15") or [], tf.get("1h") or []))
        except Exception as e:  # noqa: BLE001 - one bad series must not sink the scan
            pairs.append(_empty(symbol, "ERROR", f"analysis failed: {e}"))
    return {
        "pairs": pairs,
        "buys": sum(1 for p in pairs if p["setup"] == "BUY" and p.get("state") == "SIGNAL"),
        "sells": sum(1 for p in pairs if p["setup"] == "SELL" and p.get("state") == "SIGNAL"),
        "waiting": sum(1 for p in pairs if p.get("state") == "WAIT-CLOSE"),
    }


def read_bundle(read_candles) -> dict[str, dict]:
    return {sym: {"m15": read_candles(sym, "15min", M15_LIMIT), "1h": read_candles(sym, "1h", H1_LIMIT)}
            for sym in SWEEP_UNIVERSE}


def scan_cached(read_candles, now_ts: float) -> dict:
    candles = read_bundle(read_candles)
    result = analyze_universe(candles)
    result["stale_pairs"] = sorted(
        sym for sym, tf in candles.items()
        if market_hours.is_market_open(sym, now_ts)
        and (not tf["m15"] or now_ts - tf["m15"][-1]["ts_utc"] > STALE_AFTER_SECS))
    return result
