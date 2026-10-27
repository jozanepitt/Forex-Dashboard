"""
BTMM course rules (phase 2 + 3, 2026-10-27), taken from the user's course PDFs
as quoted in BTMM_CRT_STRATEGY_AUDIT.md (MMM Notes pp.6, 24-25, 45-47, the
setups doc and "LONDON Setup BTMM"). btmm_core.analyze() wires these in; the
dashboard tab reads them through GET /btmm-rules.

  * Sessions in New York time, as the course gives them (DST handled):
    Asian 20:30-03:00, London 03:30-09:00, NY 09:30-17:00 ET; the daily
    high/low resets at 17:00 ET. Trades only in London and NY ("avoid night
    hours"); the last two NY hours are the End of Day Reversal.
  * TDI: overbought / oversold = 68 / 32 (user choice 2026-10-27).
  * Shark Fin: RSI line breaks out of the volatility bands and reaches 68/32
    while price is 25-50 pips beyond the Asian range; entry when it crosses
    back through the signal line (1st entry) or the market baseline
    ("Blood in the Water", 2nd entry).
  * Confirmed M/W: 2nd leg closes back beyond the 13 EMA and the RSI line is
    beyond the market baseline, crossing the signal line.
  * Half Batman Type 1 / Type 2 with the shift-candle / apex entry.
  * Trade plan: stop 3-5 pips beyond the shift candle or HOD/LOD, R:R at
    least 1:1, break-even at 1:1, exit on a close back over the 13 EMA.
  * Market-maker levels from EMA crosses (13/50, 50/200, 50|200/800) with
    resets.

Pip distances are forex pips; for gold, indices and crypto the same distance
is scaled by the instrument's average daily range (median forex ADR = 56
pips, the calibration ID50 uses), so the logic stays the same everywhere.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import instruments

ET = ZoneInfo("America/New_York")

TDI_OB, TDI_OS = 68.0, 32.0
TDI_TAIL = 120                # bars of band history kept (30h of M15)

SF_LOOKBACK = 20              # the Shark Fin spike must be within 5h (M15)
SF_LOC_MIN_PIPS, SF_LOC_MAX_PIPS = 25.0, 50.0
ENTRY_FRESH_BARS = 4          # an entry cross older than 1h is no longer an entry

HB_WINDOW = 40                # Half Batman structure window (10h of M15)
HB_FAIL_PIPS = 10.0           # Type 1 retest must fall short of leg 1 by >= 10 pips
HB_SHIFT_PIPS = 10.0          # shift candle range (course: a 10-12 pip candle)
HB_EMA50_TOL_PIPS = 3.0       # Type 2: a low/high within 3 pips of the 50 EMA = trapped off it

RESET_MIN_BARS = 8            # a reset pullback holds 13 < 50 (or >) for 2h of M15
RESET_MIN_SPACING_BARS = 96   # and comes a day (M15) after the previous level

# Shark Fin stop: 5 pips + 0.10 ADR beyond the spike (tested 2026-10-27 at
# real Exness Standard spreads, Shark Fin only, k = extra ADR fraction:
# k=0 -0.05R, 0.05 0.00R, 0.10 +0.05R, 0.15 +0.05R, 0.20 +0.01R net per trade;
# 0.10 was the value proposed before the test). The tight stop let the spread
# eat ~0.16R a trade; 0.10 ADR cuts that to ~0.09R.
SF_STOP_EXTRA_ADR = 0.10
# Shark Fin A/A+ grades only on the USD majors: forward test registered
# 2026-10-27 (replay: majors +0.19R net n=67, other pairs -0.04R n=111 --
# found AFTER looking, so it is a hypothesis until the forward test agrees).
SF_MAJORS = ("EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD")
FORWARD_TEST_START = "2026-10-27"

SL_BUFFER_PIPS = 5.0        # course: 3-5 pips beyond the shift candle / HOD-LOD
TP_PIPS_LOW_ADR, TP_PIPS_HIGH_ADR = 20.0, 30.0   # ADR < 100 pips / ADR >= 100 pips
FX_MEDIAN_ADR_PIPS = 56.0


# ── Sessions ──────────────────────────────────────────────────────────────────

def et_hour(ts_utc: int) -> float:
    d = datetime.fromtimestamp(ts_utc, timezone.utc).astimezone(ET)
    return d.hour + d.minute / 60


def session(ts_utc: int) -> Optional[str]:
    """Course session for a bar's open time; None = the gaps between them."""
    h = et_hour(ts_utc)
    if h >= 20.5 or h < 3.0:
        return "Asian"
    if 3.5 <= h < 9.0:
        return "London"
    if 9.5 <= h < 17.0:
        return "NY"
    return None


def kill_zone(ts_utc: int) -> Optional[str]:
    s = session(ts_utc)
    return s if s in ("London", "NY") else None


def cycle_phase(ts_utc: int) -> str:
    """Daily cycle in the course's own words."""
    s = session(ts_utc)
    if s == "London":
        return "Stop Hunt"
    if s == "NY":
        return "End of Day Reversal" if et_hour(ts_utc) >= 15.0 else "Trend Move"
    return "Accumulation"


def trading_day(ts_utc: int):
    """The trading day a bar belongs to: days start at 17:00 ET (the daily
    high/low reset), so 17:00 ET Monday onwards is Tuesday's day."""
    d = datetime.fromtimestamp(ts_utc, timezone.utc).astimezone(ET)
    return (d + timedelta(hours=7)).date()


# ── Units ─────────────────────────────────────────────────────────────────────

def pip_unit(symbol: Optional[str], price: float, adr: float) -> float:
    """Price distance of one forex pip of a rule on this instrument."""
    if instruments.asset_class(symbol) in ("fx", "jpy") or not adr:
        return instruments.pip_size(symbol, price)
    return adr / FX_MEDIAN_ADR_PIPS


# ── TDI series ────────────────────────────────────────────────────────────────

def tdi_series(closes: list[float], tail: int = TDI_TAIL) -> dict:
    """TDI lines per bar for the last `tail` bars: fast = RSI line (green,
    SMA2 of RSI13), slow = trade signal line (red, SMA7), mid = market
    baseline (yellow, SMA34), upper/lower = volatility bands (34, 1.6185 sd).
    Same parameters as btmm_core.calc_tdi."""
    from btmm_core import _rsi, _sma
    rsi = _rsi(closes, 13)
    fast, slow = _sma(rsi, 2), _sma(rsi, 7)
    n = len(rsi)
    start = max(0, n - tail)
    mid, up, lo = [], [], []
    for i in range(start, n):
        w = rsi[max(0, i - 33):i + 1]
        mu = sum(w) / len(w)
        sd = (sum((v - mu) ** 2 for v in w) / len(w)) ** 0.5
        mid.append(mu)
        up.append(mu + 1.6185 * sd)
        lo.append(mu - 1.6185 * sd)
    return {"fast": fast[start:], "slow": slow[start:], "mid": mid, "upper": up, "lower": lo}


def _crossed(a: list[float], b: list[float], i: int, down: bool) -> bool:
    """a crossed b at bar i (from bar i-1)."""
    if i < 1:
        return False
    return (a[i - 1] >= b[i - 1] and a[i] < b[i]) if down else (a[i - 1] <= b[i - 1] and a[i] > b[i])


def _last_cross(a, b, start: int, down: bool) -> Optional[int]:
    for i in range(len(a) - 1, max(start, 1) - 1, -1):
        if _crossed(a, b, i, down):
            return i
    return None


def tdi_confirms(ts: dict, direction: str, within: int = ENTRY_FRESH_BARS) -> bool:
    """Course: 'RSI above/below the MBL, crossing the signal line'. The RSI
    line is beyond the market baseline and the signal line now, and crossed
    the signal line within the last `within` bars."""
    f, s, m = ts["fast"], ts["slow"], ts["mid"]
    if len(f) < 2:
        return False
    sell = direction == "bearish"
    side = (f[-1] < m[-1] and f[-1] < s[-1]) if sell else (f[-1] > m[-1] and f[-1] > s[-1])
    return side and _last_cross(f, s, len(f) - within, down=sell) is not None


# ── Shark Fin / Blood in the Water ────────────────────────────────────────────

def _no_fin(reason: str) -> dict:
    return {"detected": False, "direction": None, "entry_ready": False, "entry": None,
            "leg": 0, "confidence": "low", "reason": reason}


def detect_shark_fin(bars: list[dict], ts: dict, asian: dict, unit: float,
                     lookback: int = SF_LOOKBACK, bias_bullish: Optional[bool] = None) -> dict:
    """Course p.45-46. `direction` is the TRADE direction: a fin above the
    upper band (overbought) is a sell. With `bias_bullish` (price above the
    800 EMA) a fin only becomes entry_ready in the bias direction (user
    decision 2026-10-27: replay +0.15R gross with the bias vs +0.10R)."""
    n = len(ts["fast"])
    if n < lookback + 2 or not asian.get("valid"):
        return _no_fin("no valid Asian range" if n >= lookback + 2 else "not enough bars")
    b = bars[-n:]
    f, s, m = ts["fast"], ts["slow"], ts["mid"]
    best = None
    for sell in (True, False):
        out = [i for i in range(n - lookback, n)
               if (f[i] > ts["upper"][i] if sell else f[i] < ts["lower"][i])]
        if not out:
            continue
        last_out = out[-1]
        # the excursion = the run of outside-band bars ending at last_out
        first_out = last_out
        while first_out - 1 in out:
            first_out -= 1
        peak = max(f[first_out:last_out + 1]) if sell else min(f[first_out:last_out + 1])
        ext = (max(x["high"] for x in b[first_out:last_out + 2]) if sell
               else min(x["low"] for x in b[first_out:last_out + 2]))
        loc = ((ext - asian["high"]) if sell else (asian["low"] - ext)) / unit
        fin = _last_cross(f, s, last_out + 1, down=sell)
        bitw = _last_cross(f, m, last_out + 1, down=sell)
        cand = {
            "detected": True,
            "direction": "bearish" if sell else "bullish",
            "leg": 2 if last_out < n - 1 else 1,
            "peak_rsi": round(peak, 1),
            "overbought": peak >= TDI_OB if sell else peak <= TDI_OS,
            "location_pips": round(loc, 1),
            "location_ok": SF_LOC_MIN_PIPS <= loc <= SF_LOC_MAX_PIPS,
            "spike_extreme": ext,
            "spike_ts": b[last_out]["ts_utc"],
            "fin_bars_ago": None if fin is None else n - 1 - fin,
            "bitw_bars_ago": None if bitw is None else n - 1 - bitw,
            "bars_ago": n - 1 - last_out,
        }
        if best is None or cand["bars_ago"] < best["bars_ago"]:
            best = cand
    if best is None:
        return _no_fin("RSI line has not left the volatility bands")

    sell = best["direction"] == "bearish"
    still = f[-1] < s[-1] if sell else f[-1] > s[-1]          # not already reversed
    fresh = lambda k: k is not None and k < ENTRY_FRESH_BARS
    entry = ("Blood in the Water" if fresh(best["bitw_bars_ago"])
             else "Shark Fin" if fresh(best["fin_bars_ago"]) else None)
    best["blood_in_water"] = fresh(best["bitw_bars_ago"])
    best["entry"] = entry
    best["with_bias"] = bias_bullish is None or bias_bullish == (not sell)
    best["entry_ready"] = bool(entry and still and best["overbought"] and best["location_ok"]
                               and best["with_bias"])
    best["confidence"] = ("high" if best["entry_ready"] and best["blood_in_water"]
                          else "medium" if best["entry_ready"] else "low")
    if not best["entry_ready"]:
        best["reason"] = ("price not 25-50 pips beyond the Asian range" if not best["location_ok"]
                          else f"RSI did not reach {TDI_OB:.0f}/{TDI_OS:.0f}" if not best["overbought"]
                          else "no fresh signal-line or baseline cross" if not entry
                          else "against the 800 EMA bias" if not best["with_bias"]
                          else "RSI already back the other way")
    return best


# ── Confirmed M / W ───────────────────────────────────────────────────────────

def mw_entry_candle(bars: list[dict], direction: str) -> Optional[str]:
    """Course 2nd-leg candles for an M/W: RR (railroad tracks), morning /
    evening star ("a 45 minute RRT") or COW, pointing the trade's way."""
    from btmm_id50 import _detect_star
    from btmm_core import detect_nameable_candle
    if len(bars) < 3:
        return None
    star = _detect_star(bars)
    if star.get("found") and star.get("direction") == direction:
        return star["name"]
    nc = detect_nameable_candle(bars[-2:])
    if nc.get("found") and nc.get("name") in ("RRT", "COW") and nc.get("direction") == direction:
        return nc["name"]
    return None


def confirm_mw(bars: list[dict], mw: dict, e13: list[float], ts: dict) -> dict:
    """Course "Confirmed M and W": the 2nd leg closes back beyond the 13 EMA
    with a candlestick pattern (RR, morning/evening star, COW); TDI RSI
    beyond the MBL, crossing the signal line."""
    if not mw.get("detected") or mw.get("pattern") not in ("M", "W"):
        return {**mw, "confirmed": False, "close_beyond_13": False, "tdi_confirms": False, "candle": None}
    sell = mw["pattern"] == "M"
    d = "bearish" if sell else "bullish"
    beyond = bars[-1]["close"] < e13[-1] if sell else bars[-1]["close"] > e13[-1]
    tdi_ok = tdi_confirms(ts, d)
    candle = mw_entry_candle(bars, d)
    return {**mw, "confirmed": bool(beyond and tdi_ok and candle), "close_beyond_13": beyond,
            "tdi_confirms": tdi_ok, "candle": candle}


# ── M / W off Mayo (200 EMA) ──────────────────────────────────────────────────

MAYO_TOL_PIPS = 5.0
MAYO_WINDOW = 24


def detect_mayo_mw(bars: list[dict], mw: dict, e13: list[float], e200: list[float], ts: dict,
                   mm_levels: dict, unit: float, window: int = MAYO_WINDOW) -> dict:
    """Course "M and W off Mayo": an M/W reversal off the 200 EMA, mostly at
    Level 2 -- never trade back an anchor (Level 1). Same M&W entry: the 2nd
    leg closes beyond the 13 EMA with an RR/star/COW, TDI confirming."""
    none = {"detected": False, "pattern": None, "direction": None}
    if not mw.get("detected") or mw.get("pattern") not in ("M", "W") or len(bars) < window:
        return none
    sell = mw["pattern"] == "M"
    d = "bearish" if sell else "bullish"
    if mm_levels.get("direction") != d or (mm_levels.get("level") or 0) < 2:
        return {**none, "reason": "not Level 2+ in the trade direction"}
    tol = MAYO_TOL_PIPS * unit
    seg, s200 = bars[-window:], e200[-window:]
    touched = any((x["high"] >= e - tol and x["close"] <= e + tol) if sell
                  else (x["low"] <= e + tol and x["close"] >= e - tol) for x, e in zip(seg, s200))
    if not touched:
        return {**none, "reason": "no test of the 200 EMA"}
    beyond = bars[-1]["close"] < e13[-1] if sell else bars[-1]["close"] > e13[-1]
    candle = mw_entry_candle(bars, d)
    if beyond and candle and tdi_confirms(ts, d):
        return {"detected": True, "pattern": mw["pattern"], "direction": d, "candle": candle,
                "level": mm_levels.get("level")}
    return {**none, "reason": "no 13 close / candle / TDI yet"}


# ── Straightaway, NYC Reversal, weekly cycle ──────────────────────────────────

SA_WINDOW = 12                # Level I consolidation looked at over 3h of M15
SA_MAX_RANGE_ADR = 0.35       # "consolidation" = range within 0.35 ADR
NYC_THR_PIPS = 15.0


def detect_straightaway(bars: list[dict], mm_levels: dict, e13: list[float], adr: float,
                        window: int = SA_WINDOW) -> dict:
    """Course p.32: the Straight Away trade develops at the Level I
    consolidation -- the market makers already have traders trapped from the
    reversal, so there is no 2nd move; price breaks on in the new direction.
    Never trade against the peak formation out of Level I."""
    none = {"detected": False, "direction": None}
    d = mm_levels.get("direction")
    if mm_levels.get("level") != 1 or d not in ("bullish", "bearish") or len(bars) < window + 1 or not adr:
        return none
    box = bars[-window - 1:-1]
    if mm_levels.get("origin_ts") and box[0]["ts_utc"] < mm_levels["origin_ts"]:
        return none                                  # the consolidation must be after Level 1 began
    hi, lo = max(x["high"] for x in box), min(x["low"] for x in box)
    if hi - lo > SA_MAX_RANGE_ADR * adr:
        return none
    c = bars[-1]["close"]
    breaks = (c > hi and c > e13[-1]) if d == "bullish" else (c < lo and c < e13[-1])
    return {"detected": breaks, "direction": d if breaks else None,
            "box_high": hi, "box_low": lo}


def detect_nyc_reversal(bars: list[dict], adr: float, unit: float) -> dict:
    """Course pp.30, 40: in the ~3 hours after the NY open (09:30-12:30 ET)
    the opposite HOD/LOD is reached and price reverses -- more likely when
    the day's range has met the ADR. Needs a reversal candle."""
    none = {"detected": False, "direction": None}
    if not bars or not (9.5 <= et_hour(bars[-1]["ts_utc"]) < 12.5):
        return none
    day_key = trading_day(bars[-1]["ts_utc"])
    day = [x for x in bars[-200:] if trading_day(x["ts_utc"]) == day_key]
    if len(day) < 8:
        return none
    hod_i = max(range(len(day)), key=lambda k: day[k]["high"])
    lod_i = min(range(len(day)), key=lambda k: day[k]["low"])
    hod, lod, c = day[hod_i]["high"], day[lod_i]["low"], bars[-1]["close"]
    adr_met = bool(adr) and (hod - lod) >= 0.9 * adr
    for sell in (True, False):
        ext_i, ext = (hod_i, hod) if sell else (lod_i, lod)
        fresh = len(day) - 1 - ext_i <= 6                # extreme made in the last 1.5h
        near = abs(c - ext) <= NYC_THR_PIPS * unit
        d = "bearish" if sell else "bullish"
        if fresh and near and mw_entry_candle(bars, d):
            return {"detected": True, "direction": d, "adr_met": adr_met, "extreme": ext}
    return none


def detect_weekly_cycle(bars: list[dict]) -> dict:
    """Course pp.20-23: the weekly pattern on intraday charts -- the week's
    peak formation high/low forms early in the week and the midweek reversal
    turns it. From Wednesday: high made Mon/Tue and price back in the lower
    half of the week's range = weekly M (bearish); the mirror = weekly W."""
    none = {"detected": False, "pattern": None, "direction": None}
    if not bars:
        return none
    today = trading_day(bars[-1]["ts_utc"])
    if today.weekday() < 2:
        return {**none, "reason": "before the midweek reversal"}
    monday = today - timedelta(days=today.weekday())
    week = [x for x in bars[-6 * 96:] if monday <= trading_day(x["ts_utc"]) <= today]
    if len(week) < 96:
        return none
    hi_i = max(range(len(week)), key=lambda k: week[k]["high"])
    lo_i = min(range(len(week)), key=lambda k: week[k]["low"])
    hi, lo, c = week[hi_i]["high"], week[lo_i]["low"], bars[-1]["close"]
    mid = (hi + lo) / 2
    early = lambda k: trading_day(week[k]["ts_utc"]).weekday() <= 1
    if early(hi_i) and c < mid and hi_i < lo_i:
        return {"detected": True, "pattern": "M", "direction": "bearish", "week_high": hi}
    if early(lo_i) and c > mid and lo_i < hi_i:
        return {"detected": True, "pattern": "W", "direction": "bullish", "week_low": lo}
    return none


# ── Advanced M / W ────────────────────────────────────────────────────────────

ADV_MIN_CONSOL = 8            # leg 1 holds 8+ candles on one side of the 13 EMA
ADV_WINDOW = 60


def detect_advanced_mw(bars: list[dict], e13: list[float], ts: dict,
                       window: int = ADV_WINDOW, min_consol: int = ADV_MIN_CONSOL) -> dict:
    """Course "Advanced M and W": leg 1 consolidates 8+ candles above (M) /
    below (W) the 13 EMA without closing through it, with the TDI RSI line
    outside the volatility band; then a shift candle closes through the 13.
    Leg 2 comes back to or through the 13 without beating leg 1, and the
    current bar closes back beyond the 13 with the RSI line beyond the MBL,
    crossing the signal line."""
    none = {"detected": False, "pattern": None, "direction": None}
    m = min(window, len(ts["fast"]), len(bars), len(e13))
    if m < min_consol + 4:
        return none
    b, c13 = bars[-m:], e13[-m:]
    f, up, lo = ts["fast"][-m:], ts["upper"][-m:], ts["lower"][-m:]
    last = m - 1
    for sell in (True, False):
        on_leg1 = (lambda i: b[i]["close"] >= c13[i]) if sell else (lambda i: b[i]["close"] <= c13[i])
        shifts = [s for s in range(min_consol, last - 1)
                  if not on_leg1(s) and all(on_leg1(k) for k in range(s - min_consol, s))]
        if not shifts:
            continue
        s = shifts[-1]
        k0 = s - 1
        while k0 > 0 and on_leg1(k0 - 1):
            k0 -= 1
        leg1 = (max(b[k]["high"] for k in range(k0, s + 1)) if sell
                else min(b[k]["low"] for k in range(k0, s + 1)))
        tdi_out = any((f[k] > up[k]) if sell else (f[k] < lo[k]) for k in range(max(0, k0 - 2), s + 1))
        leg2 = b[s + 1:last]
        if not tdi_out or not leg2:
            continue
        leg2_ext = max(x["high"] for x in leg2) if sell else min(x["low"] for x in leg2)
        if (leg2_ext > leg1 * 1.0002) if sell else (leg2_ext < leg1 * 0.9998):
            continue                                   # leg 2 beat leg 1: not an M/W
        retest = any(on_leg1(k) for k in range(s + 1, last))
        closes_back = not on_leg1(last)
        d = "bearish" if sell else "bullish"
        if retest and closes_back and tdi_confirms(ts, d):
            return {"detected": True, "pattern": "M" if sell else "W", "direction": d,
                    "leg1": leg1, "leg2": leg2_ext, "consolidation_bars": s - k0}
    return none


# ── London patterns Type 1 / 2 / 3 ────────────────────────────────────────────

L3_EMA50_TOL_PIPS = 3.0


def detect_london_pattern(bars: list[dict], e50: list[float], ts: dict, asian: dict,
                          unit: float, mw: Optional[dict] = None) -> dict:
    """Course "London Patterns", London session only, same entry/exit as M&W:
      Type 1: the M/W forms above (M) / below (W) the Asian range
      Type 2: the M/W forms inside the Asian range
      Type 3 "50/50/50": price bounces off the 50 EMA in the middle 50% of
              the Asian range, RSI line beyond the MBL and the signal line
              crossing the 50 level.
    `mw` = a confirmed or advanced M/W for Types 1/2 (its 2nd leg = the
    extreme of the last 8 bars)."""
    none = {"detected": False, "type": None, "direction": None}
    if len(bars) < 10 or not asian.get("valid") or session(bars[-1]["ts_utc"]) != "London":
        return none
    hi, lo = asian["high"], asian["low"]
    if mw and mw.get("pattern") in ("M", "W") and (mw.get("confirmed") or mw.get("direction")):
        sell = mw["pattern"] == "M"
        ext = max(x["high"] for x in bars[-8:]) if sell else min(x["low"] for x in bars[-8:])
        outside = ext > hi if sell else ext < lo
        inside = lo <= ext <= hi
        if outside or inside:
            return {"detected": True, "type": 1 if outside else 2,
                    "direction": "bearish" if sell else "bullish"}
    rng = hi - lo
    close = bars[-1]["close"]
    if rng <= 0 or abs(close - (hi + lo) / 2) > 0.25 * rng:
        return none
    f, s, m = ts["fast"], ts["slow"], ts["mid"]
    fifty = [50.0] * len(s)
    tol = L3_EMA50_TOL_PIPS * unit
    for sell in (True, False):
        bounced = any((x["high"] >= e - tol and x["close"] < e) if sell else (x["low"] <= e + tol and x["close"] > e)
                      for x, e in zip(bars[-4:], e50[-4:]))
        rsi_side = f[-1] < m[-1] if sell else f[-1] > m[-1]
        sig50 = _last_cross(s, fifty, len(s) - ENTRY_FRESH_BARS, down=sell) is not None
        if bounced and rsi_side and sig50:
            return {"detected": True, "type": 3, "direction": "bearish" if sell else "bullish"}
    return none


# ── Half Batman Type 1 / Type 2 ───────────────────────────────────────────────

def detect_half_batman(bars: list[dict], e13: list[float], e50: list[float], ts: dict,
                       unit: float, window: int = HB_WINDOW) -> dict:
    """Course ("Half a Batman", LONDON Setup BTMM): an incomplete M/W, the
    market maker does not give the 2nd leg.
      Type 1: price closes beyond the 13 EMA, retests back through it but
              falls short of leg 1 by >= 10 pips.
      Type 2: price closes beyond the 13 EMA, traps off the 50 EMA and never
              goes back through the 13.
    Entry (both): a 10-12 pip shift candle closing beyond the apex (the
    structure's midpoint) and the 13 EMA, TDI RSI beyond its signal line and
    the MBL. The current bar is the shift candle."""
    none = {"detected": False, "type": None, "direction": None, "pattern": None}
    if len(bars) < window or len(ts["fast"]) < 2:
        return none
    seg, s13, s50 = bars[-window:], e13[-window:], e50[-window:]
    cur = seg[-1]
    for sell in (True, False):
        d = "bearish" if sell else "bullish"
        last = len(seg) - 1
        leg_i = (max(range(last - 3), key=lambda i: seg[i]["high"]) if sell
                 else min(range(last - 3), key=lambda i: seg[i]["low"]))
        leg1 = seg[leg_i]["high"] if sell else seg[leg_i]["low"]
        c13 = next((i for i in range(leg_i + 1, last)
                    if (seg[i]["close"] < s13[i] if sell else seg[i]["close"] > s13[i])), None)
        if c13 is None or c13 >= last - 1:
            continue
        mid = seg[c13:last]
        back_13 = [i for i in range(c13 + 1, last)
                   if (seg[i]["close"] > s13[i] if sell else seg[i]["close"] < s13[i])]
        far = (min(x["low"] for x in seg[leg_i + 1:last]) if sell
               else max(x["high"] for x in seg[leg_i + 1:last]))
        apex = (leg1 + far) / 2
        hb_type = None
        if back_13:
            retest = (max(x["high"] for x in seg[back_13[0]:last]) if sell
                      else min(x["low"] for x in seg[back_13[0]:last]))
            if ((leg1 - retest) if sell else (retest - leg1)) >= HB_FAIL_PIPS * unit:
                hb_type = 1
        else:
            tol = HB_EMA50_TOL_PIPS * unit
            trapped = any((x["low"] <= s50[c13 + k] + tol) if sell else (x["high"] >= s50[c13 + k] - tol)
                          for k, x in enumerate(mid))
            if trapped:
                hb_type = 2
        if hb_type is None:
            continue
        shift = ((cur["close"] < cur["open"]) if sell else (cur["close"] > cur["open"])) \
            and (cur["high"] - cur["low"]) >= HB_SHIFT_PIPS * unit \
            and ((cur["close"] < apex and cur["close"] < s13[-1]) if sell
                 else (cur["close"] > apex and cur["close"] > s13[-1]))
        f, s, m = ts["fast"][-1], ts["slow"][-1], ts["mid"][-1]
        tdi_ok = (f < s and f < m) if sell else (f > s and f > m)
        if shift and tdi_ok:
            return {"detected": True, "type": hb_type, "direction": d,
                    "pattern": f"half_batman_type{hb_type}", "leg1": leg1, "apex": apex,
                    "shift_extreme": cur["high"] if sell else cur["low"]}
    return none


# ── Market-maker levels and resets ────────────────────────────────────────────

def detect_mm_levels(bars: list[dict], e13, e50, e200, e800) -> dict:
    """Course pp.104-126: Level 1 = 13/50 EMA cross, Level 2 = 50/200 cross,
    Level 3 = 50/800 or 200/800 cross. A Reset: the market maker books profit
    and continues instead of reversing -- the 13/50 crosses back while the
    50/200 stays in the trend -- and the count carries on (4 or 5 levels in
    one trend). Confirmed when price then breaks the peak/trough made before
    the reset."""
    n = min(len(bars), len(e13), len(e50), len(e200), len(e800))
    if n < 3:
        return {"level": 0, "direction": None, "resets": 0, "last_reset_confirmed": None}
    e13, e50, e200, e800, bars = e13[-n:], e50[-n:], e200[-n:], e800[-n:], bars[-n:]
    bull = e13[-1] > e50[-1]
    up = lambda a, b, i: a[i] > b[i] if bull else a[i] < b[i]
    into = [i for i in range(1, n) if up(e13, e50, i) and not up(e13, e50, i - 1)]
    if not into:
        return {"level": 0, "direction": "bullish" if bull else "bearish",
                "resets": 0, "last_reset_confirmed": None}
    # origin = latest 13/50 cross into the trend made while the 50/200 was not yet with it
    origin = next((i for i in reversed(into) if not up(e50, e200, i)), into[0])
    # a reset needs a real profit-taking pullback: the 13 stays on the wrong
    # side of the 50 for RESET_MIN_BARS bars, not a one-bar whipsaw
    def pullback_bars(i):
        k = i - 1
        while k > origin and not up(e13, e50, k):
            k -= 1
        return i - 1 - k
    base, l3 = 1, None
    l2 = next((i for i in range(origin, n) if up(e50, e200, i)), None)
    if l2 is not None:
        base = 2
        l3 = next((i for i in range(l2, n) if up(e50, e800, i) or up(e200, e800, i)), None)
        if l3 is not None:
            base = 3
    # ...and, like every level (course: ~1 ADR, 3 levels in 2.5-5 days), comes
    # at least RESET_MIN_SPACING_BARS after the previous level
    events = [e for e in (origin, l2, l3) if e is not None]
    resets = []
    for i in into:
        if i <= origin or not up(e50, e200, i) or pullback_bars(i) < RESET_MIN_BARS:
            continue
        if i - max(e for e in events + resets if e < i) >= RESET_MIN_SPACING_BARS:
            resets.append(i)
    confirmed = None
    if resets:
        r = resets[-1]
        prior = bars[origin:r]
        if prior:
            peak = max(x["high"] for x in prior) if bull else min(x["low"] for x in prior)
            confirmed = any((x["high"] > peak) if bull else (x["low"] < peak) for x in bars[r:])
    return {"level": base + len(resets), "base_level": base,
            "direction": "bullish" if bull else "bearish", "resets": len(resets),
            "last_reset_confirmed": confirmed, "origin_ts": bars[origin]["ts_utc"]}


def level_context(bars: list[dict], adr: float, direction: Optional[str],
                  origin_ts: Optional[int] = None, days: int = 6) -> dict:
    """The other two ways the course counts levels (pp.104-108):
      asian_stack: consecutive daily Asian boxes stepping in the trend
                   direction (each box's midpoint beyond the previous one)
      adr_legs:    how many ADRs price has travelled since the level origin
                   (each level is about 1x ADR)"""
    out = {"asian_stack": 0, "adr_legs": None}
    if not bars or direction not in ("bullish", "bearish"):
        return out
    boxes: dict = {}
    for b in bars[-days * 96:]:
        if session(b["ts_utc"]) == "Asian":
            boxes.setdefault(trading_day(b["ts_utc"]), []).append(b)
    mids = [(max(x["high"] for x in v) + min(x["low"] for x in v)) / 2
            for k, v in sorted(boxes.items()) if len(v) >= 4]
    stack = 0
    for a, c in zip(reversed(mids[:-1]), reversed(mids[1:])):
        if (c > a) if direction == "bullish" else (c < a):
            stack += 1
        else:
            break
    out["asian_stack"] = stack
    if origin_ts is not None and adr:
        start = next((b for b in bars if b["ts_utc"] >= origin_ts), None)
        if start:
            sgn = 1 if direction == "bullish" else -1
            out["adr_legs"] = round(max(0.0, (bars[-1]["close"] - start["close"]) * sgn) / adr, 2)
    return out


def h1_trend(bars: list[dict]) -> str:
    """Top-down bias from H1 (course pp.126-132: confirm a clear H1 cycle
    before the M15 entry): H1 close vs its 50 and 200 EMA."""
    from btmm_core import resample_to_h1
    return resample_to_h1(bars).get("trend", "neutral")


# ── Trade plan and management ─────────────────────────────────────────────────

def trade_plan(entry: float, direction: str, anchor: float, unit: float,
               adr: float, e200: Optional[float] = None, exit_13: bool = True) -> dict:
    """Stop 5 pips beyond `anchor` (shift candle / HOD-LOD / spike); TP1 = 20
    pips (ADR < 100 pips) or 30 pips, never under 1:1; TP2 = the 200 EMA when
    it is past TP1, else 2x TP1's distance; stop to break-even at 1:1.
    exit_13=False (Shark Fin, user decision 2026-10-27) holds to the stop or
    TP1: the 13 EMA exit cut the fin's replay edge from +0.10R to +0.01R."""
    sell = direction == "bearish"
    sgn = -1 if sell else 1
    sl = anchor + SL_BUFFER_PIPS * unit if sell else anchor - SL_BUFFER_PIPS * unit
    if (sl - entry) * sgn > -2 * unit:            # stop must sit beyond entry
        sl = entry - sgn * 2 * unit
    risk = abs(entry - sl)
    adr_units = adr / unit if unit else 0       # forex: pips; scaled instruments: 56
    target = (TP_PIPS_LOW_ADR if adr_units < 100 else TP_PIPS_HIGH_ADR) * unit
    t1 = max(risk, target)
    tp1 = entry + sgn * t1
    tp2 = e200 if (e200 is not None and (e200 - tp1) * sgn > 0) else entry + sgn * 2 * t1
    return {"entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
            "break_even_at": entry + sgn * risk, "risk": risk,
            "rr_tp1": round(t1 / risk, 2) if risk else None, "exit_13": exit_13,
            "exit_rules": ["stop to break-even at 1:1"]
                          + (["exit on a close back over the 13 EMA"] if exit_13 else ["hold to the stop or TP1"])
                          + ["exit when the RSI line reaches the opposite band and re-crosses the signal line",
                             "optional: exit if 61.8% of a 1R+ move is given back (Fib; replay: no change)"]}


def manage_trade(plan: dict, direction: str, bars_after: list[dict], e13_after: list[float],
                 fib: Optional[float] = None) -> dict:
    """Walk the bars after entry: stop first (same-bar ties count against
    us), then TP1, then break-even at 1:1, then a close back over the 13 EMA
    (skipped when the plan has exit_13=False). With `fib` (course: Fib
    38.2 / 61.8 breached): once the trade is 1R up, exit on a close that
    gives back more than that share of the best move."""
    if not plan.get("exit_13", True):
        e13_after = []
    sell = direction == "bearish"
    entry, sl, tp1, be = plan["entry"], plan["sl"], plan["tp1"], plan["break_even_at"]
    risk = abs(entry - plan["sl"])
    moved, best = False, entry
    for i, b in enumerate(bars_after):
        if (b["high"] >= sl) if sell else (b["low"] <= sl):
            return {"outcome": "breakeven" if moved else "stop", "exit": sl, "bars": i + 1}
        if (b["low"] <= tp1) if sell else (b["high"] >= tp1):
            return {"outcome": "tp1", "exit": tp1, "bars": i + 1}
        if not moved and ((b["low"] <= be) if sell else (b["high"] >= be)):
            sl, moved = entry, True
        best = min(best, b["low"]) if sell else max(best, b["high"])
        move = abs(best - entry)
        if fib and risk and move >= risk and abs(b["close"] - best) > fib * move:
            return {"outcome": "exit_fib", "exit": b["close"], "bars": i + 1}
        if i < len(e13_after) and ((b["close"] > e13_after[i]) if sell else (b["close"] < e13_after[i])):
            return {"outcome": "exit_13", "exit": b["close"], "bars": i + 1}
    return {"outcome": "open", "exit": bars_after[-1]["close"] if bars_after else entry,
            "bars": len(bars_after)}
