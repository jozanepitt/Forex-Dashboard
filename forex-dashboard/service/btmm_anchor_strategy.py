"""BTMM Level 3 anchor scanner: the 12, 21 and 22 setups. Alerts only.

Kar (T6aANhLD_88) names these by two digits. The first is yesterday's shape
at its high or low. The second is today's test of that high or low (the
blue tracer), counted the same way.

  2 = two legs: an M at a high, a W at a low, or a head and shoulders.
  1 = one leg: an A top, a V bottom, a half-bat.
  12 = one leg yesterday, M or W today.
  21 = M/W/H&S yesterday, half-bat today.
  22 = M or W yesterday and another one today.
  A sell is the pattern at the highs. A buy is the pattern at the lows.

The peak test is the one Kar says Steve taught: 3 to 5 days, at least
3 times ADR, EMAs fanned, TDI overbought or oversold. The trigger is a
close back through the 13 EMA with TDI (RSI 13, band 34, line 2, signal 7)
crossing that way, or a shark fin. Too close to the 50 EMA (water) waits.
A later close back through the 13, 50 or 200, or a close beyond the
extreme, kills it.

This is an untested watchlist. It does not place a trade.

Defaults for things the videos leave UNKNOWN (each one is a constant):

* No clock filter. Kar never gives a session time for 12/21/22. The day
  boundary is 00:00 SAST, which is the dashboard trading day. A short
  Sunday reopen is not "yesterday" (same 40-bar rule as the HOD/LOB scan).
* "Within one point" of the tracer was never defined. A test counts when
  price reaches the tracer within NEAR_PIPS, or pokes through it.
* "Too close to the water" was never a distance. WATER_PIPS is that gap.
* The 21 stop in the Q&A is 3 to 5 pips beyond the shift candle, and how
  that scales is unknown. STOP_PIPS (5) beyond the extreme, in this
  symbol's pip, is the stop idea.
* The only spoken target is the Mayo (200 EMA) on a 12 example. If that
  line is on the trade side of the trigger, it is the target. Otherwise
  the target is the far side of yesterday's range.
* H1 is not scanned. Kar says the same anchors appear there. M15 is the
  frame he calls the working one, and a second frame would double alerts.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import instruments
import market_hours
import session_clock
from btmm_core import _rsi, _sma, calc_ema
from hodlob_strategy import HODLOB_UNIVERSE, PRIOR_DAY_MIN_BARS

# Same book as HOD/LOB: majors, crosses, exotics, metals, indices, oil.
ANCHOR_UNIVERSE = list(HODLOB_UNIVERSE)
M15_LIMIT = 3200

# Peak (Kar / Steve, as he lists it).
PEAK_DAYS_MIN = 3
PEAK_DAYS_MAX = 5
ADR_MULT = 3.0
ADR_DAYS = 14
TDI_OB = 68.0             # dashboard overbought; "usually overbought" was not a number
TDI_OS = 32.0
TDI_BAND_MULT = 1.6185    # same volatility-band multiple as btmm_core

# Pattern shape. UNKNOWN distances, labelled here.
SWING_WING = 3            # bars each side of a swing (45 minutes on M15)
NEAR_PIPS = 8             # how far short of the tracer still counts as a test
VALLEY_PIPS = 5           # dip between the two legs of an M or W
WATER_PIPS = 10           # closer than this to the 50 EMA, and the trigger waits
STOP_PIPS = 5             # beyond the extreme; Q&A said 3 to 5 on one example

FRESH_SECONDS = 20 * 60
RECENT_SECONDS = 3 * 86400
_SAST = timezone(timedelta(hours=2))


def _pip(symbol: str, price: float) -> float:
    return instruments.pip_size(symbol, price)


def _days(bars: list[dict]) -> tuple[list[int], dict[int, list[int]]]:
    order: list[int] = []
    groups: dict[int, list[int]] = {}
    for i, b in enumerate(bars):
        if b.get("ts_utc") is None:
            continue
        day = session_clock.vwap_day_start(int(b["ts_utc"]))
        if day not in groups:
            groups[day] = []
            order.append(day)
        groups[day].append(i)
    return order, groups


def _usable(order, groups):
    """Sessions long enough to be a real day. A Sunday reopen is not one."""
    return [d for d in order if len(groups[d]) >= PRIOR_DAY_MIN_BARS]


def _adr(bars, days) -> Optional[float]:
    ranges = []
    for d in days[-ADR_DAYS:]:
        hh = max(bars[i]["high"] for i in d)
        ll = min(bars[i]["low"] for i in d)
        if hh > ll:
            ranges.append(hh - ll)
    if len(ranges) < PEAK_DAYS_MIN:
        return None
    return sum(ranges) / len(ranges)


def _peak(bars, days, adr: float) -> Optional[dict]:
    """3 to 5 days and at least 3x ADR, into yesterday's high (sell) or low (buy)."""
    if len(days) < PEAK_DAYS_MIN:
        return None
    window = days[-PEAK_DAYS_MAX:]
    yday = window[-1]
    y_high = max(bars[i]["high"] for i in yday)
    y_low = min(bars[i]["low"] for i in yday)
    y_high_i = max(yday, key=lambda i: bars[i]["high"])
    y_low_i = min(yday, key=lambda i: bars[i]["low"])

    lows = [(min(bars[i]["low"] for i in d), n) for n, d in enumerate(window)]
    highs = [(max(bars[i]["high"] for i in d), n) for n, d in enumerate(window)]
    low_px, low_n = min(lows)
    high_px, high_n = max(highs)
    rise_days = len(window) - low_n
    drop_days = len(window) - high_n
    rise = y_high - low_px
    drop = high_px - y_low
    sell = rise_days >= PEAK_DAYS_MIN and rise >= ADR_MULT * adr and rise >= drop
    buy = drop_days >= PEAK_DAYS_MIN and drop >= ADR_MULT * adr and drop > rise
    if not sell and not buy:
        return None
    if sell:
        return {"side": "sell", "days": rise_days, "move": rise,
                "tracer": y_high, "yday": yday, "extreme_i": y_high_i,
                "far": y_low}
    return {"side": "buy", "days": drop_days, "move": drop,
            "tracer": y_low, "yday": yday, "extreme_i": y_low_i,
            "far": y_high}


def _fanned(ema5, ema13, ema50, i: int, side: str) -> bool:
    a, b, c = ema5[i], ema13[i], ema50[i]
    if side == "sell":
        return a > b > c
    return a < b < c


def _tdi(closes: list[float]) -> dict:
    rsi = _rsi(closes, 13)
    fast, slow, mid = _sma(rsi, 2), _sma(rsi, 7), _sma(rsi, 34)
    upper, lower = [], []
    for i in range(len(rsi)):
        w = rsi[max(0, i - 33):i + 1]
        mu = sum(w) / len(w)
        sd = (sum((v - mu) ** 2 for v in w) / len(w)) ** 0.5
        upper.append(mu + TDI_BAND_MULT * sd)
        lower.append(mu - TDI_BAND_MULT * sd)
    return {"rsi": rsi, "fast": fast, "slow": slow, "mid": mid,
            "upper": upper, "lower": lower}


def _swings(values: list[float], hi: bool) -> list[int]:
    out = []
    w = SWING_WING
    for i in range(w, len(values) - w):
        sample = values[i - w:i + w + 1]
        if hi and values[i] == max(sample) and values[i] > min(sample):
            out.append(i)
        if not hi and values[i] == min(sample) and values[i] < max(sample):
            out.append(i)
    return out


def _digit(bars, idxs, side: str, tracer: float, near: float, valley: float) -> tuple[int, Optional[int]]:
    """1 or 2 at this level, and the bar of the last leg. 0 if price never tests it.

    A test may stop short by `near` pips, sit on the level, or run through it
    (the stop-hunt). Two legs need a real dip or bounce between them.
    """
    sell = side == "sell"
    if sell:
        hits = [i for i in idxs if bars[i]["high"] >= tracer - near]
        swings = [i for i in _swings([bars[i]["high"] for i in idxs], True)
                  if bars[idxs[i]]["high"] >= tracer - near]
    else:
        hits = [i for i in idxs if bars[i]["low"] <= tracer + near]
        swings = [i for i in _swings([bars[i]["low"] for i in idxs], False)
                  if bars[idxs[i]]["low"] <= tracer + near]
    swings = [idxs[s] for s in swings]
    if not hits and not swings:
        return 0, None
    # The first completed pair is the pattern. Later wiggles are not a new digit.
    for a, b in zip(swings, swings[1:]):
        between = [bars[i]["high" if sell else "low"] for i in idxs if a < i < b]
        if not between:
            continue
        if sell and min(bars[a]["high"], bars[b]["high"]) - min(between) >= valley:
            return 2, b
        if (not sell) and max(between) - max(bars[a]["low"], bars[b]["low"]) >= valley:
            return 2, b
    if swings:
        return 1, swings[0]
    if hits:
        return 1, hits[0]
    return 0, None


def _tdi_ok(tdi: dict, i: int, side: str) -> bool:
    """Signal cross in the trade direction, or a shark fin back inside the band.

    The cross often prints one bar before the 13 EMA close, so a cross in the
    last three bars still counts while the fast line stays on that side.
    """
    if i < 1:
        return False
    fast, slow = tdi["fast"], tdi["slow"]
    sell = side == "sell"
    held = fast[i] < slow[i] if sell else fast[i] > slow[i]
    for j in range(max(1, i - 2), i + 1):
        crossed = (fast[j - 1] >= slow[j - 1] and fast[j] < slow[j]) if sell else (
            fast[j - 1] <= slow[j - 1] and fast[j] > slow[j])
        if crossed and held:
            return True
    band = tdi["upper"] if sell else tdi["lower"]
    was_out = any((fast[j] > band[j]) if sell else (fast[j] < band[j])
                  for j in range(max(0, i - 6), i))
    back_in = (fast[i] <= band[i]) if sell else (fast[i] >= band[i])
    return was_out and back_in and held


def _ob(tdi: dict, i: int, side: str) -> bool:
    return tdi["fast"][i] >= TDI_OB if side == "sell" else tdi["fast"][i] <= TDI_OS


def _fmt(ts: int) -> tuple[str, str]:
    utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    sast = utc.astimezone(_SAST)
    return utc.strftime("%Y-%m-%d %H:%M UTC"), sast.strftime("%Y-%m-%d %H:%M SAST")


def _bar_secs(bars, i: int) -> int:
    if i + 1 < len(bars):
        d = int(bars[i + 1]["ts_utc"]) - int(bars[i]["ts_utc"])
        if 60 <= d <= 3600:
            return d
    if i > 0:
        d = int(bars[i]["ts_utc"]) - int(bars[i - 1]["ts_utc"])
        if 60 <= d <= 3600:
            return d
    return 900


def find_setups(symbol: str, bars: list[dict]) -> list[dict]:
    """12/21/22 anchors in `bars` (oldest first). FULL and FORMING only."""
    bars = [b for b in bars if b.get("ts_utc") is not None and b.get("close") is not None]
    if len(bars) < 200:
        return []
    order, groups = _days(bars)
    usable = _usable(order, groups)
    if len(usable) < PEAK_DAYS_MIN + 1:
        return []
    closes = [b["close"] for b in bars]
    ema5, ema13 = calc_ema(closes, 5), calc_ema(closes, 13)
    ema50, ema200 = calc_ema(closes, 50), calc_ema(closes, 200)
    tdi = _tdi(closes)
    pip = _pip(symbol, bars[-1]["close"])
    last_ts = int(bars[-1]["ts_utc"])
    found = []

    # Today is the last usable day only when it is also the last day in the
    # series. Older days are scanned too, so a test can sit a setup in the past.
    for n in range(PEAK_DAYS_MIN, len(usable)):
        today = usable[n]
        history = usable[:n]
        adr = _adr(bars, [groups[d] for d in history])
        if not adr:
            continue
        peak = _peak(bars, [groups[d] for d in history], adr)
        if not peak:
            continue
        side = peak["side"]
        if not _fanned(ema5, ema13, ema50, peak["extreme_i"], side):
            continue
        if not _ob(tdi, peak["extreme_i"], side):
            continue
        near, valley = NEAR_PIPS * pip, VALLEY_PIPS * pip
        water = WATER_PIPS * pip
        y_digit, _ = _digit(bars, peak["yday"], side, peak["tracer"], near, valley)
        t_digit, leg_i = _digit(bars, groups[today], side, peak["tracer"], near, valley)
        if y_digit not in (1, 2) or t_digit not in (1, 2) or leg_i is None:
            continue
        label = f"{y_digit}{t_digit}"
        if label not in ("12", "21", "22"):
            continue

        trigger = _trigger(bars, ema13, tdi, groups[today], leg_i, side)
        extreme = _extreme(bars, groups[today], peak, side)
        if trigger is not None and _killed(bars, ema13, ema50, ema200, trigger, extreme, side):
            continue
        grade = None
        wait = False
        if trigger is not None:
            wait = abs(bars[trigger]["close"] - ema50[trigger]) < water
            grade = "FORMING" if wait else "FULL"
        elif int(bars[groups[today][-1]]["ts_utc"]) >= last_ts - RECENT_SECONDS:
            grade = "FORMING"
            trigger = groups[today][-1]
        if grade is None:
            continue

        entry = float(bars[trigger]["close"])
        stop = extreme + STOP_PIPS * pip if side == "sell" else extreme - STOP_PIPS * pip
        target = _target(entry, ema200[trigger], peak["far"], side)
        ts = int(bars[trigger]["ts_utc"])
        utc_s, sast_s = _fmt(ts + _bar_secs(bars, trigger))
        setup = "SELL" if side == "sell" else "BUY"
        found.append({
            "symbol": symbol,
            "timeframe": "M15",
            "label": label,
            "setup": setup,
            "direction_label": f"{label} {'sell' if side == 'sell' else 'buy'}",
            "grade": grade,
            "extreme": float(extreme),
            "entry": entry,
            "sl": float(stop),
            "tp1": float(target) if target is not None else None,
            "invalidation": float(extreme),
            "trigger_ts": ts,
            "time_utc": utc_s,
            "time_sast": sast_s,
            "fresh": last_ts - ts <= FRESH_SECONDS,
            "days": peak["days"],
            "move_x_adr": round(peak["move"] / adr, 2),
            "notes": _notes(label, side, grade, wait),
        })
    return found


def _trigger(bars, ema13, tdi, idxs, leg_i, side) -> Optional[int]:
    """First close back through the 13 after the last leg, with TDI agreeing."""
    for i in idxs:
        if i <= leg_i:
            continue
        sell = side == "sell"
        through = bars[i]["close"] < ema13[i] if sell else bars[i]["close"] > ema13[i]
        if through and _tdi_ok(tdi, i, side):
            return i
    return None


def _extreme(bars, idxs, peak, side) -> float:
    if side == "sell":
        return max(peak["tracer"], max(bars[i]["high"] for i in idxs))
    return min(peak["tracer"], min(bars[i]["low"] for i in idxs))


def _killed(bars, ema13, ema50, ema200, trigger, extreme, side) -> bool:
    """A close back through 13/50/200 after we were through it, or beyond the extreme."""
    sell = side == "sell"
    for i in range(trigger + 1, len(bars)):
        close = bars[i]["close"]
        if sell and close > extreme:
            return True
        if not sell and close < extreme:
            return True
        for ema in (ema13, ema50, ema200):
            if sell and bars[trigger]["close"] < ema[trigger] and close > ema[i]:
                return True
            if not sell and bars[trigger]["close"] > ema[trigger] and close < ema[i]:
                return True
    return False


def _target(entry: float, ema200: float, far: float, side: str) -> float:
    """Mayo (200 EMA) when it sits on the trade side; else yesterday's far side."""
    if side == "sell" and ema200 < entry:
        return ema200
    if side == "buy" and ema200 > entry:
        return ema200
    return far


def _notes(label: str, side: str, grade: str, wait: bool) -> str:
    way = "sell" if side == "sell" else "buy"
    if grade == "FULL":
        why = "close back through the 13 with TDI"
    elif wait:
        why = "too close to the 50 EMA, wait"
    else:
        why = "structure only, trigger not ready"
    return f"{label} {way}, {grade}. {why}."


def _quiet(symbol: str) -> dict:
    return {"symbol": symbol, "setup": "NONE", "grade": None, "timeframe": "M15",
            "fresh": False, "label": None}


def analyze_universe(candles_by_pair: dict) -> dict:
    pairs, setups = [], []
    for sym in ANCHOR_UNIVERSE:
        bundle = candles_by_pair.get(sym) or {}
        try:
            raw = market_hours.drop_closed_market_bars(sym, list(bundle.get("m15") or []))
            found = find_setups(sym, raw)
        except Exception as e:  # noqa: BLE001 — one symbol must not blank the scan
            row = _quiet(sym)
            row["error"] = str(e)
            pairs.append(row)
            continue
        last = 0
        m15 = bundle.get("m15") or []
        if m15 and m15[-1].get("ts_utc"):
            last = int(m15[-1]["ts_utc"])
        recent = [s for s in found if last - s["trigger_ts"] <= RECENT_SECONDS]
        setups.extend(recent)
        latest = max(recent, key=lambda s: s["trigger_ts"]) if recent else None
        pairs.append(latest if latest and latest.get("setup") in ("BUY", "SELL") else _quiet(sym))
    setups.sort(key=lambda s: s["trigger_ts"], reverse=True)
    return {
        "pairs": pairs,
        "setups": setups,
        "scanned": len(pairs),
        "full": sum(1 for s in setups if s["grade"] == "FULL"),
        "forming": sum(1 for s in setups if s["grade"] == "FORMING"),
    }
