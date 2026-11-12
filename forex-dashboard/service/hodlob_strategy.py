"""HOD/LOB and LOD/HOB setup scanner (Xtrillions / BTMM market-maker pattern).

HOD/LOB is a long. Through Asia, price hugs the high of day and the Bollinger
Bands are tight. Around the morning stop-hunt, a spike closes below the
lower band and breaks the Asian low while TDI makes a lower low than price
(divergence). The entry is the first close back inside the bands. The stop
sits beyond the spike. LOD/HOB is the mirror, a short.

This is a setup alert, not a signal. The research pass on this pattern was
negative after costs. Nothing here places a trade.

Time is a preference, not a hard filter. A spike slightly before or after the
7-8-9 window can still grade. Grade A needs every core and soft check. Grade B
is the core pattern with exactly one soft check missing. Anything weaker is
not alerted.

Thresholds the videos left open (each one is a constant below):

* Price Bollinger is 20, 2.0, population standard deviation, on the close.
  The videos say "the bands" and the dashboard never set a price-BB length.
  20/2 is the chart default and the setting used in the research engine.
  TDI is the dashboard's 13, 34, 2, 7: RSI 13, volatility band SMA 34
  (1.6185 sd, same as btmm_core), green price line SMA 2, red signal SMA 7.
  Divergence reads the green line. The 34-band is on the chart; the videos
  compare "TDI" being lower than yesterday, which is that price line.
* "Near the high of day" was never a number. The Asia close must sit in the
  top 25% of the box from min(prior-day low, Asia low) to max(prior-day high,
  Asia high). The bottom 25% is the short. Outside the middle 20% (above 60%
  or below 40%) is a directional lean: enough for a B if the hug itself
  fails, not enough for an A. A close in the middle 20% is not a setup.
* "A squeeze" was never a number. The Asian range must be at or under the
  median of the previous Asia sessions (up to 20, at least 8). Up to 1.25x
  that median is a weak squeeze: the check fails, and it can be the one soft
  miss that makes a B.
* The 7-8-9 clock is SAST (Africa/Johannesburg, UTC+2, no daylight saving).
  Jo-zane, 2026-11-12. Asia is 00:00-06:00 SAST, so the box is closed before
  the stop-hunt. Spikes are scanned from 06:00 to 11:00 SAST (by 11 he gives
  up). The preferred window, which changes the grade and does not delete a
  setup, is 07:00-09:45 SAST. These hours are ASIA_END_HM, SEARCH_*_HM and
  PREFER_*_HM below. All of them are SAST.
* Divergence is the one-day reading: at the spike the green TDI line is
  beyond the whole previous day's TDI, and price has not taken that day's
  high or low. "Three days divergence" is mentioned and never defined, so
  it is not a separate rule.
* Re-entry is the first close back inside the band. It has to happen within
  8 bars and the candle must close by 11:00 SAST, whichever comes first.
  The time on the alert is that candle's close, not its open.
* The stop is the spike extreme (the worst price from the spike bar through
  the re-entry bar) plus a buffer of one pip or 0.10 ATR, whichever is
  larger. "Midway past the spike" was not a measurable level.
* The target reference is the opposite Bollinger band at the re-entry bar.
  The far side of the Asian range is reported next to it. The videos' "G-spot"
  was never defined, and this scanner does not invent a 2R order.
* M15 is the main timeframe. M5 is scanned for the pairs the scheduler
  already fetches at 5 minutes. One Discord alert per symbol, direction,
  trading day and timeframe, so an earlier M5 B cannot block the M15 alert.
* Monday's prior day is the last session with at least PRIOR_DAY_MIN_BARS
  bars. From March to November Exness reopens Sunday at 21:00 UTC, and those
  few bars would otherwise be "yesterday".
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Optional

import instruments
import market_hours
import session_clock
from btmm_core import _rsi, _sma
from config import PRIORITY_PAIRS

# Exotics the videos trade (USDZAR is the one they call the driving pair).
# Included only on this scanner. A symbol the broker does not offer simply
# has no candles and produces no setup.
EXOTICS = [
    "USD/ZAR", "EUR/ZAR", "GBP/ZAR",
    "USD/TRY", "EUR/TRY",
    "USD/MXN",
    "USD/SEK", "EUR/SEK",
    "USD/NOK", "EUR/NOK",
    "USD/PLN", "EUR/PLN",
    "USD/HKD",
    "USD/SGD", "EUR/SGD",
]
# G10 crosses missing from PRIORITY_PAIRS, plus GBP/NOK and GBP/SEK.
# The USD/EUR/GBP/AUD/NZD/CAD/CHF/JPY cross matrix is otherwise already in
# PRIORITY_PAIRS or EUR/CAD. Oil is already on other scanners.
CROSSES = [
    "CHF/JPY", "NZD/JPY",
    "GBP/CAD", "EUR/NZD", "GBP/NZD",
    "NZD/CAD", "NZD/CHF", "CAD/CHF",
    "GBP/NOK", "GBP/SEK",
]
_ALSO = ["USOIL", "UKOIL", "EUR/CAD"] + CROSSES

def _dedupe(symbols: list[str]) -> list[str]:
    out: list[str] = []
    for sym in symbols:
        if sym not in out:
            out.append(sym)
    return out


HODLOB_UNIVERSE = _dedupe(list(PRIORITY_PAIRS) + EXOTICS + _ALSO)
# 5-minute bars are fetched for the main book only. Exotics stay on M15 so
# the extra fetch stays a handful of symbols, not a second full book.
M5_SYMBOLS = list(PRIORITY_PAIRS)
EXTRA_M15 = [s for s in HODLOB_UNIVERSE if s not in PRIORITY_PAIRS]

M15_LIMIT = 3200          # ~22 trading days of M15, enough for the squeeze median
# Two days of M5, the same size the old M5 fetch used. A longer pull would
# spend Twelve Data credits whenever MT5 is down. With only ~2 Asia sessions
# in that window the squeeze check fails closed, so an otherwise complete M5
# setup grades B at best. M15 is the timeframe that can grade A.
M5_LIMIT = 576

# --- indicator settings (see the module docstring) ---
BB_PERIOD = 20
BB_MULT = 2.0
TDI_RSI = 13
TDI_FAST = 2              # green price line; this is the line divergence reads
TDI_SLOW = 7              # red signal line, on the chart, not a gate
TDI_BAND = 34             # yellow baseline / volatility-band length

# --- session, all SAST (Africa/Johannesburg, UTC+2, no DST) ---
# Jo-zane, 2026-11-12: "7-8-9" is his clock, not London. One place for the hours.
ASIA_END_HM = (6, 0)       # Asia box is 00:00-06:00 SAST, closed before the stop-hunt
SEARCH_START_HM = (6, 0)   # first spike we will look at
SEARCH_END_HM = (11, 0)    # by 11:00 he gives up; a bar that opens at 11:00 is not scanned
PREFER_START_HM = (7, 0)   # preferred grade window, not a hard filter
PREFER_END_HM = (9, 45)
ASIA_MIN_BARS = 16         # under four hours of M15 is not an Asia session
# A day shorter than this is not "yesterday". The Sunday 21:00 UTC reopen
# (US daylight-saving months) is 3-4 M15 bars and must not be Monday's prior day.
PRIOR_DAY_MIN_BARS = 40

# --- vague pattern thresholds ---
HUG_PASS = 0.75           # top/bottom quarter of the prior-day + Asia box
HUG_LEAN = 0.60           # outside the middle 20% (40-60). A lean, not a pass.
SQUEEZE_LOOKBACK = 20
SQUEEZE_MIN_SAMPLES = 8
WEAK_SQUEEZE_MULT = 1.25
REENTRY_BARS = 8
STOP_ATR_FRAC = 0.10
ATR_PERIOD = 14
FRESH_SECONDS = 20 * 60   # still alertable for one scheduler cycle after the re-entry
RECENT_SECONDS = 3 * 86400

_SAST = timezone(timedelta(hours=2))


def _sast_ts(day_start: int, hour: int, minute: int) -> int:
    """`hour:minute` SAST. `day_start` is already 00:00 SAST (22:00 UTC)."""
    return day_start + hour * 3600 + minute * 60


def _fmt_clock(ts: int) -> tuple[str, str]:
    utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    sast = utc.astimezone(_SAST)
    return (
        utc.strftime("%Y-%m-%d %H:%M UTC"),
        sast.strftime("%Y-%m-%d %H:%M SAST"),
    )


def bollinger(closes: list[float], period: int = BB_PERIOD, mult: float = BB_MULT):
    """Mid / upper / lower on the close. None until `period` closes exist."""
    n = len(closes)
    mid = [None] * n
    up = [None] * n
    lo = [None] * n
    if n < period:
        return mid, up, lo
    # Running sum and sum of squares so this stays a single pass.
    run = sum(closes[:period])
    run2 = sum(c * c for c in closes[:period])
    for i in range(period - 1, n):
        if i >= period:
            run += closes[i] - closes[i - period]
            run2 += closes[i] * closes[i] - closes[i - period] * closes[i - period]
        mu = run / period
        var = max(0.0, run2 / period - mu * mu)
        sd = var ** 0.5
        mid[i] = mu
        up[i] = mu + mult * sd
        lo[i] = mu - mult * sd
    return mid, up, lo


def tdi_lines(closes: list[float]) -> dict:
    """Dashboard TDI: RSI(13), green SMA(2), red SMA(7), yellow SMA(34)."""
    rsi = _rsi(closes, TDI_RSI)
    return {"rsi": rsi, "fast": _sma(rsi, TDI_FAST), "slow": _sma(rsi, TDI_SLOW),
            "base": _sma(rsi, TDI_BAND)}


def _atr(bars: list[dict], i: int, n: int = ATR_PERIOD) -> float:
    start = max(1, i - n + 1)
    total = 0.0
    count = 0
    for k in range(start, i + 1):
        prev = bars[k - 1]["close"]
        total += max(bars[k]["high"] - bars[k]["low"],
                     abs(bars[k]["high"] - prev), abs(bars[k]["low"] - prev))
        count += 1
    if count == 0:
        return bars[i]["high"] - bars[i]["low"]
    return total / count


def _grade(soft_fails: list[str]) -> Optional[str]:
    """A = no soft miss. B = exactly one. More than one is not a setup we alert."""
    if not soft_fails:
        return "A"
    if len(soft_fails) == 1:
        return "B"
    return None


def _check(name: str, ok: bool, detail: str) -> dict:
    return {"name": name, "pass": bool(ok), "detail": detail}


def find_setups(symbol: str, bars: list[dict], timeframe: str = "M15") -> list[dict]:
    """Completed HOD/LOB and LOD/HOB setups in `bars` (oldest first).

    Only grade A and B are returned. The core pattern (close outside the
    band, Asian range broken, close back inside) is required for either grade.
    """
    bars = [b for b in bars if b.get("ts_utc") is not None and b.get("close") is not None]
    if len(bars) < BB_PERIOD + ASIA_MIN_BARS:
        return []

    closes = [b["close"] for b in bars]
    _mid, upper, lower = bollinger(closes)
    tdi = tdi_lines(closes)["fast"]

    order: list[int] = []
    groups: dict[int, list[int]] = {}
    for i, b in enumerate(bars):
        day = session_clock.vwap_day_start(int(b["ts_utc"]))
        if day not in groups:
            groups[day] = []
            order.append(day)
        groups[day].append(i)

    asia_hist: list[float] = []
    found: list[dict] = []
    last_ts = int(bars[-1]["ts_utc"])

    for di, day in enumerate(order):
        idxs = groups[day]
        asia_end = _sast_ts(day, *ASIA_END_HM)
        search_from = _sast_ts(day, *SEARCH_START_HM)
        search_to = _sast_ts(day, *SEARCH_END_HM)
        prefer_from = _sast_ts(day, *PREFER_START_HM)
        prefer_to = _sast_ts(day, *PREFER_END_HM)

        asia = [i for i in idxs if int(bars[i]["ts_utc"]) < asia_end]
        if len(asia) >= ASIA_MIN_BARS:
            a_high = max(bars[i]["high"] for i in asia)
            a_low = min(bars[i]["low"] for i in asia)
            a_close = bars[asia[-1]]["close"]
            a_range = a_high - a_low
        else:
            a_high = a_low = a_close = a_range = None

        prev_idx = _prior_day(order, groups, di)

        # Record the range even when this day has no setup, so later days
        # have a median to compare against. Today is not part of its own median.
        samples = asia_hist[-SQUEEZE_LOOKBACK:]
        if a_range is not None:
            asia_hist.append(a_range)

        if a_range is None or not prev_idx:
            continue

        pd_high = max(bars[i]["high"] for i in prev_idx)
        pd_low = min(bars[i]["low"] for i in prev_idx)
        pd_tdi_min = min(tdi[i] for i in prev_idx)
        pd_tdi_max = max(tdi[i] for i in prev_idx)
        top, bot = max(pd_high, a_high), min(pd_low, a_low)
        pos = (a_close - bot) / (top - bot) if top > bot else 0.5

        if pos > HUG_LEAN:
            direction = 1
        elif pos < 1.0 - HUG_LEAN:
            direction = -1
        else:
            # Middle 20% of the box. No hug and no lean, so this is not a B.
            continue

        spike = _first_spike(bars, idxs, lower, upper, a_low, a_high,
                             search_from, search_to, direction)
        if spike is None:
            continue
        k, direction = spike
        bar_secs = _bar_secs(bars, k, timeframe)
        entry = _reentry(bars, lower, upper, k, direction, search_to, bar_secs)
        if entry is None:
            continue

        long = direction > 0
        hug_ok = pos >= HUG_PASS if long else pos <= (1.0 - HUG_PASS)
        hug_detail = f"Asia close is {pos:.0%} up the prior-day + Asia box"
        if hug_ok:
            hug_detail += ", in the outer quarter"
        else:
            hug_detail += ", weak hug"

        squeeze_ok, squeeze_detail = _squeeze(a_range, samples)

        if long:
            div_ok = tdi[k] < pd_tdi_min and bars[k]["low"] > pd_low
            div_detail = (f"TDI {tdi[k]:.1f} vs yesterday's low {pd_tdi_min:.1f}; "
                          f"spike low {'held above' if bars[k]['low'] > pd_low else 'took'} "
                          f"yesterday's low")
        else:
            div_ok = tdi[k] > pd_tdi_max and bars[k]["high"] < pd_high
            div_detail = (f"TDI {tdi[k]:.1f} vs yesterday's high {pd_tdi_max:.1f}; "
                          f"spike high {'held below' if bars[k]['high'] < pd_high else 'took'} "
                          f"yesterday's high")

        spike_ts = int(bars[k]["ts_utc"])
        window_ok = prefer_from <= spike_ts <= prefer_to
        spike_clock = datetime.fromtimestamp(spike_ts, tz=_SAST).strftime("%H:%M")
        window_detail = f"spike at {spike_clock} SAST" + (
            "" if window_ok else ", outside 07:00-09:45")

        band = lower[k] if long else upper[k]
        outside_detail = (f"close {bars[k]['close']:.5f} "
                          f"{'below' if long else 'above'} the {'lower' if long else 'upper'} band "
                          f"{band:.5f}")
        broke = bars[k]["low"] < a_low if long else bars[k]["high"] > a_high
        break_detail = (f"spike {'low' if long else 'high'} "
                        f"{'broke' if broke else 'did not break'} the Asian "
                        f"{'low' if long else 'high'} {a_low if long else a_high:.5f}")
        re_detail = f"first close back inside at {bars[entry]['close']:.5f}"

        checks = [
            _check("Hug", hug_ok, hug_detail),
            _check("Squeeze", squeeze_ok, squeeze_detail),
            _check("Spike outside band", True, outside_detail),
            _check("Asian range break", broke, break_detail),
            _check("TDI divergence", div_ok, div_detail),
            _check("Re-entry", True, re_detail),
            _check("SAST window", window_ok, window_detail),
        ]
        # Core is the spike, the range break and the re-entry. The spike and
        # the re-entry are true by the time we get here; a range break that
        # somehow failed is not a setup.
        if not broke:
            continue
        soft_fails = [c["name"] for c in checks
                      if c["name"] in ("Hug", "Squeeze", "TDI divergence", "SAST window")
                      and not c["pass"]]
        grade = _grade(soft_fails)
        if grade is None:
            continue

        entry_px = float(bars[entry]["close"])
        if long:
            extreme = min(bars[i]["low"] for i in range(k, entry + 1))
            stop = extreme - _buffer(symbol, bars, entry, entry_px)
            target = upper[entry]
            opposite = a_high
            pattern, setup, label = "HOD/LOB", "BUY", "HOD/LOB long"
        else:
            extreme = max(bars[i]["high"] for i in range(k, entry + 1))
            stop = extreme + _buffer(symbol, bars, entry, entry_px)
            target = lower[entry]
            opposite = a_low
            pattern, setup, label = "LOD/HOB", "SELL", "LOD/HOB short"

        entry_ts = int(bars[entry]["ts_utc"])
        # The setup completes when the re-entry candle closes, one bar later.
        utc_s, sast_s = _fmt_clock(entry_ts + bar_secs)
        found.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "setup": setup,
            "pattern": pattern,
            "direction_label": label,
            "grade": grade,
            "entry": entry_px,
            "sl": float(stop),
            "tp1": None if target is None else float(target),
            "opposite": float(opposite),
            "spike": float(extreme),
            "spike_ts": spike_ts,
            "entry_ts": entry_ts,
            "time_utc": utc_s,
            "time_sast": sast_s,
            "fresh": last_ts - entry_ts <= FRESH_SECONDS,
            "conditions": checks,
            "soft_miss": soft_fails,
            "notes": _notes(label, grade, soft_fails),
        })
    return found


def _first_spike(bars, idxs, lower, upper, a_low, a_high,
                 search_from, search_to, direction):
    """First bar in the search window that closes outside the band and breaks Asia.

    `direction` 1 or -1 locks the side the hug picked. 0 tries a down-spike
    first on each bar, then an up-spike, and takes whichever prints first.
    """
    for i in idxs:
        ts = int(bars[i]["ts_utc"])
        if ts < search_from or ts >= search_to:
            continue
        if lower[i] is None:
            continue
        if direction >= 0 and bars[i]["close"] < lower[i] and bars[i]["low"] < a_low:
            return i, 1
        if direction <= 0 and bars[i]["close"] > upper[i] and bars[i]["high"] > a_high:
            return i, -1
    return None


def _prior_day(order: list[int], groups: dict, di: int) -> list[int]:
    """Bars of the last real session before this day.

    A Sunday 21:00 UTC reopen is only a few bars. It is not Friday and it is
    not Monday, so it is not yesterday. Monday uses Friday.
    """
    for j in range(di - 1, -1, -1):
        idxs = groups[order[j]]
        if len(idxs) >= PRIOR_DAY_MIN_BARS:
            return idxs
    return []


def _bar_secs(bars: list[dict], i: int, timeframe: str) -> int:
    for a, b in ((i, i + 1), (i - 1, i)):
        if 0 <= a and b < len(bars):
            d = int(bars[b]["ts_utc"]) - int(bars[a]["ts_utc"])
            if 60 <= d <= 3600:
                return d
    return 300 if timeframe == "M5" else 900


def _reentry(bars, lower, upper, spike: int, direction: int,
             deadline: int, bar_secs: int) -> Optional[int]:
    """First close back inside the band, within 8 bars and by `deadline`.

    `deadline` is 11:00 SAST. The candle counts only if it has closed by then,
    so a bar that opens at 10:45 (closes at 11:00) is in, and one that opens
    at 11:00 is not. The 8-bar cap and the clock apply together.
    """
    last = min(len(bars) - 1, spike + REENTRY_BARS)
    for j in range(spike + 1, last + 1):
        if int(bars[j]["ts_utc"]) + bar_secs > deadline:
            return None
        if direction > 0 and lower[j] is not None and bars[j]["close"] > lower[j]:
            return j
        if direction < 0 and upper[j] is not None and bars[j]["close"] < upper[j]:
            return j
    return None


def _squeeze(a_range: float, samples: list[float]) -> tuple[bool, str]:
    if len(samples) < SQUEEZE_MIN_SAMPLES:
        return False, f"only {len(samples)} prior Asia sessions (need {SQUEEZE_MIN_SAMPLES})"
    med = median(samples)
    if a_range <= med:
        return True, f"Asia range {a_range:.5f} at or under the median {med:.5f}"
    if a_range <= med * WEAK_SQUEEZE_MULT:
        return False, f"weak squeeze, Asia range {a_range:.5f} vs median {med:.5f}"
    return False, f"no squeeze, Asia range {a_range:.5f} vs median {med:.5f}"


def _buffer(symbol: str, bars, i: int, price: float) -> float:
    pip = instruments.pip_size(symbol, price)
    return max(pip, STOP_ATR_FRAC * _atr(bars, i))


def _notes(label: str, grade: str, soft_fails: list[str]) -> str:
    if soft_fails:
        return f"{label} grade {grade}. Soft check missed: {soft_fails[0]}."
    return f"{label} grade {grade}. Hug, squeeze, band spike, Asia break, TDI divergence, re-entry, SAST window."


def _quiet(symbol: str) -> dict:
    return {"symbol": symbol, "setup": "NONE", "grade": None, "timeframe": "M15",
            "fresh": False, "pattern": None, "direction_label": None}


def analyze_universe(candles_by_pair: dict) -> dict:
    """One row per symbol, plus the recent A/B setups across M15 and M5."""
    pairs = []
    setups = []
    for sym in HODLOB_UNIVERSE:
        bundle = candles_by_pair.get(sym) or {}
        try:
            found = []
            m15 = market_hours.drop_closed_market_bars(sym, list(bundle.get("m15") or []))
            found.extend(find_setups(sym, m15, "M15"))
            if bundle.get("m5"):
                m5 = market_hours.drop_closed_market_bars(sym, list(bundle["m5"]))
                found.extend(find_setups(sym, m5, "M5"))
        except Exception as e:  # noqa: BLE001 — one bad symbol must not blank the scan
            row = _quiet(sym)
            row["error"] = str(e)
            pairs.append(row)
            continue
        last_ts = _last_ts(bundle)
        recent = [s for s in found if last_ts - s["entry_ts"] <= RECENT_SECONDS]
        setups.extend(recent)
        latest = _latest(recent) or _quiet(sym)
        pairs.append(latest if latest.get("setup") in ("BUY", "SELL") else _quiet(sym))

    setups.sort(key=lambda s: s["entry_ts"], reverse=True)
    return {
        "pairs": pairs,
        "setups": setups,
        "scanned": len(pairs),
        "grade_a": sum(1 for s in setups if s["grade"] == "A"),
        "grade_b": sum(1 for s in setups if s["grade"] == "B"),
        "buys": sum(1 for s in setups if s["setup"] == "BUY"),
        "sells": sum(1 for s in setups if s["setup"] == "SELL"),
    }


def _last_ts(bundle: dict) -> int:
    best = 0
    for key in ("m15", "m5"):
        bars = bundle.get(key) or []
        if bars and bars[-1].get("ts_utc"):
            best = max(best, int(bars[-1]["ts_utc"]))
    return best


def _latest(rows: list[dict]) -> Optional[dict]:
    if not rows:
        return None
    # Prefer an M15 reading of the same moment; the list is not pre-sorted.
    return max(rows, key=lambda s: (s["entry_ts"], 1 if s["timeframe"] == "M15" else 0))
