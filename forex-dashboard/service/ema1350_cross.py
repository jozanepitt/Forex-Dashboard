"""BTMM 13/50 EMA cross — a standalone M15 check (user request 2026-10-20).

Lives beside the VWAP+9EMA scanner but is NOT part of its checklist: it is
evaluated on its own and can fire on its own. Rules, as confirmed with the user:

  Trigger   The 13 EMA crosses the 50 EMA on M15, confirmed by the FIRST candle
            (within CONFIRM_MAX_BARS of the cross) whose whole body (open and
            close) is beyond the 50 EMA on the cross side. Wicks may touch it.
  Anchor    Required. Before the cross (pattern within PATTERN_LOOKBACK_BARS)
            price formed a W (longs) / M (shorts), or a half batman: the same
            reversal with the second leg missing (MMM Notes: "an M or W
            formation would normally occur but there has been an adequate
            build-up of volume already"). The pattern's extreme must be the
            extreme of the lookback window, i.e. the low/high the cross is
            reversing from. No anchor, no signal.
  Divergence  W/M only: RSI(13) (the TDI's RSI line) makes a higher low at leg
            2 (by DIV_MIN_RSI points) while price makes an equal-or-lower low,
            within 0.1 ATR (mirrored for M).
  VWAP      Confluence: entry on the trade side of the daily VWAP (00:00 SAST
            anchor, matching TradingView) -- above it for BUY, below for SELL.
  Grade     A+ = pattern extreme at the LOD/HOD, previous day's low/high or
            previous week's low/high, AND RSI divergence; A = previous day's/
            week's low/high without divergence; B = the rest. VWAP side and
            plain LOD/HOD are confluence only (see grade_setup).

Per the notes the 50 EMA is the "balance line" (intraday trend) and the
13/50 crossover is confirmation that follows a reversal pattern, which is why
the pattern is the anchor and the cross is only the trigger.
UNVALIDATED: never backtested.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from btmm_core import _rsi, calc_ema
from config import PRIORITY_PAIRS
from tdi_cycle_123 import _atr, _prev_week_hlc

EMA1350_UNIVERSE = list(PRIORITY_PAIRS)
M15_FETCH_LIMIT = 400        # 50 EMA fully seeded + 48-bar pattern window, with room to spare
D1_FETCH_LIMIT = 30          # previous day + previous week
FAST, SLOW = 13, 50
MIN_BARS = 60
CONFIRM_MAX_BARS = 4         # confirmation candle must close within 4 bars (1h) of the cross
PATTERN_LOOKBACK_BARS = 48   # 12h of M15 before the cross
FIRST_LEG_ATR = 1.5          # the move into the pattern must be at least this many ATRs
LEG_MATCH_ATR = 1.0          # W/M legs within this many ATRs of the extreme
NECK_MIN_ATR = 1.0           # neckline at least this far off the legs
DIV_MIN_RSI = 2.0            # RSI points the second leg must improve by
ORIGIN_TOL_ATR = 0.5         # how close the extreme must sit to a key level
STOP_BUFFER_ATR = 0.1
RR = 2.0
# Freshness limits (review 2026-10-20: the USD/JPY A+ entered 11 bars and
# 4.5 ATR after the W low, with a 4.6 ATR stop). None = off.
MAX_BARS_SINCE_EXTREME: Optional[int] = None   # cross must come within this many bars of the pattern low/high
MAX_ENTRY_ATR: Optional[float] = None          # entry no further than this many ATRs from the pattern extreme


# ── confirmation ─────────────────────────────────────────────────────────────

def find_cross_confirmation(bars: list[dict], e13: list[float], e50: list[float],
                            end_idx: int) -> Optional[dict]:
    """The 13/50 cross confirmed AT end_idx: bar end_idx is the first candle
    from the cross candle on whose whole body is beyond the 50 EMA, and the
    cross is at most CONFIRM_MAX_BARS bars earlier. None otherwise.

    The cross candle itself may confirm (user decision 2026-10-21, reverting a
    one-candle delay added 2026-10-20 that made the EUR/JPY alert 15 minutes
    later for no win/loss benefit)."""
    if end_idx < 1:
        return None
    cross_idx, direction = None, None
    for k in range(end_idx, max(0, end_idx - CONFIRM_MAX_BARS - 1), -1):
        if e13[k - 1] <= e50[k - 1] and e13[k] > e50[k]:
            cross_idx, direction = k, "bullish"
            break
        if e13[k - 1] >= e50[k - 1] and e13[k] < e50[k]:
            cross_idx, direction = k, "bearish"
            break
    if cross_idx is None:
        return None

    def body_past(j: int) -> bool:
        b = bars[j]
        if direction == "bullish":
            return min(b["open"], b["close"]) > e50[j]
        return max(b["open"], b["close"]) < e50[j]

    if not body_past(end_idx) or any(body_past(j) for j in range(cross_idx, end_idx)):
        return None
    return {"direction": direction, "cross_idx": cross_idx, "confirm_idx": end_idx}


# ── anchor pattern (computed for longs; shorts run on negated prices) ────────

def _negate(bars: list[dict]) -> list[dict]:
    return [{"ts_utc": b["ts_utc"], "open": -b["open"], "close": -b["close"],
             "high": -b["low"], "low": -b["high"]} for b in bars]


def _bullish_anchor(bars: list[dict], rsi: list[float], cross_idx: int, confirm_idx: int,
                    atr: float) -> Optional[dict]:
    """W or half batman whose low is the lowest low of the lookback window."""
    start = max(2, cross_idx - PATTERN_LOOKBACK_BARS)
    lows = [b["low"] for b in bars]
    highs = [b["high"] for b in bars]
    ext = min(lows[start:cross_idx + 1])

    def swing_low(i: int) -> bool:
        return (i + 2 <= confirm_idx and lows[i] <= min(lows[i - 2:i])
                and lows[i] <= min(lows[i + 1:i + 3]))

    swings = [i for i in range(start, cross_idx) if swing_low(i)]
    ext_idx = next((i for i in swings if lows[i] == ext), None)
    if ext_idx is None:
        return None   # the window's low is at its edge: no reversal inside the lookback

    legs = [i for i in swings if lows[i] - ext <= LEG_MATCH_ATR * atr]
    i1, i2 = legs[0], legs[-1]
    first_leg = max(highs[start:i1 + 1]) - lows[i1]
    if first_leg < FIRST_LEG_ATR * atr:
        return None

    if i2 - i1 >= 3:
        neck = max(highs[i1 + 1:i2])
        if neck - max(lows[i1], lows[i2]) >= NECK_MIN_ATR * atr:
            r1 = min(rsi[max(0, i1 - 1):i1 + 2])
            r2 = min(rsi[max(0, i2 - 1):i2 + 2])
            divergence = lows[i2] <= lows[i1] + 0.1 * atr and r2 > r1 + DIV_MIN_RSI
            return {"type": "W", "extreme": ext, "extreme_idx": ext_idx,
                    "leg1_idx": i1, "leg2_idx": i2, "neckline": neck, "divergence": divergence}

    return {"type": "half_batman", "extreme": ext, "extreme_idx": ext_idx,
            "leg1_idx": i1, "leg2_idx": None, "neckline": None, "divergence": False}


def _day_start(ts: int) -> int:
    return ts - ts % 86400


def _previous_trading_day(d1: list[dict], day0: int, skip_weekends: bool = True) -> Optional[dict]:
    """The last D1 bar before day0 that is a weekday. Broker D1 data includes a
    ~2h Sunday bar, which used to become Monday's 'previous day' (review
    2026-10-20); Friday is the real previous day. Crypto trades all weekend,
    so for it (skip_weekends=False) the previous day is simply the last bar."""
    for b in reversed(d1):
        if b["ts_utc"] < day0 and (not skip_weekends
                                   or datetime.fromtimestamp(b["ts_utc"], timezone.utc).weekday() < 5):
            return b
    return None


def _bullish_origin(bars: list[dict], d1: list[dict], ext: float, ext_idx: int,
                    cross_idx: int, atr: float, skip_weekends: bool = True) -> list[str]:
    """Key lows the pattern extreme sits on (bullish naming: LOD/PDL/PWL)."""
    tol = ORIGIN_TOL_ATR * atr
    ts = bars[ext_idx]["ts_utc"]
    day0 = _day_start(ts)
    out = []
    day_low = min(b["low"] for b in bars[:cross_idx + 1] if _day_start(b["ts_utc"]) == day0)
    if ext <= day_low + tol:
        out.append("LOD")
    prev_day = _previous_trading_day(d1, day0, skip_weekends)
    if prev_day and abs(ext - prev_day["low"]) <= tol:
        out.append("PDL")
    pw = _prev_week_hlc(d1, ts) if d1 else None
    if pw and abs(ext - pw[1]) <= tol:
        out.append("PWL")
    return out


_BEAR_NAMES = {"LOD": "HOD", "PDL": "PDH", "PWL": "PWH"}


def daily_vwap(bars: list[dict]) -> Optional[float]:
    """Volume-weighted (H+L+C)/3 since the start of the trading day, 00:00
    SAST (session_clock; matches the user's TradingView session VWAP)."""
    if not bars:
        return None
    import session_clock
    day0 = session_clock.vwap_day_start(bars[-1]["ts_utc"])
    pv = v = 0.0
    for b in bars:
        if b["ts_utc"] >= day0:
            vol = b.get("volume") or 1.0
            pv += (b["high"] + b["low"] + b["close"]) / 3.0 * vol
            v += vol
    return pv / v if v else None


def grade_setup(origin: list[str], divergence: bool, vwap_aligned: bool) -> str:
    """A+ = pattern at any key high/low (HOD/LOD/PDH/PDL/PWH/PWL) WITH RSI
    divergence; A = pattern at the previous day's/week's high/low; B = the rest.

    User decision 2026-10-20 after a 10-day replay of real M15 data: the first
    rules (A+ = any key level, A = divergence or VWAP side) sent ~49 alerts/day,
    because the VWAP side (95% of signals) and plain HOD/LOD (85%) are true
    almost every time. They stay in the row as confluence ticks only."""
    if origin and divergence:
        return "A+"
    if set(origin) - {"LOD", "HOD"}:
        return "A"
    return "B"


# ── per pair ─────────────────────────────────────────────────────────────────

def analyze_pair(symbol: str, m15_candles: list[dict], d1_candles: Optional[list[dict]] = None) -> dict:
    out: dict = {"symbol": symbol, "timeframe": "M15", "setup": "NO-TRADE", "grade": "NO-TRADE",
                 "validated": False}
    bars = m15_candles or []
    n = len(bars)
    if n < MIN_BARS:
        out["notes"] = f"Insufficient data ({n} M15 bars, need >= {MIN_BARS})."
        return out

    closes = [b["close"] for b in bars]
    e13, e50 = calc_ema(closes, FAST), calc_ema(closes, SLOW)
    last = n - 1
    out.update({"ema13": round(e13[last], 6), "ema50": round(e50[last], 6)})

    conf = find_cross_confirmation(bars, e13, e50, last)
    if conf is None:
        out["notes"] = "No freshly confirmed 13/50 EMA cross on the last closed M15 bar."
        return out

    bull = conf["direction"] == "bullish"
    sig = 1 if bull else -1
    cross_idx = conf["cross_idx"]
    atr = _atr(bars[:cross_idx + 1])
    if not atr:
        out["notes"] = "13/50 cross confirmed but no ATR yet."
        return out

    work = bars if bull else _negate(bars)
    d1 = d1_candles or []
    work_d1 = d1 if bull else _negate(d1)
    rsi = _rsi(closes, 13)
    work_rsi = rsi if bull else [100.0 - r for r in rsi]

    pat = _bullish_anchor(work, work_rsi, cross_idx, last, atr)
    side = "above" if bull else "below"
    if pat is None:
        out["notes"] = (f"13 EMA crossed {side} the 50 EMA (body confirmed) but no "
                        f"{'W' if bull else 'M'} / half batman formed in the prior "
                        f"{PATTERN_LOOKBACK_BARS} bars -- no anchor, no signal.")
        return out

    import instruments
    origin = _bullish_origin(work, work_d1, pat["extreme"], pat["extreme_idx"], cross_idx, atr,
                             skip_weekends=instruments.asset_class(symbol) != "crypto")
    ext = pat["extreme"] * sig
    if not bull:
        origin = [_BEAR_NAMES[o] for o in origin]
        pat = {**pat, "type": "M" if pat["type"] == "W" else pat["type"], "extreme": ext,
               "neckline": -pat["neckline"] if pat["neckline"] is not None else None}

    entry = closes[last]
    stale = []
    if MAX_BARS_SINCE_EXTREME is not None and cross_idx - pat["extreme_idx"] > MAX_BARS_SINCE_EXTREME:
        stale.append(f"cross came {cross_idx - pat['extreme_idx']} bars after the pattern "
                     f"{'low' if bull else 'high'} (max {MAX_BARS_SINCE_EXTREME})")
    if MAX_ENTRY_ATR is not None and abs(entry - ext) > MAX_ENTRY_ATR * atr:
        stale.append(f"entry {abs(entry - ext) / atr:.1f} ATR from the pattern extreme (max {MAX_ENTRY_ATR})")
    if stale:
        out["notes"] = "13/50 cross and anchor found, but the move has already happened: " + "; ".join(stale) + "."
        return out
    stop = ext - sig * STOP_BUFFER_ATR * atr
    risk = (entry - stop) * sig
    if risk <= 0:
        out["notes"] = "Setup found but the stop is on the wrong side of entry -- skipped."
        return out
    divergence = bool(pat["divergence"])
    vwap = daily_vwap(bars)
    vwap_aligned = vwap is not None and (entry - vwap) * sig > 0
    grade = grade_setup(origin, divergence, vwap_aligned)
    pattern_name = {"W": "W", "M": "M", "half_batman": "half batman"}[pat["type"]]

    out.update({
        "setup": "BUY" if bull else "SELL",
        "grade": grade,
        "entry": entry, "sl": stop, "tp1": entry + sig * RR * risk,
        "pattern": {"type": pat["type"], "extreme": ext,
                    "extreme_ts": bars[pat["extreme_idx"]]["ts_utc"],
                    "leg1_ts": bars[pat["leg1_idx"]]["ts_utc"],
                    "leg2_ts": bars[pat["leg2_idx"]]["ts_utc"] if pat["leg2_idx"] is not None else None,
                    "neckline": pat["neckline"]},
        "divergence": divergence,
        "origin": origin,
        "daily_vwap": round(vwap, 6) if vwap is not None else None,
        "vwap_aligned": vwap_aligned,
        "cross_ts": bars[cross_idx]["ts_utc"],
        "confirm_ts": bars[last]["ts_utc"],
        "bars_since_extreme": cross_idx - pat["extreme_idx"],
        "entry_atr": round(abs(entry - ext) / atr, 2),
        "notes": (f"13 EMA crossed {side} the 50 EMA, confirmed by a full-body close {side} it. "
                  f"Anchor: {pattern_name}"
                  + (" with RSI divergence" if divergence else "")
                  + (f" from {'/'.join(origin)}" if origin else "")
                  + (f", price {side} the daily VWAP" if vwap_aligned else "")
                  + f". Grade {grade}. UNVALIDATED."),
    })
    return out


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    pairs = [analyze_pair(sym, tf.get("m15") or [], tf.get("1d")) for sym, tf in candles_by_pair.items()]
    return {"pairs": pairs,
            "buys": sum(1 for p in pairs if p["setup"] == "BUY"),
            "sells": sum(1 for p in pairs if p["setup"] == "SELL")}
