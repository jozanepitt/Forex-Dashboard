"""Live VWAP+9EMA scanner — deliberately simplified per explicit user
request (2026-09-20) to just: the 9 EMA crosses the VWAP on the M5 chart,
nothing else. The user judges volume/context manually before trading.

This REPLACES the earlier "vp-climax" variant (trend-alignment + EMA-
pullback-reject + volume-profile filter + volume-climax filter), which
failed its own honest backtest (0/48, see
docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md) even
with all those extra filters. This simpler cross-only rule has not been
backtested at all — it is not a re-tuned or lighter version of the old
rule, it's a different rule entirely.

Built for manual trading with the user's own judgment, not an automated
trusted signal. Since 2026-10-19 every signal carries a grade (user request,
after the feed flooded with flip-flopping and post-news alerts):
  A = no opposite cross on the same pair in the previous 2 hours, stop no
      wider than 1.5x the M15 ATR, target at least 1.5x the stop (always true:
      the target is fixed at 2R), and on the right side of the H1 200 EMA.
  B = any other signal.
Only A goes to Discord by default; the news check lives in the alert layer.
The grade is a filter, not evidence of an edge: still UNVALIDATED.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "vwap9ema_backtest"))
from backtest_mt5 import ema  # noqa: E402

from config import PRIORITY_PAIRS  # noqa: E402
import session_clock  # noqa: E402

# Full PRIORITY_PAIRS universe per explicit user request (same pairs TDI123/
# BTMM123 scan, including BTC/USD and ETH/USD). This simplified cross-only
# rule has zero backtesting for every pair, not just a failed one.
VWAP9EMA_UNIVERSE = list(PRIORITY_PAIRS)
# Live M5 fetch window used by app.py's /vwap9ema and scheduler.py's
# refresh loop. Two full UTC days (2 x 24h x 12 = 576 M5 bars). Covers
# both requirements:
#   * VWAP accuracy: the window must contain the current day's very first
#     bar (the 00:00 UTC anchor; the longest a day can be is 288 bars, at
#     23:55) or the VWAP silently re-anchors mid-day.
#   * 9EMA fidelity: the EMA runs over the whole fetched history (not a
#     daily reset), so it needs plenty of pre-day bars to be fully seeded
#     and match the chart's true EMA(9) -- 2 days guarantees that even
#     right after a weekend/holiday backfill gap.
# Previously limit=100 (8h20m); the VWAP mis-anchored for ~16h/day and the
# 9EMA had almost no history. Single source of truth -- do NOT hardcode a
# different number in app.py/scheduler.py.
M5_FETCH_LIMIT = 576
EMA_LEN = 9
RR = 2.0
STOP_BUFFER = 0.10
# London+NY session in UTC, per explicit user request ("between London and NY
# session only those 2"). Exness MT5 stamps bar times in UTC directly (offset
# = 0s, confirmed live -- see providers/exness_mt5.py's module docstring and
# service.log's per-connection "offset=0s"), so these are raw UTC hours, not
# broker-server-time needing a +2 conversion.
#
# This matches backtest_mt5.py's SESSIONS["Both"] = (9, 23) -- the UNION of
# SESSIONS["London"] = (9, 17) and SESSIONS["NY"] = (15, 23) (they overlap
# 15-17, so the union is one continuous span, no gap).
#
# IMPORTANT: "Both" was never itself run in the 48-combination validation
# sweep (VALIDATION_RESULTS.md) -- only "London" and "NY" were tested as
# SEPARATE windows, never their union. The near-miss numbers quoted
# elsewhere (USTEC/AUD-USD, vp-climax, +0.043/+0.017 expR) are for LONDON
# ONLY; the same sweep's NY-only numbers for those same two pairs were
# worse (USTEC NY vp-climax: -0.116 expR/PF 0.80; AUD-USD NY vp-climax:
# -0.204 expR/PF 0.68). Widening to 9-23 UTC means every NY-session hour
# (17-23) now scanned live has ZERO backtest support at all for any pair,
# not just a failed one -- an explicit, informed user choice, same pattern
# as the earlier full-universe expansion.
SESSION_START_UTC = 9
SESSION_END_UTC = 23
# The cache holds closed bars only and M5 is refreshed once per 15-minute
# cycle, so each scan must look at every bar that closed since the last one.
CROSS_LOOKBACK_BARS = 3
MIN_BARS = 12  # matches backtest_mt5.run_day()'s own floor (n<12 -> return []); below this the EMA hasn't converged
H1_FETCH_LIMIT = 300
FLIP_LOOKBACK_BARS = 24      # 2 hours of M5: an opposite cross inside this window = flip-flopping
MAX_STOP_ATR = 1.5           # stop distance cap, in M15 ATR(14)
MIN_RR_A = 1.5
TREND_EMA = 200


def _m15_atr(m5_candles: list[dict], n: int = 14) -> float | None:
    """ATR(14) of the M15 bars built from the M5 series."""
    groups: dict[int, list[dict]] = {}
    for c in m5_candles:
        groups.setdefault(c["ts_utc"] // 900, []).append(c)
    m15 = [{"high": max(x["high"] for x in g), "low": min(x["low"] for x in g), "close": g[-1]["close"]}
           for _, g in sorted(groups.items())]
    if len(m15) < n + 1:
        return None
    trs = [max(b["high"] - b["low"], abs(b["high"] - a["close"]), abs(b["low"] - a["close"]))
           for a, b in zip(m15, m15[1:])]
    atr = sum(trs[:n]) / n
    for tr in trs[n:]:
        atr = (atr * (n - 1) + tr) / n
    return atr


def _h1_trend(h1_candles: list[dict] | None) -> str | None:
    closes = [c["close"] for c in (h1_candles or [])]
    if len(closes) < TREND_EMA:
        return None
    e = ema(closes, TREND_EMA)[-1]
    return "bullish" if closes[-1] > e else "bearish"


def grade_signal(sig: int, i: int, e9: list[float], vwap: list[float], risk: float,
                 rr: float, atr: float | None, trend: str | None) -> tuple[str, list[str]]:
    """Grade A or B for a cross at index i, plus the reasons A was missed."""
    misses = []
    for k in range(max(1, i - FLIP_LOOKBACK_BARS), i):
        up = e9[k - 1] <= vwap[k - 1] and e9[k] > vwap[k]
        down = e9[k - 1] >= vwap[k - 1] and e9[k] < vwap[k]
        if (sig == 1 and down) or (sig == -1 and up):
            misses.append("opposite cross in the last 2h (flip-flop)")
            break
    if atr is None or risk > MAX_STOP_ATR * atr:
        misses.append(f"stop wider than {MAX_STOP_ATR}x M15 ATR" if atr else "no M15 ATR")
    if rr < MIN_RR_A:
        misses.append(f"target under {MIN_RR_A}x the stop")
    want = "bullish" if sig == 1 else "bearish"
    if trend != want:
        misses.append(f"against the H1 200 EMA ({trend or 'no H1 data'})")
    return ("A" if not misses else "B"), misses


def _in_session(ts_utc: int) -> bool:
    hour = datetime.fromtimestamp(ts_utc, tz=timezone.utc).hour
    return SESSION_START_UTC <= hour < SESSION_END_UTC


def _forex_day_start_utc(ts_utc: int) -> int:
    """The VWAP accumulation anchor: 00:00 SAST (22:00 UTC), shared by every
    dashboard VWAP via session_clock. History: 22:00 UTC (2026-09-22, matched
    to TradingView) -> 00:00 UTC / MT5 midnight (2026-10-06) -> back to
    00:00 SAST on 2026-10-21 at the user's request, after verifying their
    TradingView session VWAP resets there. Deliberately independent of
    SESSION_START_UTC/SESSION_END_UTC below, which gate when signals are
    allowed to FIRE, not when the VWAP clock itself starts."""
    return session_clock.vwap_day_start(ts_utc)


def _session_bars_for_today(m5_candles: list[dict]) -> list[dict]:
    """All bars since the most recent forex-day boundary (00:00 UTC).
    Session-anchored accumulator for the VWAP and its SD bands (NOT the
    9EMA -- the EMA runs over the full fetched history so it matches the
    chart's true EMA, see analyze_pair). Includes the overnight Asian-session
    bars even though those fall outside SESSION_START_UTC/SESSION_END_UTC's
    alert-firing window, matching how a real VWAP indicator behaves on a
    chart that's open continuously, not just during London+NY."""
    if not m5_candles:
        return []
    day_start = _forex_day_start_utc(m5_candles[-1]["ts_utc"])
    return [c for c in m5_candles if c["ts_utc"] >= day_start]


def analyze_pair(symbol: str, m5_candles: list[dict], h1_candles: list[dict] | None = None) -> dict:
    """VWAP+9EMA signal: the 9 EMA crossing the session VWAP on the M5 chart
    within the last CROSS_LOOKBACK_BARS closed bars, nothing else. Entry is
    the newest bar's close. Bullish cross (9EMA moves from at-or-below
    VWAP to above it) -> BUY; bearish cross -> SELL. The 9EMA runs over the
    FULL fetched M5 history (chart-true, never day-reset); the VWAP is
    anchored at the current forex day (00:00 UTC)."""
    out: dict = {"symbol": symbol, "setup": "NO-TRADE", "grade": "NO-TRADE", "validated": False}

    if not m5_candles or not _in_session(m5_candles[-1]["ts_utc"]):
        out["in_active_session"] = False
        out["notes"] = f"Outside London+NY session (UTC {SESSION_START_UTC:02d}:00-{SESSION_END_UTC:02d}:00)."
        return out
    out["in_active_session"] = True

    bars = _session_bars_for_today(m5_candles)
    n = len(bars)
    if n < MIN_BARS:
        out["notes"] = f"Insufficient data ({n} session bars today, need >= {MIN_BARS})."
        return out

    opens = [c["open"] for c in bars]
    highs = [c["high"] for c in bars]
    lows = [c["low"] for c in bars]
    closes = [c["close"] for c in bars]
    volumes = [c.get("volume") or 0.0 for c in bars]

    cum_pv = 0.0
    cum_v = 0.0
    cum_pv2 = 0.0  # sum(vol * typ^2) -- for the volume-weighted variance behind the SD bands
    vwap: list[float] = []
    stdev: list[float] = []
    for k in range(n):
        typ = (highs[k] + lows[k] + closes[k]) / 3.0
        vol = volumes[k] if volumes[k] > 0 else 1.0
        cum_pv += typ * vol
        cum_v += vol
        cum_pv2 += vol * typ * typ
        v = cum_pv / cum_v
        vwap.append(v)
        variance = max(0.0, cum_pv2 / cum_v - v * v)  # Var = E[X^2] - E[X]^2, volume-weighted
        stdev.append(variance ** 0.5)
    # 9EMA runs over the FULL fetched M5 history (not re-seeded at each
    # 00:00 UTC boundary) so it matches the chart's true EMA(9) -- a
    # day-reset EMA was 'fake' near the day open. The session bars are a
    # contiguous suffix of the fetched series, so the values at the session
    # positions are e9_full[offset : offset + n].
    e9_full = ema([c["close"] for c in m5_candles], EMA_LEN)
    offset = len(m5_candles) - n
    e9 = e9_full[offset:offset + n]

    # SD bands, visual reference only -- not part of the signal logic (still
    # purely the 9EMA/VWAP cross below). Standard VWAP-band setup: session
    # anchor, (H+L+C)/3 source, +-1sigma/+-2sigma. Exposed whenever there's
    # enough session data, regardless of whether a cross signal fires.
    out.update({
        "vwap":         round(vwap[-1], 6),
        "vwap_upper_1": round(vwap[-1] + stdev[-1], 6),
        "vwap_lower_1": round(vwap[-1] - stdev[-1], 6),
        "vwap_upper_2": round(vwap[-1] + 2 * stdev[-1], 6),
        "vwap_lower_2": round(vwap[-1] - 2 * stdev[-1], 6),
    })

    last = n - 1  # newest bar; the cache only ever holds closed bars
    if last < CROSS_LOOKBACK_BARS + 1:
        out["notes"] = "Insufficient bars into today's session for a signal check."
        return out

    # The cross, and nothing else: 9EMA moving from at-or-below VWAP to above
    # it (or the mirror image). Newest bar first, so the most recent cross wins
    # and its direction is by construction still in effect on the newest bar.
    sig = 0
    i = last
    for i in range(last, last - CROSS_LOOKBACK_BARS, -1):
        if e9[i - 1] <= vwap[i - 1] and e9[i] > vwap[i]:
            sig = 1
            break
        if e9[i - 1] >= vwap[i - 1] and e9[i] < vwap[i]:
            sig = -1
            break
    if sig == 0:
        out["notes"] = (f"No 9EMA/VWAP cross in the last {CROSS_LOOKBACK_BARS} bars "
                        f"({CROSS_LOOKBACK_BARS * 5} minutes).")
        return out

    entry = closes[last]

    # Swing extreme from the bar before the cross through the newest bar.
    if sig == 1:
        sw = min(lows[i - 1:last + 1])
        stop = sw - STOP_BUFFER * (entry - sw)
    else:
        sw = max(highs[i - 1:last + 1])
        stop = sw + STOP_BUFFER * (sw - entry)

    # Defensive guard: stop must be on the correct side of entry
    if (sig == 1 and stop >= entry) or (sig == -1 and stop <= entry):
        out["notes"] = "Signal confirmed but computed stop is on the wrong side of entry (extreme entry gap) -- skipped."
        return out

    risk = abs(entry - stop)
    if risk <= 0:
        out["notes"] = "Signal confirmed but computed risk is zero (degenerate stop) -- skipped."
        return out
    target = entry + sig * RR * risk

    grade, misses = grade_signal(sig, i, e9, vwap, risk, RR, _m15_atr(m5_candles), _h1_trend(h1_candles))
    out.update({
        "setup": "BUY" if sig == 1 else "SELL",
        "grade": grade, "grade_misses": misses,
        "entry": entry, "sl": stop, "tp1": target,
        "cross_ts": bars[i]["ts_utc"], "bars_since_cross": last - i,
        "notes": (f"9EMA crossed {'above' if sig == 1 else 'below'} VWAP. "
                  + (f"Grade B: {'; '.join(misses)}. " if misses else "Grade A. ")
                  + "UNVALIDATED strategy -- check volume yourself before trading."),
    })
    return out


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    pairs = []
    for symbol, tf in candles_by_pair.items():
        m5 = tf.get("m5") or []
        pairs.append(analyze_pair(symbol, m5, tf.get("1h")))
    buys = sum(1 for p in pairs if p["setup"] == "BUY")
    sells = sum(1 for p in pairs if p["setup"] == "SELL")
    return {"pairs": pairs, "buys": buys, "sells": sells}
