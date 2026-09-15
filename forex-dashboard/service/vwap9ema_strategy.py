"""Live VWAP+9EMA scanner — the vp-climax variant that came closest to
passing its own honest backtest (0/48, see
docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md),
at ema=9, rr=2.0, stop_buffer=0.10, volume_climax_mult=1.3 -- the exact
configuration behind the near-miss numbers, not re-tuned.

Built anyway per explicit user decision (see
docs/superpowers/specs/2026-09-14-vwap9ema-live-replaces-1am-crt-design.md):
manual trading with the user's own judgment, not an automated trusted
signal. There are deliberately NO grade tiers -- every row is either a
real signal or NO-TRADE, and `grade` is always the literal string
"UNVALIDATED" so the dashboard/alert layers can never accidentally render
this as if it were a proven Grade-A signal like the other strategies.

Reuses the already-tested math from the backtest tooling rather than
reimplementing it -- see the sys.path insert below.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "vwap9ema_backtest"))
from volume_profile import compute_session_volume_profile, price_passes_vp_filter  # noqa: E402
from backtest_mt5 import ema  # noqa: E402

from config import PRIORITY_PAIRS  # noqa: E402

# Full PRIORITY_PAIRS universe per explicit user request (same pairs TDI123/
# BTMM123 scan). Only USTEC/AUD-USD were ever backtested (0/48, see module
# docstring); every other pair here runs the identical UNVALIDATED rule with
# ZERO backtesting at all, not just a failed one.
VWAP9EMA_UNIVERSE = list(PRIORITY_PAIRS)
EMA_LEN = 9
RR = 2.0
STOP_BUFFER = 0.10
VOLUME_CLIMAX_MULT = 1.3
# London session in UTC. Exness MT5 stamps bar times in UTC directly (offset
# = 0s, confirmed live -- see providers/exness_mt5.py's module docstring and
# service.log's per-connection "offset=0s"). The backtest's SESSIONS["London"]
# = (9, 17) is therefore already UTC 9-17, not broker-server-time needing a
# +2 conversion -- this constant must match that raw window exactly, since
# it is the only thing standing between "scanning what was validated" and
# "scanning 2 hours of never-tested market".
SESSION_START_UTC = 9
SESSION_END_UTC = 17
MIN_BARS = 12  # matches backtest_mt5.run_day()'s own floor (n<12 -> return []); below this the EMA hasn't converged


def _in_session(ts_utc: int) -> bool:
    hour = datetime.fromtimestamp(ts_utc, tz=timezone.utc).hour
    return SESSION_START_UTC <= hour < SESSION_END_UTC


def _session_bars_for_today(m5_candles: list[dict]) -> list[dict]:
    """Filter to the most recent calendar day's London-session bars only,
    matching backtest_mt5.backtest()'s exact day+session grouping (a fresh
    VWAP/EMA/volume-profile computation per session-day, not a running
    multi-day series)."""
    session_bars = [c for c in m5_candles if _in_session(c["ts_utc"])]
    if not session_bars:
        return []
    last_day = datetime.fromtimestamp(session_bars[-1]["ts_utc"], tz=timezone.utc).date()
    return [c for c in session_bars
            if datetime.fromtimestamp(c["ts_utc"], tz=timezone.utc).date() == last_day]


def analyze_pair(symbol: str, m5_candles: list[dict]) -> dict:
    """Most-recent-bar VWAP+9EMA signal, vp-climax variant. Mirrors
    backtest_mt5.run_day()'s exact conditions, adapted from "simulate a
    whole historical day" to "is there a signal as of the most recent
    closed bar" -- the same adaptation every other live scanner on this
    dashboard makes relative to its own backtest logic."""
    out: dict = {"symbol": symbol, "setup": "NO-TRADE", "grade": "UNVALIDATED"}

    if not m5_candles or not _in_session(m5_candles[-1]["ts_utc"]):
        out["in_active_session"] = False
        out["notes"] = "Outside London session (UTC 07:00-15:00)."
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
    vwap: list[float] = []
    for k in range(n):
        typ = (highs[k] + lows[k] + closes[k]) / 3.0
        vol = volumes[k] if volumes[k] > 0 else 1.0
        cum_pv += typ * vol
        cum_v += vol
        vwap.append(cum_pv / cum_v)
    e9 = ema(closes, EMA_LEN)

    i = n - 2  # most recently closed signal/confirmation bar; i+1 is the entry bar
    if i < 3:
        out["notes"] = "Insufficient bars into today's session for a signal check."
        return out

    up = closes[i] > vwap[i] and e9[i] > vwap[i]
    dn = closes[i] < vwap[i] and e9[i] < vwap[i]
    sig = 0
    if up and (lows[i] <= e9[i] or lows[i - 1] <= e9[i - 1]) and closes[i] > e9[i] and closes[i] > opens[i]:
        sig = 1
    elif dn and (highs[i] >= e9[i] or highs[i - 1] >= e9[i - 1]) and closes[i] < e9[i] and closes[i] < opens[i]:
        sig = -1
    if sig == 0:
        out["notes"] = "No 9EMA+VWAP pullback/reject signal on the most recent bar."
        return out

    j = i + 1
    profile = compute_session_volume_profile(highs, lows, closes, volumes)
    entry = opens[j]
    if not price_passes_vp_filter(entry, profile):
        out["notes"] = "Signal found but entry price fails the volume-profile filter."
        return out

    window_start = max(0, j - 20)
    avg_vol = sum(volumes[window_start:j]) / max(1, j - window_start)
    if volumes[j] < VOLUME_CLIMAX_MULT * avg_vol:
        out["notes"] = "Signal + volume-profile filter pass, but no volume climax on the entry bar."
        return out

    if sig == 1:
        sw = min(lows[i], lows[i - 1])
        stop = sw - STOP_BUFFER * (entry - sw)
    else:
        sw = max(highs[i], highs[i - 1])
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

    out.update({
        "setup": "BUY" if sig == 1 else "SELL",
        "entry": entry, "sl": stop, "tp1": target,
        "notes": (f"VWAP+9EMA vp-climax signal: pullback to 9EMA rejected "
                  f"{'up' if sig == 1 else 'down'}, volume-profile filter and "
                  f"volume climax both confirmed. UNVALIDATED strategy (failed "
                  f"backtest 0/48) -- trade at your own judgment."),
    })
    return out


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    pairs = []
    for symbol, tf in candles_by_pair.items():
        m5 = tf.get("m5") or []
        pairs.append(analyze_pair(symbol, m5))
    buys = sum(1 for p in pairs if p["setup"] == "BUY")
    sells = sum(1 for p in pairs if p["setup"] == "SELL")
    return {"pairs": pairs, "buys": buys, "sells": sells}
