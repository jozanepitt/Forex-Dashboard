"""May a LIVE trade alert fire for this symbol right now?

Every strategy takes "now" from the newest candle it is handed. That is right
for a backtest replay and wrong for live dispatch: when the feed freezes, the
newest candle stops moving and the strategy keeps seeing a market that is
permanently "in session" at whatever time the feed died. On 2026-10-11 that
re-sent a CAD/JPY SELL all weekend off two bars from Friday morning.

So live dispatch (scheduler.py) asks the wall clock two questions before any
strategy alert goes out:

  1. Is the market open?   Non-crypto instruments are closed over the weekend.
  2. Is the data live?     The newest bar must be recent for its timeframe.

Both are needed. The weekend rule alone misses a feed that dies mid-week; the
freshness rule alone misses fallback providers that keep printing indicative
weekend quotes for a closed market.

Pure functions, no I/O — the strategies and their backtests are untouched.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import instruments

# Retail FX/metals/index CFDs stop late Friday and resume late Sunday (UTC).
# Exness FX runs Sun 21:05 -> Fri 20:59 in summer and one hour later in winter;
# metals and indices reopen an hour after FX. The close is the earliest of
# those and each reopen the latest, so the window is never too short: it costs
# the last Friday hour in winter and the first hour or two on Sunday night,
# and in exchange a fallback provider's indicative weekend quotes (TwelveData
# prints them) can never look like a live market.
_WEEKEND_CLOSE_MIN = 4 * 1440 + 21 * 60        # Friday 21:00 UTC, minutes since Monday 00:00
_FX_REOPEN_MIN = 6 * 1440 + 22 * 60 + 5        # Sunday 22:05 UTC
_OTHER_REOPEN_MIN = 6 * 1440 + 23 * 60 + 5     # Sunday 23:05 UTC — metals, indices
_FX_CLASSES = ("fx", "jpy")

# Fetch fan-out + alert runners take a few minutes after the bar closes.
FANOUT_ALLOWANCE_SECS = 600


def is_market_open(symbol: Optional[str], now_ts: float) -> bool:
    """False for non-crypto instruments over the weekend (Friday 21:00 UTC until
    Sunday 22:05 for FX, 23:05 for metals and indices). Daily maintenance
    breaks are not modelled; the freshness rule covers those."""
    cls = instruments.asset_class(symbol)
    if cls == "crypto":
        return True
    now = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    minute_of_week = now.weekday() * 1440 + now.hour * 60 + now.minute
    reopen = _FX_REOPEN_MIN if cls in _FX_CLASSES else _OTHER_REOPEN_MIN
    return not (_WEEKEND_CLOSE_MIN <= minute_of_week < reopen)


def stale_after_secs(interval_secs: int) -> int:
    """Oldest the newest bar may be: two bar-lengths, or one bar plus the
    fan-out allowance for short bars. MT5 hands back closed bars only, so a
    healthy M15 feed peaks near 1230s and M5 near 700s; one missed 15-minute
    cycle pushes both past the limit (1800s / 900s)."""
    return max(2 * interval_secs, interval_secs + FANOUT_ALLOWANCE_SECS)


def is_data_fresh(last_bar_ts: float, interval_secs: int, now_ts: float) -> bool:
    """True when the newest bar opened recently enough to be live. A bar stamped
    more than one bar-length in the future is mis-stamped data, not fresh data."""
    age = now_ts - last_bar_ts
    return -interval_secs <= age <= stale_after_secs(interval_secs)


def live_block_reason(symbol: Optional[str], bars: list[dict], interval_secs: int,
                      now_ts: float) -> Optional[str]:
    """Why a live alert must NOT fire for `symbol`, or None when it may."""
    if not is_market_open(symbol, now_ts):
        return "market closed (weekend)"
    if not bars:
        return "no data"
    try:
        last_bar_ts = float(bars[-1]["ts_utc"])
    except (KeyError, TypeError, ValueError, IndexError):
        return "bad data"
    if not is_data_fresh(last_bar_ts, interval_secs, now_ts):
        return f"stale data (newest bar {int((now_ts - last_bar_ts) // 60)} min old)"
    return None
