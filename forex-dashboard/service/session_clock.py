"""The dashboard's single start of the trading day for every session VWAP.

User decision 2026-10-21: match TradingView. The user's TradingView session
VWAP resets at 00:00 SAST (Africa/Johannesburg) = 22:00 UTC; this was
verified against their chart on 2026-10-20 and 2026-10-21 (the 22:00 UTC
anchor was the only one with the same offset on both days). SAST has no
daylight saving, so it is a fixed UTC hour all year.

MT5 (Exness, server time = UTC) starts its day two hours later, at 00:00 UTC
= 02:00 SAST: its D1 candles and an MT5 "daily" VWAP will not line up with
this. Daily-candle levels (previous day high/low) still come from MT5's D1
bars.
"""
from __future__ import annotations

VWAP_DAY_START_UTC_HOUR = 22          # 00:00 SAST
_OFFSET = VWAP_DAY_START_UTC_HOUR * 3600


def vwap_day_start(ts_utc: int) -> int:
    """Most recent 00:00 SAST (22:00 UTC) at or before ts_utc."""
    return (ts_utc - _OFFSET) // 86400 * 86400 + _OFFSET
