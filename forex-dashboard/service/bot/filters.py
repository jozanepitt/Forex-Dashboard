"""Session clock and news gate. News events are passed in; this module does
not fetch them, so tests stay offline.
"""
from __future__ import annotations

from datetime import datetime, timezone


def in_session(ts: int, start_hour: int, end_hour: int) -> bool:
    """UTC hour gate. ``end_hour`` is exclusive. Supports an overnight wrap."""
    hour = datetime.fromtimestamp(int(ts), tz=timezone.utc).hour
    if start_hour == end_hour:
        return True
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def pair_currencies(symbol: str) -> set[str]:
    if "/" in symbol:
        a, b = symbol.split("/", 1)
        return {a.strip().upper(), b.strip().upper()}
    return {symbol.strip().upper()}


def news_blocks(symbol: str, now_ts: int, events: list[dict], window_min: int,
                high_only: bool = True) -> bool:
    """True when a high-impact event for either currency falls inside ±window.

    Each event is ``{"ts": epoch_seconds, "currency": "USD", "impact": "high"}``.
    """
    if window_min <= 0:
        return False
    wanted = pair_currencies(symbol)
    lo = now_ts - window_min * 60
    hi = now_ts + window_min * 60
    for ev in events:
        if not (lo <= int(ev.get("ts", 0)) <= hi):
            continue
        if high_only and str(ev.get("impact", "")).lower() != "high":
            continue
        if str(ev.get("currency", "")).upper() in wanted:
            return True
    return False
