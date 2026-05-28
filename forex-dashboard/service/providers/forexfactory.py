"""ForexFactory weekly calendar feed — no API key required."""
from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger("forexfactory")

_FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
_CACHE_TTL = 3600  # re-fetch at most once per hour

_cache: list[dict] = []
_cache_ts: float = 0.0


def _fetch_feed() -> list[dict]:
    """Download and parse the ForexFactory XML feed."""
    resp = requests.get(_FEED_URL, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    events = []
    for ev in root.iter("event"):
        try:
            currency = (ev.findtext("country") or "").upper()
            title    = ev.findtext("title") or ""
            impact   = (ev.findtext("impact") or "").lower()
            date_str = ev.findtext("date") or ""
            time_str = ev.findtext("time") or ""

            # Build UTC timestamp. FF dates are "Month Day, Year" e.g. "April 28, 2026"
            # times are "Hour:MinAM/PM" e.g. "8:30am" or "All Day" / "Tentative"
            ts = _parse_ts(date_str, time_str)
            if ts is None:
                continue

            events.append({
                "ts":       ts,
                "currency": currency,
                "title":    title,
                "impact":   impact,  # "high" | "medium" | "low" | "holiday" | "non-economic"
            })
        except Exception:
            pass
    return events


def _parse_ts(date_str: str, time_str: str) -> Optional[int]:
    """Return UTC Unix timestamp or None if unparseable."""
    try:
        dt_str = f"{date_str} {time_str.strip()}".strip()
        # Try common ForexFactory formats
        for fmt in ("%B %d, %Y %I:%M%p", "%B %d, %Y %I%p"):
            try:
                dt = datetime.strptime(dt_str, fmt)
                # FF times are Eastern Time. Approximate: UTC = ET + 4 (EDT) or +5 (EST)
                # Use +4 as a reasonable default for trading hours
                return int(dt.timestamp()) + 4 * 3600
            except ValueError:
                pass
        # "All Day" or "Tentative" — return midnight UTC of the given date
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return None


def get_events(force_refresh: bool = False) -> list[dict]:
    """Return all events from the current-week feed (cached for 1 hour)."""
    global _cache, _cache_ts
    now = time.time()
    if force_refresh or now - _cache_ts > _CACHE_TTL or not _cache:
        try:
            _cache = _fetch_feed()
            _cache_ts = now
            log.info("forexfactory: fetched %d events", len(_cache))
        except Exception as e:
            log.warning("forexfactory: fetch failed: %s", e)
    return _cache


def upcoming(minutes: int = 60, impact_filter: Optional[str] = None) -> list[dict]:
    """Return events within ±minutes of now, optionally filtered by impact."""
    now = time.time()
    window_start = now - minutes * 60
    window_end   = now + minutes * 60
    events = get_events()
    result = []
    for ev in events:
        if window_start <= ev["ts"] <= window_end:
            if impact_filter and ev["impact"] != impact_filter:
                continue
            result.append({**ev, "minutes_away": round((ev["ts"] - now) / 60, 1)})
    return sorted(result, key=lambda e: e["ts"])


def currencies_in_window(minutes: int = 30, high_only: bool = True) -> set[str]:
    """Return set of currencies with an event within ±minutes (default: high-impact only)."""
    evs = upcoming(minutes)
    blocked: set[str] = set()
    for ev in evs:
        if high_only and ev["impact"] not in ("high",):
            continue
        blocked.add(ev["currency"])
    return blocked
