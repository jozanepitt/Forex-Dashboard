"""Orchestrates cache-first, incremental fetch, with Stooq CSV failover."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import cache
from config import DEFAULT_BACKFILL, INTERVAL_SECS
from providers import stooq
from providers.stooq import StooqError, StooqNeedsApiKey
from providers.twelvedata import ProviderError, QuotaExhausted, TwelveDataClient

log = logging.getLogger("fetcher")

_td: Optional[TwelveDataClient] = None


def client() -> TwelveDataClient:
    global _td
    if _td is None:
        _td = TwelveDataClient()
    return _td


def get_candles(symbol: str, interval: str = "15min",
                limit: int = DEFAULT_BACKFILL) -> tuple[list[dict], bool]:
    """Return (candles, stale). stale=True when we couldn't refresh this call."""
    last_ts = cache.max_ts(symbol, interval)
    interval_secs = INTERVAL_SECS[interval]
    now_ts = int(datetime.now(timezone.utc).timestamp())

    if last_ts is None:
        stale = _backfill(symbol, interval, limit)
        return cache.read_candles(symbol, interval, limit), stale

    if now_ts < last_ts + interval_secs + 60:
        return cache.read_candles(symbol, interval, limit), False

    stale = _incremental(symbol, interval, last_ts)
    return cache.read_candles(symbol, interval, limit), stale


def _backfill(symbol: str, interval: str, limit: int) -> bool:
    try:
        bars = client().fetch(symbol, interval, outputsize=limit)
        n = cache.upsert_candles(symbol, interval, bars)
        cache.log_refresh(symbol, interval, "twelvedata", n, credits=len(bars))
        return False
    except QuotaExhausted as e:
        log.warning("backfill TD quota exhausted for %s: %s; trying Stooq", symbol, e)
        cache.log_refresh(symbol, interval, "twelvedata", 0, error=str(e))
    except Exception as e:
        log.error("backfill TD failed for %s: %s; trying Stooq", symbol, e)
        cache.log_refresh(symbol, interval, "twelvedata", 0, error=str(e))

    return _stooq_fallback(symbol, interval, start_ts=None)


def _incremental(symbol: str, interval: str, last_ts: int) -> bool:
    start_ts = last_ts + INTERVAL_SECS[interval]
    try:
        bars = client().fetch(symbol, interval,
                              outputsize=DEFAULT_BACKFILL, start_ts=start_ts)
        n = cache.upsert_candles(symbol, interval, bars)
        cache.log_refresh(symbol, interval, "twelvedata", n, credits=len(bars))
        return False
    except QuotaExhausted as e:
        log.info("incremental TD quota exhausted for %s: %s; trying Stooq", symbol, e)
        cache.log_refresh(symbol, interval, "twelvedata", 0, error=str(e))
    except ProviderError as e:
        log.warning("incremental TD provider error for %s: %s; trying Stooq", symbol, e)
        cache.log_refresh(symbol, interval, "twelvedata", 0, error=str(e))
    except Exception as e:
        log.error("incremental TD failed for %s: %s; trying Stooq", symbol, e)
        cache.log_refresh(symbol, interval, "twelvedata", 0, error=str(e))

    return _stooq_fallback(symbol, interval, start_ts=start_ts)


def _stooq_fallback(symbol: str, interval: str, start_ts: Optional[int]) -> bool:
    """Attempt Stooq CSV. Returns stale=True if it also fails or adds nothing."""
    try:
        bars = stooq.fetch_since(symbol, interval, start_ts=start_ts)
    except StooqNeedsApiKey as e:
        log.info("stooq dormant for %s: %s", symbol, e)
        cache.log_refresh(symbol, interval, "stooq", 0, error="needs apikey")
        return True
    except StooqError as e:
        log.warning("stooq fallback failed for %s: %s", symbol, e)
        cache.log_refresh(symbol, interval, "stooq", 0, error=str(e))
        return True
    except Exception as e:
        log.error("stooq fallback unexpected error for %s: %s", symbol, e)
        cache.log_refresh(symbol, interval, "stooq", 0, error=str(e))
        return True

    if not bars:
        cache.log_refresh(symbol, interval, "stooq", 0, error="no new bars")
        return True

    n = cache.upsert_candles(symbol, interval, bars)
    cache.log_refresh(symbol, interval, "stooq", n, credits=0)
    log.info("stooq fallback supplied %d bars for %s", n, symbol)
    return True  # served from fallback — flag as stale so dashboard can badge it


def status_summary(interval: str = "15min") -> dict:
    from config import PRIORITY_PAIRS
    now_ts = int(datetime.now(timezone.utc).timestamp())
    interval_secs = INTERVAL_SECS[interval]
    pairs = []
    for sym in PRIORITY_PAIRS:
        last_ts = cache.max_ts(sym, interval)
        pairs.append({
            "symbol":  sym,
            "lastTs":  last_ts,
            "lastIso": (datetime.fromtimestamp(last_ts, tz=timezone.utc)
                         .strftime("%Y-%m-%d %H:%M:%S") if last_ts else None),
            "fresh":   bool(last_ts and now_ts < last_ts + 2 * interval_secs),
        })
    return {"pairs": pairs, "keys": client().status()}
