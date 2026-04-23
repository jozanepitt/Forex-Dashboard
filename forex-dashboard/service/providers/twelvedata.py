"""Twelve Data client with key rotation, per-key daily quota tracking, and
incremental `start_date` fetching.

Twelve Data charges **1 credit per candle** on the free plan. We track the
daily counter per key in SQLite (keyed by UTC date so it resets at 00:00 UTC,
matching Twelve Data's own reset).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

import requests

import cache
from config import (
    CREDIT_SAFETY_MARGIN,
    DAILY_CREDIT_LIMIT,
    INTERVAL_SECS,
    load_keys,
)

log = logging.getLogger("twelvedata")

BASE_URL = "https://api.twelvedata.com/time_series"
TIMEOUT = 12
QUOTA_MARKERS = ("credit", "quota", "daily", "limit")


class QuotaExhausted(Exception):
    pass


class ProviderError(Exception):
    pass


class TwelveDataClient:
    def __init__(self, keys: Optional[list[dict]] = None):
        self._keys = keys if keys is not None else load_keys()
        if not self._keys:
            raise RuntimeError("No TWELVEDATA_KEY_* entries loaded from .env")
        self._exhausted_runtime: set[str] = set()          # daily quota exhausted
        self._rate_limited_until: dict[str, float] = {}    # temporary 429 cooldown
        self._last_error: dict[str, str] = {}

    # ------------------------------------------------------------------ helpers
    def status(self) -> list[dict]:
        usage = cache.all_key_usage()
        now = time.time()
        out = []
        for k in self._keys:
            used = usage.get(k["name"], 0)
            rate_limited = now < self._rate_limited_until.get(k["name"], 0)
            out.append({
                "name": k["name"],
                "used": used,
                "limit": DAILY_CREDIT_LIMIT,
                "exhausted": (
                    k["name"] in self._exhausted_runtime
                    or used >= DAILY_CREDIT_LIMIT - CREDIT_SAFETY_MARGIN
                ),
                "rateLimited": rate_limited,
                "lastError": self._last_error.get(k["name"]),
            })
        return out

    def _key_budget(self) -> dict[str, int]:
        """Return remaining credits per key, 0 if exhausted or rate-limited."""
        usage = cache.all_key_usage()
        now = time.time()
        out = {}
        for k in self._keys:
            if k["name"] in self._exhausted_runtime:
                out[k["name"]] = 0
                continue
            if now < self._rate_limited_until.get(k["name"], 0):
                out[k["name"]] = 0   # temporarily rate-limited; try again later
                continue
            used = usage.get(k["name"], 0)
            out[k["name"]] = max(0, DAILY_CREDIT_LIMIT - used)
        return out

    def _available_keys(self, expected_credits: int) -> list[dict]:
        """Keys that can afford `expected_credits` for incremental fetches."""
        usage = cache.all_key_usage()
        threshold = DAILY_CREDIT_LIMIT - CREDIT_SAFETY_MARGIN
        now = time.time()
        out = []
        for k in self._keys:
            if k["name"] in self._exhausted_runtime:
                continue
            if now < self._rate_limited_until.get(k["name"], 0):
                continue
            used = usage.get(k["name"], 0)
            if used < threshold and used + expected_credits <= DAILY_CREDIT_LIMIT:
                out.append(k)
        return out

    # ------------------------------------------------------------------ fetch
    def fetch(self, symbol: str, interval: str, outputsize: int = 800,
              start_ts: Optional[int] = None) -> list[dict]:
        """Fetch candles. If start_ts given, uses start_date for incremental.

        For backfills (start_ts=None) each key is asked for as many bars as its
        remaining budget allows, up to `outputsize`. This avoids the safety-margin
        blocking a legitimate 800-bar request on a fresh key.
        """
        if start_ts is not None:
            # Incremental: estimate new candles since last_ts
            elapsed = int(datetime.now(timezone.utc).timestamp()) - start_ts
            expected = max(1, min(outputsize, elapsed // INTERVAL_SECS[interval] + 2))
            keys = self._available_keys(expected)
            if not keys:
                raise QuotaExhausted(f"No keys have budget for {expected} credits")
            for k in keys:
                try:
                    bars, _ = self._request(symbol, interval, expected, start_ts, k["value"], k["name"])
                    return bars
                except QuotaExhausted:
                    continue
                except ProviderError as e:
                    log.warning("provider error on %s: %s", k["name"], e)
                    continue
            raise QuotaExhausted("All keys exhausted during incremental fetch")

        # Backfill: pick the key with the most remaining budget and cap the request.
        # Index `i` is the tiebreaker so equal-budget dicts are never compared directly.
        budget = self._key_budget()
        candidates = sorted(
            [(budget[k["name"]], i, k)
             for i, k in enumerate(self._keys)
             if budget.get(k["name"], 0) > 0],
            reverse=True,
        )
        if not candidates:
            raise QuotaExhausted(f"No keys have budget for {outputsize} credits")

        for avail, _, k in candidates:
            req_size = min(outputsize, avail)
            try:
                bars, _ = self._request(symbol, interval, req_size, None, k["value"], k["name"])
                return bars
            except QuotaExhausted:
                continue
            except ProviderError as e:
                log.warning("provider error on %s: %s", k["name"], e)
                continue
        raise QuotaExhausted("All keys exhausted during backfill")

    # --------------------------------------------------------------- internals
    def _request(self, symbol: str, interval: str, outputsize: int,
                 start_ts: Optional[int], apikey: str, key_name: str
                 ) -> tuple[list[dict], int]:
        params = {
            "symbol":    symbol,
            "interval":  interval,
            "format":    "JSON",
            "apikey":    apikey,
            "timezone":  "UTC",
        }
        if start_ts is not None:
            params["start_date"] = datetime.fromtimestamp(
                start_ts, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")
        else:
            params["outputsize"] = outputsize

        try:
            resp = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
        except requests.RequestException as e:
            raise ProviderError(f"network error: {e}") from e

        if resp.status_code == 429:
            # Rate limit is per-minute — don't blacklist permanently.
            self._rate_limited_until[key_name] = time.time() + 60
            self._last_error[key_name] = "HTTP 429 rate-limited (cooldown 60s)"
            raise QuotaExhausted(f"{key_name}: HTTP 429")

        try:
            data = resp.json()
        except ValueError as e:
            raise ProviderError(f"non-JSON response ({resp.status_code}): {resp.text[:200]}") from e

        if isinstance(data, dict) and data.get("status") == "error":
            msg = str(data.get("message", "unknown")).lower()
            self._last_error[key_name] = data.get("message", "error")
            if any(m in msg for m in QUOTA_MARKERS):
                self._exhausted_runtime.add(key_name)
                raise QuotaExhausted(f"{key_name}: {data.get('message')}")
            raise ProviderError(f"{key_name}: {data.get('message')}")

        values = data.get("values") if isinstance(data, dict) else None
        if not values:
            raise ProviderError(f"{key_name}: empty response")

        bars = _normalize(values)
        credits_used = len(bars)
        cache.bump_key_usage(key_name, credits_used)
        self._last_error.pop(key_name, None)
        return bars, credits_used


def _normalize(values: Iterable[dict]) -> list[dict]:
    """Twelve Data returns newest-first. Convert to our oldest-first schema."""
    rows = []
    for v in values:
        dt = v.get("datetime")
        if not dt:
            continue
        try:
            ts = int(datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                     .replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            try:
                ts = int(datetime.strptime(dt, "%Y-%m-%d")
                         .replace(tzinfo=timezone.utc).timestamp())
            except ValueError:
                continue
        rows.append({
            "ts_utc": ts,
            "open":   float(v["open"]),
            "high":   float(v["high"]),
            "low":    float(v["low"]),
            "close":  float(v["close"]),
            "volume": float(v.get("volume") or 0),
        })
    rows.sort(key=lambda b: b["ts_utc"])
    return rows
