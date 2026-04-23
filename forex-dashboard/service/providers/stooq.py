"""Stooq CSV fallback for forex pairs.

Stooq's historical CSV endpoints now require an apikey (captcha-gated).
Set STOOQ_APIKEY in .env to enable the fallback. Without it, this provider
short-circuits with a clear error so the dashboard keeps serving cache.

Endpoints used:
    https://stooq.com/q/d/l/?s=eurusd&i=5&f=csv&d1=YYYYMMDD&d2=YYYYMMDD&a=<apikey>  (5-min intraday)
    https://stooq.com/q/l/?s=eurusd&f=sd2t2ohlcv&h&e=csv                              (latest quote, no key)

Intraday is 5-minute resolution; we resample to 15-min buckets.
Symbol format: lowercase, no slash. EUR/USD -> eurusd.
"""
from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger("stooq")

HIST_URL = "https://stooq.com/q/d/l/"
QUOTE_URL = "https://stooq.com/q/l/"
TIMEOUT = 15

STOOQ_INTERVAL = {
    "5min":  "5",
    "15min": "5",   # resample from 5
    "1h":    "5",   # resample from 5 (best available free)
    "1day":  "d",
}

APIKEY_MARKERS = ("get your apikey", "get_apikey", "captcha")


class StooqError(Exception):
    pass


class StooqNeedsApiKey(StooqError):
    """Endpoint is behind captcha / apikey wall."""


def symbol_to_stooq(symbol: str) -> str:
    """EUR/USD -> eurusd."""
    return symbol.replace("/", "").lower()


def _apikey() -> Optional[str]:
    v = os.environ.get("STOOQ_APIKEY")
    return v.strip() if v else None


def fetch_since(symbol: str, interval: str = "15min",
                start_ts: Optional[int] = None) -> list[dict]:
    """Download Stooq CSV and return bars newer than start_ts.

    Returns normalized bars: {ts_utc, open, high, low, close, volume}.
    Raises StooqNeedsApiKey when the historical endpoint is gated.
    Raises StooqError on other transport/parse failures.
    """
    if interval not in STOOQ_INTERVAL:
        raise StooqError(f"unsupported interval for Stooq: {interval}")

    stooq_symbol = symbol_to_stooq(symbol)
    stooq_i = STOOQ_INTERVAL[interval]
    key = _apikey()

    params: dict[str, str] = {"s": stooq_symbol, "i": stooq_i, "f": "csv"}
    if key:
        params["a"] = key

    try:
        resp = requests.get(HIST_URL, params=params, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise StooqError(f"network error: {e}") from e

    if resp.status_code != 200:
        raise StooqError(f"HTTP {resp.status_code}")

    text = resp.text.strip()
    low = text.lower()
    if not text or low.startswith("no data"):
        raise StooqError(f"no data for {stooq_symbol}")
    if any(m in low for m in APIKEY_MARKERS):
        raise StooqNeedsApiKey(
            "Stooq historical CSV requires STOOQ_APIKEY in .env "
            "(https://stooq.com/q/d/?s=eurusd&get_apikey)"
        )

    bars_5m = _parse_csv(text)
    if not bars_5m:
        raise StooqError(f"empty parse for {stooq_symbol}")

    if interval == "15min":
        bars = _resample(bars_5m, 900)
    elif interval == "1h":
        bars = _resample(bars_5m, 3600)
    else:
        bars = bars_5m

    if start_ts is not None:
        bars = [b for b in bars if b["ts_utc"] >= start_ts]

    return bars


def fetch_latest_quote(symbol: str) -> Optional[dict]:
    """Return latest aggregate OHLC (single bar). Keyless endpoint.

    Useful only as a 'keep something flowing' signal — not a candle series.
    Returns None on any failure.
    """
    stooq_symbol = symbol_to_stooq(symbol)
    params = {"s": stooq_symbol, "f": "sd2t2ohlcv", "h": "", "e": "csv"}
    try:
        resp = requests.get(QUOTE_URL, params=params, timeout=TIMEOUT)
        if resp.status_code != 200:
            return None
        rows = _parse_csv(resp.text)
        return rows[-1] if rows else None
    except requests.RequestException:
        return None


# ------------------------------------------------------------------ internals
def _parse_csv(text: str) -> list[dict]:
    """Parse Stooq CSV. Expected columns: Date, Time, Open, High, Low, Close, Volume."""
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict] = []
    for r in reader:
        date = (r.get("Date") or "").strip()
        time = (r.get("Time") or "").strip()
        if not date:
            continue
        iso = f"{date} {time}" if time else date
        fmt = "%Y-%m-%d %H:%M:%S" if time else "%Y-%m-%d"
        try:
            dt = datetime.strptime(iso, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        try:
            o = float(r["Open"]); h = float(r["High"])
            l = float(r["Low"]);  c = float(r["Close"])
        except (KeyError, ValueError, TypeError):
            continue
        vol = 0.0
        if r.get("Volume"):
            try:
                vol = float(r["Volume"])
            except ValueError:
                vol = 0.0
        rows.append({
            "ts_utc": int(dt.timestamp()),
            "open":   o,
            "high":   h,
            "low":    l,
            "close":  c,
            "volume": vol,
        })
    rows.sort(key=lambda b: b["ts_utc"])
    return rows


def _resample(bars: list[dict], bucket_secs: int) -> list[dict]:
    """Group input bars into buckets of `bucket_secs`. Bucket ts = floor(open_ts)."""
    buckets: dict[int, dict] = {}
    for b in bars:
        key = (b["ts_utc"] // bucket_secs) * bucket_secs
        agg = buckets.get(key)
        if agg is None:
            buckets[key] = {
                "ts_utc": key,
                "open":   b["open"],
                "high":   b["high"],
                "low":    b["low"],
                "close":  b["close"],
                "volume": b["volume"],
            }
        else:
            agg["high"]   = max(agg["high"], b["high"])
            agg["low"]    = min(agg["low"],  b["low"])
            agg["close"]  = b["close"]
            agg["volume"] = agg["volume"] + b["volume"]
    return [buckets[k] for k in sorted(buckets.keys())]
