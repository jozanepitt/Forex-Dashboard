"""Key levels from MT5 daily candles (user request 2026-10-27), matching the
user's MT5 "Previous Day High and lows" indicator: the high, low and close of
each of the last 5 trading days (PDH/PDL/PDC, then 2d..5d) plus last week's
high and low (PWH/PWL).

Days are MT5 server days (Exness = UTC, so they start at 02:00 SAST), the
same days the MT5 indicator draws. The ~2h Sunday stub bar is skipped for
markets closed at weekends, so Monday's previous day is Friday; crypto keeps
its weekend days. Today's (still forming) day is never a level.

"At a level" = within TOL_ADR_FRAC of the instrument's average daily range
(median of the last 14 trading days), so the tolerance scales from forex to
gold and indices on its own.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import instruments

DAYS = 5
TOL_ADR_FRAC = 0.10


def _weekday(ts: int) -> int:
    return datetime.fromtimestamp(ts, timezone.utc).weekday()


def _completed_days(d1: list[dict], now_ts: int, symbol: str) -> list[dict]:
    """Completed daily bars, oldest first, without weekend stubs (non-crypto)."""
    today = now_ts - now_ts % 86400
    crypto = instruments.asset_class(symbol) == "crypto"
    return [b for b in d1 if b["ts_utc"] < today and (crypto or _weekday(b["ts_utc"]) < 5)]


def daily_levels(d1: list[dict], now_ts: int, symbol: str, days: int = DAYS) -> list[dict]:
    done = _completed_days(d1, now_ts, symbol)
    out: list[dict] = []
    for k, b in enumerate(reversed(done[-days:]), start=1):
        prefix = "" if k == 1 else f"{k}d "
        for kind, field in (("high", "high"), ("low", "low"), ("close", "close")):
            out.append({"name": f"{prefix}PD{field[0].upper()}", "price": b[field], "kind": kind,
                        "days_ago": k, "ts_utc": b["ts_utc"]})
    # previous calendar week (Mon-Fri) before the current one
    today = now_ts - now_ts % 86400
    week_start = today - _weekday(today) * 86400
    prev = [b for b in done if week_start - 7 * 86400 <= b["ts_utc"] < week_start]
    if prev:
        out.append({"name": "PWH", "price": max(b["high"] for b in prev), "kind": "high", "days_ago": None,
                    "ts_utc": prev[0]["ts_utc"]})
        out.append({"name": "PWL", "price": min(b["low"] for b in prev), "kind": "low", "days_ago": None,
                    "ts_utc": prev[0]["ts_utc"]})
    return out


def tolerance(d1: list[dict], now_ts: int, symbol: str) -> Optional[float]:
    done = _completed_days(d1, now_ts, symbol)[-14:]
    ranges = sorted(b["high"] - b["low"] for b in done if b["high"] > b["low"])
    if not ranges:
        return None
    return TOL_ADR_FRAC * ranges[len(ranges) // 2]


def near(levels: list[dict], price: float, tol: float) -> dict:
    """Levels price is sitting on (within tol), and the nearest one above and
    below. `cluster` = how many levels price is on at once (stronger area)."""
    at = [lv for lv in levels if abs(lv["price"] - price) <= tol]
    above = min((lv for lv in levels if lv["price"] > price + tol), key=lambda lv: lv["price"], default=None)
    below = max((lv for lv in levels if lv["price"] < price - tol), key=lambda lv: lv["price"], default=None)
    return {"at": sorted(at, key=lambda lv: abs(lv["price"] - price)), "above": above, "below": below,
            "cluster": len(at)}


def levels_for(symbol: str, d1: list[dict], now_ts: int) -> dict:
    return {"symbol": symbol, "levels": daily_levels(d1, now_ts, symbol),
            "tol": tolerance(d1, now_ts, symbol), "as_of": now_ts}
