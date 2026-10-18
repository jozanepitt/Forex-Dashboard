"""Trade statistics. R is P&L divided by the strategy's planned stop distance,
after the fill model. A robust result has to clear every hurdle below on
out-of-sample trades; the hurdles were set before the run.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np

# Fixed before seeing results. A row is "robust" only when all of these hold.
MIN_OOS_TRADES = 80
MIN_OOS_EXP_R = 0.05
MIN_OOS_PF = 1.15
MIN_YEAR_TRADES = 15


def _empty() -> dict:
    return {
        "trades": 0, "win_rate": None, "profit_factor": None,
        "expectancy_r": None, "expectancy_pips": None,
        "expectancy_r_wide": None, "max_drawdown_r": None,
        "sharpe": None, "max_concurrent": 0, "years": [],
        "outcomes": {},
    }


def summarize(trades: list[dict], start_ts: int, end_ts: int) -> dict:
    if not trades:
        return _empty()
    ordered = sorted(trades, key=lambda t: (t["exit_ts"], t["entry_ts"]))
    r = np.array([t["r"] for t in ordered], dtype=np.float64)
    pips = np.array([t["pips"] for t in ordered], dtype=np.float64)
    r_wide = np.array([t["r_wide"] for t in ordered], dtype=np.float64)
    equity = np.cumsum(r)
    peak = np.maximum.accumulate(np.concatenate(([0.0], equity)))
    # Drawdown against a curve that starts at 0.
    dd = float(np.max(peak[1:] - equity)) if len(equity) else 0.0
    wins = r[r > 0].sum()
    losses = -r[r < 0].sum()
    if losses > 1e-12:
        pf = float(wins / losses)
    elif wins > 0:
        pf = float("inf")
    else:
        pf = None
    outcomes: dict[str, int] = {}
    for t in ordered:
        outcomes[t["outcome"]] = outcomes.get(t["outcome"], 0) + 1

    by_year: dict[int, list[float]] = {}
    for t in ordered:
        year = datetime.fromtimestamp(int(t["exit_ts"]), tz=timezone.utc).year
        by_year.setdefault(year, []).append(float(t["r"]))
    years = [{
        "year": year,
        "trades": len(vals),
        "expectancy_r": float(np.mean(vals)),
    } for year, vals in sorted(by_year.items())]

    return {
        "trades": int(len(r)),
        "win_rate": float(np.mean(r > 0)),
        "profit_factor": pf,
        "expectancy_r": float(np.mean(r)),
        "expectancy_pips": float(np.mean(pips)),
        "expectancy_r_wide": float(np.mean(r_wide)),
        "max_drawdown_r": dd,
        "sharpe": _sharpe(ordered, start_ts, end_ts),
        "max_concurrent": _max_concurrent(ordered),
        "years": years,
        "outcomes": outcomes,
        "equity_ts": [int(t["exit_ts"]) for t in ordered],
        "equity_r": [float(x) for x in equity],
    }


def _sharpe(trades: list[dict], start_ts: int, end_ts: int) -> float | None:
    """Mean weekday R / std, annualized with sqrt(252). Quiet days are 0.

    This is a trade-exit Sharpe, not a mark-to-market Sharpe.
    """
    if end_ts <= start_ts:
        return None
    start = datetime.fromtimestamp(start_ts, tz=timezone.utc).date()
    end = datetime.fromtimestamp(end_ts, tz=timezone.utc).date()
    days = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    if len(days) < 20:
        return None
    index = {day: i for i, day in enumerate(days)}
    daily = np.zeros(len(days), dtype=np.float64)
    for t in trades:
        day = datetime.fromtimestamp(int(t["exit_ts"]), tz=timezone.utc).date()
        i = index.get(day)
        if i is not None:
            daily[i] += t["r"]
    std = float(daily.std(ddof=1)) if len(daily) > 1 else 0.0
    if std < 1e-12:
        return None
    return float(daily.mean() / std * np.sqrt(252))


def _max_concurrent(trades: list[dict]) -> int:
    events = []
    for t in trades:
        events.append((int(t["entry_ts"]), 1))
        events.append((int(t["exit_ts"]), -1))
    events.sort()
    cur = mx = 0
    for _, delta in events:
        cur += delta
        if cur > mx:
            mx = cur
    return mx


def is_robust(oos: dict, ins: dict) -> bool:
    """Every hurdle is required. Failing one means the row is not robust."""
    if oos["trades"] < MIN_OOS_TRADES:
        return False
    if oos["expectancy_r"] is None or oos["expectancy_r"] <= MIN_OOS_EXP_R:
        return False
    pf = oos["profit_factor"]
    if pf is None or pf < MIN_OOS_PF:
        return False
    if ins["trades"] > 0 and (ins["expectancy_r"] is None or ins["expectancy_r"] <= 0):
        return False
    if oos["expectancy_r_wide"] is None or oos["expectancy_r_wide"] <= 0:
        return False
    years = [y for y in oos["years"] if y["trades"] >= MIN_YEAR_TRADES]
    if years:
        positive = sum(1 for y in years if y["expectancy_r"] > 0)
        if positive * 2 <= len(years):
            return False
    return True
