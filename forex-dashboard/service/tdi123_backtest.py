"""
Walk-forward backtest for TDI Cycle 123, on H1 and M15 independently.

Mirrors backtest.py's engine (worst-case SL/TP1 tie-break, EMA-800 warmup
convention, single-open-trade-per-series) but drives tdi_cycle_123's
two-timeframe pipeline instead of btmm_core's single-timeframe one, and
reuses the exact live alert-quality gate from alerts.py so results reflect
what the Discord alerting would actually have fired on, not raw scanner
noise.

Uses ONLY cached candles (cache.read_candles) — no live fetch, no API
quota spent. Available history as of 2026-09-17: H1 back to 2025-11-20
(~300d), M15 back to 2026-04-08 (~162d), H4/D1 much further back (bias/ADR
only, never the walk-forward axis).

Known deviations from live alerting (both make this an optimistic upper
bound, not a pessimistic one):
  - No historical high-impact-news gate (TDI123_NEWS_FILTER) — no offline
    news dataset to replay. Live would suppress some of these.
  - No alert throttle (_is_throttled) — irrelevant here since "one open
    trade at a time per pair+timeframe" already prevents re-entering a
    still-open setup, which is the throttle's practical effect.

Exit convention matches the live auto-tracked journal exactly: win at
TP1, loss at SL, worst-case tie-break if a single bar touches both.
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Optional

import cache
from alerts import _should_alert_tdi123
from config import PRIORITY_PAIRS
from tdi_cycle_123 import _analyze_timeframe

log = logging.getLogger("tdi123_backtest")

EMA800_WARMUP_BARS = 2400   # same convergence standard used everywhere else
BIAS_WINDOW_BARS = 2400     # trailing bias-candles window fed per step
MIN_ENTRY_BARS = 100        # _analyze_timeframe's own floor

METAL = {"XAU/USD", "XAG/USD"}
INDEX = {"DE30", "US30", "USTEC", "DXY"}
FX_MAJOR = {"EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD"}


def _instrument_class(pair: str) -> str:
    if pair in METAL:
        return "METAL"
    if pair in INDEX:
        return "INDEX"
    if pair in FX_MAJOR:
        return "FX-MAJOR"
    return "FX-CROSS"


def _passes_full_alert_gate(row: dict, setup: str) -> bool:
    """Mirror alerts.alert_tdi123_setup's gate chain (minus the news filter
    and throttle — see module docstring). Kept as an inline duplicate,
    not a shared import, because alert_tdi123_setup itself isn't a pure
    predicate (it also builds/sends the Discord embed)."""
    if not _should_alert_tdi123(row):
        return False
    if not (row.get("signal_cross") or {}).get("present"):
        return False

    plan = row.get("trade_plan") or {}
    entry, sl, tp1 = plan.get("entry"), plan.get("sl"), plan.get("tp1")
    if not (entry and sl and tp1):
        return False

    sl_dist = abs(entry - sl)
    if sl_dist < 1e-9:
        return False

    def direction_ok(tp: float) -> bool:
        return (setup == "BUY" and tp > entry) or (setup == "SELL" and tp < entry)

    valid_tiers = [plan.get(k) for k in ("tp1", "tp2", "tp3")
                   if plan.get(k) and direction_ok(plan.get(k))]
    if not valid_tiers:
        return False
    best_rr = max(abs(tp - entry) / sl_dist for tp in valid_tiers)
    return best_rr >= 0.8


def _pip(price: float) -> float:
    return 0.0001 if price < 10 else 0.01


def _run_one(pair: str, timeframe: str, bias_timeframe: str,
             entry_bars: list[dict], bias_bars: list[dict], d1_bars: list[dict],
             verbose: bool = False) -> list[dict]:
    """Walk-forward one pair/timeframe. Returns closed+open trade dicts."""
    trades: list[dict] = []
    open_trade: Optional[dict] = None

    n = len(entry_bars)
    if n < EMA800_WARMUP_BARS + MIN_ENTRY_BARS:
        return trades

    bias_i = 0  # rolling pointer into bias_bars, avoids O(n) rescan per step
    d1_i = 0

    for i in range(EMA800_WARMUP_BARS, n):
        bar = entry_bars[i]
        ts = bar["ts_utc"]

        if open_trade:
            hi, lo = bar["high"], bar["low"]
            direction = open_trade["direction"]
            sl, tp1 = open_trade["sl"], open_trade["tp1"]
            hit_sl = (direction == "BUY" and lo <= sl) or (direction == "SELL" and hi >= sl)
            hit_tp1 = (direction == "BUY" and hi >= tp1) or (direction == "SELL" and lo <= tp1)
            if hit_sl or hit_tp1:
                exit_p = sl if hit_sl else tp1  # worst-case tie-break
                pip = _pip(bar["close"])
                pips = ((exit_p - open_trade["entry"]) / pip if direction == "BUY"
                         else (open_trade["entry"] - exit_p) / pip)
                result = "win" if pips > 0 else "loss" if pips < -1 else "be"
                open_trade.update(ts_close=ts, exit=exit_p, result=result, pips=round(pips, 1))
                trades.append(open_trade)
                open_trade = None
            else:
                continue  # trade still open, don't evaluate a new entry this bar

        # advance rolling bias/d1 pointers to include everything up to `ts`
        while bias_i < len(bias_bars) and bias_bars[bias_i]["ts_utc"] <= ts:
            bias_i += 1
        while d1_i < len(d1_bars) and d1_bars[d1_i]["ts_utc"] <= ts:
            d1_i += 1

        bias_window = bias_bars[max(0, bias_i - BIAS_WINDOW_BARS):bias_i]
        d1_window = d1_bars[:d1_i]
        entry_window = entry_bars[max(0, i - EMA800_WARMUP_BARS):i + 1]

        row = _analyze_timeframe(pair, entry_window, bias_window or None, d1_window or None,
                                  timeframe=timeframe, bias_timeframe=bias_timeframe)
        setup = row.get("setup")
        if setup not in ("BUY", "SELL"):
            continue
        if not _passes_full_alert_gate(row, setup):
            continue

        plan = row["trade_plan"]
        open_trade = {
            "pair": pair, "timeframe": timeframe,
            "ts_open": ts, "ts_close": None,
            "direction": setup, "grade": row.get("grade"),
            "entry": plan["entry"], "sl": plan["sl"], "tp1": plan["tp1"],
            "exit": None, "result": "open", "pips": None,
        }
        if verbose:
            log.info("%s %s: opened %s @ %.5f (grade %s) ts=%s",
                      pair, timeframe, setup, plan["entry"], row.get("grade"), ts)

    if open_trade:
        trades.append(open_trade)  # left "open" — excluded from win/loss stats

    return trades


def _aggregate(trades: list[dict]) -> dict:
    closed = [t for t in trades if t["result"] != "open"]
    wins = [t for t in closed if t["result"] == "win"]
    losses = [t for t in closed if t["result"] == "loss"]
    be = [t for t in closed if t["result"] == "be"]
    total_pips = sum(t["pips"] for t in closed) if closed else 0.0
    win_pips = sum(t["pips"] for t in wins) if wins else 0.0
    loss_pips = sum(t["pips"] for t in losses) if losses else 0.0
    win_rate = len(wins) / len(closed) if closed else 0.0
    avg_win = win_pips / len(wins) if wins else 0.0
    avg_loss = abs(loss_pips / len(losses)) if losses else 0.0
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
    return {
        "trades": len(closed), "open": len(trades) - len(closed),
        "wins": len(wins), "losses": len(losses), "be": len(be),
        "win_rate_pct": round(win_rate * 100, 1),
        "total_pips": round(total_pips, 1),
        "avg_win_pips": round(avg_win, 1), "avg_loss_pips": round(avg_loss, 1),
        "expectancy_pips": round(expectancy, 2),
    }


def run_timeframe(pairs: list[str], timeframe: str, bias_timeframe: str,
                   d1_limit: int = 4000, verbose: bool = False) -> dict:
    entry_interval = {"H1": "1h", "M15": "15min"}[timeframe]
    bias_interval = {"H4": "4h", "H1": "1h"}[bias_timeframe]

    all_trades: list[dict] = []
    per_pair: dict[str, dict] = {}

    for pair in pairs:
        t0 = time.time()
        entry_bars = cache.read_candles(pair, entry_interval, limit=999999)
        bias_bars = cache.read_candles(pair, bias_interval, limit=999999)
        d1_bars = cache.read_candles(pair, "1day", limit=d1_limit)

        trades = _run_one(pair, timeframe, bias_timeframe, entry_bars, bias_bars, d1_bars, verbose)
        all_trades.extend(trades)
        per_pair[pair] = _aggregate(trades)
        log.info("%s %s: %d bars walked, %d trades (%.1fs)",
                  pair, timeframe, len(entry_bars) - EMA800_WARMUP_BARS, len(trades), time.time() - t0)

    by_class: dict[str, list[dict]] = {}
    for t in all_trades:
        by_class.setdefault(_instrument_class(t["pair"]), []).append(t)

    return {
        "timeframe": timeframe,
        "overall": _aggregate(all_trades),
        "by_class": {k: _aggregate(v) for k, v in sorted(by_class.items())},
        "per_pair": per_pair,
        "trades": all_trades,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pairs", nargs="*", default=PRIORITY_PAIRS)
    parser.add_argument("--timeframes", nargs="*", default=["H1", "M15"])
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--out", default=None, help="write full JSON result here")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    results = {}
    if "H1" in args.timeframes:
        results["H1"] = run_timeframe(args.pairs, "H1", "H4", verbose=args.verbose)
    if "M15" in args.timeframes:
        results["M15"] = run_timeframe(args.pairs, "M15", "H1", verbose=args.verbose)

    for tf, res in results.items():
        print(f"\n=== {tf} — overall ===")
        print(json.dumps(res["overall"], indent=2))
        print(f"=== {tf} — by instrument class ===")
        print(json.dumps(res["by_class"], indent=2))

    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nFull result written to {args.out}")
