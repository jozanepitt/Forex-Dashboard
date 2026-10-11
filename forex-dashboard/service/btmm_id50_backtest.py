"""
Walk-forward backtest for BTMM ID50 (M15 50-EMA bounce).

Purpose: decide whether ID50 earns live Discord alerts (BTMM_ID50_ALERTS_ENABLED
defaults to false and only flips post-backtest).

Scope: read-only on the cache (cache.read_candles, limit=DEFAULT_BACKFILL) —
no live fetch, no API quota spent. M15 is the walk-forward axis; H1 feeds bias
only. One open trade per pair per config (mirror tdi123_backtest). Outcome
convention matches the live auto-tracked journal: win at TP1, loss at SL, and
worst-case tie-break if a single bar touches both (SL wins). A trade still open
after BARRIER_BARS (96 M15 bars = one trading day) is closed as "scratch" (0
pips, own bucket, excluded from win rate / expectancy).

Known deviations from live alerting (both optimistic, not pessimistic):
  - No high-impact-news gate (BTMM_ID50_NEWS_FILTER) — no offline news dataset
    to replay. Live would suppress a subset of these signals.
  - No alert throttle (_is_throttled) — redundant here: "one open trade at a
    time per pair" already prevents re-entering a still-open setup.
  - Session gate IS honoured (in_active_session is False blocks), or when
    BTMM_ID50_SESSION_FILTER is set. Grade gate is --grades, default A+/A/B.

Filter contribution: every bar where the pipeline emits a BUY/SELL setup is
funnelled through the gates (plan complete -> grade tier -> R:R -> session ->
trade open?) and reported per gate with the taken-trade win rate / expectancy,
so you can see what each filter removes and what it leaves.

How to run (from service/):
    python btmm_id50_backtest.py --pair EUR/USD GBP/CHF
    python btmm_id50_backtest.py --pair EUR/USD --grades ALL
    python btmm_id50_backtest.py --pair EUR/USD GBP/CHF --sweep

Writes btmm_id50_backtest_results.txt + .json into service/.

Rollback: this module is read-only — delete the two result files to roll back.
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Optional

import btmm_id50
import cache
from alerts import _check_rr
from config import DEFAULT_BACKFILL, PRIORITY_PAIRS

log = logging.getLogger("btmm_id50_backtest")

WARMUP_BARS = 220                 # MIN_DATA_BARS — enough for e50/e200 + cross
WARMUP_H1_BARS = 400              # trailing H1 bias window per step
BARRIER_BARS = btmm_id50.BARRIER_BARS

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


def _pip(price: float) -> float:
    return 0.0001 if price < 10 else 0.01


def _passes_gate(row: dict, setup: str, grades: set[str]) -> tuple[bool, Optional[str]]:
    """Trade-entry gate. Mirrors alert_id50_setup's chain minus news/throttle
    (see module docstring) but lets --grades broaden past the live A+/A/B set
    so tier behaviour can be compared. Returns (ok, failed_gate_name)."""
    grade = row.get("grade")
    if grade not in grades:
        return False, "grade"
    if row.get("in_active_session") is False:
        return False, "session"
    plan = row.get("trade_plan") or {}
    entry, sl, tp1 = plan.get("entry"), plan.get("sl"), plan.get("tp1")
    if not (entry and sl and tp1):
        return False, "plan"
    if abs(entry - sl) < 1e-9:
        return False, "rr"
    if not _check_rr(entry, sl, tp1, "buy" if setup == "BUY" else "sell", min_rr=0.8, symbol=row.get("symbol")):
        return False, "rr"
    return True, None


def _run_one(pair: str, m15_bars: list[dict], h1_bars: list[dict],
             grades: set[str], params: Optional[dict] = None,
             verbose: bool = False) -> dict:
    """Walk-forward one pair. Opens at most one trade at a time."""
    trades: list[dict] = []
    open_trade: Optional[dict] = None
    open_bars = 0
    funnel = {"emitted": 0, "plan": 0, "grade": 0, "session": 0, "rr": 0,
              "blocked_open": 0, "taken": 0}

    n = len(m15_bars)
    if n < WARMUP_BARS:
        return {"trades": [], "funnel": funnel}

    h1_i = 0  # rolling pointer into H1 bars (ts_utc ascending)

    for i in range(WARMUP_BARS, n):
        bar = m15_bars[i]
        ts = bar["ts_utc"]

        if open_trade:
            open_bars += 1
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
                open_trade, open_bars = None, 0
                continue
            if open_bars >= BARRIER_BARS:
                open_trade.update(ts_close=ts, exit=bar["close"], result="scratch", pips=0)
                trades.append(open_trade)
                open_trade, open_bars = None, 0
                continue
            continue  # trade still open, read no new entry this bar

        while h1_i < len(h1_bars) and h1_bars[h1_i]["ts_utc"] <= ts:
            h1_i += 1

        entry_window = m15_bars[max(0, i - WARMUP_BARS):i + 1]
        h1_window = h1_bars[max(0, h1_i - WARMUP_H1_BARS):h1_i]

        row = btmm_id50.analyze_pair(pair, entry_window,
                                     h1_candles=h1_window or None, params=params)
        setup = row.get("setup")
        if setup not in ("BUY", "SELL"):
            continue
        funnel["emitted"] += 1

        ok, failed = _passes_gate(row, setup, grades)
        if failed:
            funnel[failed] += 1
            continue
        if open_trade:
            funnel["blocked_open"] += 1
            continue

        plan = row["trade_plan"]
        open_trade = {
            "pair": pair, "timeframe": "M15",
            "ts_open": ts, "ts_close": None,
            "direction": setup, "grade": row.get("grade"),
            "score": row.get("score"), "entry": plan["entry"],
            "sl": plan["sl"], "tp1": plan["tp1"],
            "exit": None, "result": "open", "pips": None,
        }
        open_bars = 0
        funnel["taken"] += 1
        if verbose:
            log.info("%s M15: opened %s @ %.5f (grade %s) ts=%s",
                      pair, setup, plan["entry"], row.get("grade"), ts)

    if open_trade:
        trades.append(open_trade)  # left "open" — excluded from win/loss stats

    return {"trades": trades, "funnel": funnel}


def _aggregate(trades: list[dict]) -> dict:
    closed = [t for t in trades if t["result"] != "open"]
    wins = [t for t in closed if t["result"] == "win"]
    losses = [t for t in closed if t["result"] == "loss"]
    be = [t for t in closed if t["result"] == "be"]
    scratch = [t for t in closed if t["result"] == "scratch"]
    decisive = wins + losses
    total_pips = sum(t["pips"] for t in closed) if closed else 0.0
    win_pips = sum(t["pips"] for t in wins) if wins else 0.0
    loss_pips = sum(t["pips"] for t in losses) if losses else 0.0
    win_rate = len(wins) / len(decisive) if decisive else 0.0
    avg_win = win_pips / len(wins) if wins else 0.0
    avg_loss = abs(loss_pips / len(losses)) if losses else 0.0
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
    return {
        "trades": len(closed), "open": len(trades) - len(closed),
        "wins": len(wins), "losses": len(losses), "be": len(be), "scratch": len(scratch),
        "win_rate_pct": round(win_rate * 100, 1),
        "total_pips": round(total_pips, 1),
        "avg_win_pips": round(avg_win, 1), "avg_loss_pips": round(avg_loss, 1),
        "expectancy_pips": round(expectancy, 2),
    }


def _grade_breakdown(trades: list[dict]) -> dict[str, dict]:
    by: dict[str, list[dict]] = {}
    for t in trades:
        if t["result"] == "open":
            continue
        by.setdefault(t.get("grade") or "?", []).append(t)
    return {g: _aggregate(v) for g, v in sorted(by.items())}


def _score_breakdown(trades: list[dict]) -> list[dict]:
    """Score-band filter contribution on the trades that actually filled: how
    win rate / expectancy change as the score threshold tightens (>= 12, 15,
    19, 22 = C, B, A, A+)."""
    closed = [t for t in trades if t["result"] != "open" and t["result"] != "scratch"]
    out = []
    for label, floor in ((">=12 (C)", 12), (">=15 (B)", 15), (">=19 (A)", 19), (">=22 (A+)", 22)):
        subset = [t for t in closed if (t.get("score") or 0) >= floor]
        agg = _aggregate(subset)
        agg["label"] = label
        out.append(agg)
    return out


def run_pairs(pairs: list[str], grades: set[str], params: Optional[dict] = None,
              verbose: bool = False) -> dict:
    all_trades: list[dict] = []
    per_pair: dict[str, dict] = {}
    funnel_total: dict[str, int] = {}

    for pair in pairs:
        t0 = time.time()
        m15 = cache.read_candles(pair, "15min", limit=DEFAULT_BACKFILL)
        h1 = cache.read_candles(pair, "1h", limit=DEFAULT_BACKFILL)
        res = _run_one(pair, m15, h1, grades, params=params, verbose=verbose)
        all_trades.extend(res["trades"])
        per_pair[pair] = _aggregate(res["trades"])
        for k, v in res["funnel"].items():
            funnel_total[k] = funnel_total.get(k, 0) + v
        log.info("%s M15: %d bars walked, %d trades (%.1fs)",
                  pair, max(0, len(m15) - WARMUP_BARS), len(res["trades"]), time.time() - t0)

    by_class: dict[str, list[dict]] = {}
    for t in all_trades:
        by_class.setdefault(_instrument_class(t["pair"]), []).append(t)

    return {
        "params": params or {},
        "grades": sorted(grades),
        "overall": _aggregate(all_trades),
        "by_class": {k: _aggregate(v) for k, v in sorted(by_class.items())},
        "per_pair": per_pair,
        "grade_tiers": _grade_breakdown(all_trades),
        "score_bands": _score_breakdown(all_trades),
        "funnel": funnel_total,
        "trades": all_trades,
    }


def _write_results(results: dict, out_txt: str = "btmm_id50_backtest_results.txt",
                   out_json: str = "btmm_id50_backtest_results.json") -> None:
    with open(out_json, "w") as f:
        json.dump({k: results[k] for k in ("params", "grades", "overall", "by_class",
                                           "per_pair", "grade_tiers", "score_bands",
                                           "funnel", "trades") if k in results},
                  f, indent=2, default=str)

    lines = ["BTMM ID50 backtest — cached-candles walk-forward (M15 axis, H1 bias)",
             f"grades={results.get('grades')} params={results.get('params') or 'live defaults'}",
             ""]
    overall = results["overall"]
    lines.append("=== overall ===")
    lines.append(json.dumps(overall, indent=2))
    lines.append("\n=== by instrument class ===")
    for k, v in results["by_class"].items():
        lines.append(f"{k}: {json.dumps(v)}")
    lines.append("\n=== grade tiers (closed, decisive win rate) ===")
    for g, v in results["grade_tiers"].items():
        lines.append(f"{g}: {json.dumps(v)}")
    lines.append("\n=== score-band filter contribution (closed trades) ===")
    for r in results["score_bands"]:
        lines.append(f"{r['label']}: {json.dumps({k: r[k] for k in ('trades','wins','losses','win_rate_pct','expectancy_pips')})}")
    lines.append("\n=== filter funnel (every emitted BUY/SELL, per cause) ===")
    lines.append(json.dumps(results["funnel"], indent=2))
    with open(out_txt, "w") as f:
        f.write("\n".join(lines) + "\n")


def _sweep(pairs: list[str], grades: set[str], verbose: bool = False) -> list[dict]:
    """Grid over the sweepable ID50 knobs, ranked best-first by expectancy."""
    grid = []
    for atr_mult in (0.5, 0.75, 1.0, 1.25):
        for cross_good in (10, 20):
            for retest_tol_atr in (0.25, 0.5, 0.75):
                grid.append({"MOVE_AWAY_ATR_MULT": atr_mult,
                             "CROSS_AGE_GOOD_MAX": cross_good,
                             "RETEST_TOL_ATR": retest_tol_atr})

    rows = []
    for params in grid:
        res = run_pairs(pairs, grades, params=params, verbose=verbose)
        rows.append({"params": params, "overall": res["overall"],
                     "by_class": res["by_class"], "grade_tiers": res["grade_tiers"],
                     "funnel": res["funnel"]})
        log.info("sweep %s -> expectancy %s (%d trades)",
                  params, res["overall"]["expectancy_pips"], res["overall"]["trades"])

    rows.sort(key=lambda r: r["overall"]["expectancy_pips"], reverse=True)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", dest="pairs", nargs="*", default=PRIORITY_PAIRS)
    parser.add_argument("--grades", nargs="*", default=["A+", "A", "B"])
    parser.add_argument("--sweep", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    grades_in = {g.upper() for g in args.grades}
    if "ALL" in grades_in:
        grades_in = {"A+", "A", "B", "C"}

    if args.sweep:
        print(f"\n=== ID50 SWEEP over {len(PRIORITY_PAIRS)} pairs — grid ranked by expectancy ===")
        ranked = _sweep(args.pairs, grades_in, verbose=args.verbose)
        print("config | trades | wins | WR% | total_pips | expectancy")
        for r in ranked:
            o = r["overall"]
            print(f"{r['params']} | {o['trades']} | {o['wins']} | {o['win_rate_pct']} | "
                  f"{o['total_pips']} | {o['expectancy_pips']}")
        with open("btmm_id50_backtest_sweep.json", "w") as f:
            json.dump(ranked, f, indent=2, default=str)
        print("\nSweep written to service/btmm_id50_backtest_sweep.json")
    else:
        results = run_pairs(args.pairs, grades_in, verbose=args.verbose)
        _write_results(results)
        print(f"\n=== ID50 — overall (grades {sorted(grades_in)}) ===")
        print(json.dumps(results["overall"], indent=2))
        print("=== grade tiers ===")
        print(json.dumps(results["grade_tiers"], indent=2))
        print("=== score-band filter contribution ===")
        for r in results["score_bands"]:
            print(f"{r['label']}: {json.dumps(r)}")
        print("=== filter funnel ===")
        print(json.dumps(results["funnel"], indent=2))
        print("\nResults written to service/btmm_id50_backtest_results.txt + .json")