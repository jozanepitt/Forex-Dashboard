"""
Walk-forward backtester for the VWAP Mean Reversion (M15) strategy.

Replays cached M15 candles through vwap_mean_reversion_strategy.analyze_pair()
bar by bar, entering trades on confirmed BUY/SELL setups at or above
`min_grade`, using the exact same entry/SL/TP the live scanner and Discord
alerts already use. This is a quick historical eyeball-check tool, NOT the
source document's walk-forward validation framework (out of scope — see
docs/superpowers/specs/2026-09-10-vwap-mean-reversion-strategy-design.md).

Two exit modes:
  "tp1" — exit at TP1 (VWAP, z=0) or the initial SL, whichever the bar
          range hits first (worst-case-if-both convention below). Default.
  "tp2" — exit at TP2 (the overshoot target) or the initial SL instead.

Both modes also force-close at market if neither SL nor the target is hit
within TIME_STOP_BARS bars of entry (doc §3.5 time stop), matching the live
scanner's "fresh" vs "stale" distinction — the retired VWAP+9EMA backtester
had no time-stop at all.

Mirrors vwap9ema_backtest.py's walk-forward shape (worst-case-if-both-hit-
in-one-bar rule, stats formula, result shape) so the existing dashboard
rendering can display either strategy's results unchanged.
"""
from __future__ import annotations

import logging
from typing import Optional

import cache
import vwap_mean_reversion_strategy
from config import INTERVAL_SECS

log = logging.getLogger("vwap_mean_reversion_backtest")

MIN_BARS_WARMUP = 100    # ~1 day of M15 -- enough for a clean VWAP day-reset + ER lookback
ANALYSIS_WINDOW = 400    # trailing bars fed to analyze_pair per call -- matches the live /vwap-mr fetch

_GRADE_RANK = {"A": 3, "B": 2, "C": 1}


def run(pair: str, start_ts: int, end_ts: int,
        min_grade: str = "B", interval: str = "15min",
        exit_mode: str = "tp1") -> dict:
    """
    Walk-forward backtest for one VWAP Mean Reversion pair over a date range.

    Returns the same {stats, equity_curve, trades} shape as backtest.run().
    """
    if pair not in vwap_mean_reversion_strategy.VWAP_MR_UNIVERSE:
        return {"error": f"VWAP Mean Reversion only supports majors: {', '.join(vwap_mean_reversion_strategy.VWAP_MR_UNIVERSE)}"}
    if exit_mode not in ("tp1", "tp2"):
        return {"error": f"unknown exit_mode '{exit_mode}'"}

    min_rank = _GRADE_RANK.get(min_grade, 2)
    time_stop_bars = vwap_mean_reversion_strategy.TIME_STOP_BARS

    interval_secs = INTERVAL_SECS.get(interval, 900)
    bars_in_range = int((end_ts - start_ts) / interval_secs) + 10
    fetch_limit = bars_in_range + ANALYSIS_WINDOW
    all_bars = cache.read_candles(pair, interval, limit=fetch_limit)

    bars = [b for b in all_bars if start_ts <= b["ts_utc"] <= end_ts]
    if len(bars) < MIN_BARS_WARMUP + 10:
        return {"error": f"Not enough bars ({len(bars)}) for {pair} in range"}

    # Need bars before range for the analysis window's warmup.
    pre_bars = [b for b in all_bars if b["ts_utc"] < start_ts]
    full = pre_bars + bars

    trades = []
    open_trade: Optional[dict] = None
    bars_since_entry = 0
    equity = 0.0
    equity_curve = []
    peak_equity = 0.0
    max_dd = 0.0

    for i in range(MIN_BARS_WARMUP, len(full)):
        bar = full[i]
        if bar["ts_utc"] < start_ts:
            continue

        if open_trade:
            bars_since_entry += 1
            hi, lo, close = bar["high"], bar["low"], bar["close"]
            direction = open_trade["direction"]
            sl = open_trade["sl"]
            target = open_trade["target"]

            hit_sl = (direction == "bullish" and lo <= sl) or (direction == "bearish" and hi >= sl)
            hit_target = (direction == "bullish" and hi >= target) or (direction == "bearish" and lo <= target)
            timed_out = bars_since_entry >= time_stop_bars

            exit_p = None
            if hit_sl or hit_target:
                # Intrabar path unknown from OHLC alone -- assume worst-case
                # (stop hit first) when a bar's range touches both, same
                # convention as the BTMM and retired VWAP+9EMA backtesters.
                exit_p = sl if hit_sl else target
            elif timed_out:
                exit_p = close  # doc §3.5 time stop -- force-close at market

            if exit_p is not None:
                pip = 0.0001 if bar["close"] < 10 else 0.01
                pips = ((exit_p - open_trade["entry"]) / pip
                        if direction == "bullish"
                        else (open_trade["entry"] - exit_p) / pip)
                result = "win" if pips > 0 else "loss" if pips < -1 else "be"
                open_trade.update({
                    "ts_close": bar["ts_utc"], "exit": exit_p,
                    "result": result, "pips": round(pips, 1),
                })
                trades.append(open_trade)
                equity += pips
                peak_equity = max(peak_equity, equity)
                max_dd = max(max_dd, peak_equity - equity)
                equity_curve.append({"ts": bar["ts_utc"], "equity": round(equity, 1), "pips": round(pips, 1)})
                open_trade = None
                bars_since_entry = 0

        if open_trade:
            continue

        # Windowed history, same rationale as vwap9ema_backtest.py: VWAP and
        # the ER regime filter both reset/recompute per UTC day or trailing
        # lookback using each candle's own timestamp, so a 400-bar trailing
        # window (matching the live /vwap-mr fetch) is correctness-safe and
        # keeps this O(n) instead of O(n^2) over a long backtest range.
        window = full[max(0, i - ANALYSIS_WINDOW + 1): i + 1]
        sig = vwap_mean_reversion_strategy.analyze_pair(pair, window)

        setup = sig.get("setup")
        if setup not in ("BUY", "SELL"):
            continue
        if _GRADE_RANK.get(sig.get("grade"), 0) < min_rank:
            continue

        entry, sl = sig.get("entry"), sig.get("sl")
        target = sig.get("tp1") if exit_mode == "tp1" else sig.get("tp2")
        if not (entry and sl and target):
            continue

        open_trade = {
            "ts_open": bar["ts_utc"], "ts_close": None,
            "direction": "bullish" if setup == "BUY" else "bearish",
            "entry": entry, "sl": sl, "target": target,
            "grade": sig.get("grade"), "gates": None,
            "exit": None, "result": "open", "pips": None,
        }
        bars_since_entry = 0

    # Force-close any open trade at last bar
    if open_trade:
        last = full[-1]
        pip = 0.0001 if last["close"] < 10 else 0.01
        pips = ((last["close"] - open_trade["entry"]) / pip
                if open_trade["direction"] == "bullish"
                else (open_trade["entry"] - last["close"]) / pip)
        open_trade.update({
            "ts_close": last["ts_utc"], "exit": last["close"],
            "result": "open", "pips": round(pips, 1),
        })
        trades.append(open_trade)

    # Aggregate stats
    closed = [t for t in trades if t["result"] != "open"]
    wins = [t for t in closed if t["result"] == "win"]
    losses = [t for t in closed if t["result"] == "loss"]
    be = [t for t in closed if t["result"] == "be"]

    win_pips = sum(t["pips"] for t in wins) if wins else 0
    loss_pips = sum(t["pips"] for t in losses) if losses else 0
    avg_win = win_pips / len(wins) if wins else 0
    avg_loss = abs(loss_pips / len(losses)) if losses else 1
    win_rate = len(wins) / len(closed) if closed else 0
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    return {
        "stats": {
            "trades": len(closed), "wins": len(wins), "losses": len(losses), "be": len(be),
            "win_rate": round(win_rate * 100, 1), "total_pips": round(equity, 1),
            "avg_win_pips": round(avg_win, 1), "avg_loss_pips": round(avg_loss, 1),
            "max_drawdown": round(max_dd, 1), "expectancy": round(expectancy, 2),
        },
        "equity_curve": equity_curve,
        "trades": trades,
    }
