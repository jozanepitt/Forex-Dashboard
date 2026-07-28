"""
Walk-forward BTMM backtester.
Replays cached candles through btmm_core.analyze() bar by bar.
"""
from __future__ import annotations

import logging
from typing import Optional

import cache
from btmm_core import analyze

log = logging.getLogger("backtest")

MIN_BARS_WARMUP = 200


def run(pair: str, start_ts: int, end_ts: int,
        setups: Optional[list[str]] = None,
        min_gates: int = 5,
        interval: str = "15min") -> dict:
    """
    Walk-forward backtest for one pair over a date range.

    Returns:
      {
        stats: { trades, wins, losses, be, win_rate, expectancy,
                 avg_win_pips, avg_loss_pips, max_drawdown, total_pips },
        equity_curve: [ {ts, equity, pips} ],
        trades: [ {ts_open, ts_close, direction, entry, sl, tp1, exit, result, pips, setup} ]
      }
    """
    setups = setups or ["safety"]
    all_bars = cache.read_candles(pair, interval, limit=2000)

    # Filter to range
    bars = [b for b in all_bars if start_ts <= b["ts_utc"] <= end_ts]
    if len(bars) < MIN_BARS_WARMUP + 10:
        return {"error": f"Not enough bars ({len(bars)}) for {pair} in range"}

    # Need bars before range for warmup
    pre_bars = [b for b in all_bars if b["ts_utc"] < start_ts]
    full = pre_bars + bars

    trades = []
    open_trade: Optional[dict] = None
    equity = 0.0
    equity_curve = []
    peak_equity = 0.0
    max_dd = 0.0

    for i in range(MIN_BARS_WARMUP, len(full)):
        bar = full[i]
        if bar["ts_utc"] < start_ts:
            continue

        # Close open trade if SL or TP1 hit
        if open_trade:
            hi, lo = bar["high"], bar["low"]
            direction = open_trade["direction"]
            sl, tp1 = open_trade["sl"], open_trade["tp1"]

            hit_sl  = (direction == "bullish" and lo <= sl) or (direction == "bearish" and hi >= sl)
            hit_tp1 = (direction == "bullish" and hi >= tp1) or (direction == "bearish" and lo <= tp1)

            if hit_sl or hit_tp1:
                # Intrabar path is unknown from OHLC alone. When a single bar's
                # range touches both SL and TP1, assume worst-case (stop hit
                # first) instead of always crediting a win — avoids inflating
                # win rate/expectancy on wide-range or gap bars.
                exit_p = sl if hit_sl else tp1
                pip    = 0.0001 if bar["close"] < 10 else 0.01
                pips   = ((exit_p - open_trade["entry"]) / pip
                          if direction == "bullish"
                          else (open_trade["entry"] - exit_p) / pip)
                result = "win" if pips > 0 else "loss" if pips < -1 else "be"
                open_trade.update({
                    "ts_close": bar["ts_utc"],
                    "exit":     exit_p,
                    "result":   result,
                    "pips":     round(pips, 1),
                })
                trades.append(open_trade)
                equity += pips
                peak_equity = max(peak_equity, equity)
                max_dd = max(max_dd, peak_equity - equity)
                equity_curve.append({"ts": bar["ts_utc"], "equity": round(equity, 1), "pips": round(pips, 1)})
                open_trade = None

        # Don't open a new trade while one is open
        if open_trade:
            continue

        # Analyse current bar
        sig = analyze(full[max(0, i - 400): i + 1])

        if sig.get("signal") == "insufficient_data":
            continue

        setup = sig.get("active_setup")
        if not setup:
            continue
        if setup.get("key") not in setups:
            continue
        if setup.get("gatesPassed", 0) < min_gates:
            continue

        # Enter trade
        open_trade = {
            "ts_open":   bar["ts_utc"],
            "ts_close":  None,
            "direction": setup["direction"],
            "entry":     setup["entry"],
            "sl":        setup["sl"],
            "tp1":       setup["tp1"],
            "setup":     setup["key"],
            "gates":     setup["gatesPassed"],
            "exit":      None,
            "result":    "open",
            "pips":      None,
        }

    # Force-close any open trade at last bar
    if open_trade:
        last = full[-1]
        pip  = 0.0001 if last["close"] < 10 else 0.01
        pips = ((last["close"] - open_trade["entry"]) / pip
                if open_trade["direction"] == "bullish"
                else (open_trade["entry"] - last["close"]) / pip)
        open_trade.update({
            "ts_close": last["ts_utc"],
            "exit":     last["close"],
            "result":   "open",
            "pips":     round(pips, 1),
        })
        trades.append(open_trade)

    # Aggregate stats
    closed = [t for t in trades if t["result"] != "open"]
    wins   = [t for t in closed if t["result"] == "win"]
    losses = [t for t in closed if t["result"] == "loss"]
    be     = [t for t in closed if t["result"] == "be"]

    win_pips  = sum(t["pips"] for t in wins)   if wins   else 0
    loss_pips = sum(t["pips"] for t in losses) if losses else 0
    avg_win   = win_pips  / len(wins)   if wins   else 0
    avg_loss  = abs(loss_pips / len(losses)) if losses else 1
    win_rate  = len(wins) / len(closed) if closed else 0
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    return {
        "stats": {
            "trades":         len(closed),
            "wins":           len(wins),
            "losses":         len(losses),
            "be":             len(be),
            "win_rate":       round(win_rate * 100, 1),
            "total_pips":     round(equity, 1),
            "avg_win_pips":   round(avg_win, 1),
            "avg_loss_pips":  round(avg_loss, 1),
            "max_drawdown":   round(max_dd, 1),
            "expectancy":     round(expectancy, 2),
        },
        "equity_curve": equity_curve,
        "trades": trades,
    }
