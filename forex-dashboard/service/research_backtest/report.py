"""Write BACKTEST_RESULTS.md, summary.json, and equity-curve charts."""
from __future__ import annotations

import json
import math
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from bot.costs import describe
from research_backtest.engine import SPLIT_TS
from research_backtest.metrics import is_robust, summarize

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
PUBLIC_ARTIFACTS = Path("/opt/cursor/artifacts")
IS_START = int(datetime(2023, 1, 2, tzinfo=timezone.utc).timestamp())
OOS_END = int(datetime(2026, 10, 17, tzinfo=timezone.utc).timestamp())

STRATEGY_NOTES = """
## What each scanner actually does

These are the rules the harness called. The fill is the next tradable price after the signal bar closes, not the signal bar's own close.

- **Engulfing** (`engulfing_strategy.py`). The second candle's body covers the first. With the higher-timeframe trend it is a trade immediately; against the trend it waits for a second engulfing within 8 bars. Stop beyond the engulfing wick plus 0.1 ATR. Target is 2R. Native legs are H1 (trend from H4) and H4 (trend from daily). M1, M5, and M15 in this report are the same rules on a lower timeframe, not a separate system the dashboard runs. `engulfing_*_retrace50` is one extra variant: a limit at the 50% retrace of the engulfing candle, valid for three bars. It is the only entry-style variant in the run, not a search. Live Discord grade gate is A and B.
- **VWAP bounce** (`vwap_bounce_strategy.py`). M15. Session VWAP anchored at 00:00 UTC, 50-bar average of that VWAP sloping, price on the trend side. Trigger is a rejection that tags VWAP or a reclaim that holds the average. Stop beyond the pullback, at least 0.5 ATR. Target is the prior swing or 1.5R. Grade A needs relative tick-volume and a full body. The dashboard does not run this on M1 or M5; those rows are the same function on those bars.
- **VWAP mean reversion** (`vwap_mean_reversion_strategy.py`). M15. Fade a move of at least 2 session-sigma when the Kaufman efficiency ratio says the day is not trending. Stop is the larger of the extension wick and 0.75 ATR. Target is the VWAP. Time stop is 20 bars. Live alerts are grade A with score at least 9. M5 is the same function, not a live timeframe.
- **VWAP + 9 EMA** (`vwap9ema_strategy.py`). M5. During 09:00–23:00 UTC, the 9 EMA crossing the session VWAP. Stop just beyond the swing around the cross. Target 2R. There is no grade. An older, stricter version of this idea failed its own validation; this cross-only rule is a different rule and had not been backtested. M1 is the same cross on one-minute bars.
- **BTMM ID50** (`btmm_id50.py`). M15. First retest of the 50 EMA after a 13/50 cross, with an anchor, a move away, a trap, and a nameable entry candle. Stop under the anchor. Target is the move-away peak (trades with no target in the right direction are skipped). Live alerts are grade A and A+ inside the session. M5 is the same bar-count logic on five-minute bars, which is not what "ID50" means on the dashboard.
- **TDI 1-2-3** (`tdi_cycle_123.py`). H1 biased by H4, and M15 biased by H1. Swing 1-2-3 (the default mode, not the opt-in peak finder) plus a TDI baseline extreme, which is a hard gate. Stop beyond point 3 and at least 1 ATR away. Targets are the 50/200 EMAs in the trade direction, or a measured move. The 800 EMA target is included on H1 (2400-bar window) and not on M5/M15 (500-bar window), because the scanner itself ignores an 800 EMA until 2400 bars exist. Live alerts are grade A and B inside the London/New York window. M5 is a transplant.
- **BTMM core** (`btmm_core.py`). The Python backtester/alert implementation, which the repo's own audit says is not the same rules as the browser dashboard. A trade is taken only when a named setup or an A+ setup is active. Live A+-only mode is the alert filter. Window is 2500 M15 bars so the 800 EMA has its full seed.
- **CRT 1AM and 5AM** (`crt_strategy.py`). One decision per New York day at the end of the key window (04:00 NY for 1AM, 08:30 NY for 5AM), using only M15 bars that had closed by then. The setup candle is still forming, which is how the live scanner works. Entry is a limit at the order-block mid or the candle open, from `alerts._build_crt_trade_plan` (2R / 3R; the CRT notes do not define a stop). Limit expires at 08:00 NY (1AM) or 12:00 NY (5AM). EUR/USD and GBP/USD include each other as the SMT partner. 1AM CRT is not wired to live alerts anymore; it is still in the repo, so it is in the test.

## Method

- **Prices.** Dukascopy BID M1, 2023-01-02 through 2026-10-17, resampled to M5, M15, H1, H4, and D1 on UTC boundaries. A higher-timeframe bar is used once the next bucket has started, or once its own last minute exists. Friday's cash session ends before midnight, so the Friday daily bar and the 20:00 UTC H4 bar are the last print of that session, not a bar that runs to 23:59.
- **Split.** In-sample: signals before 2025-04-01. Out-of-sample: signals from that date on. About 2.25 years in, 1.5 years out. Scanner parameters were not fitted on the out-of-sample window.
- **Costs.** Exness Standard spreads from the table below, plus a small adverse slippage, commission $0. The spread is constant except during 21:00–22:00 UTC, where it is tripled as a stand-in for the rollover widen. That multiplier is not a recorded Exness schedule, and summer rollover is an hour later than winter, so one fixed hour does not match the broker's clock all year. There is no swap and no extra widening around news. A short pays the spread when it buys back, including on a take-profit. A buy limit fills only when the ask trades the limit, at the limit price. The 2× column walks the same barrier path at twice the spread (and twice the rollover widen) and adds one extra base spread to a buy-limit entry. It does not delete the trade.
- **One position per symbol per strategy.** A new signal while a trade is open is ignored. The same TDI pattern, ID50 cross, VWAP-MR extension, or 9-EMA cross is not re-entered until the scanner goes quiet.
- **Both-in-one-bar.** Stop fills before target. A market order can reach its target on the entry minute, because the fill is the open and the open is first. A limit order cannot: the target is ignored on the fill minute, and the stop still counts.
- **R.** P&L after the fill, divided by the distance from the scanner's entry to its stop.
- **Sharpe-like.** Sum of R by exit date, including weekdays with no trade as zero, times sqrt(252). It is not a mark-to-market Sharpe.
- **Robust.** These hurdles are the ones used for this report. They were committed together with the results, so this is not a pre-registered protocol. Out-of-sample trades at least 80, expectancy above +0.05R, profit factor at least 1.15, in-sample expectancy also positive, expectancy still positive at twice the spread, and a majority of out-of-sample years with at least 15 trades also positive. Missing any one of those, the row is not marked robust.
- **VWAP+9EMA speed filter.** The cross is rare, and recomputing it on every bar is most of the runtime. A vectorized 9 EMA and session VWAP marks candidate bars (a real cross, or the two lines within 1e-8 of price). The real `analyze_pair` still runs on every candidate. A sample of bars the filter skips is checked against `analyze_pair`; if that sample contains a signal, the filter is turned off for that symbol and the scanner runs on every bar.

Pooled pip averages mix euro-pips with gold pips. Use R for the pooled number. Per-symbol pips are in the symbol table.
"""


def _fmt_r(value) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "—"
    return f"{value:+.3f}"


def _fmt_pct(value) -> str:
    if value is None:
        return "—"
    return f"{100 * value:.1f}%"


def _fmt_pf(value) -> str:
    if value is None:
        return "—"
    if math.isinf(value):
        return "inf"
    return f"{value:.2f}"


def _fmt_n(value) -> str:
    if value is None:
        return "—"
    return f"{value:.2f}"


def _row(stats: dict) -> str:
    return " | ".join([
        str(stats["trades"]),
        _fmt_pct(stats["win_rate"]),
        _fmt_pf(stats["profit_factor"]),
        _fmt_r(stats["expectancy_r"]),
        _fmt_n(stats["expectancy_pips"]),
        _fmt_n(stats["max_drawdown_r"]),
        _fmt_n(stats["sharpe"]),
        _fmt_r(stats["expectancy_r_wide"]),
    ])


def _group(trades: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for trade in trades:
        out[trade["strategy"]].append(trade)
    return out


def _pack(trades: list[dict], start: int, end: int) -> dict:
    stats = summarize(trades, start, end)
    # Equity series is for the chart; keep the stats copy small in JSON.
    equity_ts = stats.pop("equity_ts", [])
    equity_r = stats.pop("equity_r", [])
    return stats, equity_ts, equity_r


def write_report(trades: list[dict], symbols: list[str]) -> Path:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    grouped = _group(trades)
    names = sorted(grouped)
    rows = []
    curves = []
    for name in names:
        bucket = grouped[name]
        ins, _, _ = _pack([t for t in bucket if t["split"] == "is"], IS_START, SPLIT_TS)
        oos, ts, eq = _pack([t for t in bucket if t["split"] == "oos"], SPLIT_TS, OOS_END)
        alert = [t for t in bucket if t["split"] == "oos" and t.get("alert")]
        alert_is = [t for t in bucket if t["split"] == "is" and t.get("alert")]
        oos_alert, _, _ = _pack(alert, SPLIT_TS, OOS_END)
        is_alert, _, _ = _pack(alert_is, IS_START, SPLIT_TS)
        by_symbol = {}
        for symbol in symbols:
            sym_oos = [t for t in bucket if t["split"] == "oos" and t["symbol"] == symbol]
            if not sym_oos:
                continue
            sym_stats, _, _ = _pack(sym_oos, SPLIT_TS, OOS_END)
            by_symbol[symbol] = {
                "trades": sym_stats["trades"],
                "expectancy_r": sym_stats["expectancy_r"],
                "expectancy_pips": sym_stats["expectancy_pips"],
                "profit_factor": sym_stats["profit_factor"],
                "win_rate": sym_stats["win_rate"],
                "max_drawdown_r": sym_stats["max_drawdown_r"],
            }
        robust = is_robust(oos, ins)
        robust_alert = is_robust(oos_alert, is_alert)
        rows.append({
            "strategy": name,
            "is": ins,
            "oos": oos,
            "oos_alert": oos_alert,
            "is_alert": is_alert,
            "by_symbol": by_symbol,
            "robust": robust,
            "robust_alert": robust_alert,
        })
        curves.append((name, ts, eq, oos["trades"]))

    _charts(rows, curves)
    summary = {
        "split": "2025-04-01",
        "symbols": symbols,
        "robust_rule": {
            "min_oos_trades": 80,
            "min_oos_expectancy_r": 0.05,
            "min_oos_profit_factor": 1.15,
            "in_sample_expectancy_positive": True,
            "wide_spread_expectancy_positive": True,
            "majority_of_oos_years_positive": True,
        },
        "costs": describe(),
        "rows": rows,
    }
    (ARTIFACTS / "summary.json").write_text(
        json.dumps(_sanitize(summary), indent=2, allow_nan=False)
    )
    path = ROOT / "BACKTEST_RESULTS.md"
    path.write_text(_markdown(summary))
    _publish()
    return path


def _sanitize(value):
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _charts(rows: list[dict], curves: list[tuple]) -> None:
    if not rows:
        return
    labels = [r["strategy"] for r in rows]
    vals = [r["oos"]["expectancy_r"] if r["oos"]["expectancy_r"] is not None else 0.0 for r in rows]
    colors = ["#2c7a4b" if v > 0 else "#9b2c2c" for v in vals]
    fig, ax = plt.subplots(figsize=(11, max(4, 0.38 * len(labels))))
    ax.barh(labels, vals, color=colors)
    ax.axvline(0, color="#333", linewidth=0.8)
    ax.set_xlabel("Out-of-sample expectancy (R per trade, after costs)")
    ax.set_title("Pooled across tested symbols")
    fig.tight_layout()
    fig.savefig(ARTIFACTS / "oos_expectancy.png", dpi=120)
    plt.close(fig)

    drawable = [(n, ts, eq) for n, ts, eq, count in curves if count >= 5 and eq]
    if not drawable:
        return
    cols = 2
    rows_n = math.ceil(len(drawable) / cols)
    fig, axes = plt.subplots(rows_n, cols, figsize=(12, 2.6 * rows_n), squeeze=False)
    for i, (name, ts, eq) in enumerate(drawable):
        ax = axes[i // cols][i % cols]
        xs = [datetime.fromtimestamp(t, tz=timezone.utc) for t in ts]
        ax.plot(xs, eq, color="#1f4e79", linewidth=1.0)
        ax.axhline(0, color="#999", linewidth=0.6)
        ax.set_title(name, fontsize=9)
        ax.tick_params(labelsize=7)
    for j in range(len(drawable), rows_n * cols):
        axes[j // cols][j % cols].axis("off")
    fig.suptitle("Out-of-sample cumulative R after costs", fontsize=12)
    fig.tight_layout()
    fig.savefig(ARTIFACTS / "equity_oos_grid.png", dpi=110)
    plt.close(fig)

    for name, ts, eq, count in curves:
        if count < 5 or not eq:
            continue
        fig, ax = plt.subplots(figsize=(8, 3.2))
        xs = [datetime.fromtimestamp(t, tz=timezone.utc) for t in ts]
        ax.plot(xs, eq, color="#1f4e79", linewidth=1.1)
        ax.axhline(0, color="#999", linewidth=0.6)
        ax.set_title(f"{name} out-of-sample cumulative R")
        ax.set_ylabel("R")
        fig.tight_layout()
        fig.savefig(ARTIFACTS / f"equity_{name}.png", dpi=110)
        plt.close(fig)


def _markdown(summary: dict) -> str:
    header = [
        "# Backtest results",
        "",
        "Walk-forward test of the scanners in this repo on Dukascopy BID data, "
        "with an Exness Standard spread and slippage added. Generated by "
        "`python -m research_backtest.run`.",
        "",
        STRATEGY_NOTES.strip(),
        "",
        "## Spread and slippage assumptions",
        "",
        "Exness Standard, commission $0. Not a live quote. These are the base spreads. "
        "During 21:00–22:00 UTC the backtest multiplies the spread by 3. There is no swap.",
        "",
        "| Symbol | Spread (pips) | Slippage (pips) |",
        "|---|---:|---:|",
    ]
    tested = set(summary["symbols"]) | {"EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD", "EUR/JPY", "GBP/JPY",
                                        "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD"}
    for row in summary["costs"]:
        if row["symbol"] in tested:
            header.append(f"| {row['symbol']} | {row['spread_pips']} | {row['slippage_pips']} |")

    cols = "Trades | Win rate | Profit factor | Exp (R) | Exp (pips) | Max DD (R) | Sharpe | Exp R at 2x spread"
    lines = header + [
        "",
        "## Pooled results, every signal",
        "",
        "In-sample is before 2025-04-01. Out-of-sample is from that date through 2026-10-17. "
        "Pooled pips are not comparable across gold and FX; read the R column.",
        "",
        f"| Strategy | Split | {cols} | Robust |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in summary["rows"]:
        for split, key in (("in-sample", "is"), ("out-of-sample", "oos")):
            flag = "yes" if (split == "out-of-sample" and row["robust"]) else ""
            lines.append(f"| {row['strategy']} | {split} | {_row(row[key])} | {flag} |")

    lines += [
        "",
        "## Pooled out-of-sample, live alert gate only",
        "",
        "Same trades, kept only when they match the scanner's default Discord gate "
        "(grade, and the session gate where the alerter turns that on by default). "
        "This was not tuned on the out-of-sample window.",
        "",
        f"| Strategy | {cols} | Robust |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in summary["rows"]:
        flag = "yes" if row["robust_alert"] else ""
        lines.append(f"| {row['strategy']} | {_row(row['oos_alert'])} | {flag} |")

    lines += [
        "",
        "## Out-of-sample by symbol",
        "",
        "Expectancy in R, then trade count. A blank cell means that symbol was not in that run "
        "(M1 and a few heavy M5 tests ran on EUR/USD, GBP/USD, USD/JPY, and XAU/USD only).",
        "",
    ]
    symbols = summary["symbols"]
    lines.append("| Strategy | " + " | ".join(symbols) + " |")
    lines.append("|---|" + "|".join(["---:" ] * len(symbols)) + "|")
    for row in summary["rows"]:
        cells = []
        for symbol in symbols:
            cell = row["by_symbol"].get(symbol)
            if not cell or cell["expectancy_r"] is None:
                cells.append("—")
            else:
                cells.append(f"{cell['expectancy_r']:+.2f} (n={cell['trades']})")
        lines.append(f"| {row['strategy']} | " + " | ".join(cells) + " |")

    lines += ["", "## Year by year, out-of-sample expectancy (R)", ""]
    lines.append("| Strategy | Years |")
    lines.append("|---|---|")
    for row in summary["rows"]:
        bits = [f"{y['year']}: {_fmt_r(y['expectancy_r'])} (n={y['trades']})" for y in row["oos"]["years"]]
        lines.append(f"| {row['strategy']} | {', '.join(bits) if bits else '—'} |")

    robust = [r["strategy"] for r in summary["rows"] if r["robust"]]
    robust_alert = [r["strategy"] for r in summary["rows"] if r["robust_alert"]]
    ranked = sorted(
        summary["rows"],
        key=lambda r: (r["oos"]["expectancy_r"] is not None, r["oos"]["expectancy_r"] or -999, r["oos"]["trades"]),
        reverse=True,
    )
    lines += ["", "## Verdict", ""]
    hurdles = (
        "The hurdles used here are at least 80 out-of-sample trades, expectancy above +0.05R, "
        "profit factor at least 1.15, in-sample expectancy also positive, expectancy still "
        "positive at twice the spread, and a majority of out-of-sample years with at least "
        "15 trades also positive. They were committed with these results, not registered beforehand."
    )
    if robust or robust_alert:
        lines.append(
            "Unfiltered rows that cleared every hurdle: "
            + ", ".join(robust or ["none"])
            + ". Alert-gate rows that cleared them: "
            + ", ".join(robust_alert or ["none"])
            + ". " + hurdles
        )
    else:
        lines.append(
            "No unfiltered row and no alert-gated row cleared every hurdle. " + hurdles
        )
    unfiltered_pos = [
        r for r in summary["rows"]
        if r["oos"]["trades"] and (r["oos"]["expectancy_r"] or 0) > 0
    ]
    alert_pos = [
        r for r in summary["rows"]
        if r["oos_alert"]["trades"] and (r["oos_alert"]["expectancy_r"] or 0) > 0
    ]
    if unfiltered_pos:
        bits = [
            f"{r['strategy']} {_fmt_r(r['oos']['expectancy_r'])}R over {r['oos']['trades']} trades"
            for r in unfiltered_pos
        ]
        lines.append("Unfiltered pooled out-of-sample rows with a positive expectancy: " + "; ".join(bits) + ".")
    else:
        lines.append("Every unfiltered pooled out-of-sample expectancy in this run is negative.")
    if alert_pos:
        bits = [
            f"{r['strategy']} {_fmt_r(r['oos_alert']['expectancy_r'])}R over {r['oos_alert']['trades']} trades"
            for r in alert_pos
        ]
        lines.append(
            "The alert-gated table is a separate cut, and it is not all negative: "
            + "; ".join(bits)
            + ". A positive expectancy on a handful of trades, or one that fails the other hurdles, "
            "is not an edge."
        )
    else:
        lines.append("Every alert-gated out-of-sample expectancy in this run is negative or has no trades.")
    if ranked:
        best = ranked[0]
        exp = best["oos"]["expectancy_r"]
        dd = best["oos"]["max_drawdown_r"]
        dd_txt = "—" if dd is None else f"{dd:.2f}"
        lines.append(
            f"The highest unfiltered pooled out-of-sample expectancy in this run was **{best['strategy']}** "
            f"at {_fmt_r(exp)}R over {best['oos']['trades']} trades "
            f"(profit factor {_fmt_pf(best['oos']['profit_factor'])}, "
            f"max drawdown {dd_txt}R). "
            "Ranking the rows does not turn the top of a losing list into an edge."
        )
    lines.append(
        "Charts: `service/research_backtest/artifacts/oos_expectancy.png` and "
        "`equity_oos_grid.png`, plus one equity file per strategy with at least five out-of-sample trades."
    )
    lines += [
        "",
        "## What to run on the bot",
        "",
        "Nothing in this run is a candidate for automated orders. Leave `BOT_STRATEGY` empty. "
        "The hurdles above were applied to each pooled strategy, not to individual symbols. "
        "A positive cell in the symbol table was not a separate test. With this many cells, "
        "a few positive ones show up even when the rule has no edge. They are not a system to trade.",
        "",
        "The live-alert column is the same trades after the scanner's default Discord gate. "
        "Where that column has only a handful of trades, the gate almost never fires, so that "
        "win rate is not a sample. CRT's alert column is empty because the harness decides at "
        "the end of the key window, and the grade/status gate used for Discord did not pass on "
        "those rows. The raw CRT rows are the result that counts, and they lost money after costs.",
        "",
    ]
    return "\n".join(lines) + "\n"


def _publish() -> None:
    if not PUBLIC_ARTIFACTS.exists():
        return
    dest = PUBLIC_ARTIFACTS / "backtest"
    dest.mkdir(parents=True, exist_ok=True)
    for path in ARTIFACTS.glob("*.png"):
        shutil.copy2(path, dest / path.name)
