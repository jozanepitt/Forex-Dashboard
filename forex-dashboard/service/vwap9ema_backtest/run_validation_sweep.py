"""Runs the full VWAP+9EMA validation sweep: every symbol x session x variant
combination in the approved universe, applying the spec's own pass/fail
acceptance criteria, and writes VALIDATION_RESULTS.md.

See docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md
("Validation gate") for the exact criteria this implements.
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from backtest_mt5 import backtest, stats, VARIANTS  # noqa: E402

SYMBOLS = ["USTECm", "EURUSDm", "GBPUSDm", "USDJPYm", "USDCHFm", "AUDUSDm", "USDCADm", "NZDUSDm"]
SESSIONS_TO_TEST = ("London", "NY")
DATADIR = Path(__file__).parent / "data"
MIN_TRADES = 150
MIN_PF = 1.2


def passes_acceptance(oos_stats: dict, per_year_stats: dict) -> bool:
    """The spec's exact go/no-go rule for one symbol/session/variant
    combination's out-of-sample stats and full-sample per-year breakdown."""
    if oos_stats.get("n", 0) < MIN_TRADES:
        return False
    if oos_stats.get("exp_r", 0) <= 0:
        return False
    if oos_stats.get("pf", 0) < MIN_PF:
        return False
    if not per_year_stats:
        return False
    positive_years = sum(1 for s in per_year_stats.values() if s.get("exp_r", 0) > 0)
    return positive_years > len(per_year_stats) / 2


def per_year_breakdown(trades: list[dict]) -> dict:
    years: dict = {}
    for t in trades:
        years.setdefault(t["year"], []).append(t)
    return {y: stats(trs) for y, trs in years.items()}


def run_one(symbol: str, session: str, variant: str) -> dict:
    df = pd.read_csv(DATADIR / f"{symbol}_M5.csv", parse_dates=["time"])
    full_trades = backtest(df, session, variant=variant)
    cut = df["time"].quantile(0.60)
    oos_df = df[df["time"] > cut]
    oos_trades = backtest(oos_df, session, variant=variant)
    oos_stats = stats(oos_trades)
    year_stats = per_year_breakdown(full_trades)
    passed = passes_acceptance(oos_stats, year_stats)
    return dict(symbol=symbol, session=session, variant=variant,
                oos_stats=oos_stats, year_stats=year_stats, passed=passed)


def run_sweep() -> list:
    results = []
    for symbol in SYMBOLS:
        for session in SESSIONS_TO_TEST:
            for variant in VARIANTS:
                results.append(run_one(symbol, session, variant))
    return results


def render_report(results: list) -> str:
    lines = ["# VWAP+9EMA Validation Results", ""]
    passed = [r for r in results if r["passed"]]
    lines.append(f"**{len(passed)} / {len(results)} combinations passed.**")
    lines.append("")
    lines.append("| Symbol | Session | Variant | OOS n | OOS expR | OOS PF | Result |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in results:
        s = r["oos_stats"]
        verdict = "PASS" if r["passed"] else "FAIL"
        lines.append(f"| {r['symbol']} | {r['session']} | {r['variant']} | "
                      f"{s.get('n', 0)} | {s.get('exp_r', 0):+.3f} | {s.get('pf', 0):.2f} | {verdict} |")
    return "\n".join(lines) + "\n"


def main():
    results = run_sweep()
    report = render_report(results)
    out_path = Path(__file__).parent / "VALIDATION_RESULTS.md"
    out_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"Written to {out_path}")


if __name__ == "__main__":
    main()
