"""Run every scanner through the walk-forward harness.

    python -m research_backtest.run
    python -m research_backtest.run --symbols EUR/USD --strategies engulfing_h1

In-sample is signal time before 2025-04-01. Out-of-sample is the rest.
Nothing in the scanner parameters is chosen from the out-of-sample window.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

SERVICE = Path(__file__).resolve().parents[1]
if str(SERVICE) not in sys.path:
    sys.path.insert(0, str(SERVICE))

from bot.signals import (  # noqa: E402
    LOOKBACK, extract_bounce, extract_btmm, extract_engulfing, extract_id50,
    extract_mr, extract_tdi, extract_vwap9, mr_prefilter,
)
from research_backtest.data_io import as_bars, load_m1, resample_all  # noqa: E402
from research_backtest.engine import Spec, run_crt, run_spec  # noqa: E402

ALL = [
    # Heaviest four first so the worker pool starts them before the lighter symbols.
    "EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD",
    "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD", "EUR/JPY", "GBP/JPY",
]
CORE = ["EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD"]

SPECS: list[Spec] = [
    Spec("engulfing_m1", "engulfing", "M1", "core", extract_engulfing),
    Spec("engulfing_m5", "engulfing", "M5", "all", extract_engulfing),
    Spec("engulfing_m15", "engulfing", "M15", "all", extract_engulfing),
    Spec("engulfing_h1", "engulfing", "H1", "all", extract_engulfing),
    Spec("engulfing_h4", "engulfing", "H4", "all", extract_engulfing),
    Spec("engulfing_h1_retrace50", "engulfing", "H1", "all", extract_engulfing, entry_mode="retrace50"),
    Spec("engulfing_m15_retrace50", "engulfing", "M15", "all", extract_engulfing, entry_mode="retrace50"),
    Spec("vwap_bounce_m1", "vwap_bounce", "M1", "core", extract_bounce),
    Spec("vwap_bounce_m5", "vwap_bounce", "M5", "all", extract_bounce),
    Spec("vwap_bounce_m15", "vwap_bounce", "M15", "all", extract_bounce),
    Spec("vwap_mr_m5", "vwap_mr", "M5", "core", extract_mr, prefilter=mr_prefilter),
    Spec("vwap_mr_m15", "vwap_mr", "M15", "all", extract_mr, prefilter=mr_prefilter),
    Spec("vwap9ema_m1", "vwap9ema", "M1", "core", extract_vwap9),
    Spec("vwap9ema_m5", "vwap9ema", "M5", "all", extract_vwap9),
    Spec("id50_m5", "id50", "M5", "all", extract_id50),
    Spec("id50_m15", "id50", "M15", "all", extract_id50),
    Spec("tdi_m5", "tdi123", "M5", "core", extract_tdi, needs_d1=True),
    Spec("tdi_m15", "tdi123", "M15", "all", extract_tdi, needs_d1=True),
    Spec("tdi_h1", "tdi123", "H1", "all", extract_tdi, needs_d1=True),
    Spec("btmm_m15", "btmm", "M15", "all", extract_btmm),
]


def _specs_for(symbol: str, names: set[str] | None) -> list[Spec]:
    out = []
    for spec in SPECS:
        if names is not None and spec.name not in names:
            continue
        if spec.symbols == "core" and symbol not in CORE:
            continue
        out.append(spec)
    return out


def run_symbol(symbol: str, names: list[str] | None = None) -> list[dict]:
    wanted = set(names) if names else None
    specs = _specs_for(symbol, wanted)
    if not specs and wanted is not None and not ({"crt_5am", "crt_1am"} & wanted):
        return []
    print(f"LOAD {symbol}", flush=True)
    m1 = load_m1(symbol)
    arrays = resample_all(m1)
    needed = set()
    for spec in specs:
        needed.add(spec.timeframe)
        _, htf_name, _ = LOOKBACK[(spec.strategy, spec.timeframe)]
        if htf_name:
            needed.add(htf_name)
        if spec.needs_d1:
            needed.add("D1")
    bars = {tf: as_bars(arrays[tf]) for tf in needed}
    trades: list[dict] = []
    for spec in specs:
        t0 = time.perf_counter()
        got = run_spec(symbol, bars, arrays, m1, spec)
        trades.extend(got)
        print(f"  {symbol} {spec.name}: {len(got)} trades in {time.perf_counter() - t0:.1f}s", flush=True)
    return trades


def _pool_worker(payload: tuple[str, list[str] | None]) -> list[dict]:
    symbol, names = payload
    return run_symbol(symbol, names)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Walk-forward backtest of the dashboard scanners")
    parser.add_argument("--symbols", default="", help="Comma-separated subset")
    parser.add_argument("--strategies", default="", help="Comma-separated spec names")
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args(argv)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] or ALL
    names = [s.strip() for s in args.strategies.split(",") if s.strip()] or None

    if len(symbols) == 1 or args.workers <= 1:
        trades = []
        for symbol in symbols:
            trades.extend(run_symbol(symbol, names))
    else:
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor

        ctx = mp.get_context("fork")
        trades = []
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=ctx) as pool:
            for got in pool.map(_pool_worker, [(s, names) for s in symbols]):
                trades.extend(got)

    want_crt = names is None or any(n.startswith("crt_") for n in names)
    if want_crt:
        print("CRT (daily, no look-ahead past the key-window cutoff)", flush=True)
        partners = {}
        for symbol in ("EUR/USD", "GBP/USD"):
            if symbol not in symbols:
                continue
            m1 = load_m1(symbol)
            arrays = resample_all(m1)
            partners[symbol] = (m1, arrays, as_bars(arrays["M15"]))
        for symbol in symbols:
            if symbol in partners:
                m1, arr, m15 = partners[symbol]
            else:
                m1 = load_m1(symbol)
                arr = resample_all(m1)
                m15 = as_bars(arr["M15"])
            partner = partner_ts = None
            if symbol == "EUR/USD" and "GBP/USD" in partners:
                partner = partners["GBP/USD"][2]
                partner_ts = partners["GBP/USD"][1]["M15"]["ts"]
            elif symbol == "GBP/USD" and "EUR/USD" in partners:
                partner = partners["EUR/USD"][2]
                partner_ts = partners["EUR/USD"][1]["M15"]["ts"]
            for which in ("5am", "1am"):
                if names is not None and f"crt_{which}" not in names:
                    continue
                t0 = time.perf_counter()
                got = run_crt(symbol, m15, arr["M15"]["ts"], partner, partner_ts, m1, which)
                trades.extend(got)
                print(f"  {symbol} crt_{which}: {len(got)} in {time.perf_counter() - t0:.1f}s", flush=True)
            if symbol not in partners:
                del m1, arr, m15

    print(f"TOTAL TRADES {len(trades)}", flush=True)
    from research_backtest.report import write_report
    path = write_report(trades, symbols)
    print(f"WROTE {path}", flush=True)


if __name__ == "__main__":
    main()
