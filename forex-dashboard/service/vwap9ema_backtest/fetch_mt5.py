"""
fetch_mt5.py  —  pull M5 bars (with REAL per-bar spread + tick volume) from MT5.

Ported near-verbatim from the source MT5_Execution_Plan.md's provided script
(C:\\Users\\jzpit\\Downloads\\fetch_mt5.py) into the repo for reproducibility.
Runs standalone — does not touch app.py/scheduler.py/alerts.py.

Requires a running, logged-in MetaTrader 5 terminal (Exness) — the Python
`MetaTrader5` package attaches to it. Confirmed working on this machine.

Examples:
    python fetch_mt5.py --list
    python fetch_mt5.py --symbols USTECm EURUSDm GBPUSDm --from 2021-01-01
"""
import argparse, sys, datetime as dt
import pandas as pd, numpy as np

def connect():
    import MetaTrader5 as mt5
    if not mt5.initialize():
        print("initialize() failed:", mt5.last_error()); sys.exit(1)
    return mt5

def list_symbols(mt5):
    want = ("US100","USTEC","NAS","NDX","EUR","GBP","USD","JPY","CHF","CAD","AUD","NZD")
    for s in mt5.symbols_get():
        if any(w in s.name.upper() for w in want):
            print(f"{s.name:16s} digits={s.digits} point={s.point}")

def detect_server_offset(df):
    """Guess broker server UTC offset from the tick-volume profile.
    The daily volume trough sits ~21:00-22:00 UTC (FX rollover). We map the
    observed trough hour to 22:00 UTC and report the implied offset."""
    vh = df.groupby(df['time'].dt.hour)['tick_volume'].mean()
    trough = int(vh.idxmin())
    offset = (trough - 22) % 24
    if offset > 12: offset -= 24
    return offset, trough

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--symbols", nargs="*", default=[])
    ap.add_argument("--from", dest="dfrom", default="2021-01-01")
    ap.add_argument("--to", dest="dto", default=None)
    ap.add_argument("--tf", default="M5")
    ap.add_argument("--outdir", default="data")
    a = ap.parse_args()

    mt5 = connect()
    print("MT5:", mt5.version(), "| terminal:", mt5.terminal_info().name if mt5.terminal_info() else "?")
    if a.list:
        list_symbols(mt5); mt5.shutdown(); return

    TF = {"M1":mt5.TIMEFRAME_M1,"M5":mt5.TIMEFRAME_M5,"M15":mt5.TIMEFRAME_M15}[a.tf]
    d0 = dt.datetime.fromisoformat(a.dfrom)
    d1 = dt.datetime.fromisoformat(a.dto) if a.dto else dt.datetime.now()
    import os; os.makedirs(a.outdir, exist_ok=True)

    for sym in a.symbols:
        if not mt5.symbol_select(sym, True):
            print(f"!! {sym}: cannot select (check exact name via --list)"); continue
        info = mt5.symbol_info(sym)

        # Fetch data in 90-day chunks to stay under MT5's ~44k-bar copy_rates_range ceiling
        dfs = []
        chunk_days = 90
        current = d0
        while current < d1:
            window_end = current + dt.timedelta(days=chunk_days)
            if window_end > d1:
                window_end = d1
            rates = mt5.copy_rates_range(sym, TF, current, window_end)
            if rates is None or len(rates) == 0:
                print(f"!! {sym}: no data for window {current.date()}..{window_end.date()} ({mt5.last_error()})")
            else:
                chunk_df = pd.DataFrame(rates)
                chunk_df['time'] = pd.to_datetime(chunk_df['time'], unit='s')
                dfs.append(chunk_df)
            current = window_end

        if len(dfs) == 0:
            print(f"!! {sym}: no data"); continue

        df = pd.concat(dfs, ignore_index=True)
        # Drop duplicates at window boundaries
        df = df.drop_duplicates(subset=['time'], keep='first')
        # Sort by time ascending
        df = df.sort_values('time').reset_index(drop=True)
        df = df.rename(columns={'spread':'spread_points'})
        df['point'] = info.point
        keep = ['time','open','high','low','close','tick_volume','spread_points','point']
        df = df[keep]
        off, trough = detect_server_offset(df)
        path = f"{a.outdir}/{sym}_{a.tf}.csv"
        df.to_csv(path, index=False)
        print(f"OK {sym}: {len(df):>7} bars {df['time'].min()}..{df['time'].max()} "
              f"| median spread={df['spread_points'].median():.0f}pts point={info.point} "
              f"| server offset≈UTC{off:+d} (vol trough {trough:02d}h)  -> {path}")
    mt5.shutdown()

if __name__ == "__main__":
    main()
