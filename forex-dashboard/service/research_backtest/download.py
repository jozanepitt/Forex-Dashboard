"""Download Dukascopy BID M1 candles for the research backtest.

Source: Dukascopy's public chart feed (freeserv.dukascopy.com, via the
dukascopy-python client). BID prices, not Exness prices. The backtest overlays
an Exness Standard spread on top of these bids — see bot/costs.py.

The files are large and gitignored. Re-run this module to reproduce them:

    python -m research_backtest.download

Range is 2023-01-02 through 2026-10-17 (about 3.8 years).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

DATA_DIR = Path(__file__).resolve().parent / "data"
START = datetime(2023, 1, 2, tzinfo=timezone.utc)
END = datetime(2026, 10, 17, tzinfo=timezone.utc)

# Majors, liquid JPY crosses the scanners already cover, and gold.
SYMBOLS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD",
    "USD/CAD", "NZD/USD", "EUR/JPY", "GBP/JPY", "XAU/USD",
]


def _symbol_path(symbol: str) -> Path:
    return DATA_DIR / f"{symbol.replace('/', '_')}.npz"


def _fetch_symbol(symbol: str) -> dict:
    import dukascopy_python as dk

    parts_ts = []
    parts = []
    cursor = START
    step = timedelta(days=16)
    while cursor < END:
        chunk_end = min(cursor + step + timedelta(hours=6), END)
        last_err = None
        df = None
        for attempt in range(5):
            try:
                df = dk.fetch(
                    symbol, dk.INTERVAL_MIN_1, dk.OFFER_SIDE_BID,
                    cursor, chunk_end, max_retries=5,
                )
                break
            except Exception as exc:  # noqa: BLE001 — network feed, retry
                last_err = exc
                time.sleep(2.0 * (attempt + 1))
        if df is None:
            raise RuntimeError(f"{symbol} {cursor.date()} failed: {last_err}")
        if len(df):
            # pandas 2 stores this index as datetime64[ms], so convert to
            # nanoseconds before dividing down to epoch seconds.
            ts = np.asarray(df.index.as_unit("ns").asi8 // 1_000_000_000, dtype=np.int64)
            parts_ts.append(ts)
            parts.append(np.column_stack([
                df["open"].to_numpy(np.float64),
                df["high"].to_numpy(np.float64),
                df["low"].to_numpy(np.float64),
                df["close"].to_numpy(np.float64),
                df["volume"].to_numpy(np.float64),
            ]))
        print(f"  {symbol} {cursor.date()} -> {chunk_end.date()} rows={0 if df is None else len(df)}", flush=True)
        cursor = cursor + step

    if not parts_ts:
        raise RuntimeError(f"no rows for {symbol}")
    ts = np.concatenate(parts_ts)
    ohlcv = np.concatenate(parts)
    order = np.argsort(ts, kind="mergesort")
    ts = ts[order]
    ohlcv = ohlcv[order]
    # Drop duplicate timestamps from the chunk overlap. Keep the first.
    keep = np.ones(len(ts), dtype=bool)
    keep[1:] = ts[1:] != ts[:-1]
    ts = ts[keep]
    ohlcv = ohlcv[keep]
    path = _symbol_path(symbol)
    tmp = path.with_suffix(".tmp.npz")
    np.savez_compressed(
        tmp, ts=ts, open=ohlcv[:, 0], high=ohlcv[:, 1],
        low=ohlcv[:, 2], close=ohlcv[:, 3], volume=ohlcv[:, 4],
    )
    tmp.replace(path)
    info = {
        "symbol": symbol,
        "rows": int(len(ts)),
        "start": datetime.fromtimestamp(int(ts[0]), timezone.utc).isoformat(),
        "end": datetime.fromtimestamp(int(ts[-1]), timezone.utc).isoformat(),
        "bid": True,
        "source": "dukascopy freeserv chart/json3 BID M1",
    }
    print(f"SAVED {symbol} {info['rows']} {info['start']} .. {info['end']}", flush=True)
    return info


def download_all(symbols: list[str] | None = None) -> list[dict]:
    from concurrent.futures import ProcessPoolExecutor, as_completed

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    symbols = symbols or SYMBOLS
    pending = []
    done = []
    for symbol in symbols:
        path = _symbol_path(symbol)
        if path.exists():
            with np.load(path) as z:
                ts = z["ts"]
                if len(ts) > 100_000 and int(ts[0]) <= int(START.timestamp()) + 86400 \
                        and int(ts[-1]) >= int(END.timestamp()) - 3 * 86400:
                    print(f"skip {symbol} ({len(ts)} rows already)", flush=True)
                    done.append({
                        "symbol": symbol, "rows": int(len(ts)),
                        "start": datetime.fromtimestamp(int(ts[0]), timezone.utc).isoformat(),
                        "end": datetime.fromtimestamp(int(ts[-1]), timezone.utc).isoformat(),
                        "bid": True, "source": "dukascopy freeserv chart/json3 BID M1",
                    })
                    continue
        pending.append(symbol)

    infos = list(done)
    if pending:
        with ProcessPoolExecutor(max_workers=min(3, len(pending))) as pool:
            futs = {pool.submit(_fetch_symbol, s): s for s in pending}
            for fut in as_completed(futs):
                infos.append(fut.result())
    infos.sort(key=lambda r: r["symbol"])
    manifest = {
        "start": START.isoformat(),
        "end": END.isoformat(),
        "offer_side": "BID",
        "interval": "M1",
        "symbols": infos,
    }
    (DATA_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return infos


if __name__ == "__main__":
    download_all()
