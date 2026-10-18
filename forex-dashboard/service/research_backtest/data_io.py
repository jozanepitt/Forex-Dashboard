"""Load Dukascopy M1 archives and resample them without look-ahead.

A higher-timeframe bar is emitted only when its last minute is present, so
the close of an H1 bar is never taken from a partial hour at the end of the
file. Interior hours that the feed itself left thin (session gaps) are kept.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np

DATA_DIR = Path(__file__).resolve().parent / "data"
TF_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600, "H4": 14400, "D1": 86400}


def load_m1(symbol: str) -> dict[str, np.ndarray]:
    path = DATA_DIR / f"{symbol.replace('/', '_')}.npz"
    with np.load(path) as z:
        return {
            "ts": np.asarray(z["ts"], dtype=np.int64),
            "open": np.asarray(z["open"], dtype=np.float64),
            "high": np.asarray(z["high"], dtype=np.float64),
            "low": np.asarray(z["low"], dtype=np.float64),
            "close": np.asarray(z["close"], dtype=np.float64),
            "volume": np.asarray(z["volume"], dtype=np.float64),
        }


def resample(m1: dict[str, np.ndarray], tf: str) -> dict[str, np.ndarray]:
    if tf == "M1":
        return m1
    secs = TF_SECONDS[tf]
    ts = m1["ts"]
    if len(ts) == 0:
        empty = np.array([], dtype=np.float64)
        return {"ts": np.array([], dtype=np.int64), "open": empty, "high": empty,
                "low": empty, "close": empty, "volume": empty}
    bucket = (ts // secs) * secs
    change = np.flatnonzero(np.diff(bucket)) + 1
    starts = np.concatenate(([0], change))
    ends = np.concatenate((change, [len(ts)]))
    last_ts = ts[ends - 1]
    # A bucket counts only when its own final minute printed. Reduce on the
    # unfiltered starts first so a trailing partial hour is not folded into
    # the previous hour.
    complete = last_ts + 60 >= bucket[starts] + secs
    high = np.maximum.reduceat(m1["high"], starts)
    low = np.minimum.reduceat(m1["low"], starts)
    vol = np.add.reduceat(m1["volume"], starts)
    return {
        "ts": bucket[starts][complete].astype(np.int64),
        "open": m1["open"][starts][complete],
        "high": high[complete],
        "low": low[complete],
        "close": m1["close"][ends - 1][complete],
        "volume": vol[complete],
    }


def as_bars(arr: dict[str, np.ndarray]) -> list[dict]:
    ts = arr["ts"]
    out = [None] * len(ts)
    o, h, l, c, v = arr["open"], arr["high"], arr["low"], arr["close"], arr["volume"]
    for i in range(len(ts)):
        out[i] = {
            "ts_utc": int(ts[i]),
            "open": float(o[i]),
            "high": float(h[i]),
            "low": float(l[i]),
            "close": float(c[i]),
            "volume": float(v[i]),
        }
    return out


def resample_all(m1: dict[str, np.ndarray]) -> dict[str, dict[str, np.ndarray]]:
    return {tf: resample(m1, tf) for tf in ("M1", "M5", "M15", "H1", "H4", "D1")}
