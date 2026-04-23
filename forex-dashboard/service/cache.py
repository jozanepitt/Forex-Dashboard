"""SQLite cache for OHLCV candles + per-key daily credit usage."""
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable, Optional

from config import DB_PATH

_lock = threading.Lock()
_SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
    symbol    TEXT NOT NULL,
    interval  TEXT NOT NULL,
    ts_utc    INTEGER NOT NULL,
    open      REAL NOT NULL,
    high      REAL NOT NULL,
    low       REAL NOT NULL,
    close     REAL NOT NULL,
    volume    REAL,
    PRIMARY KEY (symbol, interval, ts_utc)
);

CREATE INDEX IF NOT EXISTS idx_candles_sym_int_ts
    ON candles(symbol, interval, ts_utc DESC);

CREATE TABLE IF NOT EXISTS key_usage (
    key_name TEXT NOT NULL,
    date_utc TEXT NOT NULL,
    credits  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (key_name, date_utc)
);

CREATE TABLE IF NOT EXISTS refresh_log (
    ts_utc   INTEGER NOT NULL,
    symbol   TEXT NOT NULL,
    interval TEXT NOT NULL,
    source   TEXT NOT NULL,
    bars     INTEGER NOT NULL,
    credits  INTEGER NOT NULL DEFAULT 0,
    error    TEXT
);
"""


def init_db():
    with _connect() as conn:
        conn.executescript(_SCHEMA)


@contextmanager
def _connect():
    conn = sqlite3.connect(str(DB_PATH), timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
    finally:
        conn.close()


def max_ts(symbol: str, interval: str) -> Optional[int]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT MAX(ts_utc) AS mx FROM candles WHERE symbol=? AND interval=?",
            (symbol, interval),
        ).fetchone()
        return row["mx"] if row and row["mx"] is not None else None


def read_candles(symbol: str, interval: str, limit: int = 800) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """SELECT ts_utc, open, high, low, close, volume FROM candles
               WHERE symbol=? AND interval=?
               ORDER BY ts_utc DESC LIMIT ?""",
            (symbol, interval, limit),
        ).fetchall()
    out = []
    for r in reversed(rows):
        out.append({
            "datetime": datetime.fromtimestamp(r["ts_utc"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "open":   r["open"],
            "high":   r["high"],
            "low":    r["low"],
            "close":  r["close"],
            "volume": r["volume"] or 0,
            "ts_utc": r["ts_utc"],
        })
    return out


def upsert_candles(symbol: str, interval: str, bars: Iterable[dict]) -> int:
    rows = [
        (symbol, interval, int(b["ts_utc"]),
         float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
         float(b.get("volume") or 0))
        for b in bars
    ]
    if not rows:
        return 0
    with _lock, _connect() as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO candles
               (symbol, interval, ts_utc, open, high, low, close, volume)
               VALUES (?,?,?,?,?,?,?,?)""",
            rows,
        )
    return len(rows)


def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def bump_key_usage(key_name: str, credits_used: int) -> int:
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO key_usage (key_name, date_utc, credits)
               VALUES (?, ?, ?)
               ON CONFLICT(key_name, date_utc)
               DO UPDATE SET credits = credits + excluded.credits""",
            (key_name, today_utc(), credits_used),
        )
        row = conn.execute(
            "SELECT credits FROM key_usage WHERE key_name=? AND date_utc=?",
            (key_name, today_utc()),
        ).fetchone()
        return row["credits"] if row else credits_used


def get_key_usage(key_name: str) -> int:
    with _connect() as conn:
        row = conn.execute(
            "SELECT credits FROM key_usage WHERE key_name=? AND date_utc=?",
            (key_name, today_utc()),
        ).fetchone()
        return row["credits"] if row else 0


def all_key_usage() -> dict[str, int]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT key_name, credits FROM key_usage WHERE date_utc=?",
            (today_utc(),),
        ).fetchall()
    return {r["key_name"]: r["credits"] for r in rows}


def log_refresh(symbol: str, interval: str, source: str, bars: int,
                credits: int = 0, error: Optional[str] = None):
    now = int(datetime.now(timezone.utc).timestamp())
    with _lock, _connect() as conn:
        conn.execute(
            """INSERT INTO refresh_log (ts_utc, symbol, interval, source, bars, credits, error)
               VALUES (?,?,?,?,?,?,?)""",
            (now, symbol, interval, source, bars, credits, error),
        )
