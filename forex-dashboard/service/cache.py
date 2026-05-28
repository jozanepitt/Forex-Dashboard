"""SQLite cache for OHLCV candles + per-key daily credit usage."""
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable, Optional

from config import DB_PATH

_lock = threading.Lock()
_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_open     INTEGER NOT NULL,
    ts_close    INTEGER,
    pair        TEXT NOT NULL,
    direction   TEXT NOT NULL,
    entry       REAL NOT NULL,
    sl          REAL NOT NULL,
    tp1         REAL NOT NULL,
    tp2         REAL,
    exit_price  REAL,
    setup       TEXT,
    signal      TEXT,
    signal_score REAL,
    gates_json  TEXT,
    result      TEXT DEFAULT 'open',
    pl_pips     REAL,
    pl_dollars  REAL,
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_trades_pair ON trades(pair);
CREATE INDEX IF NOT EXISTS idx_trades_ts   ON trades(ts_open DESC);

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


# ── Trade Journal ─────────────────────────────────────────────────────────────

def open_trade(pair: str, direction: str, entry: float, sl: float, tp1: float,
               tp2: Optional[float] = None, setup: Optional[str] = None,
               signal: Optional[str] = None, signal_score: Optional[float] = None,
               gates_json: Optional[str] = None, notes: Optional[str] = None) -> int:
    now = int(datetime.now(timezone.utc).timestamp())
    with _lock, _connect() as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (ts_open, pair, direction, entry, sl, tp1, tp2, setup, signal, signal_score, gates_json, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (now, pair, direction, entry, sl, tp1, tp2, setup, signal, signal_score, gates_json, notes),
        )
        return cur.lastrowid


def close_trade(trade_id: int, exit_price: float, result: str,
                pl_pips: Optional[float] = None, pl_dollars: Optional[float] = None,
                notes: Optional[str] = None) -> bool:
    now = int(datetime.now(timezone.utc).timestamp())
    with _lock, _connect() as conn:
        cur = conn.execute(
            """UPDATE trades SET ts_close=?, exit_price=?, result=?, pl_pips=?, pl_dollars=?,
               notes=COALESCE(?, notes) WHERE id=?""",
            (now, exit_price, result, pl_pips, pl_dollars, notes, trade_id),
        )
        return cur.rowcount > 0


def get_trades(limit: int = 50, pair: Optional[str] = None,
               setup: Optional[str] = None) -> list[dict]:
    conditions, params = [], []
    if pair:
        conditions.append("pair=?"); params.append(pair)
    if setup:
        conditions.append("setup=?"); params.append(setup)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT * FROM trades {where} ORDER BY ts_open DESC LIMIT ?", params
        ).fetchall()
    return [dict(r) for r in rows]


def get_journal_stats(setup: Optional[str] = None, pair: Optional[str] = None) -> dict:
    conditions, params = ["result != 'open'"], []
    if setup:
        conditions.append("setup=?"); params.append(setup)
    if pair:
        conditions.append("pair=?"); params.append(pair)
    where = "WHERE " + " AND ".join(conditions)
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT result, pl_pips FROM trades {where}", params
        ).fetchall()
    if not rows:
        return {"trades": 0, "wins": 0, "losses": 0, "be": 0,
                "win_rate": 0, "avg_pl_pips": 0, "expectancy": 0}
    wins = sum(1 for r in rows if r["result"] == "win")
    losses = sum(1 for r in rows if r["result"] == "loss")
    be = sum(1 for r in rows if r["result"] == "be")
    pl_list = [r["pl_pips"] for r in rows if r["pl_pips"] is not None]
    total = len(rows)
    avg_pl = sum(pl_list) / len(pl_list) if pl_list else 0
    win_rate = wins / total if total else 0
    avg_win = sum(p for p in pl_list if p > 0) / wins if wins else 0
    avg_loss = abs(sum(p for p in pl_list if p < 0) / losses) if losses else 1
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
    return {
        "trades": total, "wins": wins, "losses": losses, "be": be,
        "win_rate": round(win_rate * 100, 1),
        "avg_pl_pips": round(avg_pl, 1),
        "expectancy": round(expectancy, 2),
        "avg_win_pips": round(avg_win, 1),
        "avg_loss_pips": round(avg_loss, 1),
    }
