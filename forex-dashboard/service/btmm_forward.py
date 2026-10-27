"""Forward test of the BTMM Shark Fin trigger (registered 2026-10-27).

The rule was fixed BEFORE any of these trades happened, so this is the real
test of the replay result (Shark Fin + 800 EMA bias, stop 5 pips + 0.10 ADR
beyond the spike, held to the stop or TP1 with break-even at 1:1, USD majors
only; replay +0.19R net on 67 trades, found after looking at the data).

Every refresh: log each new A / A+ Shark Fin on the majors (one row per
spike), then resolve open rows on the M15 candles printed since entry with
btmm_rules.manage_trade. R is reported gross and net of the measured Exness
Standard spread (no commission on Standard). Rows older than MAX_HOLD_BARS
close at the last close as 'expired'.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import btmm_core
import btmm_rules
import cache

log = logging.getLogger("btmm_forward")

MAX_HOLD_BARS = 192      # 2 days of M15, same as the replay
BARS = 2000

# Median London/NY spread from MT5 M15 bar history, Exness Standard, 2026-10-27
EXNESS_STANDARD_SPREAD = {
    "EUR/USD": 0.00008, "GBP/USD": 0.00010, "USD/JPY": 0.010, "USD/CHF": 0.00013,
    "AUD/USD": 0.00009, "USD/CAD": 0.00014, "NZD/USD": 0.00014,
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS btmm_forward (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol     TEXT NOT NULL,
    direction  TEXT NOT NULL,
    grade      TEXT NOT NULL,
    spike_ts   INTEGER NOT NULL,
    entry_ts   INTEGER NOT NULL,
    entry      REAL NOT NULL,
    sl         REAL NOT NULL,
    tp1        REAL NOT NULL,
    be         REAL NOT NULL,
    risk       REAL NOT NULL,
    spread     REAL NOT NULL,
    score      INTEGER,
    outcome    TEXT DEFAULT 'open',
    exit_ts    INTEGER,
    exit       REAL,
    r_gross    REAL,
    r_net      REAL,
    UNIQUE (symbol, spike_ts, direction)
);
"""


def _init():
    with cache._connect() as conn:
        conn.executescript(_SCHEMA)


def _start_ts() -> int:
    d = datetime.strptime(btmm_rules.FORWARD_TEST_START, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(d.timestamp())


def grade_of(a: dict) -> Optional[str]:
    """'A+' / 'A' as the tab grades it (A+ needs score >= 40 toward the
    trade), None when there is no Shark Fin trigger in a session."""
    sf = a.get("shark_fin") or {}
    d = "bullish" if a.get("setup_bullish") else "bearish"
    if not (a.get("kz") and sf.get("entry_ready") and sf.get("direction") == d):
        return None
    return "A+" if str(a.get("signal", "")).startswith("A+") else "A"


def record(symbol: str, bars: list[dict]) -> bool:
    """Log a new A/A+ Shark Fin on the latest closed bar. True if inserted."""
    if symbol not in btmm_rules.SF_MAJORS or len(bars) < 900 or bars[-1]["ts_utc"] < _start_ts():
        return False
    a = btmm_core.analyze(bars, symbol=symbol)
    g = grade_of(a)
    if not g:
        return False
    sf, plan = a["shark_fin"], a["trade_plan"]
    with cache._connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO btmm_forward (symbol, direction, grade, spike_ts, entry_ts, entry, sl,"
            " tp1, be, risk, spread, score) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, sf["direction"], g, sf["spike_ts"], bars[-1]["ts_utc"] + 900, plan["entry"],
             plan["sl"], plan["tp1"], plan["break_even_at"], plan["risk"],
             EXNESS_STANDARD_SPREAD.get(symbol, 0.0), a.get("score")))
        return cur.rowcount > 0


def resolve(symbol: str, bars: list[dict]) -> int:
    """Close open rows whose stop / TP1 / break-even resolved. Returns count."""
    with cache._connect() as conn:
        rows = conn.execute("SELECT * FROM btmm_forward WHERE symbol=? AND outcome='open'", (symbol,)).fetchall()
    done = 0
    for row in rows:
        after = [b for b in bars if b["ts_utc"] >= row["entry_ts"]]
        if not after:
            continue
        plan = {"entry": row["entry"], "sl": row["sl"], "tp1": row["tp1"],
                "break_even_at": row["be"], "exit_13": False}
        m = btmm_rules.manage_trade(plan, row["direction"], after[:MAX_HOLD_BARS], [])
        if m["outcome"] == "open":
            if len(after) < MAX_HOLD_BARS:
                continue
            m["outcome"] = "expired"
        sgn = -1 if row["direction"] == "bearish" else 1
        r_gross = (m["exit"] - row["entry"]) * sgn / row["risk"]
        r_net = r_gross - row["spread"] / row["risk"]
        exit_ts = after[min(m["bars"], len(after)) - 1]["ts_utc"]
        with cache._connect() as conn:
            conn.execute("UPDATE btmm_forward SET outcome=?, exit_ts=?, exit=?, r_gross=?, r_net=? WHERE id=?",
                         (m["outcome"], exit_ts, m["exit"], r_gross, r_net, row["id"]))
        done += 1
    return done


def run(interval: str = "15min") -> dict:
    """Scheduler entry: record then resolve for every USD major."""
    _init()
    added = closed = 0
    for sym in btmm_rules.SF_MAJORS:
        try:
            bars = cache.read_candles(sym, interval, limit=BARS)
            added += record(sym, bars)
            closed += resolve(sym, bars)
        except Exception as e:  # noqa: BLE001
            log.warning("btmm forward %s failed: %s", sym, e)
    if added or closed:
        log.info("btmm forward: %d new, %d resolved", added, closed)
    return {"added": added, "closed": closed}


def stats() -> dict:
    _init()
    with cache._connect() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM btmm_forward ORDER BY entry_ts DESC")]
    closed = [r for r in rows if r["outcome"] != "open"]
    net = [r["r_net"] for r in closed]
    gross = [r["r_gross"] for r in closed]
    # The user judges results before spread (2026-10-27): the headline is
    # gross; net is kept alongside for reference.
    return {
        "rule": "Shark Fin + 800 EMA bias, stop 5p + 0.10 ADR beyond the spike, hold to SL/TP1, BE at 1:1, "
                "USD majors, London/NY",
        "since": btmm_rules.FORWARD_TEST_START,
        "replay_expectation_r": 0.28,          # gross, majors, replay 2026-10-27 (n=67)
        "open": len(rows) - len(closed), "closed": len(closed),
        "wins": sum(1 for v in gross if v > 0),
        "avg_r_gross": round(sum(gross) / len(gross), 3) if gross else None,
        "total_r_gross": round(sum(gross), 2) if gross else 0.0,
        "avg_r_net": round(sum(net) / len(net), 3) if net else None,
        "total_r_net": round(sum(net), 2) if net else 0.0,
        "trades": rows[:100],
    }
