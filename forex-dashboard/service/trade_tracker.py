"""Auto-resolve journaled TDI 123 trades against cached candles.

For every open trade, walk the H1 candles that printed AFTER it opened and mark
it win (TP1 reached) or loss (SL reached), whichever came first. When a single
candle spans both levels we count the LOSS (conservative — we can't see the
intrabar path). Trades with no resolution and older than EXPIRE_HOURS are closed
flat as "expired" so the journal doesn't fill with stale opens.

Kept dependency-light and pure-ish (candle fetch + close are injected as module
functions) so the core decision `_resolve_one` is unit-testable offline.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import instruments

log = logging.getLogger("trade_tracker")

EXPIRE_HOURS = 48   # give a trade this long to hit TP1 or SL before flat-closing


def _resolve_one(direction: str, entry: float, sl: float, tp1: float,
                 candles: list[dict]) -> Optional[tuple[str, float]]:
    """Return (result, exit_price) or None if still open.

    result ∈ {"win","loss"}. candles must be chronological and already filtered
    to bars AFTER the trade opened.
    """
    buy = direction.upper() == "BUY"
    for c in candles:
        hi, lo = c["high"], c["low"]
        if buy:
            if lo <= sl:          # SL checked first → same-bar ties count as loss
                return ("loss", sl)
            if hi >= tp1:
                return ("win", tp1)
        else:
            if hi >= sl:
                return ("loss", sl)
            if lo <= tp1:
                return ("win", tp1)
    return None


def resolve_open_trades(setup: str = "TDI123") -> dict:
    """Resolve all open journaled trades for the given setup. Returns a summary."""
    import cache
    opened = [t for t in cache.get_trades(limit=1000)
              if (t.get("setup") == setup) and (t.get("result") in (None, "", "open"))]
    wins = losses = expired = still_open = 0
    now = int(datetime.now(timezone.utc).timestamp())

    for tr in opened:
        pair, direction = tr["pair"], tr["direction"]
        entry, sl, tp1 = tr["entry"], tr["sl"], tr["tp1"]
        ts_open = tr["ts_open"]
        if None in (entry, sl, tp1):
            continue
        candles = [c for c in cache.read_candles(pair, "1h", limit=800)
                   if c["ts_utc"] > ts_open]
        res = _resolve_one(direction, entry, sl, tp1, candles)
        if res is None:
            if now - ts_open > EXPIRE_HOURS * 3600 and candles:
                exit_price = candles[-1]["close"]
                pip = instruments.pip_size(pair, entry)
                pl = ((exit_price - entry) if direction.upper() == "BUY"
                      else (entry - exit_price)) / pip
                cache.close_trade(tr["id"], exit_price, "expired", round(pl, 1))
                expired += 1
            else:
                still_open += 1
            continue
        result, exit_price = res
        pip = instruments.pip_size(pair, entry)
        pl = ((exit_price - entry) if direction.upper() == "BUY"
              else (entry - exit_price)) / pip
        cache.close_trade(tr["id"], exit_price, result, round(pl, 1))
        if result == "win":
            wins += 1
        else:
            losses += 1

    if wins or losses or expired:
        log.info("TDI123 journal resolved: %dW / %dL / %d expired (%d still open)",
                 wins, losses, expired, still_open)
    return {"wins": wins, "losses": losses, "expired": expired, "still_open": still_open}
