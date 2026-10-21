"""Exness MT5 provider.

Reads candle data from a locally-running MT5 terminal via MetaQuotes' Python
package. Zero quota, broker-grade prices, lower latency than polling REST APIs.

Requirements:
- MT5 terminal must be installed, running, and logged in (demo or live).
- `pip install MetaTrader5` in the same Python the service runs under.
- Symbols use a broker-specific suffix (Exness uses "m" for Standard accounts).

Time handling: Exness MT5 stamps bar times in UTC (offset = 0). We still detect
the offset at startup defensively — if you switch to a broker whose server runs
e.g. GMT+3, this code will subtract the offset so cached candles stay in UTC.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("mt5")

from providers.mt5_worker import WorkerError, WorkerProxy

try:
    import MetaTrader5 as _mt5_module
    MT5_AVAILABLE = True
    # Every call goes to a killable child process: the binding holds the GIL
    # for the whole of a call, so a stalled call made in-process froze the
    # entire service (2026-10-18/19). See providers/mt5_worker.py.
    mt5 = WorkerProxy(_mt5_module)
except ImportError:
    mt5 = None  # type: ignore[assignment]
    MT5_AVAILABLE = False
    log.warning("MetaTrader5 package not importable; MT5 provider disabled")


class MT5Error(Exception):
    """Generic MT5 provider failure (recoverable, try fallback)."""


class MT5NotConnected(MT5Error):
    """MT5 terminal is not running or not logged in."""


_INTERVAL_TO_TIMEFRAME: dict[str, int] = {}
_INTERVAL_SECS = {
    "1min":   60,   "5min":   300,  "15min":  900,
    "30min":  1800, "1h":     3600, "4h":     14400,
    "1day":   86400,
}


def _build_interval_map() -> None:
    global _INTERVAL_TO_TIMEFRAME
    if not MT5_AVAILABLE or _INTERVAL_TO_TIMEFRAME:
        return
    _INTERVAL_TO_TIMEFRAME = {
        "1min":  mt5.TIMEFRAME_M1,
        "5min":  mt5.TIMEFRAME_M5,
        "15min": mt5.TIMEFRAME_M15,
        "30min": mt5.TIMEFRAME_M30,
        "1h":    mt5.TIMEFRAME_H1,
        "4h":    mt5.TIMEFRAME_H4,
        "1day":  mt5.TIMEFRAME_D1,
    }


_build_interval_map()


class MT5Client:
    """Thin wrapper over the MetaTrader5 package with lazy init and a singleton-friendly shape."""

    INIT_RETRY_COOLDOWN_SECS = 30
    STATUS_CACHE_SECS = 10

    def __init__(self, symbol_suffix: str = "m"):
        self._suffix = symbol_suffix
        self._initialized = False
        self._broker_offset_secs = 0
        self._last_init_attempt = 0.0
        self._lock = threading.Lock()
        self._status_cache: Optional[dict] = None
        self._status_cache_ts = 0.0

    # ------------------------------------------------------------------ init
    # MetaTrader5 IPC error codes: the link to the terminal is gone (typically
    # the terminal restarted). Seen 2026-10-20: 17:16 UTC terminal restart,
    # then 3h of (-10001, 'IPC send failed') because nothing reconnected.
    IPC_ERROR_CODES = {-10001, -10002, -10003, -10004, -10005}

    def _drop_if_ipc_error(self) -> Optional[tuple]:
        """Call with self._lock held after an MT5 call failed. If the failure
        is an IPC error, forget the connection so the next call re-initializes
        (subject to the retry cooldown). Returns last_error()."""
        try:
            err = mt5.last_error()
        except WorkerError:
            err = None
        code = err[0] if isinstance(err, tuple) and err else None
        if code in self.IPC_ERROR_CODES and self._initialized:
            log.warning("MT5 IPC link lost (%s) - will re-initialize on the next call", err)
            self._initialized = False
            self._status_cache = None
            # Kill the worker rather than calling shutdown() through it: a
            # fresh process attaches cleanly to the restarted terminal, and a
            # dead worker would otherwise be respawned (and re-initialized)
            # just to be shut down. Fakes in tests may only have shutdown().
            try:
                (getattr(mt5, "close", None) or mt5.shutdown)()
            except Exception:  # noqa: BLE001 - the link is already broken
                pass
        return err

    def _ensure_init(self) -> bool:
        """Initialize the MT5 connection if needed. Cooldown prevents tight retry loops."""
        if not MT5_AVAILABLE:
            return False
        if self._initialized:
            return True
        # Never queue behind a reconnect: initialize() can take up to ~75s with
        # the terminal down, and callers waiting on the lock used to time out in
        # the scheduler without ever reaching the TwelveData fallback (review
        # 2026-10-20). Whoever finds a reconnect in progress falls through now.
        if not self._lock.acquire(timeout=0.5):
            return False
        try:
            if self._initialized:
                return True
            if time.time() - self._last_init_attempt < self.INIT_RETRY_COOLDOWN_SECS:
                return False
            try:
                ok = mt5.initialize()
            except WorkerError as e:
                log.warning("mt5.initialize() failed: %s", e)
                return False
            finally:
                # cooldown counts from the END of the attempt: a 60s failed
                # init must not be retried the moment it returns
                self._last_init_attempt = time.time()
            if not ok:
                try:
                    err = mt5.last_error()
                except WorkerError as e:
                    err = e
                log.warning("mt5.initialize() failed: %s", err)
                return False
            self._initialized = True
            self._detect_broker_offset()
            try:
                acct = mt5.account_info()
            except WorkerError:
                acct = None
            log.info("MT5 connected: login=%s server=%s offset=%ds",
                     acct.login if acct else "?",
                     acct.server if acct else "?",
                     self._broker_offset_secs)
            return True
        finally:
            self._lock.release()

    def _detect_broker_offset(self) -> None:
        """Compare a live tick's server time with local UTC to derive offset (rounded to hour).

        Guards:
        - Tick must be recent (< 90 min old) — a stale tick from a closed market would
          produce a wildly wrong offset (e.g. −46 h over a weekend gap).
        - Offset is clamped to ±14 h — the maximum real broker offset is UTC+14.
          Any value outside this range means the tick is stale; fall back to 0.
        """
        try:
            sample = f"EURUSD{self._suffix}"
            if not mt5.symbol_select(sample, True):
                return
            tick = mt5.symbol_info_tick(sample)
            if tick and tick.time > 0:
                now = int(time.time())
                tick_age_secs = now - tick.time
                if tick_age_secs > 5400:          # > 90 min → market closed / tick stale
                    log.info("MT5 broker offset skipped: last tick is %dh old (market closed?)",
                             tick_age_secs // 3600)
                    self._broker_offset_secs = 0
                    return
                offset = tick.time - now
                offset_rounded = round(offset / 3600) * 3600
                if abs(offset_rounded) > 14 * 3600:   # sanity clamp
                    log.warning("MT5 broker offset %dh out of range — defaulting to 0", offset_rounded // 3600)
                    self._broker_offset_secs = 0
                    return
                self._broker_offset_secs = offset_rounded
        except Exception as e:
            log.debug("broker offset detection failed: %s", e)
            self._broker_offset_secs = 0

    # ------------------------------------------------------------------ public
    def to_mt5_symbol(self, pair: str) -> str:
        """'EUR/USD' -> 'EURUSDm' (or whatever suffix is configured).

        Symbols whose broker root differs from the standard (e.g. Exness
        exposes the US Dollar Index as 'DYXm' instead of 'DXYm') can set
        `mt5_symbol` in their instruments.py spec — we use that as the root
        and still apply the broker suffix.
        """
        import instruments  # local import: providers pkg must stay importable standalone
        s = instruments.spec(pair)
        if s and s.get("mt5_symbol"):
            return s["mt5_symbol"] + self._suffix
        return pair.replace("/", "").upper() + self._suffix

    def fetch(self, pair: str, interval: str = "15min", outputsize: int = 800,
              start_ts: Optional[int] = None) -> list[dict]:
        """Return normalized candle bars for `pair` and `interval`.

        Always pulls the most recent `outputsize` bars via copy_rates_from_pos —
        the cache layer dedupes by (symbol, interval, ts_utc), so overlap is free
        and we always include the freshest data. `start_ts` is accepted for
        interface compatibility with twelvedata but ignored: re-pulling 800 bars
        from MT5 is local IPC, no quota cost.

        Serialized via `self._lock` — the MT5 Python binding mutates a global
        Market Watch via `symbol_select` before each `copy_rates_from_pos`, so
        concurrent calls with different symbols can race. Lock keeps fetches
        atomic per pair; latency is dominated by IPC (<50ms typical), so the
        serialization cost is minor and concurrent callers still benefit from
        overlapping non-MT5 work (twelvedata, cache write).
        """
        if not self._ensure_init():
            raise MT5NotConnected("MT5 terminal unavailable (not running or not logged in)")

        timeframe = _INTERVAL_TO_TIMEFRAME.get(interval)
        if timeframe is None:
            raise MT5Error(f"unsupported interval: {interval}")

        symbol = self.to_mt5_symbol(pair)
        with self._lock:
            try:
                if not mt5.symbol_select(symbol, True):
                    raise MT5Error(f"cannot select symbol {symbol}: {self._drop_if_ipc_error()}")

                rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, outputsize)
                if rates is None or len(rates) == 0:
                    raise MT5Error(f"no data for {symbol} {interval}: {self._drop_if_ipc_error()}")
            except WorkerError as e:   # timed out / worker died: one failed fetch, not a frozen service
                raise MT5Error(f"{symbol} {interval}: {e}") from e

        return self._normalize(rates, interval)

    def _normalize(self, rates, interval: str) -> list[dict]:
        """MT5 numpy struct -> service candle schema. Drops the still-forming bar."""
        now_ts = int(time.time())
        interval_secs = _INTERVAL_SECS.get(interval, 0)
        bars: list[dict] = []
        for r in rates:
            ts_utc = int(r["time"]) - self._broker_offset_secs
            # Skip the in-progress bar (close time still in the future).
            # Cache treats every persisted bar as closed/final.
            if ts_utc + interval_secs > now_ts:
                continue
            bars.append({
                "ts_utc": ts_utc,
                "open":   float(r["open"]),
                "high":   float(r["high"]),
                "low":    float(r["low"]),
                "close":  float(r["close"]),
                "volume": float(r["tick_volume"]),
            })
        bars.sort(key=lambda b: b["ts_utc"])
        return bars

    def status(self) -> dict:
        """Lightweight health snapshot for /status."""
        if not MT5_AVAILABLE:
            return {"available": False, "reason": "MetaTrader5 package not installed"}
        if not self._ensure_init():
            with self._lock:
                try:
                    err = mt5.last_error() if MT5_AVAILABLE else "unavailable"
                except WorkerError as e:
                    err = e
            return {"available": False, "reason": str(err)}
        # Served from a short cache: the dashboard polls /status, and every MT5
        # call must go through the lock (the binding is not thread-safe and
        # holds the GIL during IPC — an unlocked call here overlapping a fetch
        # froze the whole process on 2026-10-13).
        now = time.time()
        if self._status_cache and now - self._status_cache_ts < self.STATUS_CACHE_SECS:
            return self._status_cache
        with self._lock:
            try:
                acct = mt5.account_info()
                term = mt5.terminal_info()
            except WorkerError as e:
                return {"available": False, "reason": str(e)}
            if acct is None and term is None:
                # Both calls failing together means the IPC link is dead; do
                # not report "available" with a dead link (2026-10-20).
                err = self._drop_if_ipc_error()
                return {"available": False, "reason": f"MT5 calls failed: {err}"}
        self._status_cache_ts = now
        self._status_cache = {
            "available": True,
            "broker_offset_secs": self._broker_offset_secs,
            "symbol_suffix":      self._suffix,
            "login":              acct.login if acct else None,
            "server":             acct.server if acct else None,
            "trade_mode":         _trade_mode_label(getattr(acct, "trade_mode", None)),
            "terminal_connected": bool(term and term.connected),
        }
        return self._status_cache


def _trade_mode_label(mode: Optional[int]) -> Optional[str]:
    if mode is None:
        return None
    # ACCOUNT_TRADE_MODE_DEMO=0, CONTEST=1, REAL=2
    return {0: "demo", 1: "contest", 2: "real"}.get(mode, str(mode))


# ── Singleton ─────────────────────────────────────────────────────────────────

_client: Optional[MT5Client] = None
_client_lock = threading.Lock()


def client() -> MT5Client:
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                from config import MT5_SYMBOL_SUFFIX
                _client = MT5Client(symbol_suffix=MT5_SYMBOL_SUFFIX)
    return _client
