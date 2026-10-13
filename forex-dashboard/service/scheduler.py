"""Background scheduler: refresh every 15 minutes (UTC-aligned, 2-min grace).

Resilience: every data fetch is timeout-guarded so a blocking provider call
(notably MT5's `copy_rates_from_pos`, which has no native timeout) can never
hang `refresh_all` and — because the job runs with max_instances=1 — wedge the
whole scheduler indefinitely. A separate watchdog fires a Discord warning if no
successful refresh has happened recently, so a stall is loud, never silent.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

import alerts
import fetcher
import market_hours
from btmm_core import active_kill_zone
import vwap9ema_strategy
from config import (BTMM_ALERTS_ENABLED, BTMM_APLUS_ONLY, DEFAULT_BACKFILL, DEFAULT_INTERVAL,
                    INTERVAL_SECS, PRIORITY_PAIRS, SERVICE_ROOT)

log = logging.getLogger("scheduler")

FANOUT_DELAY_SECS = 2     # spread 15-pair fanout across ~30s so a single key never blows 8/min
FETCH_TIMEOUT_SECS = 15   # max wall-time for any single provider fetch before we abandon it
STALL_THRESHOLD_SECS = 1200  # 20 min with no successful refresh → watchdog warns

_prev_kill_zone: Optional[str] = None
_STATE_PATH = SERVICE_ROOT / "scheduler_state.json"
_last_ok_refresh: float = 0.0


def _record_refresh(ts: float) -> None:
    """Persist the last successful-refresh timestamp so the watchdog and the
    external healthcheck can detect a stalled scheduler."""
    global _last_ok_refresh
    _last_ok_refresh = ts
    try:
        _STATE_PATH.write_text(json.dumps({"last_ok_refresh": ts}))
    except Exception as e:
        log.debug("could not persist scheduler state: %s", e)


def last_refresh_ts() -> float:
    """Last successful refresh time (epoch secs), from memory or disk. 0 if never."""
    if _last_ok_refresh:
        return _last_ok_refresh
    try:
        return float(json.loads(_STATE_PATH.read_text()).get("last_ok_refresh", 0))
    except Exception:
        return 0.0


def _fetch_guarded(sym: str, interval: str, limit: Optional[int] = None,
                   timeout: float = FETCH_TIMEOUT_SECS) -> None:
    """Run one fetch in a daemon thread and abandon it if it exceeds `timeout`.

    MT5's copy_rates is a blocking C call with no timeout; without this guard a
    single hung call would freeze refresh_all forever (max_instances=1)."""
    box: dict = {}

    def _do():
        try:
            if limit is not None:
                fetcher.get_candles(sym, interval, limit=limit)
            else:
                fetcher.get_candles(sym, interval)
            box["ok"] = True
        except Exception as e:  # noqa: BLE001 — propagate to caller below
            box["err"] = e

    t = threading.Thread(target=_do, name=f"fetch-{sym}-{interval}", daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"{sym} {interval} fetch exceeded {timeout}s (provider hung)")
    if "err" in box:
        raise box["err"]


_refresh_listener = None


def set_refresh_listener(fn) -> None:
    """Register the callable to run after each refresh (app.py passes its
    WebSocket emitter). The scheduler must never import app: the service runs
    app.py as __main__, so `from app import ...` loads a SECOND copy of it —
    a second Flask/SocketIO instance nobody is connected to and a second
    RotatingFileHandler that breaks service.log at its first rollover."""
    global _refresh_listener
    _refresh_listener = fn


def _notify_refresh(pairs_updated: int) -> None:
    if _refresh_listener is None:
        return
    try:
        _refresh_listener(pairs_updated)
    except Exception as e:  # noqa: BLE001 — a dead socket must not fail the refresh
        log.debug("refresh listener failed: %s", e)


# HTTP self-check: on 2026-10-12 the web server stopped answering for 26
# minutes while this scheduler kept running, and the guardian (which only
# checks that the port is listening) never noticed.
HTTP_SELF_CHECK_FAILS = 3          # consecutive one-minute failures before exiting
HTTP_SELF_CHECK_TIMEOUT = 5
_http_fail_streak = 0


def _probe_health() -> bool:
    import requests
    from config import SERVICE_HOST, SERVICE_PORT
    r = requests.get(f"http://{SERVICE_HOST}:{SERVICE_PORT}/health", timeout=HTTP_SELF_CHECK_TIMEOUT)
    return r.status_code == 200


def _exit_for_restart() -> None:
    """Dump every thread's stack for the post-mortem, then exit hard so the
    guardian sees the port closed and relaunches the service."""
    import faulthandler
    import os
    from pathlib import Path
    try:
        with open(Path(__file__).parent / "hang_dump.log", "a", encoding="utf-8") as fh:
            fh.write(f"\n===== web server unresponsive at {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n")
            fh.flush()
            faulthandler.dump_traceback(file=fh, all_threads=True)
    except Exception as e:  # noqa: BLE001
        log.warning("hang dump failed: %s", e)
    logging.shutdown()
    os._exit(3)


def _http_self_check(probe=None, on_dead=None) -> None:
    """Runs every minute. Exits the process after HTTP_SELF_CHECK_FAILS
    consecutive failed probes of our own /health endpoint."""
    global _http_fail_streak
    try:
        ok = bool((probe or _probe_health)())
    except Exception:  # noqa: BLE001
        ok = False
    if ok:
        if _http_fail_streak:
            log.info("web server answering again after %d failed self-check(s)", _http_fail_streak)
        _http_fail_streak = 0
        return
    _http_fail_streak += 1
    log.warning("web self-check failed (%d/%d)", _http_fail_streak, HTTP_SELF_CHECK_FAILS)
    if _http_fail_streak >= HTTP_SELF_CHECK_FAILS:
        log.error("web server unresponsive for %d checks - exiting so the guardian restarts the service",
                  _http_fail_streak)
        _http_fail_streak = 0
        (on_dead or _exit_for_restart)()


_last_gate_summary: dict[str, str] = {}


def _live_symbols(runner: str, candles_by_pair: dict[str, dict],
                  feeds: list[tuple[str, str]]) -> set[str]:
    """Symbols a strategy may send a LIVE alert for this cycle: market open and
    newest bar fresh on every feed it trades off (rules in market_hours). Every
    `_run_*alerts` runner must filter its rows through this — the strategies
    themselves take "now" from the newest candle, so on a frozen feed or a
    closed market they keep reporting yesterday's setup as current.

    `feeds` is [(candles_by_pair key, interval)], e.g. [("m15", "15min"),
    ("1h", "1h")]. An allow-list, so a symbol with no candles is blocked too."""
    now = time.time()
    live: set[str] = set()
    blocked: dict[str, int] = {}
    for sym, tf in candles_by_pair.items():
        reason = None
        for key, interval in feeds:
            try:
                reason = market_hours.live_block_reason(
                    sym, (tf or {}).get(key) or [], INTERVAL_SECS[interval], now)
            except Exception as e:  # noqa: BLE001 — one bad bundle must not sink the runner
                log.debug("%s live-alert gate failed for %s %s: %s", runner, sym, key, e)
                reason = "bad data"
            if reason:
                break
        if reason is None:
            live.add(sym)
        else:
            kind = reason.split(" (")[0]
            blocked[kind] = blocked.get(kind, 0) + 1

    summary = (", ".join(f"{n} {kind}" for kind, n in sorted(blocked.items()))
               or "all symbols live")
    if _last_gate_summary.get(runner) != summary:      # log transitions, not every cycle
        _last_gate_summary[runner] = summary
        log.info("%s live-alert gate: %d/%d symbols live (%s)",
                 runner, len(live), len(candles_by_pair), summary)
    return live


def refresh_all():
    global _prev_kill_zone
    log.info("refresh_all: starting (%d pairs)", len(PRIORITY_PAIRS))
    t0 = time.time()
    updated = 0
    for i, sym in enumerate(PRIORITY_PAIRS):
        if i > 0:
            time.sleep(FANOUT_DELAY_SECS)
        try:
            _fetch_guarded(sym, DEFAULT_INTERVAL, limit=DEFAULT_BACKFILL)  # M15 — TDI123 M15 pattern detection
            _fetch_guarded(sym, "1h", limit=DEFAULT_BACKFILL)  # H1 — TDI123 primary (needs >=800 for real EMA-800)
            _fetch_guarded(sym, "4h", limit=200)           # H4 — TDI123 HTF bias
            _fetch_guarded(sym, "1day", limit=60)          # daily — TDI123 daily bias/ADR
            updated += 1
        except Exception as e:
            log.error("refresh_all: %s fetch failed/timed out: %s", sym, e)
    log.info("refresh_all: done in %.1fs (%d/%d ok)", time.time() - t0, updated, len(PRIORITY_PAIRS))
    if updated:
        _record_refresh(time.time())  # mark healthy only when we actually got fresh data

    # Fetch M5 candles for the VWAP+9EMA universe (full PRIORITY_PAIRS list --
    # paced same as the main fetch loop above so this doesn't hammer MT5).
    # No other live strategy uses M5; every other fetch above is M15/1h/4h/1day.
    for i, sym in enumerate(vwap9ema_strategy.VWAP9EMA_UNIVERSE):
        if i > 0:
            time.sleep(FANOUT_DELAY_SECS)
        try:
            _fetch_guarded(sym, "5min", limit=vwap9ema_strategy.M5_FETCH_LIMIT)
        except Exception as e:
            log.warning("VWAP9EMA M5 fetch failed for %s: %s", sym, e)

    # Fire kill zone open alert on session transitions.
    # Suppressed in BTMM A+-only mode (user wants A+ setups only, nothing else).
    if BTMM_ALERTS_ENABLED and not BTMM_APLUS_ONLY and market_hours.is_market_open(None, time.time()):
        try:
            current_kz = active_kill_zone(int(time.time()))
            if current_kz and current_kz != _prev_kill_zone:
                alerts.alert_kill_zone_open(current_kz)
            _prev_kill_zone = current_kz
        except Exception as e:
            log.warning("kill zone alert failed: %s", e)

    # Run BTMM Discord alerts after fresh candle data is in cache (paused unless enabled)
    if BTMM_ALERTS_ENABLED:
        try:
            _run_alerts()
        except Exception as e:
            log.warning("alerts failed: %s", e)

    # Run 5AM CRT scanner + Discord alerts (NY Open kill zone)
    try:
        _run_crt_5am_alerts()
    except Exception as e:
        log.warning("CRT-5AM alerts failed: %s", e)

    # Run TDI Cycle 123 scanner (improvements-on-BTMM) + Discord alerts
    try:
        _run_tdi123_alerts()
    except Exception as e:
        log.warning("TDI123 alerts failed: %s", e)

    # Run BTMM 123 scanner (BTMM-doctrine 1-2-3) — dashboard-visible; Discord
    # stays silent until BTMM123_ALERTS_ENABLED is turned on post-backtest.
    try:
        _run_btmm123_alerts()
    except Exception as e:
        log.warning("BTMM123 alerts failed: %s", e)

    # Run BTMM ID50 (M15 50-EMA-bounce retest) scanner — dashboard-visible;
    # Discord stays silent until BTMM_ID50_ALERTS_ENABLED is turned on
    # post-backtest (same default-off discipline as BTMM123).
    try:
        _run_id50_alerts()
    except Exception as e:
        log.warning("BTMM_ID50 alerts failed: %s", e)

    # Run VWAP Mean Reversion (M15) scanner + Discord alerts (real + watch)
    try:
        _run_vwap_mr_alerts()
    except Exception as e:
        log.warning("VWAP Mean Reversion alerts failed: %s", e)

    # Run VWAP Bounce (M15) scanner — dashboard-visible; Discord stays silent
    # until VWAP_BOUNCE_ALERTS_ENABLED is turned on post-backtest.
    try:
        _run_vwap_bounce_alerts()
    except Exception as e:
        log.warning("VWAP Bounce alerts failed: %s", e)

    # Run live VWAP+9EMA scanner (UNVALIDATED — failed backtest 0/48) + Discord alerts
    try:
        _run_vwap9ema_alerts()
    except Exception as e:
        log.warning("VWAP9EMA alerts failed: %s", e)

    # Notify dashboard subscribers via WebSocket (listener registered by app.main).
    _notify_refresh(updated)


def _run_alerts():
    """Evaluate alert rules for each priority pair using cached candle data."""
    import cache
    from btmm_core import analyze
    from config import DEFAULT_BACKFILL

    candles_by_pair = {sym: {"m15": cache.read_candles(sym, DEFAULT_INTERVAL, DEFAULT_BACKFILL)}
                       for sym in PRIORITY_PAIRS}
    live = _live_symbols("BTMM", candles_by_pair, [("m15", DEFAULT_INTERVAL)])
    for sym in PRIORITY_PAIRS:
        if sym not in live:
            continue
        try:
            bars = candles_by_pair[sym]["m15"]
            if len(bars) < 200:
                continue
            result = analyze(bars, symbol=sym)
            if result.get("signal") == "insufficient_data":
                continue
            alerts.evaluate_pair(
                pair_symbol=sym,
                signal=result["signal"],
                score=result["score"],
                checklist_score=result["checklist_score"],
                active_setup=result.get("active_setup"),
                adr_consumed=result["adr_consumed"],
                price=bars[-1]["close"],
                kz=result.get("kz"),
                cross_513=result.get("cross_513"),
                asian_range=result.get("asian_range"),
                confidence=result.get("confidence"),
            )
        except Exception as e:
            log.debug("alert eval failed for %s: %s", sym, e)


def _run_crt_5am_alerts():
    """Run the 5AM CRT scanner and fire Discord alerts for Grade A setups.

    Key windows per MADO spec:
      London Lunch  06:00–07:00 NY (08:00–09:00 SAST)
      NY Open       07:00–08:30 NY (09:00–10:30 SAST)
    """
    import cache
    import crt_strategy

    candles_by_pair: dict[str, dict] = {}
    for sym in crt_strategy.CRT_UNIVERSE:
        candles_by_pair[sym] = {
            "m15": cache.read_candles(sym, "15min", limit=400),
        }
    live = _live_symbols("CRT-5AM", candles_by_pair, [("m15", "15min")])
    result = crt_strategy.analyze_universe_5am(candles_by_pair)
    for row in result.get("pairs", []):
        if row.get("symbol") not in live:
            continue
        try:
            alerts.alert_crt_5am_setup(row["symbol"], row)
        except Exception as e:
            log.debug("CRT-5AM alert eval failed for %s: %s", row.get("symbol"), e)


def _run_vwap_mr_alerts():
    """Run the VWAP Mean Reversion (M15) scanner against the cache and fire
    Discord alerts (real + watch) for qualifying setups."""
    import cache
    import vwap_mean_reversion_strategy

    candles_by_pair: dict[str, dict] = {}
    for sym in vwap_mean_reversion_strategy.VWAP_MR_UNIVERSE:
        candles_by_pair[sym] = {
            "m15": cache.read_candles(sym, "15min", limit=400),
        }
    live = _live_symbols("VWAP_MR", candles_by_pair, [("m15", "15min")])
    result = vwap_mean_reversion_strategy.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        if row.get("symbol") not in live:
            continue
        try:
            alerts.alert_vwap_mr_setup(row["symbol"], row)
        except Exception as e:
            log.debug("VWAP_MR alert eval failed for %s: %s", row.get("symbol"), e)
        try:
            alerts.alert_vwap_mr_watch(row["symbol"], row)
        except Exception as e:
            log.debug("VWAP_MR watch eval failed for %s: %s", row.get("symbol"), e)


def _run_vwap_bounce_alerts():
    """Run the VWAP Bounce (M15) scanner against the cache and fire Discord
    alerts for confirmed setups (off by default — see config)."""
    import cache
    import vwap_bounce_strategy

    candles_by_pair: dict[str, dict] = {}
    for sym in vwap_bounce_strategy.VWAP_BOUNCE_UNIVERSE:
        candles_by_pair[sym] = {
            "m15": cache.read_candles(sym, "15min", limit=vwap_bounce_strategy.M15_FETCH_LIMIT),
        }
    live = _live_symbols("VWAP_BOUNCE", candles_by_pair, [("m15", "15min")])
    result = vwap_bounce_strategy.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        if row.get("symbol") not in live:
            continue
        try:
            alerts.alert_vwap_bounce_setup(row["symbol"], row)
        except Exception as e:
            log.debug("VWAP_BOUNCE alert eval failed for %s: %s", row.get("symbol"), e)


def _run_vwap9ema_alerts():
    """Run the live VWAP+9EMA scanner (UNVALIDATED — failed backtest 0/48)
    against the cache and fire Discord alerts for confirmed setups."""
    import cache

    candles_by_pair: dict[str, dict] = {}
    for sym in vwap9ema_strategy.VWAP9EMA_UNIVERSE:
        candles_by_pair[sym] = {
            "m5": cache.read_candles(sym, "5min", limit=vwap9ema_strategy.M5_FETCH_LIMIT),
        }
    live = _live_symbols("VWAP9EMA", candles_by_pair, [("m5", "5min")])
    result = vwap9ema_strategy.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        if row.get("symbol") not in live:
            continue
        try:
            alerts.alert_vwap9ema_setup(row["symbol"], row)
        except Exception as e:
            log.debug("VWAP9EMA alert eval failed for %s: %s", row.get("symbol"), e)


def _run_tdi123_alerts():
    """Run the TDI Cycle 123 scanner and fire Discord alerts for confirmed setups."""
    import cache
    import tdi_cycle_123

    candles_by_pair: dict[str, dict] = {}
    for sym in tdi_cycle_123.TDI123_UNIVERSE:
        candles_by_pair[sym] = {
            "1h": cache.read_candles(sym, "1h", limit=DEFAULT_BACKFILL),
            "4h": cache.read_candles(sym, "4h", limit=200),
            "1d": cache.read_candles(sym, "1day", limit=60),
            "m15": cache.read_candles(sym, "15min", limit=DEFAULT_BACKFILL),
        }
    live = _live_symbols("TDI123", candles_by_pair, [("m15", "15min"), ("1h", "1h")])
    result = tdi_cycle_123.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        if row.get("symbol") not in live:
            continue
        try:
            alerts.alert_tdi123_setup(row["symbol"], row)
        except Exception as e:
            log.debug("TDI123 alert eval failed for %s: %s", row.get("symbol"), e)
        try:
            alerts.alert_tdi123_watch(row["symbol"], row)
        except Exception as e:
            log.debug("TDI123 watch eval failed for %s: %s", row.get("symbol"), e)

    # Resolve previously-journaled signals against the candles that have printed
    # since, so the dashboard accumulates a REAL win/loss record over time.
    try:
        from config import TDI123_JOURNAL_ENABLED
        if TDI123_JOURNAL_ENABLED:
            import trade_tracker
            trade_tracker.resolve_open_trades()
    except Exception as e:  # noqa: BLE001
        log.debug("TDI123 journal resolve failed: %s", e)


def _run_btmm123_alerts():
    """Run the BTMM 123 scanner and fire Discord alerts for confirmed setups."""
    import cache
    import btmm_123

    candles_by_pair: dict[str, dict] = {}
    for sym in btmm_123.BTMM123_UNIVERSE:
        candles_by_pair[sym] = {
            "1h": cache.read_candles(sym, "1h", limit=DEFAULT_BACKFILL),
            "4h": cache.read_candles(sym, "4h", limit=200),
            "m15": cache.read_candles(sym, "15min", limit=DEFAULT_BACKFILL),
        }
    live = _live_symbols("BTMM123", candles_by_pair, [("m15", "15min"), ("1h", "1h")])
    result = btmm_123.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        if row.get("symbol") not in live:
            continue
        try:
            alerts.alert_btmm123_setup(row["symbol"], row)
        except Exception as e:
            log.debug("BTMM123 alert eval failed for %s: %s", row.get("symbol"), e)
        try:
            alerts.alert_btmm123_watch(row["symbol"], row)
        except Exception as e:
            log.debug("BTMM123 watch eval failed for %s: %s", row.get("symbol"), e)


def _run_id50_alerts():
    """Run the BTMM ID50 scanner and fire Discord alerts for confirmed setups.

    ID50 is M15-primary (biased by H1) — the mirror of BTMM123's structure,
    with the grade/session/news handles already wired into alerts.py."""
    import cache
    import btmm_id50

    candles_by_pair: dict[str, dict] = {}
    for sym in btmm_id50.BTMM_ID50_UNIVERSE:
        candles_by_pair[sym] = {
            "m15": cache.read_candles(sym, "15min", limit=DEFAULT_BACKFILL),
            "1h": cache.read_candles(sym, "1h", limit=DEFAULT_BACKFILL),
        }
    live = _live_symbols("BTMM_ID50", candles_by_pair, [("m15", "15min"), ("1h", "1h")])
    result = btmm_id50.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        if row.get("symbol") not in live:
            continue
        try:
            alerts.alert_id50_setup(row["symbol"], row)
        except Exception as e:
            log.debug("BTMM_ID50 alert eval failed for %s: %s", row.get("symbol"), e)
        try:
            alerts.alert_id50_watch(row["symbol"], row)
        except Exception as e:
            log.debug("BTMM_ID50 watch eval failed for %s: %s", row.get("symbol"), e)


_stall_warned = False


def _watchdog():
    """Fire a Discord warning if the scheduler hasn't refreshed in a while.

    Runs as an independent job so it stays alive even if refresh_all is slow.
    Turns a silent multi-hour blackout into a single loud, throttled alert."""
    global _stall_warned
    last = last_refresh_ts()
    if last <= 0:
        return  # never refreshed yet (just started) — give it a cycle
    stale_secs = time.time() - last
    if stale_secs > STALL_THRESHOLD_SECS:
        if not _stall_warned:
            try:
                alerts.alert_scheduler_stall(int(stale_secs // 60))
                _stall_warned = True
            except Exception as e:
                log.warning("stall alert failed: %s", e)
    else:
        _stall_warned = False  # recovered → re-arm


def start() -> BackgroundScheduler:
    sched = BackgroundScheduler(timezone="UTC", daemon=True)
    # Run 2 min after each 15-min mark so the candle is fully closed by broker.
    sched.add_job(refresh_all, "cron", minute="2,17,32,47", id="refresh_all",
                  misfire_grace_time=120, max_instances=1, coalesce=True)
    # Independent watchdog every 5 min — alerts if refresh_all has stalled.
    sched.add_job(_watchdog, "cron", minute="*/5", id="watchdog",
                  misfire_grace_time=60, max_instances=1, coalesce=True)
    # Web-server self-check every minute (see _http_self_check).
    sched.add_job(_http_self_check, "interval", seconds=60, id="http_self_check",
                  misfire_grace_time=30, max_instances=1, coalesce=True)
    sched.start()
    log.info("scheduler started (refresh cron :02,:17,:32,:47 UTC; watchdog every 5m; web self-check every 1m)")
    return sched
