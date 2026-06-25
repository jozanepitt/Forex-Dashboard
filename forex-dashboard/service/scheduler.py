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
from btmm_core import active_kill_zone
from config import BTMM_ALERTS_ENABLED, BTMM_APLUS_ONLY, DEFAULT_INTERVAL, PRIORITY_PAIRS, SERVICE_ROOT

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


def refresh_all():
    global _prev_kill_zone
    log.info("refresh_all: starting (%d pairs)", len(PRIORITY_PAIRS))
    t0 = time.time()
    updated = 0
    for i, sym in enumerate(PRIORITY_PAIRS):
        if i > 0:
            time.sleep(FANOUT_DELAY_SECS)
        try:
            _fetch_guarded(sym, DEFAULT_INTERVAL)          # M15
            _fetch_guarded(sym, "1h", limit=200)           # H1 — M15 SNR scanner (~8 days)
            _fetch_guarded(sym, "1day", limit=60)          # daily — SNR-H4 scanner
            updated += 1
        except Exception as e:
            log.error("refresh_all: %s fetch failed/timed out: %s", sym, e)
    log.info("refresh_all: done in %.1fs (%d/%d ok)", time.time() - t0, updated, len(PRIORITY_PAIRS))
    if updated:
        _record_refresh(time.time())  # mark healthy only when we actually got fresh data

    # Fire kill zone open alert on session transitions.
    # Suppressed in BTMM A+-only mode (user wants A+ setups only, nothing else).
    if BTMM_ALERTS_ENABLED and not BTMM_APLUS_ONLY:
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

    # Run 1AM CRT scanner + Discord alerts
    try:
        _run_crt_alerts()
    except Exception as e:
        log.warning("CRT alerts failed: %s", e)

    # Run 5AM CRT scanner + Discord alerts (NY Open kill zone)
    try:
        _run_crt_5am_alerts()
    except Exception as e:
        log.warning("CRT-5AM alerts failed: %s", e)

    # Run Malaysian SNR Emperor scanner + Discord alerts
    try:
        _run_snr_alerts()
    except Exception as e:
        log.warning("SNR alerts failed: %s", e)

    # Run M15 SNR Fast Scanner + Discord alerts
    try:
        _run_snr_m15_alerts()
    except Exception as e:
        log.warning("SNR-M15 alerts failed: %s", e)

    # Notify dashboard subscribers via WebSocket. Late import keeps scheduler importable
    # standalone (e.g. for tests) without pulling Flask-SocketIO into the import graph.
    try:
        from app import emit_refresh_complete
        emit_refresh_complete(updated)
    except Exception as e:
        log.debug("emit_refresh_complete unavailable: %s", e)


def _run_alerts():
    """Evaluate alert rules for each priority pair using cached candle data."""
    import cache
    from btmm_core import analyze
    from config import DEFAULT_BACKFILL

    for sym in PRIORITY_PAIRS:
        try:
            bars = cache.read_candles(sym, DEFAULT_INTERVAL, DEFAULT_BACKFILL)
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


def _run_crt_alerts():
    """Run the 1AM CRT scanner against the cache and fire Discord alerts for A/B grades
    that fall inside the WAITING or ACTIVE key-time window of a live session."""
    import cache
    import crt_strategy

    candles_by_pair: dict[str, dict] = {}
    for sym in crt_strategy.CRT_UNIVERSE:
        candles_by_pair[sym] = {
            "m15": cache.read_candles(sym, "15min", limit=400),
        }
    result = crt_strategy.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        try:
            alerts.alert_crt_setup(row["symbol"], row)
        except Exception as e:
            log.debug("CRT alert eval failed for %s: %s", row.get("symbol"), e)


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
    result = crt_strategy.analyze_universe_5am(candles_by_pair)
    for row in result.get("pairs", []):
        try:
            alerts.alert_crt_5am_setup(row["symbol"], row)
        except Exception as e:
            log.debug("CRT-5AM alert eval failed for %s: %s", row.get("symbol"), e)


def _run_snr_alerts():
    """Run the Malaysian SNR Emperor scanner and fire Discord alerts for confirmed setups."""
    import cache
    import snr_strategy

    candles_by_pair: dict[str, dict] = {}
    for sym in snr_strategy.SNR_UNIVERSE:
        candles_by_pair[sym] = {
            "m15": cache.read_candles(sym, "15min", limit=400),
            "1d": cache.read_candles(sym, "1day", limit=60),
        }
    result = snr_strategy.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        try:
            alerts.alert_snr_setup(row["symbol"], row)
        except Exception as e:
            log.debug("SNR alert eval failed for %s: %s", row.get("symbol"), e)


def _run_snr_m15_alerts():
    """Run the M15 SNR Fast Scanner and fire Discord alerts for confirmed setups.

    The EMS gate (see alerts.alert_snr_m15_setup) needs the H4 scanner's
    storyline for the same pair, so we run the H4 analysis here and pass each
    pair's H4 row + raw M15 candles into the alert evaluator.
    """
    import cache
    import snr_m15_strategy
    import snr_strategy

    candles_by_pair: dict[str, dict] = {}
    for sym in snr_m15_strategy.SNR_M15_UNIVERSE:
        candles_by_pair[sym] = {
            "1h": cache.read_candles(sym, "1h", limit=200),
            "m15": cache.read_candles(sym, "15min", limit=400),
        }

    # H4 storyline context for the EMS gate (uses M15 + daily candles).
    h4_by_pair: dict[str, dict] = {}
    try:
        h4_candles_by_pair = {
            sym: {
                "m15": candles_by_pair[sym]["m15"],
                "1d": cache.read_candles(sym, "1day", limit=60),
            }
            for sym in snr_strategy.SNR_UNIVERSE
        }
        h4_result = snr_strategy.analyze_universe(h4_candles_by_pair)
        h4_by_pair = {r["symbol"]: r for r in h4_result.get("pairs", [])}
    except Exception as e:
        log.debug("SNR-M15 EMS gate: H4 context unavailable: %s", e)

    result = snr_m15_strategy.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        sym = row["symbol"]
        try:
            alerts.alert_snr_m15_setup(
                sym, row,
                h4_row=h4_by_pair.get(sym),
                m15_candles=candles_by_pair.get(sym, {}).get("m15"),
            )
        except Exception as e:
            log.debug("SNR-M15 alert eval failed for %s: %s", sym, e)


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
    sched.start()
    log.info("scheduler started (refresh cron :02,:17,:32,:47 UTC; watchdog every 5m)")
    return sched
