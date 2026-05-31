"""Background scheduler: refresh every 15 minutes (UTC-aligned, 2-min grace)."""
from __future__ import annotations

import logging
import time
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

import alerts
import fetcher
from btmm_core import active_kill_zone
from config import BTMM_ALERTS_ENABLED, DEFAULT_INTERVAL, PRIORITY_PAIRS

log = logging.getLogger("scheduler")

FANOUT_DELAY_SECS = 2  # spread 15-pair fanout across ~30s so a single key never blows 8/min

_prev_kill_zone: Optional[str] = None


def refresh_all():
    global _prev_kill_zone
    log.info("refresh_all: starting (%d pairs)", len(PRIORITY_PAIRS))
    t0 = time.time()
    updated = 0
    for i, sym in enumerate(PRIORITY_PAIRS):
        if i > 0:
            time.sleep(FANOUT_DELAY_SECS)
        try:
            fetcher.get_candles(sym, DEFAULT_INTERVAL)  # M15
            fetcher.get_candles(sym, "1h", limit=200)   # H1 — needed by M15 SNR scanner (~8 days)
            fetcher.get_candles(sym, "1day", limit=60)  # daily — needed by SNR-H4 scanner
            updated += 1
        except Exception as e:
            log.error("refresh_all: %s failed: %s", sym, e)
    log.info("refresh_all: done in %.1fs (%d/%d ok)", time.time() - t0, updated, len(PRIORITY_PAIRS))

    # Fire kill zone open alert on session transitions
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
            result = analyze(bars)
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
    """Run the M15 SNR Fast Scanner and fire Discord alerts for confirmed setups."""
    import cache
    import snr_m15_strategy

    candles_by_pair: dict[str, dict] = {}
    for sym in snr_m15_strategy.SNR_M15_UNIVERSE:
        candles_by_pair[sym] = {
            "1h": cache.read_candles(sym, "1h", limit=200),
            "m15": cache.read_candles(sym, "15min", limit=400),
        }
    result = snr_m15_strategy.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        try:
            alerts.alert_snr_m15_setup(row["symbol"], row)
        except Exception as e:
            log.debug("SNR-M15 alert eval failed for %s: %s", row.get("symbol"), e)


def start() -> BackgroundScheduler:
    sched = BackgroundScheduler(timezone="UTC", daemon=True)
    # Run 2 min after each 15-min mark so the candle is fully closed by broker.
    sched.add_job(refresh_all, "cron", minute="2,17,32,47", id="refresh_all",
                  misfire_grace_time=120, max_instances=1, coalesce=True)
    sched.start()
    log.info("scheduler started (cron: :02,:17,:32,:47 UTC)")
    return sched
