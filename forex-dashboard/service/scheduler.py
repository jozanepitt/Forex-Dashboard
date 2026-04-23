"""Background scheduler: refresh every 15 minutes (UTC-aligned, 2-min grace)."""
from __future__ import annotations

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler

import fetcher
from config import DEFAULT_INTERVAL, PRIORITY_PAIRS

log = logging.getLogger("scheduler")


def refresh_all():
    log.info("refresh_all: starting (%d pairs)", len(PRIORITY_PAIRS))
    t0 = time.time()
    for sym in PRIORITY_PAIRS:
        try:
            fetcher.get_candles(sym, DEFAULT_INTERVAL)
        except Exception as e:
            log.error("refresh_all: %s failed: %s", sym, e)
    log.info("refresh_all: done in %.1fs", time.time() - t0)


def start() -> BackgroundScheduler:
    sched = BackgroundScheduler(timezone="UTC", daemon=True)
    # Run 2 min after each 15-min mark so the candle is fully closed by broker.
    sched.add_job(refresh_all, "cron", minute="2,17,32,47", id="refresh_all",
                  misfire_grace_time=120, max_instances=1, coalesce=True)
    sched.start()
    log.info("scheduler started (cron: :02,:17,:32,:47 UTC)")
    return sched
