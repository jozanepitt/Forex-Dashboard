"""Stack dumps for a frozen interpreter.

scheduler._http_self_check is a Python thread: when a C call holds the GIL it
freezes with everything else, so on 2026-10-18/19 six hangs left no
hang_dump.log at all. faulthandler's timer runs on its own C thread and dumps
every Python stack without taking the GIL.

A heartbeat thread re-arms faulthandler.dump_traceback_later every `beat`
seconds; each call cancels the previous timer. While the interpreter runs the
timer never fires. Once it stops, the timer fires after `timeout` seconds and,
with repeat=True, again every `timeout` seconds until the guardian kills the
process. Several dumps of the same stack prove a hang rather than a slow call.
faulthandler.enable() on the same file also records fatal crashes, which
pythonw otherwise loses.
"""
from __future__ import annotations

import faulthandler
import logging
import os
import threading
import time

log = logging.getLogger("hang_watchdog")

TIMEOUT_SECS = 20   # the guardian needs 45-105s to kill a hung service: 2+ dumps first
BEAT_SECS = 5

_file = None        # faulthandler writes to the fd: the file object must stay open


def start(path, timeout: float = TIMEOUT_SECS, beat: float = BEAT_SECS) -> None:
    global _file
    if _file is not None:
        return
    _file = open(path, "a", encoding="utf-8")
    _file.write(f"\n===== watchdog armed: pid {os.getpid()} at "
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} (dumps if frozen for {timeout:.0f}s) =====\n")
    _file.flush()
    faulthandler.enable(file=_file, all_threads=True)

    def _beat():
        while True:
            faulthandler.dump_traceback_later(timeout, repeat=True, file=_file)
            time.sleep(beat)

    threading.Thread(target=_beat, name="hang-watchdog", daemon=True).start()
    log.info("hang watchdog armed: stack dump to %s if the interpreter freezes for %.0fs",
             path, timeout)
