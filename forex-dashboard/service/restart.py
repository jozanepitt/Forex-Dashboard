"""The dashboard's "Restart services" button (POST /admin/restart).

The service cannot restart itself from inside: the moment it stops, nothing is
left to start it again. So it hands the job to restart-services.ps1, launched
as a separate, detached process, which stops the guardians, app.py and the
Discord bot, starts them again and logs each step to guardian.log.

/health reports this process's pid and start time so the dashboard can tell
the NEW process answering from the old one still winding down.
"""
from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from urllib.parse import urlsplit

from config import SERVICE_PORT, SERVICE_ROOT

log = logging.getLogger("restart")

STARTED_AT = time.time()
SCRIPT = SERVICE_ROOT.parent / "restart-services.ps1"
REFRESH_WAIT_SECS = 180     # a data refresh normally takes ~2 min; don't cut it off half-written
ALLOWED_ORIGINS = {f"http://localhost:{SERVICE_PORT}", f"http://127.0.0.1:{SERVICE_PORT}"}

_lock = threading.Lock()
_pending = False


def health_info() -> dict:
    return {"status": "ok", "pid": os.getpid(), "started_at": int(STARTED_AT),
            "restart_pending": _pending}


def origin_allowed(origin: str | None, referer: str | None) -> bool:
    """Only the dashboard page itself may restart the service. CORS is open to
    every origin, so without this any website open in the browser could post
    here and restart it."""
    if origin:
        return origin in ALLOWED_ORIGINS
    if referer:
        parts = urlsplit(referer)
        return f"{parts.scheme}://{parts.netloc}" in ALLOWED_ORIGINS
    return False


def is_pending() -> bool:
    """True from the click until this process is stopped (the scheduler skips
    starting a new data refresh meanwhile)."""
    return _pending


def _launch_script(pid: int):
    flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    return subprocess.Popen(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden",
         "-ExecutionPolicy", "Bypass", "-File", str(SCRIPT), "-OldPid", str(pid)],
        cwd=str(SCRIPT.parent), creationflags=flags, close_fds=True,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _spawn_thread(target, args) -> None:
    threading.Thread(target=target, args=args, daemon=True, name="restart").start()


def _run(is_refreshing, launch, wait, now) -> None:
    global _pending
    try:
        deadline = now() + REFRESH_WAIT_SECS
        busy = is_refreshing()
        if busy:
            log.info("restart: waiting for the running data refresh to finish")
        while busy and now() < deadline:
            wait(2)
            busy = is_refreshing()
        log.info("restart: launching %s (this pid %d)", SCRIPT.name, os.getpid())
        proc = launch(os.getpid())
        # A working script kills this process, so wait() never returns. If it
        # does, the script ended without restarting us (blocked by policy,
        # crashed, or the old process could not be stopped): free the button.
        rc = proc.wait() if proc is not None else None
        log.error("restart: %s exited (rc=%s) without restarting this process; see guardian.log",
                  SCRIPT.name, rc)
    except Exception:
        log.exception("restart: could not launch %s", SCRIPT.name)
    with _lock:
        _pending = False


def request_restart(is_refreshing, launch=_launch_script, spawn=_spawn_thread,
                    wait=time.sleep, now=time.time) -> tuple[bool, str]:
    """Start a restart unless one is already under way. Returns at once; the
    wait for a running refresh happens on a background thread."""
    global _pending
    with _lock:
        if _pending:
            return False, "already restarting"
        _pending = True
    spawn(_run, (is_refreshing, launch, wait, now))
    return True, "restarting"
