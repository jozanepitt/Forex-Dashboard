"""Runs MetaTrader5 calls in a child process the service can kill.

The MetaTrader5 binding holds the GIL for the whole of every call (measured
2026-10-19: a 1.1s copy_rates_from_pos let a 1ms ticker thread run zero
times). One stalled IPC call to the terminal therefore froze the entire
service: /health, the scheduler, the in-process self-check, every timeout.
The guardian killed it six times on 10-18/19.

Here the binding lives in a worker process. WorkerProxy looks like the
`MetaTrader5` module: `proxy.copy_rates_from_pos(...)` pickles the call to the
worker and waits for the answer on a queue, which releases the GIL, with a
timeout. A call that does not answer in time kills the worker and raises
WorkerTimeout; the next call starts a fresh worker and replays the last
`initialize()` first. Constants (TIMEFRAME_*) are read from the local module.

Run as a script, this file is the worker: `python mt5_worker.py MetaTrader5`.
It imports nothing from the service, so it can be launched standalone.
"""
from __future__ import annotations

import importlib
import logging
import os
import pickle
import queue
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

log = logging.getLogger("mt5")

_NS = "__mt5_ns__"   # wire marker for the binding's namedtuples (AccountInfo, Tick, ...)


class WorkerError(Exception):
    """The MT5 call failed inside the worker (raised by the binding)."""


class WorkerTimeout(WorkerError):
    """The MT5 call did not answer in time; the worker was killed."""


class WorkerLost(WorkerError):
    """The worker process died or could not be started."""


# ─────────────────────────────────────────────────────────────── parent side

class WorkerProxy:
    CALL_TIMEOUT_SECS = 20     # normal calls take <1s; a 60k-bar pull ~1s
    INIT_TIMEOUT_SECS = 75     # mt5.initialize() waits up to 60s for the terminal itself

    def __init__(self, module, module_name: str = "MetaTrader5",
                 call_timeout: float = CALL_TIMEOUT_SECS,
                 init_timeout: float = INIT_TIMEOUT_SECS):
        self._module = module
        self._module_name = module_name
        self._call_timeout = call_timeout
        self._init_timeout = init_timeout
        self._lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._replies: Optional[queue.Queue] = None
        self._init_args: Optional[tuple] = None   # replayed on every fresh worker
        self._job = None

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        if name.isupper():                        # TIMEFRAME_M15 etc.: no IPC
            return getattr(self._module, name)
        return lambda *args, **kwargs: self.call(name, *args, **kwargs)

    def call(self, name: str, *args, **kwargs) -> Any:
        with self._lock:
            if name == "initialize":
                self._init_args = (args, kwargs)
            fresh = self._proc is None
            if fresh:
                self._spawn()
            if fresh and name != "initialize" and self._init_args is not None:
                a, k = self._init_args
                if not self._roundtrip("initialize", a, k, self._init_timeout):
                    raise WorkerLost("mt5.initialize() failed in the replacement worker")
            timeout = self._init_timeout if name == "initialize" else self._call_timeout
            return self._roundtrip(name, args, kwargs, timeout)

    def close(self) -> None:
        with self._lock:
            self._kill()

    # -------------------------------------------------------------- internals
    def _spawn(self) -> None:
        argv = [sys.executable, str(Path(__file__).resolve()), self._module_name]
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        try:
            proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, creationflags=flags)
        except OSError as e:
            raise WorkerLost(f"could not start the MT5 worker: {e}") from e
        self._bind_to_parent(proc)
        replies: queue.Queue = queue.Queue()
        threading.Thread(target=_read_replies, args=(proc.stdout, replies),
                         name=f"mt5-worker-reader-{proc.pid}", daemon=True).start()
        self._proc, self._replies = proc, replies
        log.info("MT5 worker started (pid %d)", proc.pid)

    def _roundtrip(self, name: str, args: tuple, kwargs: dict, timeout: float) -> Any:
        proc, replies = self._proc, self._replies
        try:
            pickle.dump((name, args, kwargs), proc.stdin)
            proc.stdin.flush()
        except (OSError, ValueError) as e:
            self._kill()
            raise WorkerLost(f"MT5 worker gone before {name}(): {e}") from e
        try:
            status, value = replies.get(timeout=timeout)
        except queue.Empty:
            log.error("MT5 %s() did not answer in %.0fs - killing worker pid %d",
                      name, timeout, proc.pid)
            self._kill()
            raise WorkerTimeout(f"MT5 {name}() timed out after {timeout:.0f}s") from None
        if status == "ok":
            return _from_wire(value)
        if status == "err":
            raise WorkerError(f"MT5 {name}() failed: {value}")
        self._kill()
        raise WorkerLost(f"MT5 worker exited during {name}() ({value})")

    def _kill(self) -> None:
        proc, self._proc, self._replies = self._proc, None, None
        if proc is None:
            return
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception as e:  # noqa: BLE001
            log.warning("could not kill MT5 worker pid %d: %s", proc.pid, e)
        for pipe in (proc.stdin, proc.stdout):
            try:
                pipe.close()
            except Exception:  # noqa: BLE001
                pass

    def _bind_to_parent(self, proc: subprocess.Popen) -> None:
        """Put the worker in a kill-on-close job so it dies with this process.
        The guardian Stop-Process'es a hung app.py; without this the worker
        would linger as an orphan. Best effort: non-Windows or failure → skip."""
        if sys.platform != "win32":
            return
        try:
            if self._job is None:
                self._job = _create_kill_on_close_job()
            _assign_to_job(self._job, proc)
        except Exception as e:  # noqa: BLE001
            log.warning("MT5 worker not bound to a job object (may outlive app.py): %s", e)


def _read_replies(stream, replies: queue.Queue) -> None:
    while True:
        try:
            replies.put(pickle.load(stream))
        except Exception as e:  # noqa: BLE001 — EOF/broken pipe: the worker is gone
            replies.put(("lost", type(e).__name__))
            return


def _from_wire(value: Any) -> Any:
    if isinstance(value, dict) and _NS in value:
        return SimpleNamespace(**value[_NS])
    if isinstance(value, list):
        return [_from_wire(v) for v in value]
    return value


def _create_kill_on_close_job():
    import ctypes
    from ctypes import wintypes

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [(n, ctypes.c_ulonglong) for n in (
            "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
            "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

    class BASIC_LIMIT(ctypes.Structure):
        _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64),
                    ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", wintypes.DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", wintypes.DWORD),
                    ("SchedulingClass", wintypes.DWORD)]

    class EXTENDED_LIMIT(ctypes.Structure):
        _fields_ = [("BasicLimitInformation", BASIC_LIMIT),
                    ("IoInfo", IO_COUNTERS),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t)]

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.CreateJobObjectW.restype = wintypes.HANDLE
    k32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    k32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    job = k32.CreateJobObjectW(None, None)
    if not job:
        raise ctypes.WinError(ctypes.get_last_error())
    info = EXTENDED_LIMIT()
    info.BasicLimitInformation.LimitFlags = 0x2000   # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not k32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
        raise ctypes.WinError(ctypes.get_last_error())
    return job   # never closed: closing it is what kills the workers


def _assign_to_job(job, proc: subprocess.Popen) -> None:
    import ctypes
    from ctypes import wintypes
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    if not k32.AssignProcessToJobObject(job, int(proc._handle)):
        raise ctypes.WinError(ctypes.get_last_error())


# ─────────────────────────────────────────────────────────────── worker side

def _to_wire(value: Any) -> Any:
    if hasattr(value, "_asdict"):
        return {_NS: dict(value._asdict())}
    if isinstance(value, tuple) and value and hasattr(value[0], "_asdict"):
        return [_to_wire(v) for v in value]     # e.g. symbols_get()
    return value


def serve(module_name: str) -> int:
    rd = sys.stdin.buffer if sys.stdin else os.fdopen(0, "rb")
    wr = sys.stdout.buffer if sys.stdout else os.fdopen(1, "wb")
    # nothing the binding prints may corrupt the reply channel
    sys.stdout = sys.stderr = open(os.devnull, "w")
    mod = importlib.import_module(module_name)
    while True:
        try:
            name, args, kwargs = pickle.load(rd)
        except EOFError:
            return 0   # parent closed the pipe (or died)
        try:
            reply = ("ok", _to_wire(getattr(mod, name)(*args, **kwargs)))
        except Exception as e:  # noqa: BLE001
            reply = ("err", f"{type(e).__name__}: {e}")
        pickle.dump(reply, wr)
        wr.flush()


if __name__ == "__main__":
    sys.exit(serve(sys.argv[1] if len(sys.argv) > 1 else "MetaTrader5"))
