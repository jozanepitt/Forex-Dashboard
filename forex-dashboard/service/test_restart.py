"""Tests for the dashboard's "Restart services" button (restart.py)."""
import os

import pytest

import restart


@pytest.fixture(autouse=True)
def _reset():
    restart._pending = False
    yield
    restart._pending = False


def _inline(target, args):
    target(*args)


def test_origin_check_only_accepts_the_dashboard_itself():
    assert restart.origin_allowed("http://localhost:3002", None)
    assert restart.origin_allowed("http://127.0.0.1:3002", None)
    assert restart.origin_allowed(None, "http://localhost:3002/")
    assert restart.origin_allowed(None, "http://127.0.0.1:3002/?tab=engulfing")
    # any other website the user has open must not be able to restart the service
    assert not restart.origin_allowed("https://evil.example", None)
    assert not restart.origin_allowed("http://localhost:3002.evil.example", None)
    assert not restart.origin_allowed(None, "http://localhost:3002.evil.example/")
    assert not restart.origin_allowed("null", None)          # file:// or a sandboxed iframe
    assert not restart.origin_allowed(None, None)


class _ScriptThatGaveUp:
    """A launched script that exited without killing us (wait() returned)."""
    returncode = 1

    def wait(self):
        return self.returncode


def _launcher(record):
    def launch(pid):
        record.append(pid)
        return _ScriptThatGaveUp()
    return launch


def _not_yet(target, args):
    """The restart thread has not finished yet (the script is still running)."""


def test_restart_launches_the_script_with_this_process_id():
    launched = []
    ok, msg = restart.request_restart(lambda: False, launch=_launcher(launched), spawn=_inline)
    assert ok and launched == [os.getpid()]


def test_restart_is_pending_until_the_script_finishes():
    assert restart.request_restart(lambda: False, launch=_launcher([]), spawn=_not_yet)[0]
    assert restart.health_info()["restart_pending"] is True
    assert restart.is_pending() is True


def test_second_click_while_restarting_is_ignored():
    assert restart.request_restart(lambda: False, launch=_launcher([]), spawn=_not_yet)[0]
    ok, msg = restart.request_restart(lambda: False, launch=_launcher([]), spawn=_not_yet)
    assert not ok and "already" in msg


def test_script_that_exits_without_restarting_frees_the_button():
    """Review finding: a script blocked by policy, crashed, or unable to stop
    the old process used to leave every later click answering 409."""
    launched = []
    restart.request_restart(lambda: False, launch=_launcher(launched), spawn=_inline)
    assert launched and restart.is_pending() is False
    assert restart.request_restart(lambda: False, launch=_launcher(launched), spawn=_inline)[0]
    assert len(launched) == 2


def test_waits_for_a_running_data_refresh_before_restarting():
    busy = iter([True, True, True, False])
    waits, launched = [], []
    restart.request_restart(lambda: next(busy), launch=_launcher(launched), spawn=_inline,
                            wait=waits.append)
    assert len(waits) == 3 and launched == [os.getpid()]


def test_does_not_wait_for_a_stuck_refresh_forever():
    clock = [0.0]

    def wait(secs):
        clock[0] += secs

    launched = []
    restart.request_restart(lambda: True, launch=_launcher(launched), spawn=_inline,
                            wait=wait, now=lambda: clock[0])
    assert launched and clock[0] >= restart.REFRESH_WAIT_SECS


def test_failed_launch_clears_pending_so_the_user_can_retry():
    def boom(pid):
        raise OSError("powershell missing")

    restart.request_restart(lambda: False, launch=boom, spawn=_inline)
    assert restart.health_info()["restart_pending"] is False
    assert restart.request_restart(lambda: False, launch=_launcher([]), spawn=_inline)[0]


def test_health_reports_which_process_is_answering():
    h = restart.health_info()
    assert h["status"] == "ok" and h["pid"] == os.getpid()
    assert isinstance(h["started_at"], int) and h["restart_pending"] is False


def test_restart_script_exists_next_to_the_guardians():
    assert restart.SCRIPT.is_file()
    assert (restart.SCRIPT.parent / "service-guardian-loop.ps1").is_file()


def test_scheduler_reports_a_refresh_in_progress(monkeypatch):
    import scheduler
    seen = []
    monkeypatch.setattr(scheduler, "_refresh_all_inner", lambda: seen.append(scheduler.is_refreshing()))
    assert scheduler.is_refreshing() is False
    scheduler.refresh_all()
    assert seen == [True] and scheduler.is_refreshing() is False


def test_scheduler_skips_a_refresh_while_a_restart_is_pending(monkeypatch):
    """Review finding: a refresh starting in the seconds between the wait and
    the kill would be cut off half-written."""
    import scheduler
    seen = []
    monkeypatch.setattr(scheduler, "_refresh_all_inner", lambda: seen.append(1))
    restart._pending = True
    scheduler.refresh_all()
    assert seen == []


def test_scheduler_clears_the_refresh_flag_when_a_refresh_crashes(monkeypatch):
    import scheduler

    def crash():
        raise RuntimeError("mt5 gone")

    monkeypatch.setattr(scheduler, "_refresh_all_inner", crash)
    with pytest.raises(RuntimeError):
        scheduler.refresh_all()
    assert scheduler.is_refreshing() is False
