"""Tests for providers.forexfactory's fetch-failure backoff.

Covers the 2026-09-11 fix: a failed fetch previously left `_cache_ts`
untouched, so `get_events()` retried on literally every call once the
upstream feed started rate-limiting -- which kept it rate-limited
indefinitely and left the news-filter gate on every alert type silently
fail-open. A failed fetch must now back off for `_FAIL_RETRY_SECS` before
retrying, while a successful fetch is still cached for the full TTL.
"""
from __future__ import annotations

from providers import forexfactory


def _reset_module_state():
    forexfactory._cache = []
    forexfactory._cache_ts = 0.0
    forexfactory._last_attempt_ts = 0.0


def test_repeated_failures_back_off_instead_of_retrying_every_call(monkeypatch):
    _reset_module_state()
    calls = []

    def _failing_fetch():
        calls.append(1)
        raise RuntimeError("429 Client Error: Too Many Requests")

    monkeypatch.setattr(forexfactory, "_fetch_feed", _failing_fetch)

    now = [1_000_000.0]
    monkeypatch.setattr(forexfactory.time, "time", lambda: now[0])

    forexfactory.get_events()
    assert len(calls) == 1  # first call always attempts

    # Immediately calling again (same instant) must NOT re-attempt --
    # this is exactly the bug: previously _cache_ts stayed 0 forever on
    # failure, so every single call re-fetched and got rate-limited again.
    forexfactory.get_events()
    forexfactory.get_events()
    assert len(calls) == 1

    # Still within the backoff window (< 300s later) -- still no retry.
    now[0] += 120
    forexfactory.get_events()
    assert len(calls) == 1

    # Past the backoff window -- retries.
    now[0] += 200  # total 320s since first attempt
    forexfactory.get_events()
    assert len(calls) == 2


def test_successful_fetch_is_cached_for_the_full_ttl(monkeypatch):
    _reset_module_state()
    calls = []

    def _ok_fetch():
        calls.append(1)
        return [{"ts": 0, "currency": "USD", "title": "x", "impact": "high"}]

    monkeypatch.setattr(forexfactory, "_fetch_feed", _ok_fetch)

    now = [2_000_000.0]
    monkeypatch.setattr(forexfactory.time, "time", lambda: now[0])

    events = forexfactory.get_events()
    assert len(calls) == 1
    assert len(events) == 1

    # Well past the failure-backoff window, but within the 1-hour TTL --
    # a successful cache must not be discarded just because
    # _FAIL_RETRY_SECS elapsed.
    now[0] += 600
    forexfactory.get_events()
    assert len(calls) == 1

    # Past the full TTL -- re-fetches.
    now[0] += forexfactory._CACHE_TTL
    forexfactory.get_events()
    assert len(calls) == 2


def test_force_refresh_always_attempts(monkeypatch):
    _reset_module_state()
    calls = []
    monkeypatch.setattr(forexfactory, "_fetch_feed", lambda: (calls.append(1), [])[1])
    monkeypatch.setattr(forexfactory.time, "time", lambda: 3_000_000.0)

    forexfactory.get_events()
    forexfactory.get_events(force_refresh=True)
    forexfactory.get_events(force_refresh=True)
    assert len(calls) == 3
