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


# --- feed parsing -----------------------------------------------------------
# Regression (2026-10-12): the feed sends dates as "MM-DD-YYYY" with times in
# UTC. The parser only knew "April 28, 2026" and silently dropped every event,
# so the news gate on every alert type had never blocked anything.

_SAMPLE_FEED = b"""<?xml version="1.0" encoding="windows-1252"?>
<weeklyevents>
  <event><title>Core CPI m/m</title><country>USD</country>
    <date><![CDATA[10-14-2026]]></date><time><![CDATA[12:30pm]]></time>
    <impact><![CDATA[High]]></impact></event>
  <event><title>Employment Change</title><country>AUD</country>
    <date><![CDATA[10-15-2026]]></date><time><![CDATA[12:30am]]></time>
    <impact><![CDATA[High]]></impact></event>
  <event><title>Bank Holiday</title><country>JPY</country>
    <date><![CDATA[10-11-2026]]></date><time><![CDATA[All Day]]></time>
    <impact><![CDATA[Holiday]]></impact></event>
</weeklyevents>"""


def _utc(y, m, d, hh, mm):
    import datetime as dt
    return int(dt.datetime(y, m, d, hh, mm, tzinfo=dt.timezone.utc).timestamp())


def test_parses_numeric_dates_as_utc():
    assert forexfactory._parse_ts("10-14-2026", "12:30pm") == _utc(2026, 10, 14, 12, 30)
    assert forexfactory._parse_ts("10-15-2026", "12:30am") == _utc(2026, 10, 15, 0, 30)
    assert forexfactory._parse_ts("10-11-2026", "All Day") == _utc(2026, 10, 11, 0, 0)


def test_fetch_feed_keeps_every_event(monkeypatch):
    class _Resp:
        content = _SAMPLE_FEED
        def raise_for_status(self):
            pass
    monkeypatch.setattr(forexfactory.requests, "get", lambda *a, **k: _Resp())
    events = forexfactory._fetch_feed()
    assert [(e["currency"], e["impact"]) for e in events] == [
        ("USD", "high"), ("AUD", "high"), ("JPY", "holiday")]
    assert events[0]["ts"] == _utc(2026, 10, 14, 12, 30)


def test_high_impact_event_blocks_its_currency(monkeypatch):
    _reset_module_state()
    class _Resp:
        content = _SAMPLE_FEED
        def raise_for_status(self):
            pass
    monkeypatch.setattr(forexfactory.requests, "get", lambda *a, **k: _Resp())
    monkeypatch.setattr(forexfactory.time, "time", lambda: float(_utc(2026, 10, 14, 12, 10)))
    assert forexfactory.currencies_in_window(30) == {"USD"}
    monkeypatch.setattr(forexfactory.time, "time", lambda: float(_utc(2026, 10, 14, 10, 0)))
    assert forexfactory.currencies_in_window(30) == set()
    _reset_module_state()
