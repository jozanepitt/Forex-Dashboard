"""Tests for market_hours — the "may a LIVE alert fire for this symbol right
now?" gate. Written for the 2026-10-11 incident: CAD/JPY VWAP+9EMA SELL alerts
kept firing all weekend because the scanner judged "in session" from the last
cached candle (frozen at Fri 09:20 UTC after the MT5 link died) instead of
from the wall clock.
"""
from __future__ import annotations

import datetime as dt

import market_hours as mh


def ts(y: int, m: int, d: int, hh: int, mm: int = 0) -> int:
    return int(dt.datetime(y, m, d, hh, mm, tzinfo=dt.timezone.utc).timestamp())


# 2026-10-09 is a Friday, 10-10 Saturday, 10-11 Sunday, 10-12 Monday.
INCIDENT_NOW = ts(2026, 10, 11, 10, 2)        # Sunday 10:02 UTC — when the alert fired
FROZEN_BAR = ts(2026, 10, 9, 9, 20)           # CAD/JPY's last cached M5 bar
M5, M15 = 300, 900


# ── is_market_open ────────────────────────────────────────────────────────

def test_fx_is_closed_at_the_incident_time():
    assert mh.is_market_open("CAD/JPY", INCIDENT_NOW) is False


def test_non_crypto_is_closed_all_weekend():
    assert mh.is_market_open("EUR/USD", ts(2026, 10, 10, 12)) is False     # Saturday midday
    assert mh.is_market_open("XAU/USD", ts(2026, 10, 9, 21, 0)) is False   # Friday, at the close
    assert mh.is_market_open("USTEC", ts(2026, 10, 11, 20, 59)) is False   # Sunday, just before the open
    assert mh.is_market_open("DXY", ts(2026, 10, 10, 0, 0)) is False       # Saturday 00:00
    # Sunday evening: a fallback provider prints indicative quotes before the
    # broker reopens, so the calendar (not the data) has to hold the line.
    assert mh.is_market_open("CAD/JPY", ts(2026, 10, 11, 22, 4)) is False
    assert mh.is_market_open("XAU/USD", ts(2026, 10, 11, 22, 30)) is False  # metals reopen after FX
    assert mh.is_market_open("USTEC", ts(2026, 10, 11, 23, 4)) is False


def test_non_crypto_is_open_during_the_week():
    assert mh.is_market_open("CAD/JPY", ts(2026, 10, 9, 20, 59)) is True   # Friday, last minute
    assert mh.is_market_open("CAD/JPY", ts(2026, 10, 11, 22, 5)) is True   # Sunday, FX reopen
    assert mh.is_market_open("XAU/USD", ts(2026, 10, 11, 23, 5)) is True   # Sunday, metals reopen
    assert mh.is_market_open("EUR/USD", ts(2026, 10, 12, 0, 0)) is True    # Monday 00:00
    assert mh.is_market_open("USTEC", ts(2026, 10, 7, 10, 2)) is True      # Wednesday


def test_crypto_is_always_open():
    for sym in ("BTC/USD", "ETH/USD"):
        assert mh.is_market_open(sym, INCIDENT_NOW) is True
        assert mh.is_market_open(sym, ts(2026, 10, 10, 12)) is True


# ── is_data_fresh ─────────────────────────────────────────────────────────

def test_frozen_friday_bar_is_stale_on_sunday():
    assert mh.is_data_fresh(FROZEN_BAR, M5, INCIDENT_NOW) is False


def test_forming_and_just_closed_bars_are_fresh():
    now = ts(2026, 10, 7, 10, 3)                       # scheduler runs ~3 min past the mark
    assert mh.is_data_fresh(ts(2026, 10, 7, 10, 0), M5, now) is True     # forming M5 bar
    assert mh.is_data_fresh(ts(2026, 10, 7, 9, 55), M5, now) is True     # last closed M5 bar
    assert mh.is_data_fresh(ts(2026, 10, 7, 10, 0), M15, now) is True    # forming M15 bar
    assert mh.is_data_fresh(ts(2026, 10, 7, 9, 45), M15, now) is True    # last closed M15 bar


def test_stale_limits_per_timeframe():
    assert mh.stale_after_secs(M5) == 900       # one bar + fan-out allowance
    assert mh.stale_after_secs(M15) == 1800     # two bars
    assert mh.stale_after_secs(3600) == 7200


def test_freshness_boundary_is_inclusive():
    now = ts(2026, 10, 7, 10, 0)
    for interval in (M5, M15):
        limit = mh.stale_after_secs(interval)
        assert mh.is_data_fresh(now - limit, interval, now) is True
        assert mh.is_data_fresh(now - limit - 1, interval, now) is False


def test_healthy_closed_bar_ages_are_fresh():
    # MT5 returns closed bars only; these are the worst healthy ages measured
    # at runner time (M15 ~1230s, M5 ~700s) plus the Oct-8 degraded M15 (~1610s).
    now = ts(2026, 10, 7, 10, 0)
    assert mh.is_data_fresh(now - 1230, M15, now) is True
    assert mh.is_data_fresh(now - 1610, M15, now) is True
    assert mh.is_data_fresh(now - 700, M5, now) is True


def test_one_missed_refresh_cycle_makes_the_feed_stale():
    # Newest closed M15 bar is 09:45; the 10:17 cycle was missed, so at the
    # 10:32 cycle's runner time it is ~2070-2115s old. Both ends must block.
    bar = ts(2026, 10, 7, 9, 45)
    assert mh.is_data_fresh(bar, M15, bar + 2070) is False
    assert mh.is_data_fresh(bar, M15, bar + 2115) is False
    assert mh.is_data_fresh(ts(2026, 10, 7, 9, 55), M5, ts(2026, 10, 7, 9, 55) + 1477) is False


def test_bar_stamped_in_the_future_is_not_fresh():
    now = ts(2026, 10, 7, 10, 0)
    assert mh.is_data_fresh(now + 2 * M5, M5, now) is False   # mis-stamped provider data
    assert mh.is_data_fresh(now + 60, M5, now) is True        # small clock skew is fine


# ── live_block_reason ─────────────────────────────────────────────────────

def _bars(last_ts: int, interval: int, n: int = 5) -> list[dict]:
    return [{"ts_utc": last_ts - (n - 1 - i) * interval} for i in range(n)]


def test_incident_is_blocked_as_market_closed():
    reason = mh.live_block_reason("CAD/JPY", _bars(FROZEN_BAR, M5), M5, INCIDENT_NOW)
    assert reason is not None and "closed" in reason


def test_stale_feed_is_blocked_on_a_weekday():
    # Same frozen feed, but on Friday afternoon while the market is still open:
    # the weekend rule can't catch it, the freshness rule must.
    now = ts(2026, 10, 9, 14, 2)
    reason = mh.live_block_reason("CAD/JPY", _bars(FROZEN_BAR, M5), M5, now)
    assert reason is not None and "stale" in reason


def test_no_bars_is_blocked():
    assert mh.live_block_reason("CAD/JPY", [], M5, ts(2026, 10, 7, 10, 2)) is not None


def test_malformed_newest_bar_is_blocked_not_raised():
    now = ts(2026, 10, 7, 10, 2)
    for bad in ([{}], [{"ts_utc": None}], [{"ts_utc": "soon"}]):
        assert mh.live_block_reason("CAD/JPY", bad, M5, now) == "bad data"


def test_live_weekday_feed_is_not_blocked():
    now = ts(2026, 10, 7, 10, 3)
    assert mh.live_block_reason("CAD/JPY", _bars(ts(2026, 10, 7, 10, 0), M5), M5, now) is None


def test_crypto_with_fresh_bars_is_not_blocked_on_a_weekend():
    now = INCIDENT_NOW
    assert mh.live_block_reason("BTC/USD", _bars(now - 120, M5), M5, now) is None


def test_crypto_with_frozen_bars_is_still_blocked():
    reason = mh.live_block_reason("BTC/USD", _bars(FROZEN_BAR, M5), M5, INCIDENT_NOW)
    assert reason is not None and "stale" in reason


# --- closed-market bars ------------------------------------------------------
# Audit 2026-10-12: fallback providers wrote flat zero-volume quotes for the
# weekend (and an old import left copies of Friday's bars on Sat/Sun). About
# 585 of the 3200 M15 bars every strategy reads were such bars.

def test_zero_volume_weekend_bar_is_closed_market():
    assert mh.is_closed_market_bar("EUR/USD", ts(2026, 10, 10, 12, 0), 0)      # Saturday
    assert mh.is_closed_market_bar("EUR/USD", ts(2026, 10, 9, 21, 15), 0)      # Fri after the close
    assert mh.is_closed_market_bar("XAU/USD", ts(2026, 10, 11, 20, 45), None)  # Sunday before the open


def test_weekend_bar_with_volume_is_still_closed_market():
    """Saturday never trades, whatever the volume column says."""
    assert mh.is_closed_market_bar("EUR/USD", ts(2026, 10, 10, 0, 0), 432.0)
    assert mh.is_closed_market_bar("EUR/USD", ts(2026, 10, 11, 18, 45), 250.0)


def test_real_bars_at_the_edges_of_the_week_are_kept():
    assert not mh.is_closed_market_bar("EUR/USD", ts(2026, 10, 9, 20, 45), 150.0)   # last summer bar
    assert not mh.is_closed_market_bar("EUR/USD", ts(2026, 1, 9, 21, 45), 150.0)    # winter: closes 22:00
    assert not mh.is_closed_market_bar("EUR/USD", ts(2026, 10, 11, 21, 0), 80.0)    # summer reopen
    assert not mh.is_closed_market_bar("EUR/USD", ts(2026, 10, 7, 3, 0), 0)         # weekday, no-volume feed


def test_crypto_has_no_closed_market_bars():
    assert not mh.is_closed_market_bar("BTC/USD", ts(2026, 10, 10, 12, 0), 0)


def test_drop_closed_market_bars_keeps_order_and_real_bars():
    bars = [{"ts_utc": ts(2026, 10, 9, 20, 45), "volume": 10.0},
            {"ts_utc": ts(2026, 10, 10, 9, 0), "volume": 0},
            {"ts_utc": ts(2026, 10, 11, 21, 0), "volume": 5.0}]
    assert mh.drop_closed_market_bars("EUR/USD", bars) == [bars[0], bars[2]]
