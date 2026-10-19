"""Tests for the live VWAP+9EMA Discord alert function.

Monkeypatches alerts._post_discord to a recorder so no real webhook call is
made. Mirrors the verification approach used for test_vwap_mr_alerts.py.

Regression coverage: alert_vwap9ema_setup originally shipped (Task 4) without
the R:R sanity check / incomplete-trade-plan guard that every other
trade-plan alert in this file has -- the exact gap alert_vwap_mr_setup's own
code comment warns about re: "the retired VWAP+9EMA alert, which had no such
check" (2026-09-10 review finding). Fixed same day this file was added; these
tests exist so it can't silently regress a second time.
"""
from __future__ import annotations

import pytest

import alerts


@pytest.fixture(autouse=True)
def _no_news(monkeypatch):
    """Keep the real economic calendar out of these tests."""
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda *a, **k: set())


def _good_row(setup="BUY"):
    return {
        "symbol": "USTEC", "setup": setup,
        "entry": 100.00, "sl": 99.00, "tp1": 102.00, "grade": "A",
        "notes": "test row",
    }


def _bad_rr_row():
    row = _good_row()
    row["sl"] = 99.99   # 1-point stop vs a 2-point TP -- still under min_rr, must be rejected
    return row


def _incomplete_row():
    row = _good_row()
    row["sl"] = None
    return row


def _row_with_bands():
    row = _good_row()
    row.update({
        "vwap": 100.5,
        "vwap_upper_1": 100.8, "vwap_lower_1": 100.2,
        "vwap_upper_2": 101.1, "vwap_lower_2": 99.9,
    })
    return row


def test_alert_vwap9ema_setup_posts_for_a_good_row(monkeypatch):
    """Per explicit user request (2026-09-22), the verbose UNVALIDATED/
    failed-backtest banner no longer appears in the description -- just the
    signal-specific note. The compact '(unvalidated)' tag stays in the
    footer, and a VWAP Bands field is always present (falling back to '--'
    when the row carries no band data, as here)."""
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP9EMA_ALERTS_ENABLED", True)
    alerts.alert_vwap9ema_setup("USTEC", _good_row())
    assert len(posted) == 1
    assert "VWAP+9EMA" in posted[0]["title"]
    assert posted[0]["description"] == "test row"
    assert "UNVALIDATED strategy" not in posted[0]["description"]
    assert "failed backtest (0/48)" not in posted[0]["description"]
    assert "(unvalidated)" in posted[0]["footer"]["text"]
    bands_field = next(f for f in posted[0]["fields"] if f["name"] == "VWAP Bands")
    assert bands_field["value"] == "—"


def test_alert_vwap9ema_setup_shows_bands_when_present(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP9EMA_ALERTS_ENABLED", True)
    alerts.alert_vwap9ema_setup("USTEC", _row_with_bands())
    assert len(posted) == 1
    bands_field = next(f for f in posted[0]["fields"] if f["name"] == "VWAP Bands")
    assert "100.2" in bands_field["value"] and "100.8" in bands_field["value"]
    assert "99.9" in bands_field["value"] and "101.1" in bands_field["value"]


def _recorder(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP9EMA_ALERTS_ENABLED", True)
    return posted


def test_grade_b_is_not_sent_by_default(monkeypatch):
    """User decision 2026-10-19: only grade A goes to Discord."""
    posted = _recorder(monkeypatch)
    monkeypatch.setattr(alerts, "VWAP9EMA_GRADE_A_ONLY", True)
    alerts.alert_vwap9ema_setup("USTEC", {**_good_row(), "grade": "B"})
    assert posted == []
    alerts.alert_vwap9ema_setup("USTEC", _good_row())
    assert len(posted) == 1
    assert any(f["name"] == "Grade" and "A" in f["value"] for f in posted[0]["fields"])


def test_grade_b_is_sent_when_a_only_is_switched_off(monkeypatch):
    posted = _recorder(monkeypatch)
    monkeypatch.setattr(alerts, "VWAP9EMA_GRADE_A_ONLY", False)
    alerts.alert_vwap9ema_setup("USTEC", {**_good_row(), "grade": "B"})
    assert len(posted) == 1


def test_news_blocks_the_alert(monkeypatch):
    """The 14:49 post-CPI burst: high-impact USD news within the hour."""
    posted = _recorder(monkeypatch)
    monkeypatch.setattr(alerts, "VWAP9EMA_NEWS_FILTER", True)
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda *a, **k: {"USD"})
    alerts.alert_vwap9ema_setup("EUR/USD", _good_row())
    assert posted == []


def test_alert_vwap9ema_setup_rejects_bad_rr(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP9EMA_ALERTS_ENABLED", True)
    alerts.alert_vwap9ema_setup("USTEC", _bad_rr_row())
    assert len(posted) == 0


def test_alert_vwap9ema_setup_rejects_incomplete_trade_plan(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP9EMA_ALERTS_ENABLED", True)
    alerts.alert_vwap9ema_setup("USTEC", _incomplete_row())
    assert len(posted) == 0


def test_alert_vwap9ema_setup_noop_when_disabled(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "VWAP9EMA_ALERTS_ENABLED", False)
    alerts.alert_vwap9ema_setup("USTEC", _good_row())
    assert len(posted) == 0


def test_alert_vwap9ema_setup_noop_for_no_trade(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "VWAP9EMA_ALERTS_ENABLED", True)
    alerts.alert_vwap9ema_setup("USTEC", {"symbol": "USTEC", "setup": "NO-TRADE"})
    assert len(posted) == 0


def test_alert_path_vwap_is_anchored_at_midnight_not_the_last_100_bars(monkeypatch):
    """Regression: scheduler._run_vwap9ema_alerts read only the newest 100 M5
    bars (8h20m) from the cache, so the VWAP that Discord alerts were
    evaluated against was a rolling ~8h one instead of anchored at the
    00:00 UTC day boundary -- it disagreed with the dashboard, whose
    /vwap9ema route reads vwap9ema_strategy.M5_FETCH_LIMIT bars.

    A steadily rising series makes the two anchors differ by ~2 pips, so a
    short read can't pass by coincidence. The cache stand-in keeps
    cache.read_candles' contract: the newest `limit` bars, ascending."""
    import datetime as dt
    import cache
    import scheduler

    midnight = int(dt.datetime(2026, 1, 5, 0, 0, tzinfo=dt.timezone.utc).timestamp())
    series = []
    for k in range(12 * 12 + 1):                       # 00:00 -> 12:00 UTC: 145 closed M5 bars
        p = round(1.1000 + 0.0001 * k, 6)
        series.append({"ts_utc": midnight + 300 * k, "open": p, "high": p + 0.0002,
                       "low": p - 0.0002, "close": p, "volume": 100.0})

    def fake_read_candles(symbol, interval, limit=800):
        if interval == "1h":                       # H1 only feeds the grade's trend check
            return []
        assert interval == "5min"
        return series[-limit:]

    seen = {}
    monkeypatch.setattr(cache, "read_candles", fake_read_candles)
    monkeypatch.setattr(alerts, "alert_vwap9ema_setup", lambda sym, row: seen.__setitem__(sym, row))
    # The scheduler's live-alert gate compares the newest bar to the wall
    # clock, so "now" has to sit just after this fixture's last bar (a Monday).
    monkeypatch.setattr(scheduler.time, "time", lambda: float(series[-1]["ts_utc"] + 180))

    scheduler._run_vwap9ema_alerts()

    num = sum(((c["high"] + c["low"] + c["close"]) / 3.0) * c["volume"] for c in series)
    expected = num / sum(c["volume"] for c in series)   # all 145 bars: everything since midnight
    assert seen, "alert path evaluated no pairs"
    for sym, row in seen.items():
        assert row["vwap"] == pytest.approx(expected, abs=1e-6), (sym, row["vwap"], expected)
