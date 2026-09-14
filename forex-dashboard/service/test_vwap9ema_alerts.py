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

import alerts


def _good_row(setup="BUY"):
    return {
        "symbol": "USTEC", "setup": setup,
        "entry": 100.00, "sl": 99.00, "tp1": 102.00,
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


def test_alert_vwap9ema_setup_posts_for_a_good_row(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP9EMA_ALERTS_ENABLED", True)
    alerts.alert_vwap9ema_setup("USTEC", _good_row())
    assert len(posted) == 1
    assert "VWAP+9EMA" in posted[0]["title"]
    assert "UNVALIDATED strategy" in posted[0]["description"]
    assert "failed backtest (0/48)" in posted[0]["description"]


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
