"""Tests for the VWAP Mean Reversion Discord alert functions.

Monkeypatches alerts._post_discord to a recorder so no real webhook call is
made. Mirrors the verification approach used for the TDI123/BTMM123 watch
alerts (2026-09-10 session)."""
from __future__ import annotations

import alerts


def _good_row(setup="BUY", grade="A", score=10):
    return {
        "symbol": "EUR/USD", "setup": setup, "grade": grade, "score": score,
        "entry": 1.1000, "sl": 1.0980, "tp1": 1.1020, "tp2": 1.1030,
        "sl_pips": 20.0, "tp1_pips": 20.0, "tp2_pips": 30.0,
        "vwap": 1.1020, "sigma": 0.0005, "z": -2.4, "d": -1.8, "er": 0.20,
        "regime_ok": True, "session_status": "ACTIVE",
        "extension": {"idx": 19, "direction": "long", "z": -2.4},
        "confirmation": {"idx": 20, "type": "rejection_wick", "bars_since_extension": 1},
        "exhaustion_volume": True, "fading_volume": True, "fresh": True,
        "notes": "test row",
    }


def _bad_rr_row():
    row = _good_row()
    row["sl"] = 1.0999   # 1 pip stop vs a 20-pip TP -- absurd R:R, must be rejected
    return row


def test_alert_vwap_mr_setup_posts_for_a_good_row(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP_MR_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 9)
    alerts.alert_vwap_mr_setup("EUR/USD", _good_row())
    assert len(posted) == 1
    assert "VWAP" in posted[0]["title"]


def test_alert_vwap_mr_setup_rejects_bad_rr(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP_MR_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 9)
    alerts.alert_vwap_mr_setup("EUR/USD", _bad_rr_row())
    assert len(posted) == 0


def test_alert_vwap_mr_setup_respects_min_score(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP_MR_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 9)
    alerts.alert_vwap_mr_setup("EUR/USD", _good_row(score=7))
    assert len(posted) == 0


def test_alert_vwap_mr_setup_noop_when_disabled(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "VWAP_MR_ALERTS_ENABLED", False)
    alerts.alert_vwap_mr_setup("EUR/USD", _good_row())
    assert len(posted) == 0


def test_alert_vwap_mr_setup_suppressed_by_news(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP_MR_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 9)
    monkeypatch.setattr(alerts, "VWAP_MR_NEWS_FILTER", True)
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda mins, high_only=True: {"USD"})
    alerts.alert_vwap_mr_setup("EUR/USD", _good_row())
    assert len(posted) == 0
