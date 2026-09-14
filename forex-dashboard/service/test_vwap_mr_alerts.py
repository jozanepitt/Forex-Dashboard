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


# ──────────────────────────────────────────────────────────────────────
# _should_alert_vwap_mr grade gating (2026-09-14): grade is now
# A/B+/B/C/NO-TRADE. The A-only gate must block B+ exactly like B, not
# just literal "B" — a regression here would let B+ setups leak past
# VWAP_MR_GRADE_A_ONLY=True.
# ──────────────────────────────────────────────────────────────────────

def test_should_alert_vwap_mr_blocks_b_plus_when_a_only(monkeypatch):
    monkeypatch.setattr(alerts, "VWAP_MR_GRADE_A_ONLY", True)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 0)
    assert alerts._should_alert_vwap_mr(_good_row(grade="B+", score=10)) is False


def test_should_alert_vwap_mr_blocks_b_when_a_only(monkeypatch):
    monkeypatch.setattr(alerts, "VWAP_MR_GRADE_A_ONLY", True)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 0)
    assert alerts._should_alert_vwap_mr(_good_row(grade="B", score=10)) is False


def test_should_alert_vwap_mr_allows_a_when_a_only(monkeypatch):
    monkeypatch.setattr(alerts, "VWAP_MR_GRADE_A_ONLY", True)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 0)
    assert alerts._should_alert_vwap_mr(_good_row(grade="A", score=10)) is True


def test_should_alert_vwap_mr_allows_b_plus_when_a_only_disabled(monkeypatch):
    monkeypatch.setattr(alerts, "VWAP_MR_GRADE_A_ONLY", False)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 0)
    assert alerts._should_alert_vwap_mr(_good_row(grade="B+", score=10)) is True


def test_should_alert_vwap_mr_always_blocks_grade_c(monkeypatch):
    monkeypatch.setattr(alerts, "VWAP_MR_GRADE_A_ONLY", False)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 0)
    assert alerts._should_alert_vwap_mr(_good_row(grade="C", score=10)) is False


def _partial_row():
    """A row shape analyze_pair can actually produce: regime passed and an
    extension was found, but confirmation timed out before a stall pattern
    appeared (vwap_mean_reversion_strategy.analyze_pair returns "NO-TRADE"
    with extension set and confirmation/score/trade-plan left at their base
    values in this exact path -- see analyze_pair's `if not confirmation`
    branch). This is the genuinely 'still forming' case the watch alert
    should be able to report on. (Corrected 2026-09-11: the previous version
    paired regime_ok=False with a populated extension, a combination
    analyze_pair can never produce since it returns before computing the
    extension when the regime gate fails.)"""
    row = _good_row()
    row["setup"] = "NO-TRADE"
    row["grade"] = "NO-TRADE"
    row["score"] = 0
    row["regime_ok"] = True
    row["confirmation"] = None
    row["exhaustion_volume"] = False
    row["fading_volume"] = False
    row["entry"] = row["sl"] = row["tp1"] = row["tp2"] = None
    row["notes"] = "Extension at bar 19 timed out with no stall confirmation within 6 bars."
    return row


def test_vwap_mr_watch_posts_when_something_is_missing(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP_MR_WATCH_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "VWAP_MR_ALERTS_ENABLED", True)
    alerts.alert_vwap_mr_watch("EUR/USD", _partial_row())
    assert len(posted) == 1
    assert "👀" in posted[0]["title"]


def test_vwap_mr_watch_skips_when_real_alert_covers_it(monkeypatch):
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP_MR_WATCH_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "VWAP_MR_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 9)
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda mins, high_only=True: set())
    alerts.alert_vwap_mr_watch("EUR/USD", _good_row(score=10))
    assert len(posted) == 0


def test_vwap_mr_watch_still_posts_when_real_alert_disabled(monkeypatch):
    """Critical-fix behavior from the 2026-09-10 TDI123/BTMM123 review:
    an all-pass row must still notify if the real alert's own switch is off."""
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (posted.append(embed) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "VWAP_MR_WATCH_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "VWAP_MR_ALERTS_ENABLED", False)
    monkeypatch.setattr(alerts, "VWAP_MR_MIN_SCORE", 9)
    alerts.alert_vwap_mr_watch("EUR/USD", _good_row(score=10))
    assert len(posted) == 1
