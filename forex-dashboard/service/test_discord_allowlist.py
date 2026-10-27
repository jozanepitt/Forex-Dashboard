"""User decision 2026-10-21: only the starred TDI Cycle 123 [M15] grade-A
signals go to Discord; every other strategy stays on the dashboard."""
import inspect

import pytest

import alerts
import config


@pytest.fixture
def production(monkeypatch):
    monkeypatch.setattr(alerts, "DISCORD_SIGNALS", "TDI123:M15:A")
    sent = []
    monkeypatch.setattr(alerts, "_post_discord", lambda e: sent.append(e) or True)
    monkeypatch.setattr(alerts, "_is_throttled", lambda p, r: False)
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda *a, **k: set())
    return sent


def test_default_is_tdi123_m15_grade_a_only():
    assert 'DISCORD_SIGNALS", "TDI123:M15:A"' in inspect.getsource(config)


def test_allowlist_matching(production):
    assert alerts._signal_allowed("TDI123", "M15", "A")
    assert not alerts._signal_allowed("TDI123", "M15", "B")
    assert not alerts._signal_allowed("TDI123", "H1", "A")
    assert not alerts._signal_allowed("TDI123_WATCH", "M15", "A")
    for s in ("BTMM", "CRT5AM", "ID50", "VWAP_MR", "VWAP9EMA", "EMA1350", "VWAP_BOUNCE", "ENGULF", "SWEEP"):
        assert not alerts._signal_allowed(s, "M15", "A"), s


def test_wildcards_and_lists(monkeypatch):
    monkeypatch.setattr(alerts, "DISCORD_SIGNALS", "TDI123:*:A, SWEEP")
    assert alerts._signal_allowed("TDI123", "H1", "A")
    assert alerts._signal_allowed("SWEEP", "M15", "B")
    assert not alerts._signal_allowed("TDI123", "H1", "B")


def _a_row(**kw):
    return {"setup": "SELL", "grade": "A", "timeframe": "M15", "entry": 1.10, "sl": 1.11, "tp1": 1.08,
            "trade_plan": {"entry": 1.10, "sl": 1.11, "tp1": 1.08, "tp2": 1.07}, "state": "SIGNAL",
            "notes": "test", "model": "x", **kw}


def test_other_strategies_post_nothing(production, monkeypatch):
    for flag in ("VWAP9EMA_ALERTS_ENABLED", "EMA1350_ALERTS_ENABLED", "SWEEP_ALERTS_ENABLED",
                 "ENGULF_ALERTS_ENABLED", "VWAP_BOUNCE_ALERTS_ENABLED", "BTMM_ID50_ALERTS_ENABLED"):
        monkeypatch.setattr(alerts, flag, True, raising=False)
    alerts.alert_vwap9ema_setup("EUR/USD", _a_row())
    alerts.alert_ema1350_setup("EUR/USD", _a_row())
    alerts.alert_sweep_setup("EUR/USD", _a_row())
    alerts.alert_engulfing_setup("EUR/USD", _a_row(timeframe="H1"))
    alerts.alert_vwap_bounce_setup("EUR/USD", _a_row())
    alerts.alert_id50_setup("EUR/USD", _a_row())
    alerts.alert_kill_zone_open("London")
    assert production == []


def test_the_stall_health_warning_still_goes_out(production):
    alerts.alert_scheduler_stall(45)
    assert len(production) == 1
