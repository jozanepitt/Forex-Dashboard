"""Tests for the Sweep scanner (sweep_strategy.py) and its Discord alert.

Synthetic M15 data only. The core fixture: a quiet range, one swing high,
a run up toward it, a candle that takes the swing high on 3x volume (the
dot), then a candle that closes back below it (the entry).
"""
from __future__ import annotations

import datetime as dt

import pytest

import alerts
import sweep_strategy as s

T0 = int(dt.datetime(2026, 1, 5, 0, 0, tzinfo=dt.timezone.utc).timestamp())   # Monday


def _bar(i, o, h, l, c, v=100.0):
    return {"ts_utc": T0 + 900 * i, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _sell_sweep(confirm_close=1.0993, extra=()):
    bars = []
    for i in range(61):
        if i == 40:
            bars.append(_bar(i, 1.0950, 1.1000, 1.0945, 1.0951))          # the swing high
        else:
            c = 1.0951 if i % 2 else 1.0949
            bars.append(_bar(i, 1.0950, 1.0955, 1.0945, c))
    p = 1.0956
    for i in range(61, 70):                                                 # run up toward it
        bars.append(_bar(i, p, p + 0.0004, p - 0.0002, p + 0.0003))
        p += 0.0004
    bars.append(_bar(70, 1.0992, 1.1004, 1.0990, 1.0999, v=300.0))           # dot: takes 1.1000 on 3x volume
    if confirm_close is not None:
        bars.append(_bar(71, 1.0999, 1.1001, min(confirm_close, 1.0990), confirm_close))
    bars.extend(extra)
    return bars


def _mirror(bars):
    """Turn a sell sweep into the matching buy sweep (prices reflected)."""
    out = []
    for b in bars:
        out.append({**b, "open": 2.2 - b["open"], "close": 2.2 - b["close"],
                    "high": 2.2 - b["low"], "low": 2.2 - b["high"]})
    return out


DOWN_H1 = [{"ts_utc": T0 - 3600 * (250 - k), "open": 1.2, "high": 1.2, "low": 1.2,
            "close": 1.2 - 0.0005 * k, "volume": 1.0} for k in range(250)]
UP_H1 = [{**b, "close": 2.2 - b["close"]} for b in DOWN_H1]


# ── detection ────────────────────────────────────────────────────────────────

def test_dot_needs_a_swing_level_taken_on_a_volume_surge():
    bars = _sell_sweep()
    dots = s.find_dots(bars)
    assert [(d["i"], d["side"], d["level"]) for d in dots] == [(70, "high", 1.1000)]
    assert dots[0]["vol_ratio"] == pytest.approx(3.0, rel=0.05)
    quiet = _sell_sweep()
    quiet[70]["volume"] = 120.0
    assert s.find_dots(quiet) == []                      # level taken, but no volume surge


def test_flat_highs_are_not_swing_points():
    bars = [_bar(i, 1.0, 1.001, 0.999, 1.0) for i in range(80)]
    bars.append(_bar(80, 1.0, 1.002, 0.999, 1.0, v=500.0))
    assert s.find_dots(bars) == []


def test_session_vwap_resets_at_17_00_new_york():
    # 2026-01-05 is winter: 17:00 New York = 22:00 UTC
    a = s._anchor_start(T0 + 23 * 3600, s.SESSION_HOUR_NY)
    assert dt.datetime.fromtimestamp(a, dt.timezone.utc).hour == 22
    summer = int(dt.datetime(2026, 7, 6, 23, 0, tzinfo=dt.timezone.utc).timestamp())
    assert dt.datetime.fromtimestamp(s._anchor_start(summer, 17), dt.timezone.utc).hour == 21


# ── the setup ────────────────────────────────────────────────────────────────

def test_close_back_inside_is_a_sell_signal_with_vwap_target():
    row = s.analyze_pair("EUR/USD", _sell_sweep(), DOWN_H1)
    assert row["state"] == "SIGNAL" and row["setup"] == "SELL", row["notes"]
    assert row["entry"] == 1.0993
    assert row["sl"] > 1.1004                                # beyond the sweep wick
    assert row["tp1_kind"] == "VWAP" and row["tp1"] < row["entry"]
    assert row["at_band"] is True and row["with_trend"] is True
    assert row["rr1"] >= s.RR_A
    assert row["grade"] == "A"


def test_buy_is_the_mirror_image():
    row = s.analyze_pair("EUR/USD", _mirror(_sell_sweep()), UP_H1)
    assert row["state"] == "SIGNAL" and row["setup"] == "BUY", row["notes"]
    assert row["sl"] < row["entry"] < row["tp1"]
    assert row["grade"] == "A"


def test_dot_on_the_newest_candle_waits_for_the_close():
    row = s.analyze_pair("EUR/USD", _sell_sweep(confirm_close=None), DOWN_H1)
    assert row["state"] == "WAIT-CLOSE" and row["setup"] == "NO-TRADE"


def test_closing_beyond_the_level_is_a_breakout_not_a_trade():
    hold = [_bar(72, 1.1003, 1.1010, 1.1002, 1.1008)]
    row = s.analyze_pair("EUR/USD", _sell_sweep(confirm_close=1.1005, extra=hold), DOWN_H1)
    assert row["state"] == "BREAKOUT" and row["setup"] == "NO-TRADE"


def test_against_the_h1_trend_drops_to_b():
    row = s.analyze_pair("EUR/USD", _sell_sweep(), UP_H1)
    assert row["with_trend"] is False and row["grade"] == "B"


def test_forex_without_the_band_is_not_b(monkeypatch):
    """FX volume is tick volume, so a forex sweep inside the bands is C; the
    same sweep on a real-volume market (index here) is B."""
    real = s.session_vwap
    monkeypatch.setattr(s, "session_vwap",
                        lambda bars: (real(bars)[0], [x * 50 for x in real(bars)[1]]))   # bands far away
    fx = s.analyze_pair("EUR/USD", _sell_sweep(), DOWN_H1)
    assert fx["tick_volume"] is True and fx["at_band"] is False and fx["grade"] == "C"
    monkeypatch.setattr(s.instruments, "asset_class", lambda sym: "index")
    idx = s.analyze_pair("EUR/USD", _sell_sweep(), DOWN_H1)
    assert idx["tick_volume"] is False and idx["at_band"] is False and idx["grade"] == "B"


def test_setup_expires_after_a_candle():
    later = [_bar(72, 1.0993, 1.0995, 1.0985, 1.0988), _bar(73, 1.0988, 1.0990, 1.0980, 1.0984)]
    row = s.analyze_pair("EUR/USD", _sell_sweep(extra=later), DOWN_H1)
    assert row["state"] in ("EXPIRED", "NO-SWEEP")


def test_universe_includes_oil_and_eurcad():
    assert {"USOIL", "UKOIL", "EUR/CAD"} <= set(s.SWEEP_UNIVERSE)
    assert len(s.SWEEP_UNIVERSE) == len(set(s.SWEEP_UNIVERSE))


# ── Discord ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sent(monkeypatch):
    out = []
    monkeypatch.setattr(alerts, "_post_discord", lambda e: (out.append(e) or True))
    monkeypatch.setattr(alerts, "_is_throttled", lambda p, r: False)
    monkeypatch.setattr(alerts, "_mark_sent", lambda p, r: None)
    monkeypatch.setattr(alerts, "SWEEP_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "SWEEP_GRADE_A_ONLY", False)
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda *a, **k: set())
    alerts._sweep_last_alerted.clear()
    return out


def _row(grade="A", **kw):
    r = s.analyze_pair("EUR/USD", _sell_sweep(), DOWN_H1)
    r.update(grade=grade, **kw)
    return r


def test_grades_a_and_b_are_sent_c_is_not(sent):
    alerts.alert_sweep_setup("EUR/USD", _row("A"))
    alerts._sweep_last_alerted.clear()
    alerts.alert_sweep_setup("EUR/USD", _row("B"))
    alerts._sweep_last_alerted.clear()
    alerts.alert_sweep_setup("EUR/USD", _row("C"))
    assert len(sent) == 2
    assert "Sweep" in sent[0]["title"] and "unvalidated" in sent[0]["footer"]["text"].lower()


def test_a_only_switch_drops_b(sent, monkeypatch):
    monkeypatch.setattr(alerts, "SWEEP_GRADE_A_ONLY", True)
    alerts.alert_sweep_setup("EUR/USD", _row("B"))
    assert sent == []


def test_one_alert_per_sweep(sent):
    alerts.alert_sweep_setup("EUR/USD", _row("A"))
    alerts.alert_sweep_setup("EUR/USD", _row("A"))
    assert len(sent) == 1


def test_waiting_and_news_are_not_sent(sent, monkeypatch):
    alerts.alert_sweep_setup("EUR/USD", _row("A", state="WAIT-CLOSE"))
    monkeypatch.setattr(alerts.forexfactory, "currencies_in_window", lambda *a, **k: {"EUR"})
    alerts.alert_sweep_setup("EUR/USD", _row("A"))
    assert sent == []


def test_config_defaults_send_a_and_b():
    import inspect
    import config
    src = inspect.getsource(config)
    assert 'SWEEP_ALERTS_ENABLED", "true"' in src
    assert 'SWEEP_GRADE_A_ONLY", "false"' in src
    assert 'VWAP9EMA_GRADE_A_ONLY", "true"' in src
