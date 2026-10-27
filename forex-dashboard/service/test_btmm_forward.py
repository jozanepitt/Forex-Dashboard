"""BTMM Shark Fin forward-test journal. Synthetic data, temp DB."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

import btmm_core
import btmm_forward as fw
import cache

T0 = int(datetime(2026, 10, 28, 8, tzinfo=timezone.utc).timestamp())


def _bars(n, price=1.1000, t0=T0 - 900 * 899):
    return [{"ts_utc": t0 + i * 900, "open": price, "high": price + 0.0002,
             "low": price - 0.0002, "close": price} for i in range(n)]


def _analysis(signal="A+ Sell", ready=True, kz="London"):
    return {"signal": signal, "setup_bullish": False, "kz": kz, "score": -55,
            "shark_fin": {"entry_ready": ready, "direction": "bearish", "spike_ts": T0 - 3600},
            "trade_plan": {"entry": 1.1000, "sl": 1.1020, "tp1": 1.0980, "break_even_at": 1.0980,
                           "risk": 0.0020}}


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "DB_PATH", tmp_path / "fw.db")
    fw._init()


def test_grade_of():
    assert fw.grade_of(_analysis()) == "A+"
    assert fw.grade_of(_analysis(signal="Strong Sell")) == "A"
    assert fw.grade_of(_analysis(ready=False)) is None
    assert fw.grade_of(_analysis(kz=None)) is None


def test_records_each_spike_once_and_only_majors(db, monkeypatch):
    monkeypatch.setattr(btmm_core, "analyze", lambda bars, symbol=None: _analysis())
    bars = _bars(900)
    assert fw.record("EUR/USD", bars) is True
    assert fw.record("EUR/USD", bars) is False                 # same spike: not logged twice
    assert fw.record("EUR/GBP", bars) is False                 # not a USD major
    s = fw.stats()
    assert s["open"] == 1 and s["closed"] == 0


def test_ignores_bars_before_the_registration_date(db, monkeypatch):
    monkeypatch.setattr(btmm_core, "analyze", lambda bars, symbol=None: _analysis())
    old = _bars(900, t0=int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp()))
    assert fw.record("EUR/USD", old) is False


def test_resolves_tp1_net_of_spread(db, monkeypatch):
    monkeypatch.setattr(btmm_core, "analyze", lambda bars, symbol=None: _analysis())
    bars = _bars(900)
    fw.record("EUR/USD", bars)
    after = [{"ts_utc": bars[-1]["ts_utc"] + 900, "open": 1.0995, "high": 1.0998, "low": 1.0978, "close": 1.0980}]
    assert fw.resolve("EUR/USD", bars + after) == 1
    t = fw.stats()["trades"][0]
    assert t["outcome"] == "tp1"
    assert t["r_gross"] == pytest.approx(1.0)
    assert t["r_net"] == pytest.approx(1.0 - fw.EXNESS_STANDARD_SPREAD["EUR/USD"] / 0.0020)


def test_open_trade_stays_open_until_resolved(db, monkeypatch):
    monkeypatch.setattr(btmm_core, "analyze", lambda bars, symbol=None: _analysis())
    bars = _bars(900)
    fw.record("EUR/USD", bars)
    flat = _bars(5, t0=bars[-1]["ts_utc"] + 900)
    assert fw.resolve("EUR/USD", bars + flat) == 0


def test_a_plus_only_on_usd_majors():
    fin = {"entry_ready": True, "direction": "bearish"}
    assert btmm_core._is_aplus(fin, False, "London", -50, "EUR/USD")
    assert not btmm_core._is_aplus(fin, False, "London", -50, "EUR/GBP")
