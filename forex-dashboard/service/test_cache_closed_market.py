"""cache.read_candles must not hand closed-market bars to the strategies."""
from __future__ import annotations

import datetime as dt

import cache


def ts(y, m, d, hh, mm=0):
    return int(dt.datetime(y, m, d, hh, mm, tzinfo=dt.timezone.utc).timestamp())


def _bar(t, vol):
    return {"ts_utc": t, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": vol}


def _db(monkeypatch, tmp_path):
    monkeypatch.setattr(cache, "DB_PATH", tmp_path / "candles.db")
    cache.init_db()


def test_weekend_quotes_are_not_returned(monkeypatch, tmp_path):
    _db(monkeypatch, tmp_path)
    fri = [_bar(ts(2026, 10, 9, 20, 0) + i * 900, 50.0) for i in range(4)]          # 20:00-20:45
    weekend = [_bar(ts(2026, 10, 9, 21, 0) + i * 900, 0) for i in range(192)]        # Fri 21:00 - Sun 21:00
    sun = [_bar(ts(2026, 10, 11, 21, 0) + i * 900, 40.0) for i in range(4)]
    cache.upsert_candles("EUR/USD", "15min", fri + weekend + sun)
    got = cache.read_candles("EUR/USD", "15min", limit=100)
    assert [b["ts_utc"] for b in got] == [b["ts_utc"] for b in fri + sun]


def test_limit_is_still_honoured_after_filtering(monkeypatch, tmp_path):
    _db(monkeypatch, tmp_path)
    week = [_bar(ts(2026, 10, 5, 0, 0) + i * 900, 50.0) for i in range(400)]         # Mon-Fri morning
    weekend = [_bar(ts(2026, 10, 9, 21, 0) + i * 900, 0) for i in range(192)]
    sun = [_bar(ts(2026, 10, 11, 21, 0) + i * 900, 40.0) for i in range(10)]
    cache.upsert_candles("EUR/USD", "15min", week + weekend + sun)
    got = cache.read_candles("EUR/USD", "15min", limit=300)
    assert len(got) == 300
    assert got[-1]["ts_utc"] == sun[-1]["ts_utc"]
    assert all(b["volume"] > 0 for b in got)


def test_crypto_and_daily_bars_are_untouched(monkeypatch, tmp_path):
    _db(monkeypatch, tmp_path)
    sat = [_bar(ts(2026, 10, 10, 0, 0) + i * 900, 0) for i in range(8)]
    cache.upsert_candles("BTC/USD", "15min", sat)
    cache.upsert_candles("EUR/USD", "1day", [_bar(ts(2026, 10, 10, 0, 0), 0)])
    assert len(cache.read_candles("BTC/USD", "15min", limit=50)) == 8
    assert len(cache.read_candles("EUR/USD", "1day", limit=50)) == 1
