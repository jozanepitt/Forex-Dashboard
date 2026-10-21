"""The cache holds CLOSED bars only, whichever provider supplied them.

Incident 2026-10-20: with MT5 down, TwelveData returned the still-forming M15
bar; it was stored as closed, and because incremental fetches start at
last_ts + interval it was never corrected. A frozen 2-minute-old bar fired a
false USD/JPY 13/50 cross alert.
"""
from __future__ import annotations

import fetcher


def _bar(ts):
    return {"ts_utc": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 0.0}


def test_closed_only_drops_the_forming_bar():
    now = 1_000_000 * 900 + 120                    # 2 minutes into a bar
    open_bar = now - 120
    bars = [_bar(open_bar - 1800), _bar(open_bar - 900), _bar(open_bar)]
    out = fetcher._closed_only(bars, "15min", now)
    assert [b["ts_utc"] for b in out] == [open_bar - 1800, open_bar - 900]
    exactly_closed = fetcher._closed_only([_bar(now - 900)], "15min", now)
    assert len(exactly_closed) == 1                # closes exactly now: keep


def test_a_fallback_newest_bar_is_refetched_not_trusted(monkeypatch):
    """Review 2026-10-20: after the outage, the newest H4 bar was a TwelveData
    bar (volume 0) and the 2-interval skip kept it until 04:00 the next day."""
    now = 1_000_000 * 900
    newest = now - 900                                # closed 0s ago... but from the fallback
    calls = []
    monkeypatch.setattr(fetcher.cache, "max_ts", lambda s, i: newest)
    monkeypatch.setattr(fetcher.cache, "read_candles", lambda s, i, limit=800: [_bar(newest)])
    monkeypatch.setattr(fetcher, "_now", lambda: now)
    monkeypatch.setattr(fetcher, "_incremental", lambda s, i, last: calls.append(last) or False)
    fetcher.get_candles("USD/JPY", "15min", limit=5)
    assert calls == [newest]                          # volume 0 -> refetch even inside the skip window

    calls.clear()
    real = {**_bar(newest), "volume": 400.0}
    monkeypatch.setattr(fetcher.cache, "read_candles", lambda s, i, limit=800: [real])
    fetcher.get_candles("USD/JPY", "15min", limit=5)
    assert calls == []                                # a real MT5 bar keeps the cheap skip


def test_a_good_mt5_fetch_removes_fallback_bars_newer_than_mt5(monkeypatch):
    removed = []
    monkeypatch.setattr(fetcher, "MT5_ENABLED", True)
    monkeypatch.setattr(fetcher.cache, "upsert_candles", lambda s, i, b: len(b))
    monkeypatch.setattr(fetcher.cache, "log_refresh", lambda *a, **k: None)
    monkeypatch.setattr(fetcher.cache, "delete_newer_than", lambda s, i, ts: removed.append((s, i, ts)) or 1)

    class MT5:
        def fetch(self, *a, **k):
            return [_bar(9000), _bar(9900)]

    monkeypatch.setattr(fetcher.exness_mt5, "client", lambda: MT5())
    assert fetcher._try_mt5("USD/JPY", "4h", 800, None) is False
    assert removed == [("USD/JPY", "4h", 9900)]


def test_twelvedata_fallback_never_stores_the_forming_bar(monkeypatch):
    stored = {}
    now = 1_000_000 * 900 + 120
    monkeypatch.setattr(fetcher, "_try_mt5", lambda *a, **k: None)
    monkeypatch.setattr(fetcher.cache, "upsert_candles", lambda s, i, b: stored.setdefault("bars", b) and len(b))
    monkeypatch.setattr(fetcher.cache, "log_refresh", lambda *a, **k: None)
    monkeypatch.setattr(fetcher, "_now", lambda: now)

    class TD:
        def fetch(self, *a, **k):
            return [_bar(now - 120 - 900), _bar(now - 120)]

    monkeypatch.setattr(fetcher, "client", lambda: TD())
    fetcher._incremental("USD/JPY", "15min", now - 120 - 1800)
    assert [b["ts_utc"] for b in stored["bars"]] == [now - 120 - 900]
