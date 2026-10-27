"""Tests for the VWAP-bounce scanner (vwap_bounce_strategy.py).

Synthetic M15 series only. Every scenario is built from a flat previous day
followed by a trending session, so the session VWAP sits on the trend side
of the 50-bar MVWAP before the pullback under test.
"""
from __future__ import annotations

import datetime as dt

import vwap_bounce_strategy as vb

STEP = 900
# Tuesday 00:00 SAST = Monday 22:00 UTC: the dashboard's start of day (session_clock)
DAY0 = int(dt.datetime(2026, 10, 5, 22, 0, tzinfo=dt.timezone.utc).timestamp())
DAY1 = DAY0 + 86400


def _bar(ts, o, h, l, c, v=100.0):
    return {"ts_utc": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _flat_day(price=1.1000, vol=100.0):
    """96 quiet bars: yesterday."""
    return [_bar(DAY0 + i * STEP, price, price + 0.0002, price - 0.0002, price, vol) for i in range(96)]


def _trend(bars, n, step, vol=100.0):
    """Append n trending bars continuing from the last close."""
    for _ in range(n):
        o = bars[-1]["close"]
        c = o + step
        bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, max(o, c) + 0.0001, min(o, c) - 0.0001, c, vol))
    return bars


def _vwap_now(bars):
    return vb.vwap_series(bars)[-1]


def _bull_setup(trigger_vol=320.0, pullback_vol=40.0):
    """Uptrend from 00:00, a quiet pullback that tags the VWAP, then a bull
    rejection candle as the newest (just closed) bar."""
    bars = _trend(_flat_day(), 40, +0.0002)                 # 00:00 -> 10:00, 1.1000 -> 1.1080
    # Quiet pullback towards (but staying above) the VWAP.
    for _ in range(3):
        o = bars[-1]["close"]
        c = o - 0.0010
        bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, o + 0.0001, c - 0.0001, c, pullback_vol))
    # Rejection candle: wicks down to the VWAP, closes strongly back up.
    o = bars[-1]["close"]
    probe = bars + [_bar(bars[-1]["ts_utc"] + STEP, o, o, o, o, trigger_vol)]
    v = _vwap_now(probe)
    c = o + 0.0015
    bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, c + 0.0001, v - 0.00002, c, trigger_vol))
    return bars


def _mirror(bars, pivot=1.1000):
    """Flip a series around `pivot` to get the bearish twin."""
    out = []
    for b in bars:
        out.append(_bar(b["ts_utc"], 2 * pivot - b["open"], 2 * pivot - b["low"],
                        2 * pivot - b["high"], 2 * pivot - b["close"], b["volume"]))
    return out


# ── indicators ───────────────────────────────────────────────────────────────

def test_vwap_resets_at_midnight_sast():
    bars = _flat_day(1.1000) + [_bar(DAY1, 1.2000, 1.2001, 1.1999, 1.2000)]
    vw = vb.vwap_series(bars)
    assert abs(vw[95] - 1.1000) < 1e-9
    assert abs(vw[96] - 1.2000) < 1e-9          # new day: yesterday is forgotten


def test_vwap_is_volume_weighted():
    bars = [_bar(DAY1, 1.0, 1.0, 1.0, 1.0, 100.0), _bar(DAY1 + STEP, 2.0, 2.0, 2.0, 2.0, 300.0)]
    assert abs(vb.vwap_series(bars)[-1] - 1.75) < 1e-9


def test_mvwap_is_the_50_bar_average_of_the_vwap():
    bars = _trend(_flat_day(), 40, +0.0002)
    vw = vb.vwap_series(bars)
    mv = vb.sma_series(vw, vb.MVWAP_LEN)
    assert mv[48] is None
    assert abs(mv[-1] - sum(vw[-50:]) / 50) < 1e-12


def test_slope_labels():
    assert vb.slope_label(0.1) == "flat"
    assert vb.slope_label(-0.19) == "flat"
    assert vb.slope_label(0.3) == "rising"
    assert vb.slope_label(0.6) == "strong_rising"
    assert vb.slope_label(-0.3) == "falling"
    assert vb.slope_label(-0.6) == "strong_falling"


def test_rvol_and_body():
    vols = [100.0] * 20 + [300.0]
    assert abs(vb.rvol(vols, 20) - 3.0) < 1e-9
    assert vb.rvol([0.0] * 21, 20) is None            # feed without volume
    assert abs(vb.body_pct(_bar(0, 1.0, 1.10, 0.90, 1.10)) - 0.5) < 1e-9
    assert vb.body_pct(_bar(0, 1.0, 1.0, 1.0, 1.0)) == 0.0


# ── signals ──────────────────────────────────────────────────────────────────

def test_bullish_rejection_with_volume_is_grade_a_buy():
    row = vb.analyze_pair("EUR/USD", _bull_setup())
    assert row["setup"] == "BUY"
    assert row["trigger"] == "reject"
    assert row["vol_confirmed"] is True
    assert row["grade"] == "A"
    assert row["sl"] < row["entry"] < row["tp1"]
    assert row["rr1"] >= 1.0
    assert row["vwap"] > row["mvwap"]
    assert row["slope_label"] in ("rising", "strong_rising")
    assert row["validated"] is False


def test_rejection_without_volume_is_not_grade_a():
    row = vb.analyze_pair("EUR/USD", _bull_setup(trigger_vol=100.0, pullback_vol=100.0))
    assert row["setup"] == "BUY"
    assert row["vol_confirmed"] is False
    assert row["grade"] != "A"


def test_feed_without_volume_can_never_be_grade_a():
    bars = [dict(b, volume=0.0) for b in _bull_setup()]
    row = vb.analyze_pair("EUR/USD", bars)
    assert row["rvol"] is None
    assert row["vol_confirmed"] is False
    assert row["grade"] != "A"


def test_bearish_mirror_is_a_sell():
    row = vb.analyze_pair("EUR/USD", _mirror(_bull_setup()))
    assert row["setup"] == "SELL"
    assert row["trigger"] == "reject"
    assert row["grade"] == "A"
    assert row["tp1"] < row["entry"] < row["sl"]


def test_flat_mvwap_is_no_trade():
    bars = _flat_day()
    for i in range(60):                                   # second flat day, tiny chop
        p = 1.1000 + (0.0001 if i % 2 else -0.0001)
        bars.append(_bar(DAY1 + i * STEP, p, p + 0.0002, p - 0.0002, p))
    row = vb.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"
    assert row["slope_label"] == "flat"
    assert row["state"] == "NOT-ALIGNED"


def test_break_hold_mvwap_then_reclaim_is_a_buy():
    bars = _trend(_flat_day(), 40, +0.0002)
    vw = vb.vwap_series(bars)
    mv = vb.sma_series(vw, vb.MVWAP_LEN)
    mid = (vw[-1] + mv[-1]) / 2                            # between VWAP and MVWAP
    # Flush: two bars closing below the VWAP but above the MVWAP.
    for target in (mid + 0.0002, mid):
        o = bars[-1]["close"]
        bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, o + 0.0001, target - 0.0001, target, 60.0))
    assert bars[-1]["close"] < _vwap_now(bars)
    # Reclaim candle: strong close back above the VWAP on volume.
    o = bars[-1]["close"]
    c = _vwap_now(bars) + 0.0012
    bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, c + 0.0001, o - 0.0001, c, 320.0))
    row = vb.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "BUY"
    assert row["trigger"] == "reclaim"
    assert row["sl"] < row["entry"]


def test_reclaim_after_losing_the_mvwap_is_not_a_trade():
    bars = _trend(_flat_day(), 40, +0.0002)
    mv = vb.sma_series(vb.vwap_series(bars), vb.MVWAP_LEN)
    deep = mv[-1] - 0.0015                                  # closes well below the MVWAP
    o = bars[-1]["close"]
    bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, o + 0.0001, deep - 0.0001, deep, 200.0))
    o = bars[-1]["close"]
    c = _vwap_now(bars) + 0.0012
    bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, c + 0.0001, o - 0.0001, c, 320.0))
    row = vb.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"


def test_aligned_and_near_vwap_without_a_trigger_is_watch():
    bars = _bull_setup()[:-1]                               # stop before the rejection candle
    # One more quiet bar drifting down to sit just above the VWAP.
    o = bars[-1]["close"]
    c = _vwap_now(bars) + 0.0001
    bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, o + 0.0001, c - 0.00005, c, 30.0))
    row = vb.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"
    assert row["state"] == "WATCH"


def test_insufficient_data_is_no_trade():
    row = vb.analyze_pair("EUR/USD", _flat_day()[:30])
    assert row["setup"] == "NO-TRADE"
    assert row["state"] == "NO-DATA"
    assert vb.analyze_pair("EUR/USD", [])["setup"] == "NO-TRADE"


def test_analyze_universe_shape():
    res = vb.analyze_universe({"EUR/USD": {"m15": _bull_setup()}, "GBP/USD": {"m15": []}})
    assert [p["symbol"] for p in res["pairs"]] == ["EUR/USD", "GBP/USD"]
    assert res["buys"] == 1 and res["sells"] == 0
    assert res["watch"] == 0


def test_universe_is_the_full_pair_list():
    from config import PRIORITY_PAIRS
    assert vb.VWAP_BOUNCE_UNIVERSE == list(PRIORITY_PAIRS)


# ── Discord alert gate ───────────────────────────────────────────────────────

import alerts  # noqa: E402


def _alert_harness(monkeypatch, enabled=True):
    sent = []
    monkeypatch.setattr(alerts, "VWAP_BOUNCE_ALERTS_ENABLED", enabled)
    monkeypatch.setattr(alerts, "VWAP_BOUNCE_GRADE_A_ONLY", True)
    monkeypatch.setattr(alerts, "VWAP_BOUNCE_NEWS_FILTER", False)
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (sent.append(embed), True)[1])
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "_mark_sent", lambda pair, rule: None)
    return sent


def _grade_a_row():
    row = vb.analyze_pair("EUR/USD", _bull_setup())
    assert row["grade"] == "A"
    return row


def test_alerts_are_off_by_default(monkeypatch):
    import importlib
    import config
    monkeypatch.delenv("VWAP_BOUNCE_ALERTS_ENABLED", raising=False)
    monkeypatch.setattr(config, "load_dotenv", lambda *a, **k: False, raising=False)
    assert "false" in open(config.__file__, encoding="utf-8").read().split(
        'VWAP_BOUNCE_ALERTS_ENABLED = os.environ.get("VWAP_BOUNCE_ALERTS_ENABLED", "')[1][:5]


def test_no_alert_when_disabled(monkeypatch):
    sent = _alert_harness(monkeypatch, enabled=False)
    alerts.alert_vwap_bounce_setup("EUR/USD", _grade_a_row())
    assert sent == []


def test_grade_a_alerts_when_enabled_and_says_unvalidated(monkeypatch):
    sent = _alert_harness(monkeypatch)
    alerts.alert_vwap_bounce_setup("EUR/USD", _grade_a_row())
    assert len(sent) == 1
    assert "VWAP Bounce" in sent[0]["title"]
    assert "unvalidated" in sent[0]["footer"]["text"].lower()


def test_grade_b_does_not_alert_in_a_only_mode(monkeypatch):
    sent = _alert_harness(monkeypatch)
    row = vb.analyze_pair("EUR/USD", _bull_setup(trigger_vol=100.0, pullback_vol=100.0))
    assert row["setup"] == "BUY" and row["grade"] != "A"
    alerts.alert_vwap_bounce_setup("EUR/USD", row)
    assert sent == []


def test_no_trade_row_never_alerts(monkeypatch):
    sent = _alert_harness(monkeypatch)
    alerts.alert_vwap_bounce_setup("EUR/USD", vb.analyze_pair("EUR/USD", []))
    assert sent == []


# ── cached scan (what the dashboard tab calls) ───────────────────────────────
# The tab used to trigger a live per-symbol provider fetch on every load
# (29s for 27 pairs behind the MT5 lock). It now reads the candle cache the
# scheduler already keeps fresh.

def test_scan_cached_reads_the_cache_for_every_pair_and_never_fetches():
    asked = []

    def read(sym, interval, limit):
        asked.append((sym, interval, limit))
        return _bull_setup() if sym == "EUR/USD" else []

    now = _bull_setup()[-1]["ts_utc"] + STEP + 120      # two minutes after the bar closed
    res = vb.scan_cached(read, now)
    assert [a[0] for a in asked] == vb.VWAP_BOUNCE_UNIVERSE
    assert {a[1:] for a in asked} == {("15min", vb.M15_FETCH_LIMIT)}
    assert len(res["pairs"]) == len(vb.VWAP_BOUNCE_UNIVERSE)
    assert res["buys"] == 1


def test_scan_cached_flags_stale_pairs_but_not_fresh_ones():
    fresh = _bull_setup()
    now = fresh[-1]["ts_utc"] + STEP + 120               # Wednesday morning, market open
    old = [dict(b, ts_utc=b["ts_utc"] - 6 * 3600) for b in fresh]

    def read(sym, interval, limit):
        return old if sym == "GBP/USD" else fresh

    res = vb.scan_cached(read, now)
    assert res["stale_pairs"] == ["GBP/USD"]


# ── the newest cached bar is the signal bar ──────────────────────────────────
# Providers drop the still-forming candle, so the cache only ever holds closed
# bars. The first version of this scanner treated the newest bar as "forming"
# and judged the one before it: every signal was 15 minutes late and quoted
# the stale open of an already-closed bar as the entry (found 2026-10-13).

def test_signal_is_taken_from_the_newest_bar():
    bars = _bull_setup()
    row = vb.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "BUY"
    assert row["signal_ts"] == bars[-1]["ts_utc"]
    assert row["entry"] == bars[-1]["close"]
    assert row["current_price"] == bars[-1]["close"]


def test_a_signal_one_bar_old_is_no_longer_reported():
    bars = _bull_setup()
    o = bars[-1]["close"]
    bars.append(_bar(bars[-1]["ts_utc"] + STEP, o, o + 0.0002, o - 0.0001, o + 0.0001, 60.0))
    row = vb.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"


def test_scan_cached_does_not_call_data_stale_while_the_next_refresh_is_due():
    """Just after a quarter-hour the newest cached bar closed 15+ minutes ago
    and the scheduler has not fetched the next one yet. That is normal."""
    fresh = _bull_setup()
    now = fresh[-1]["ts_utc"] + 2 * STEP + 180            # 3 minutes into the following bar
    res = vb.scan_cached(lambda sym, interval, limit: fresh, now)
    assert res["stale_pairs"] == []
