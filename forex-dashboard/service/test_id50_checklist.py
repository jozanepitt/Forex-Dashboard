"""ID50 checklist model (btmm_id50._analyze_checklist), rebuilt 2026-10-21 to
the user's checklist: 13/50 cross -> 20-50 pip move away from the 50 EMA ->
pullback to the 50 -> entry candle (RR/star/COW/engulfing/shift) closing back
beyond the 13 and the 50. Stop = bounce candle high/low + 5 pips.
Synthetic data only."""
from __future__ import annotations

import pytest

import alerts
import btmm_id50 as I

T0 = 1_791_000_000 - 1_791_000_000 % 86400


def _bar(i, o, c, hi=None, lo=None):
    return {"ts_utc": T0 + 900 * i, "open": o, "close": c,
            "high": hi if hi is not None else max(o, c) + 0.0002,
            "low": lo if lo is not None else min(o, c) - 0.0002, "volume": 100}


def _sell_setup(drop=0.0035, entry=True, pull_bars=8):
    """Slow rise, a 10-candle drop (13 crosses below 50, ~26 pip move away),
    an 8-candle pullback to the 50 EMA, then a bearish engulfing."""
    bars, p = [], 1.1000
    for i in range(300):
        o, p = p, p + (0.00005 if i % 3 else -0.00003)
        bars.append(_bar(i, o, p))
    for _ in range(10):
        o, p = p, p - drop / 10
        bars.append(_bar(len(bars), o, p))
    for _ in range(pull_bars):
        o, p = p, p + 0.00025
        bars.append(_bar(len(bars), o, p))
    if entry:
        bars.append(_bar(len(bars), p, p - 0.0007))     # engulfs the last pullback candle
    return bars


def _mirror(bars, pivot=1.1):
    return [{**b, "open": 2 * pivot - b["open"], "close": 2 * pivot - b["close"],
             "high": 2 * pivot - b["low"], "low": 2 * pivot - b["high"]} for b in bars]


def test_textbook_sell_gives_entry_stop_and_targets():
    bars = _sell_setup()
    row = I.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "SELL", row.get("reason")
    assert row["model"] == "checklist" and row["timeframe"] == "M15"
    assert row["grade"] in ("A", "B")
    assert row["entry_candle"]["reason"] == "Bearish Engulfing"
    plan = row["trade_plan"]
    bounce_high = max(b["high"] for b in bars[-9:])        # pullback candles + entry candle
    assert plan["entry"] == bars[-1]["close"]
    assert plan["sl"] == pytest.approx(bounce_high + 0.0005)   # + 5 pips
    risk = plan["sl"] - plan["entry"]
    assert plan["tp2"] == pytest.approx(plan["entry"] - 2 * risk)
    assert plan["tp1"] is not None and plan["tp1"] < plan["entry"]   # the move-away swing low
    assert 20 <= row["move_away"]["pips"] <= 50


def test_buy_is_the_mirror_image():
    row = I.analyze_pair("EUR/USD", _mirror(_sell_setup()))
    assert row["setup"] == "BUY", row.get("reason")
    assert row["trade_plan"]["sl"] < row["trade_plan"]["entry"] < row["trade_plan"]["tp2"]


def test_grade_a_needs_all_three_extras():
    row = I.analyze_pair("EUR/USD", _sell_setup())
    assert set(row["checks"]) == {"move 20-50 pips", "trap", "RSI midline"}
    assert row["grade"] == ("A" if all(row["checks"].values()) else "B")
    assert row["score"] == sum(row["checks"].values()) and row["score_max"] == 3


def test_move_away_under_the_minimum_is_no_trade(monkeypatch):
    """The textbook setup moves ~26 pips; with a 30-pip minimum it must fail
    on exactly that rule (CAD/JPY 2026-10-21 moved 12 pips: no ID50)."""
    monkeypatch.setattr(I, "CL_MOVE_MIN_PIPS", 30.0)
    row = I.analyze_pair("EUR/USD", _sell_setup())
    assert row["setup"] == "NO-TRADE" and "move away from the 50 too small" in row["reason"]


def test_a_move_beyond_50_pips_is_valid_but_not_a(monkeypatch):
    monkeypatch.setattr(I, "CL_MOVE_MAX_PIPS", 22.0)
    row = I.analyze_pair("EUR/USD", _sell_setup())
    assert row["setup"] == "SELL" and row["checks"]["move 20-50 pips"] is False and row["grade"] == "B"


def test_waiting_until_the_entry_candle():
    row = I.analyze_pair("EUR/USD", _sell_setup(entry=False))
    assert row["setup"] == "NO-TRADE" and "waiting" in row["reason"]


def test_only_the_first_entry_on_a_pullback_fires():
    bars = _sell_setup()
    p = bars[-1]["close"]
    bars.append(_bar(len(bars), p, p - 0.0003))
    row = I.analyze_pair("EUR/USD", bars)
    assert row["setup"] == "NO-TRADE"


def test_non_forex_scales_by_daily_range(monkeypatch):
    """Gold / NAS100: the same push, in their bigger units."""
    monkeypatch.setattr(I, "_adr", lambda bars, bpd, days=14: 60.0)   # $60 gold day
    s = I._cl_scale("XAU/USD", 2400.0, [], 96)
    assert s["move_min"] == pytest.approx(0.357 * 60) and s["move_max"] == pytest.approx(0.892 * 60)
    assert s["buffer"] == pytest.approx(0.089 * 60)
    fx = I._cl_scale("EUR/USD", 1.1, [], 96)
    assert fx["move_min"] == pytest.approx(0.0020) and fx["buffer"] == pytest.approx(0.0005)


def test_h1_runs_the_same_rules():
    row = I.analyze_pair_h1("EUR/USD", _sell_setup())
    assert row["setup"] == "SELL" and row["timeframe"] == "H1"


def test_universe_adds_h1_rows_only_when_they_signal():
    res = I.analyze_universe({"EUR/USD": {"m15": _sell_setup(), "1h": _sell_setup()}})
    tfs = [(p["symbol"], p["timeframe"]) for p in res["pairs"] if p["symbol"] == "EUR/USD"]
    assert ("EUR/USD", "M15") in tfs and ("EUR/USD", "H1") in tfs
    res = I.analyze_universe({"EUR/USD": {"m15": _sell_setup(), "1h": _sell_setup(entry=False)}})
    assert [p["timeframe"] for p in res["pairs"] if p["symbol"] == "EUR/USD"] == ["M15"]


def test_checklist_rows_stay_off_discord_by_default(monkeypatch):
    sent = []
    monkeypatch.setattr(alerts, "_post_discord", lambda e: sent.append(e) or True)
    monkeypatch.setattr(alerts, "_is_throttled", lambda p, r: False)
    monkeypatch.setattr(alerts, "BTMM_ID50_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "BTMM_ID50_CHECKLIST_DISCORD", False)
    row = I.analyze_pair("EUR/USD", _sell_setup())
    row["grade"] = "A"
    alerts.alert_id50_setup("EUR/USD", row)
    assert sent == []
