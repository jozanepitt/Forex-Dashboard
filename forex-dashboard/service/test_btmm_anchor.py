"""Synthetic 12, 21 and 22 anchors, plus the two negatives and a Discord dry run.

The series is built, not taken from a market. Four flat days set a small ADR.
Yesterday spikes 60 pips, as an M (two legs) or an A (one leg). Today comes
back to that high and either prints another M or a single half-bat, then
closes back through the 13 EMA with TDI.
"""
from __future__ import annotations

import datetime as dt
import json

import btmm_anchor_strategy as a

UTC = dt.timezone.utc
BAR = 900
PIP = 0.0001
# 12 Jan 2026 00:00 SAST.
D0 = int(dt.datetime(2026, 1, 11, 22, 0, tzinfo=UTC).timestamp())


def _day(start, closes):
    return [{"ts_utc": start + i * BAR, "open": c, "high": c + 0.2 * PIP,
             "low": c - 0.2 * PIP, "close": c, "volume": 100} for i, c in enumerate(closes)]


def _book(yesterday_two_legs: bool, today_two_legs: bool, quiet: int = 22):
    """Flat base, a late spike yesterday, today's test of that high, then the give-back."""
    bars = []
    for d in range(4):
        bars.extend(_day(D0 + d * 86400, [1.10] * 96))
    seq = [1.10] * 70
    for i in range(5):
        seq.append(1.10 + (i + 1) * 60 * PIP / 5)
    if yesterday_two_legs:
        top = seq[-1]
        for i in range(5):
            seq.append(top - (i + 1) * 6 * PIP)
        bot = seq[-1]
        for i in range(5):
            seq.append(bot + (i + 1) * (top - bot) / 5)
    while len(seq) < 96:
        seq.append(seq[-1] - 0.05 * PIP)
    bars.extend(_day(D0 + 4 * 86400, seq))
    tracer = max(b["high"] for b in bars[-96:])
    today = [1.101] * quiet
    for i in range(8):
        today.append(1.101 + (i + 1) * (tracer - 1.101) / 8)
    if today_two_legs:
        top = today[-1]
        for i in range(4):
            today.append(top - (i + 1) * 5 * PIP)
        bot = today[-1]
        for i in range(5):
            today.append(bot + (i + 1) * (tracer - PIP - bot) / 5)
    for _ in range(6):
        today.append(today[-1] - 4 * PIP)
    bars.extend(_day(D0 + 5 * 86400, today))
    return bars


def _full(yesterday_two_legs, today_two_legs, quiet=22):
    """Cut the series at the trigger so later bars cannot invalidate it."""
    bars = _book(yesterday_two_legs, today_two_legs, quiet)
    row = next(r for r in a.find_setups("EUR/USD", bars)
               if r["grade"] == "FULL" and r["trigger_ts"] >= D0 + 5 * 86400)
    cut = [b for b in bars if b["ts_utc"] <= row["trigger_ts"]]
    return [r for r in a.find_setups("EUR/USD", cut)
            if r["label"] == row["label"] and r["trigger_ts"] >= D0 + 5 * 86400][-1]


def test_22_is_a_full_sell():
    row = _full(True, True)
    assert row["label"] == "22" and row["setup"] == "SELL" and row["grade"] == "FULL"
    assert row["fresh"] is True
    assert row["sl"] == row["extreme"] + 5 * PIP
    assert row["invalidation"] == row["extreme"]
    assert "SAST" in row["time_sast"] and row["time_sast"].endswith("SAST")
    # The reported time is the trigger candle's close, 15 minutes after it opened.
    assert row["time_utc"].endswith("UTC")


def test_21_is_a_full_sell():
    row = _full(True, False, quiet=30)
    assert row["label"] == "21" and row["setup"] == "SELL" and row["grade"] == "FULL"


def test_12_is_a_full_sell():
    row = _full(False, True)
    assert row["label"] == "12" and row["setup"] == "SELL" and row["grade"] == "FULL"


def test_no_setup_when_the_move_is_under_three_times_adr():
    """Wide daily ranges and no net rise: the 3x ADR peak test fails."""
    bars = []
    for d in range(6):
        closes = [1.10 + (0.01 if i % 2 == 0 else -0.01) for i in range(96)]
        bars.extend(_day(D0 + d * 86400, closes))
    assert [r for r in a.find_setups("EUR/USD", bars) if r["grade"] == "FULL"] == []


def test_a_trap_close_back_through_the_13_kills_it():
    bars = _book(True, True)
    row = next(r for r in a.find_setups("EUR/USD", bars)
               if r["label"] == "22" and r["grade"] == "FULL")
    cut = [b for b in bars if b["ts_utc"] <= row["trigger_ts"]]
    trap = dict(cut[-1])
    trap["ts_utc"] = cut[-1]["ts_utc"] + BAR
    trap["close"] = row["extreme"] + 10 * PIP
    trap["high"] = trap["close"]
    trap["open"] = trap["close"]
    trap["low"] = row["entry"]
    assert [r for r in a.find_setups("EUR/USD", cut + [trap])
            if r["label"] == "22" and r["grade"] == "FULL"] == []


def test_discord_dry_run_does_not_post_twice(monkeypatch, tmp_path):
    import alerts
    sent = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: sent.append(embed) or True)
    monkeypatch.setattr(alerts, "_is_throttled", lambda p, r: False)
    monkeypatch.setattr(alerts, "BTMM_ANCHOR_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "BTMM_ANCHOR_FULL_ONLY", True)
    monkeypatch.setattr(alerts, "BTMM_ANCHOR_LOG_FILE", tmp_path / "anchor.jsonl")
    monkeypatch.setattr(alerts, "BTMM_ANCHOR_SEEN_FILE", tmp_path / "anchor_seen.json")

    row = _full(True, True)
    alerts.alert_btmm_anchor_setup("EUR/USD", row)
    alerts.alert_btmm_anchor_setup("EUR/USD", row)
    alerts.alert_btmm_anchor_setup("EUR/USD", {**row, "grade": "FORMING"})

    assert len(sent) == 1
    embed = sent[0]
    blob = json.dumps(embed)
    assert "EUR/USD" in embed["title"] and "22" in embed["title"] and "sell" in embed["title"]
    direction = next(f["value"] for f in embed["fields"] if f["name"] == "Direction")
    assert "SELL" in direction
    assert "untested watchlist setup, not a validated signal" in embed["description"].lower()
    assert row["time_sast"] in blob
    names = [f["name"] for f in embed["fields"]]
    for field in ("Label", "Grade", "Extreme", "Stop idea", "Target idea", "Invalidation"):
        assert field in names
    logged = json.loads((tmp_path / "anchor.jsonl").read_text().strip())
    assert logged["label"] == "22" and logged["discord_ok"] is True
    assert logged["grade"] == "FULL"
