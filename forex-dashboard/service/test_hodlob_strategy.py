"""HOD/LOB detector on synthetic M15 bars, plus the one-alert dedupe.

The series is built, not recorded from a market. Ten choppy days give the
squeeze a median. The day before the setup is a steady rally, so yesterday's
TDI sits high while yesterday's low stays under the spike. Asia on the setup
day is a tight shelf at the high. One spike then closes back inside the bands.
"""
from __future__ import annotations

import datetime as dt
import json

import hodlob_strategy as h

UTC = dt.timezone.utc
BAR = 900
# 13 Jan 2026 22:00 UTC = 14 Jan 00:00 SAST. January, so London time is UTC.
SETUP_DAY = int(dt.datetime(2026, 1, 13, 22, 0, tzinfo=UTC).timestamp())


def _day(day_start, closes, lows, highs):
    return [{
        "ts_utc": day_start + i * BAR,
        "open": c, "high": highs[i], "low": lows[i], "close": c, "volume": 100,
    } for i, c in enumerate(closes)]


def _chop(day_start):
    closes = [1.1010 if (i // 4) % 2 == 0 else 1.0990 for i in range(96)]
    return _day(day_start, closes, [c - 0.0002 for c in closes], [c + 0.0002 for c in closes])


def _rally(day_start):
    start, end = 1.0990, 1.1030
    step = (end - start) / 95
    closes = [start + i * step for i in range(96)]
    return _day(day_start, closes, [c - 0.00005 for c in closes], [c + 0.00005 for c in closes])


def _setup_day(day_start, spike_low, spike_close=1.0992, re_close=1.1024, shelf=1.1026):
    closes, lows, highs = [], [], []
    for _ in range(40):
        closes.append(shelf)
        lows.append(shelf - 0.0004)
        highs.append(shelf + 0.0002)
    closes.append(spike_close)
    lows.append(spike_low)
    highs.append(shelf)
    closes.append(re_close)
    lows.append(re_close - 0.0001)
    highs.append(re_close + 0.0001)
    return _day(day_start, closes, lows, highs)


def _book(spike_low, **kw):
    bars = []
    for d in range(11, 1, -1):
        bars.extend(_chop(SETUP_DAY - d * 86400))
    bars.extend(_rally(SETUP_DAY - 86400))
    bars.extend(_setup_day(SETUP_DAY, spike_low, **kw))
    return bars


def _mirror(bars):
    return [{**b, "open": 2.2 - b["open"], "close": 2.2 - b["close"],
             "high": 2.2 - b["low"], "low": 2.2 - b["high"]} for b in bars]


def _passed(row):
    return {c["name"]: c["pass"] for c in row["conditions"]}


def test_a_long_meets_every_condition():
    rows = h.find_setups("EUR/USD", _book(spike_low=1.0994), "M15")
    assert len(rows) == 1
    row = rows[0]
    assert row["grade"] == "A"
    assert row["pattern"] == "HOD/LOB" and row["setup"] == "BUY"
    assert row["direction_label"] == "HOD/LOB long"
    assert row["soft_miss"] == []
    assert all(_passed(row).values())
    assert row["fresh"] is True
    assert row["sl"] < row["spike"] < row["entry"]
    assert row["tp1"] > row["entry"]
    assert "UTC" in row["time_utc"] and "SAST" in row["time_sast"]


def test_b_long_when_only_divergence_fails():
    """The spike takes yesterday's low, so TDI and price are no longer diverging.
    Hug, squeeze, the band spike, the Asia break, the re-entry and the window stay."""
    rows = h.find_setups("EUR/USD", _book(spike_low=1.0980), "M15")
    assert len(rows) == 1
    row = rows[0]
    assert row["grade"] == "B" and row["pattern"] == "HOD/LOB"
    assert row["soft_miss"] == ["TDI divergence"]
    passed = _passed(row)
    assert passed["TDI divergence"] is False
    assert passed["Hug"] and passed["Squeeze"] and passed["London window"]
    assert passed["Spike outside band"] and passed["Asian range break"] and passed["Re-entry"]


def test_a_short_is_the_mirror():
    rows = h.find_setups("EUR/USD", _mirror(_book(spike_low=1.0994)), "M15")
    assert len(rows) == 1
    row = rows[0]
    assert row["grade"] == "A"
    assert row["pattern"] == "LOD/HOB" and row["setup"] == "SELL"
    assert row["direction_label"] == "LOD/HOB short"
    assert all(_passed(row).values())
    assert row["spike"] < row["sl"]
    assert row["entry"] < row["spike"]
    assert row["tp1"] < row["entry"]


def test_no_setup_when_london_never_leaves_the_shelf():
    quiet = _book(spike_low=1.1022, spike_close=1.1026, re_close=1.1026)
    assert h.find_setups("EUR/USD", quiet, "M15") == []


def test_preferred_window_is_london_time_including_summer():
    """7-8-9 is London, not UTC. In July, 07:00 London is 06:00 UTC."""
    import session_clock
    day = session_clock.vwap_day_start(int(dt.datetime(2026, 7, 14, 8, tzinfo=UTC).timestamp()))
    assert h._london_ts(day, 7, 0) == int(dt.datetime(2026, 7, 14, 6, 0, tzinfo=UTC).timestamp())
    assert h._london_ts(day, 9, 45) == int(dt.datetime(2026, 7, 14, 8, 45, tzinfo=UTC).timestamp())


def test_two_soft_misses_are_not_a_setup():
    assert h._grade([]) == "A"
    assert h._grade(["Squeeze"]) == "B"
    assert h._grade(["Squeeze", "London window"]) is None


def test_universe_includes_exotics_metals_and_indices():
    assert "USD/ZAR" in h.HODLOB_UNIVERSE
    assert "USD/TRY" in h.HODLOB_UNIVERSE and "USD/SGD" in h.HODLOB_UNIVERSE
    assert "XAU/USD" in h.HODLOB_UNIVERSE and "USTEC" in h.HODLOB_UNIVERSE
    assert "USOIL" in h.HODLOB_UNIVERSE
    assert h.HODLOB_UNIVERSE.count("EUR/USD") == 1


def test_alert_posts_once_and_logs(monkeypatch, tmp_path):
    import alerts
    sent = []
    monkeypatch.setattr(alerts, "_post_discord", lambda e: sent.append(e) or True)
    monkeypatch.setattr(alerts, "_is_throttled", lambda p, r: False)
    monkeypatch.setattr(alerts, "HODLOB_ALERTS_ENABLED", True)
    monkeypatch.setattr(alerts, "HODLOB_GRADE_A_ONLY", False)
    log_path = tmp_path / "hodlob_alerts.jsonl"
    seen_path = tmp_path / "hodlob_seen.json"
    monkeypatch.setattr(alerts, "HODLOB_LOG_FILE", log_path)
    monkeypatch.setattr(alerts, "HODLOB_SEEN_FILE", seen_path)

    row = h.find_setups("USD/ZAR", _book(spike_low=1.0994), "M15")[0]
    alerts.alert_hodlob_setup("USD/ZAR", row)
    alerts.alert_hodlob_setup("USD/ZAR", row)

    assert len(sent) == 1
    embed = sent[0]
    assert "HOD/LOB long" in embed["title"]
    assert "USD/ZAR" in embed["title"]
    assert "grade A" in embed["title"]
    assert "setup alert, untested, not a signal" in embed["description"].lower()
    blob = json.dumps(embed)
    assert "UTC" in blob and "SAST" in blob
    assert row["time_utc"] in blob and row["time_sast"] in blob
    names = [f["name"] for f in embed["fields"]]
    assert "Entry" in names and "Stop" in names and "Target reference" in names
    assert "Conditions" in names

    lines = log_path.read_text().strip().splitlines()
    assert len(lines) == 1
    logged = json.loads(lines[0])
    assert logged["symbol"] == "USD/ZAR" and logged["grade"] == "A"
    assert logged["discord_ok"] is True
    assert logged["label"] == "setup alert, untested, not a signal"
