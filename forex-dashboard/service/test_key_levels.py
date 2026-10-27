"""Key levels from MT5 daily candles: previous day + 2d..5d high/low/close and
last week's high/low, like the user's MT5 'Previous Day High and lows'
indicator. Synthetic data only."""
import datetime as dt

import pytest

import key_levels as kl


def _day(y, m, d, h, l, c):
    ts = int(dt.datetime(y, m, d, tzinfo=dt.timezone.utc).timestamp())
    return {"ts_utc": ts, "open": c, "high": h, "low": l, "close": c, "volume": 1}


# Week of Mon 12 Oct .. Fri 16 Oct 2026, a Sunday stub on 18 Oct, then Mon 19 .. Wed 21 Oct
D1 = [
    _day(2026, 10, 12, 1.1050, 1.0950, 1.1000),
    _day(2026, 10, 13, 1.1080, 1.0990, 1.1060),
    _day(2026, 10, 14, 1.1120, 1.1040, 1.1100),
    _day(2026, 10, 15, 1.1110, 1.1030, 1.1050),
    _day(2026, 10, 16, 1.1070, 1.1000, 1.1020),      # Friday
    _day(2026, 10, 18, 1.1030, 1.1015, 1.1025),      # Sunday stub (~2h)
    _day(2026, 10, 19, 1.1060, 1.1010, 1.1040),      # Monday
    _day(2026, 10, 20, 1.1090, 1.1035, 1.1080),      # Tuesday
    _day(2026, 10, 21, 1.1100, 1.1060, 1.1070),      # Wednesday = today (still forming)
]
NOW = int(dt.datetime(2026, 10, 21, 9, 0, tzinfo=dt.timezone.utc).timestamp())


def _by_name(levels):
    return {lv["name"]: lv["price"] for lv in levels}


def test_previous_five_trading_days_skip_today_and_the_sunday_stub():
    lv = _by_name(kl.daily_levels(D1, NOW, "EUR/USD"))
    assert lv["PDH"] == 1.1090 and lv["PDL"] == 1.1035 and lv["PDC"] == 1.1080   # Tuesday
    assert lv["2d PDH"] == 1.1060                                                 # Monday, not Sunday
    assert lv["3d PDH"] == 1.1070                                                 # Friday
    assert lv["5d PDL"] == 1.1040                                                 # Wednesday 14th
    assert not any(v == 1.1030 for k, v in lv.items() if k.endswith("PDH"))      # Sunday stub ignored
    assert "6d PDH" not in lv


def test_previous_week_high_and_low():
    lv = _by_name(kl.daily_levels(D1, NOW, "EUR/USD"))
    assert lv["PWH"] == 1.1120 and lv["PWL"] == 1.0950


def test_crypto_keeps_its_weekend_days():
    lv = _by_name(kl.daily_levels(D1, NOW, "BTC/USD"))
    assert lv["2d PDH"] == 1.1060 and lv["3d PDH"] == 1.1030                     # Sunday counts


def test_tolerance_is_a_tenth_of_the_daily_range():
    tol = kl.tolerance(D1, NOW, "EUR/USD")
    ranges = sorted([0.0100, 0.0090, 0.0080, 0.0080, 0.0070, 0.0050, 0.0055])   # Mon 12th .. Tue 20th, no stub
    assert tol == pytest.approx(0.1 * ranges[len(ranges) // 2])


def test_near_finds_levels_within_tolerance_and_the_next_above_and_below():
    levels = kl.daily_levels(D1, NOW, "EUR/USD")
    near = kl.near(levels, price=1.1082, tol=0.0005)
    names = {lv["name"] for lv in near["at"]}
    assert "PDC" in names and "PDH" not in names        # PDC 1.1080 is 2 pips away; PDH 1.1090 is 8
    assert near["above"]["price"] > 1.1082 and near["below"]["price"] < 1.1082
    assert near["cluster"] == len(near["at"])


def test_bundle_for_the_dashboard():
    out = kl.levels_for("EUR/USD", D1, NOW)
    assert out["symbol"] == "EUR/USD" and out["levels"] and out["tol"] > 0
    assert {"name", "price", "kind", "days_ago"} <= set(out["levels"][0])
