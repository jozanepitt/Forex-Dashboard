"""Tests for the EMS gate on M15 SNR signals.

EMS (Engulfing + MSNR + SMC) gate per The Alchemist EMS Trinity / MSNR
ALCHEMIST notes: an M15 signal is only tradeable when the higher timeframe
agrees AND price has shown a liquidity sweep AND a market structure shift.

These tests use synthetic candle lists so behaviour is deterministic.
Candle dict format matches the cache: {open, high, low, close, ts_utc}.
"""
from __future__ import annotations

import snr_strategy as snr


def _c(o, h, l, cl, ts=0):
    return {"open": o, "high": h, "low": l, "close": cl, "ts_utc": ts}


# ──────────────────────────────────────────────────────────────────────
# detect_liquidity_sweep
# ──────────────────────────────────────────────────────────────────────

def test_buy_sweep_wicks_below_swing_low_then_reclaims():
    """BUY: a recent candle wicks BELOW a prior swing low then closes back
    above it (sell-side liquidity swept + reclaimed) -> sweep present."""
    candles = [
        _c(1.1000, 1.1010, 1.0990, 1.1005, 0),
        _c(1.1005, 1.1010, 1.0985, 1.0990, 1),
        _c(1.0990, 1.0995, 1.0960, 1.0965, 2),   # swing low @ 1.0960
        _c(1.0965, 1.0990, 1.0962, 1.0985, 3),
        _c(1.0985, 1.1000, 1.0980, 1.0995, 4),
        _c(1.0995, 1.1005, 1.0988, 1.1000, 5),
        _c(1.0990, 1.0995, 1.0945, 1.0985, 6),   # wick 1.0945 < 1.0960, close back above
    ]
    assert snr.detect_liquidity_sweep(candles, "BUY") is True


def test_sell_sweep_wicks_above_swing_high_then_reclaims():
    """SELL: a recent candle wicks ABOVE a prior swing high then closes back
    below it (buy-side liquidity swept + reclaimed) -> sweep present."""
    candles = [
        _c(1.1000, 1.1010, 1.0990, 1.0995, 0),
        _c(1.0995, 1.1015, 1.0990, 1.1010, 1),
        _c(1.1010, 1.1040, 1.1005, 1.1035, 2),   # swing high @ 1.1040
        _c(1.1035, 1.1038, 1.1010, 1.1015, 3),
        _c(1.1015, 1.1020, 1.1000, 1.1005, 4),
        _c(1.1005, 1.1012, 1.0995, 1.1000, 5),
        _c(1.1010, 1.1055, 1.1008, 1.1015, 6),   # wick 1.1055 > 1.1040, close back below
    ]
    assert snr.detect_liquidity_sweep(candles, "SELL") is True


def test_no_sweep_when_price_just_trends_down():
    """Clean downtrend with no wick-beyond-then-reclaim -> no sweep."""
    candles = [
        _c(1.1000, 1.1005, 1.0980, 1.0985, 0),
        _c(1.0985, 1.0990, 1.0960, 1.0965, 1),
        _c(1.0965, 1.0970, 1.0940, 1.0945, 2),
        _c(1.0945, 1.0950, 1.0920, 1.0925, 3),
        _c(1.0925, 1.0930, 1.0900, 1.0905, 4),
        _c(1.0905, 1.0910, 1.0880, 1.0885, 5),
        _c(1.0885, 1.0890, 1.0860, 1.0865, 6),
    ]
    assert snr.detect_liquidity_sweep(candles, "BUY") is False


# ──────────────────────────────────────────────────────────────────────
# detect_mss (Market Structure Shift / CHoCH)
# ──────────────────────────────────────────────────────────────────────

# Bearish structure (two descending lower-highs) then a close breaking above
# the most recent lower-high → bullish market structure shift.
_BULLISH_MSS = [
    _c(1.1000, 1.1020, 1.0995, 1.1015, 0),
    _c(1.1015, 1.1035, 1.1010, 1.1030, 1),
    _c(1.1030, 1.1060, 1.1025, 1.1055, 2),   # swing high A = 1.1060
    _c(1.1035, 1.1038, 1.1015, 1.1020, 3),   # gap down
    _c(1.1020, 1.1025, 1.1005, 1.1010, 4),
    _c(1.1015, 1.1045, 1.1012, 1.1040, 5),   # swing high B = 1.1045 (lower high)
    _c(1.1038, 1.1040, 1.1008, 1.1012, 6),
    _c(1.1012, 1.1018, 1.0998, 1.1015, 7),
    _c(1.1015, 1.1070, 1.1012, 1.1065, 8),   # close 1.1065 > B → bullish MSS
]

# Bullish structure (two ascending higher-lows) then a close breaking below
# the most recent higher-low → bearish market structure shift.
_BEARISH_MSS = [
    _c(1.1000, 1.1005, 1.0980, 1.0985, 0),
    _c(1.0985, 1.0990, 1.0965, 1.0970, 1),
    _c(1.0970, 1.0975, 1.0940, 1.0945, 2),   # swing low A = 1.0940
    _c(1.0975, 1.0990, 1.0970, 1.0985, 3),   # gap up
    _c(1.0985, 1.0998, 1.0972, 1.0992, 4),
    _c(1.0985, 1.0990, 1.0960, 1.0965, 5),   # swing low B = 1.0960 (higher low)
    _c(1.0966, 1.0992, 1.0968, 1.0988, 6),
    _c(1.0988, 1.0995, 1.0970, 1.0975, 7),
    _c(1.0975, 1.0978, 1.0935, 1.0938, 8),   # close 1.0938 < B → bearish MSS
]


def test_bullish_mss_breaks_last_lower_high():
    assert snr.detect_mss(_BULLISH_MSS, "BUY") is True


def test_bearish_mss_breaks_last_higher_low():
    assert snr.detect_mss(_BEARISH_MSS, "SELL") is True


def test_no_bearish_mss_in_bullish_break_data():
    """Bullish MSS data queried for SELL -> no bearish shift."""
    assert snr.detect_mss(_BULLISH_MSS, "SELL") is False


def test_no_bullish_mss_in_bearish_break_data():
    """Bearish MSS data queried for BUY -> no bullish shift."""
    assert snr.detect_mss(_BEARISH_MSS, "BUY") is False


# ──────────────────────────────────────────────────────────────────────
# ems_gate — full EMS confluence gate for M15 signals
# ──────────────────────────────────────────────────────────────────────

import snr_m15_strategy as m15

# Bullish setup that contains BOTH a liquidity sweep (idx7 wicks below the
# idx4 swing low @1.0995 and reclaims) AND a bullish MSS (idx8 closes above
# the lower-high B @1.1045).
_PASS_DATA = [
    _c(1.1000, 1.1020, 1.0998, 1.1015, 0),
    _c(1.1015, 1.1035, 1.1010, 1.1030, 1),
    _c(1.1030, 1.1060, 1.1025, 1.1055, 2),   # swing high A = 1.1060
    _c(1.1035, 1.1040, 1.1015, 1.1020, 3),   # gap down
    _c(1.1020, 1.1025, 1.0995, 1.1000, 4),   # swing low L = 1.0995
    _c(1.1010, 1.1045, 1.1005, 1.1040, 5),   # swing high B = 1.1045 (lower high)
    _c(1.1038, 1.1042, 1.1015, 1.1020, 6),
    _c(1.1020, 1.1022, 1.0990, 1.1015, 7),   # sweep: low 1.0990 < L, close reclaims
    _c(1.1015, 1.1070, 1.1012, 1.1065, 8),   # MSS: close 1.1065 > B
]

_H4_BULLISH = {"setup": "BUY",
               "storyline": {"active": True, "confirmed": True, "direction": "bullish"}}
_H4_BEARISH = {"setup": "SELL",
               "storyline": {"active": True, "confirmed": True, "direction": "bearish"}}

_M15_BUY_ROW = {"setup": "BUY"}


def test_gate_passes_when_h4_aligned_sweep_and_mss_present():
    ok, reason = m15.ems_gate(_M15_BUY_ROW, _H4_BULLISH, _PASS_DATA)
    assert ok is True, reason
    assert reason == ""


def test_gate_rejects_when_h4_bias_disagrees():
    ok, reason = m15.ems_gate(_M15_BUY_ROW, _H4_BEARISH, _PASS_DATA)
    assert ok is False
    assert "H4" in reason


def test_gate_rejects_when_h4_row_missing():
    ok, reason = m15.ems_gate(_M15_BUY_ROW, None, _PASS_DATA)
    assert ok is False
    assert "H4" in reason


def test_gate_rejects_when_no_liquidity_sweep():
    """Clean downtrend: no sweep (and no MSS). Sweep is checked first."""
    downtrend = [
        _c(1.1000, 1.1005, 1.0980, 1.0985, 0),
        _c(1.0985, 1.0990, 1.0960, 1.0965, 1),
        _c(1.0965, 1.0970, 1.0940, 1.0945, 2),
        _c(1.0945, 1.0950, 1.0920, 1.0925, 3),
        _c(1.0925, 1.0930, 1.0900, 1.0905, 4),
        _c(1.0905, 1.0910, 1.0880, 1.0885, 5),
        _c(1.0885, 1.0890, 1.0860, 1.0865, 6),
    ]
    ok, reason = m15.ems_gate(_M15_BUY_ROW, _H4_BULLISH, downtrend)
    assert ok is False
    assert "sweep" in reason.lower()


def test_gate_rejects_when_sweep_but_no_mss():
    """PASS_DATA without the final break candle: sweep present, MSS absent."""
    ok, reason = m15.ems_gate(_M15_BUY_ROW, _H4_BULLISH, _PASS_DATA[:-1])
    assert ok is False
    assert "structure" in reason.lower()


# ──────────────────────────────────────────────────────────────────────
# Wiring: alert_snr_m15_setup must consult the gate before dispatching
# ──────────────────────────────────────────────────────────────────────

import alerts


def _valid_m15_row():
    """A row that passes every pre-gate quality check in alert_snr_m15_setup."""
    return {
        "symbol": "EUR/USD",
        "setup": "BUY",
        "grade": "A",
        "storyline": {"confirmed": True, "from_level": 1.1000, "direction": "bullish"},
        "entry_tier": {"tier": "low", "confidence": "high", "setup_num": 3},
        "trade_plan": {"entry": 1.1000, "sl": 1.0980, "tp1": 1.1040},
    }


def _stub_pre_gate_checks(monkeypatch):
    """Force all cache/RR-dependent pre-gate checks to pass so tests isolate
    the EMS gate behaviour."""
    monkeypatch.setattr(alerts, "_check_rr", lambda *a, **k: True)
    monkeypatch.setattr(alerts, "_passes_quality_filters", lambda *a, **k: (True, ""))
    monkeypatch.setattr(alerts, "_is_throttled", lambda *a, **k: False)
    monkeypatch.setattr(alerts, "_mark_sent", lambda *a, **k: None)
    monkeypatch.setattr(alerts, "SNR_M15_EMS_GATE_ENABLED", True)


def test_alert_blocked_when_ems_gate_fails(monkeypatch):
    """H4 row missing -> gate fails -> no Discord post."""
    _stub_pre_gate_checks(monkeypatch)
    posted = []
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: posted.append(embed) or True)

    alerts.alert_snr_m15_setup(
        "EUR/USD", _valid_m15_row(), h4_row=None, m15_candles=_PASS_DATA,
    )
    assert posted == [], "alert fired despite EMS gate failing (H4 not aligned)"


def test_alert_invokes_gate_with_h4_and_candles(monkeypatch):
    """Gate is wired in with the correct context arguments."""
    _stub_pre_gate_checks(monkeypatch)
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: True)
    calls = []

    def _spy_gate(row, h4_row, m15_candles):
        calls.append((row, h4_row, m15_candles))
        return False, "spy-block"

    monkeypatch.setattr(m15, "ems_gate", _spy_gate)

    row = _valid_m15_row()
    alerts.alert_snr_m15_setup("EUR/USD", row, h4_row=_H4_BULLISH, m15_candles=_PASS_DATA)

    assert len(calls) == 1, "ems_gate was not called by the alert path"
    got_row, got_h4, got_candles = calls[0]
    assert got_h4 is _H4_BULLISH
    assert got_candles is _PASS_DATA

