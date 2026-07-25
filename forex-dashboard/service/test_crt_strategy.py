"""Regression tests for CRT correctness fixes (2026-07-24 audit).

  * setup direction now requires a real range sweep + close-back-inside
    (was derived from a forming candle's OHLC drift order, which could flip)
  * SMT confluence is only credited when it AGREES with the trade direction
"""
from __future__ import annotations

from crt_strategy import _crt_setup_from_sweep, _smt_confluence_score

CRT_HIGH, CRT_LOW = 1.1050, 1.1000


def _candle(o, h, l, c):
    return {"open": o, "high": h, "low": l, "close": c}


# ── sweep-based setup ─────────────────────────────────────────────────────────

def test_swept_low_reclaimed_is_buy():
    # took out 1.1000 then closed back inside the range
    assert _crt_setup_from_sweep(_candle(1.1020, 1.1030, 1.0990, 1.1015), CRT_HIGH, CRT_LOW) == "BUY"


def test_swept_high_reclaimed_is_sell():
    assert _crt_setup_from_sweep(_candle(1.1030, 1.1065, 1.1020, 1.1035), CRT_HIGH, CRT_LOW) == "SELL"


def test_no_sweep_is_no_trade():
    # drifts down inside the range but never takes out the low -> not a setup
    assert _crt_setup_from_sweep(_candle(1.1040, 1.1045, 1.1010, 1.1015), CRT_HIGH, CRT_LOW) == "NO-TRADE"


def test_swept_low_but_closed_below_is_not_a_reclaim():
    # took out the low and CLOSED below it (no reclaim) -> not a BUY
    assert _crt_setup_from_sweep(_candle(1.1010, 1.1015, 1.0980, 1.0985), CRT_HIGH, CRT_LOW) == "NO-TRADE"


def test_both_sides_swept_is_no_trade():
    assert _crt_setup_from_sweep(_candle(1.1025, 1.1070, 1.0985, 1.1025), CRT_HIGH, CRT_LOW) == "NO-TRADE"


def test_missing_range_is_no_trade():
    assert _crt_setup_from_sweep(_candle(1.1020, 1.1030, 1.0990, 1.1015), None, None) == "NO-TRADE"
    assert _crt_setup_from_sweep(None, CRT_HIGH, CRT_LOW) == "NO-TRADE"


# ── SMT confluence must agree with direction ──────────────────────────────────

def test_smt_credited_only_when_aligned():
    assert _smt_confluence_score("BUY", "BULLISH-DIVERGENCE") == 2
    assert _smt_confluence_score("SELL", "BEARISH-DIVERGENCE") == 2
    # contradicting divergence must NOT score (the audit bug)
    assert _smt_confluence_score("BUY", "BEARISH-DIVERGENCE") == 0
    assert _smt_confluence_score("SELL", "BULLISH-DIVERGENCE") == 0


def test_smt_partner_half_credit_when_aligned():
    assert _smt_confluence_score("BUY", "BULLISH-DIVERGENCE-PARTNER") == 1
    assert _smt_confluence_score("SELL", "BULLISH-DIVERGENCE-PARTNER") == 0
    assert _smt_confluence_score("NO-TRADE", "BULLISH-DIVERGENCE") == 0
