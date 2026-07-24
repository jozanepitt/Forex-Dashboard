"""Tests for the journaled-trade outcome resolver.

`_resolve_one` is the decision core: given a direction, entry/SL/TP1 and the
candles that printed after the trade opened, decide win / loss / still-open.
Same-bar ties count as a LOSS (we can't see the intrabar path).
"""
from __future__ import annotations

from trade_tracker import _resolve_one


def _c(high, low):
    return {"high": high, "low": low, "close": (high + low) / 2}


def test_buy_hits_tp1_is_a_win():
    candles = [_c(1.1010, 1.0995), _c(1.1060, 1.1005)]   # second bar reaches TP1
    assert _resolve_one("BUY", 1.1000, 1.0950, 1.1050, candles) == ("win", 1.1050)


def test_buy_hits_sl_is_a_loss():
    candles = [_c(1.1010, 1.0990), _c(1.1005, 1.0940)]   # second bar breaks SL
    assert _resolve_one("BUY", 1.1000, 1.0950, 1.1050, candles) == ("loss", 1.0950)


def test_sell_hits_tp1_is_a_win():
    candles = [_c(1.1005, 1.0990), _c(1.1000, 1.0940)]
    assert _resolve_one("SELL", 1.1000, 1.1050, 1.0950, candles) == ("win", 1.0950)


def test_sell_hits_sl_is_a_loss():
    candles = [_c(1.1010, 1.0995), _c(1.1060, 1.1000)]
    assert _resolve_one("SELL", 1.1000, 1.1050, 1.0950, candles) == ("loss", 1.1050)


def test_same_bar_touching_both_counts_as_loss():
    """A bar whose range spans SL and TP1 is conservatively a loss."""
    candles = [_c(1.1060, 1.0940)]
    assert _resolve_one("BUY", 1.1000, 1.0950, 1.1050, candles) == ("loss", 1.0950)


def test_unresolved_returns_none():
    candles = [_c(1.1010, 1.0990), _c(1.1020, 1.0985)]   # never reaches either
    assert _resolve_one("BUY", 1.1000, 1.0950, 1.1050, candles) is None


def test_no_candles_is_still_open():
    assert _resolve_one("BUY", 1.1000, 1.0950, 1.1050, []) is None
