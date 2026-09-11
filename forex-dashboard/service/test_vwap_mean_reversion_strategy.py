"""Tests for the VWAP Mean Reversion scanner.

Deviations from the source document (VWAP_Mean_Reversion_Strategy.md) are
documented in docs/superpowers/specs/2026-09-10-vwap-mean-reversion-strategy-design.md
and echoed as comments in vwap_mean_reversion_strategy.py at each point they apply.
"""
from __future__ import annotations

import pytest

import vwap_mean_reversion_strategy as m


def _bar(ts, hi, lo, cl, open_=None, vol=0):
    return {
        "ts_utc": ts, "open": open_ if open_ is not None else cl,
        "high": hi, "low": lo, "close": cl, "volume": vol,
    }


# ──────────────────────────────────────────────────────────────────────
# VWAP / sigma / z-score — hand-computed reconciliation (doc's own
# top-priority check: "compute session VWAP... reconcile it to the cent")
# ──────────────────────────────────────────────────────────────────────

def test_vwap_series_basic_reconciliation():
    candles = [
        _bar(0, 1.10000, 1.10000, 1.10000, vol=1),
        _bar(900, 1.10000, 1.10000, 1.10000, vol=1),
        _bar(1800, 1.10100, 1.10100, 1.10100, vol=1),
    ]
    vwap = m._vwap_series(candles)
    assert vwap[0] == pytest.approx(1.10000)
    assert vwap[1] == pytest.approx(1.10000)
    assert vwap[2] == pytest.approx(1.10000 + 0.00100 / 3)


def test_sigma_and_z_hand_computed():
    """Three equal-weighted bars [x, x, x+d] give sigma_2 = d*sqrt(2)/3 and
    z_2 = sqrt(2) exactly, independent of d. Derivation: vwap_2 = x + d/3;
    deviations are [-d/3, -d/3, 2d/3]; sum of squares = 6*(d/3)^2 = 2*d^2/3;
    variance = that / 3 = 2*d^2/9; sigma = d*sqrt(2)/3; z = (2d/3) / sigma
    = 2/sqrt(2) = sqrt(2). A clean, arithmetic-slip-proof case."""
    candles = [
        _bar(0, 1.10000, 1.10000, 1.10000, vol=1),
        _bar(900, 1.10000, 1.10000, 1.10000, vol=1),
        _bar(1800, 1.10100, 1.10100, 1.10100, vol=1),
    ]
    vwap = m._vwap_series(candles)
    sigma = m._sigma_series(candles, vwap)
    z = m._z_scores(candles, vwap, sigma)
    assert sigma[2] == pytest.approx(0.00100 * (2 ** 0.5) / 3)
    assert z[2] == pytest.approx(2 ** 0.5)


def test_vwap_and_sigma_reset_at_day_boundary():
    candles = [
        _bar(0, 1.10000, 1.10000, 1.10000, vol=1),        # day 1 (epoch 0 = 1970-01-01 00:00 UTC)
        _bar(86400, 1.20000, 1.20000, 1.20000, vol=1),     # day 2, 24h later
    ]
    vwap = m._vwap_series(candles)
    sigma = m._sigma_series(candles, vwap)
    assert vwap[1] == pytest.approx(1.20000)   # not blended with day 1
    assert sigma[1] == pytest.approx(0.0)      # single bar so far this day -> zero variance


def test_atr_series_stabilizes_to_constant_true_range():
    # Every bar: high-low = 0.0010, close constant -> true range = 0.0010 always.
    candles = [_bar(i * 900, 1.1005, 1.0995, 1.1000, vol=10) for i in range(20)]
    atr = m._atr_series(candles, period=14)
    assert atr[12] is None            # only 13 bars available -> not enough
    assert atr[13] == pytest.approx(0.0010)   # 14th bar -> first valid value
    assert atr[19] == pytest.approx(0.0010)


def test_d_score_uses_atr_normalisation():
    candles = [_bar(i * 900, 1.1005, 1.0995, 1.1000, vol=10) for i in range(19)]
    candles.append(_bar(19 * 900, 1.1050, 1.1000, 1.1020, vol=10))
    vwap = m._vwap_series(candles)
    atr = m._atr_series(candles, period=14)
    d = m._d_scores(candles, vwap, atr)
    assert d[12] is None       # ATR not yet available
    assert d[19] is not None
    assert d[19] == pytest.approx((candles[19]["close"] - vwap[19]) / atr[19])
