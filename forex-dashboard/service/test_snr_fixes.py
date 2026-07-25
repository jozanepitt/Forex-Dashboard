"""Regression tests for SNR correctness fixes (2026-07-24 audit).

  * _deduplicate_levels no longer merges a support with a resistance
  * quality gates + EMS gate fail CLOSED (suppress) when data is unavailable,
    instead of failing open and letting the documented losers through
"""
from __future__ import annotations

import snr_strategy
import alerts


def _lvl(price, typ, idx):
    return {"price": price, "type": typ, "formed_idx": idx}


# ── #9  cross-type dedup ──────────────────────────────────────────────────────

def test_support_and_resistance_at_same_price_both_survive():
    """A support and a resistance within tolerance must NOT collapse into one."""
    levels = [_lvl(1.1000, "support", 5), _lvl(1.1001, "resistance", 9)]
    out = snr_strategy._deduplicate_levels(levels, tolerance=0.0010)
    types = sorted(l["type"] for l in out)
    assert types == ["resistance", "support"]   # both preserved


def test_same_type_near_levels_merge_to_freshest():
    levels = [_lvl(1.1000, "support", 5), _lvl(1.1001, "support", 9)]
    out = snr_strategy._deduplicate_levels(levels, tolerance=0.0010)
    assert len(out) == 1
    assert out[0]["formed_idx"] == 9            # kept the more recent


def test_distant_same_type_levels_both_kept():
    levels = [_lvl(1.1000, "support", 5), _lvl(1.1100, "support", 9)]
    out = snr_strategy._deduplicate_levels(levels, tolerance=0.0010)
    assert len(out) == 2


# ── #8  gates fail CLOSED on missing data ─────────────────────────────────────

def test_quality_filters_fail_closed_when_cache_unavailable(monkeypatch):
    """If the candle cache raises, the loss-avoidance gates must SUPPRESS, not
    pass. (Old behaviour returned (True, "") on any exception.)"""
    import cache
    monkeypatch.setattr(alerts, "ALERTS_DISTANCE_FILTER_PIPS", 50)
    monkeypatch.setattr(alerts, "ALERTS_TREND_FILTER_ENABLED", True)

    def _boom(*a, **k):
        raise RuntimeError("cache down")
    monkeypatch.setattr(cache, "read_candles", _boom)

    passed, reason = alerts._passes_quality_filters("EUR/USD", "SELL", 1.1000)
    assert passed is False
    assert "unavailable" in reason
