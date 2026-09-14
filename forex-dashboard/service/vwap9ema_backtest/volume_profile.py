"""Session-anchored Volume Profile — POC / Value Area / Low-Volume Nodes.

Computed per session-day group of M5 bars (the same bars already filtered
to one session window and one calendar day by backtest_mt5.backtest()),
using each bar's typical price (H+L+C)/3 — the same typical-price
convention the existing session VWAP calc uses — bucketed into N
equal-width price bands across that session's observed high/low range.

See docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md
("Volume Profile confluence layer") for the calculation spec and the
tick-volume-as-proxy caveat this is built under.
"""
from __future__ import annotations

N_BUCKETS = 24
VALUE_AREA_PCT = 0.70
LVN_PCT_OF_POC = 0.10


def compute_session_volume_profile(highs, lows, closes, volumes, n_buckets: int = N_BUCKETS) -> dict:
    """Build a volume profile from one session-day's OHLCV arrays (equal length)."""
    n = len(highs)
    if n == 0:
        raise ValueError("compute_session_volume_profile: empty input")

    range_lo = min(lows)
    range_hi = max(highs)
    if range_hi <= range_lo:
        range_hi = range_lo + 1e-9  # degenerate single-price session guard

    bucket_width = (range_hi - range_lo) / n_buckets
    bucket_edges = [range_lo + i * bucket_width for i in range(n_buckets + 1)]
    bucket_volumes = [0.0] * n_buckets

    for i in range(n):
        typ = (highs[i] + lows[i] + closes[i]) / 3.0
        idx = int((typ - range_lo) / bucket_width)
        idx = max(0, min(n_buckets - 1, idx))
        bucket_volumes[idx] += volumes[i]

    total_volume = sum(bucket_volumes)
    poc_bucket = max(range(n_buckets), key=lambda i: bucket_volumes[i])
    poc_price = (bucket_edges[poc_bucket] + bucket_edges[poc_bucket + 1]) / 2.0

    lo_idx = hi_idx = poc_bucket
    captured = bucket_volumes[poc_bucket]
    target = VALUE_AREA_PCT * total_volume
    while captured < target and (lo_idx > 0 or hi_idx < n_buckets - 1):
        vol_below = bucket_volumes[lo_idx - 1] if lo_idx > 0 else -1.0
        vol_above = bucket_volumes[hi_idx + 1] if hi_idx < n_buckets - 1 else -1.0
        if vol_below >= vol_above:
            lo_idx -= 1
            captured += bucket_volumes[lo_idx]
        else:
            hi_idx += 1
            captured += bucket_volumes[hi_idx]

    val = bucket_edges[lo_idx]
    vah = bucket_edges[hi_idx + 1]

    lvn_threshold = LVN_PCT_OF_POC * bucket_volumes[poc_bucket]
    lvn_buckets = [i for i in range(n_buckets) if bucket_volumes[i] < lvn_threshold]

    return {
        "bucket_edges": bucket_edges,
        "bucket_volumes": bucket_volumes,
        "bucket_width": bucket_width,
        "poc_bucket": poc_bucket,
        "poc_price": poc_price,
        "vah": vah,
        "val": val,
        "lvn_buckets": lvn_buckets,
    }


def _bucket_of(price: float, profile: dict) -> int:
    edges = profile["bucket_edges"]
    n_buckets = len(profile["bucket_volumes"])
    if price <= edges[0]:
        return 0
    if price >= edges[-1]:
        return n_buckets - 1
    return int((price - edges[0]) / profile["bucket_width"])


def price_passes_vp_filter(price: float, profile: dict) -> bool:
    """True if price sits inside the value area, or within one
    bucket-width of POC — the spec's soft-accept zone for a pullback entry."""
    if profile["val"] <= price <= profile["vah"]:
        return True
    return abs(price - profile["poc_price"]) <= profile["bucket_width"]


def price_in_lvn(price: float, profile: dict) -> bool:
    """True if price's bucket is a low-volume node (< 10% of POC's volume)."""
    return _bucket_of(price, profile) in profile["lvn_buckets"]
