"""Hand-computed reconciliation test for the session Volume Profile calc.

4 synthetic bars, one per bucket (n_buckets=4), volumes chosen so POC,
value area, and the one LVN bucket are all traceable by hand — see the
worked arithmetic in docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md.

Bars (h, l, c chosen so typical price (h+l+c)/3 lands exactly on 101/103/105/107):
  bucket0 [100,102): typ=101, vol=10
  bucket1 [102,104): typ=103, vol=60   <- POC
  bucket2 [104,106): typ=105, vol=20
  bucket3 [106,108]: typ=107, vol=5

total=95, value-area target=0.70*95=66.5
Expand from POC(60): compare bucket0(10) vs bucket2(20) -> take bucket2 (20).
captured=80 >= 66.5 -> stop. lo_idx=1, hi_idx=2 -> val=102, vah=106.
LVN threshold = 0.10*60=6 -> bucket3(5) qualifies, bucket0(10) does not.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "vwap9ema_backtest"))
from volume_profile import compute_session_volume_profile, price_passes_vp_filter, price_in_lvn  # noqa: E402

HIGHS = [102, 104, 106, 108]
LOWS = [100, 102, 104, 106]
CLOSES = [101, 103, 105, 107]
VOLUMES = [10, 60, 20, 5]


def _profile():
    return compute_session_volume_profile(HIGHS, LOWS, CLOSES, VOLUMES, n_buckets=4)


def test_bucket_edges_and_width():
    p = _profile()
    assert p["bucket_width"] == 2.0
    assert p["bucket_edges"] == [100.0, 102.0, 104.0, 106.0, 108.0]


def test_bucket_volumes_assigned_correctly():
    p = _profile()
    assert p["bucket_volumes"] == [10.0, 60.0, 20.0, 5.0]


def test_poc_is_highest_volume_bucket():
    p = _profile()
    assert p["poc_bucket"] == 1
    assert p["poc_price"] == 103.0


def test_value_area_expands_toward_larger_neighbor():
    p = _profile()
    assert p["val"] == 102.0
    assert p["vah"] == 106.0


def test_lvn_bucket_below_ten_percent_of_poc():
    p = _profile()
    assert p["lvn_buckets"] == [3]


def test_price_passes_vp_filter_inside_value_area():
    p = _profile()
    assert price_passes_vp_filter(104.0, p) is True


def test_price_passes_vp_filter_at_poc():
    p = _profile()
    assert price_passes_vp_filter(103.0, p) is True


def test_price_fails_vp_filter_outside_value_area_and_far_from_poc():
    p = _profile()
    assert price_passes_vp_filter(107.0, p) is False


def test_price_in_lvn_true_for_lvn_bucket():
    p = _profile()
    assert price_in_lvn(107.0, p) is True


def test_price_in_lvn_false_for_non_lvn_bucket():
    p = _profile()
    assert price_in_lvn(101.0, p) is False
