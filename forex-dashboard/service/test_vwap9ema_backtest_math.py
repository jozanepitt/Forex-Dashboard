"""Hand-computed reconciliation for backtest_mt5.py's ported VWAP/EMA math,
addressing the design spec's "Open risk" commitment to independently verify
this before trusting the validation sweep's numbers (see
docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md,
"Open risk"). Values below were computed by hand (plain Python arithmetic,
not by calling the functions under test) for a 6-bar fixture:

  typical price = (h+l+c)/3
  session VWAP   = cumsum(typ*v) / cumsum(v)
  9-EMA          = k=2/10=0.2, seeded at close[0], ema[i] = close[i]*k + ema[i-1]*(1-k)
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent / "vwap9ema_backtest"))
from backtest_mt5 import ema  # noqa: E402

HIGH = [100.5, 100.7, 101.0, 101.2, 101.3, 101.5]
LOW = [99.5, 100.0, 100.3, 100.6, 100.7, 100.85]
CLOSE = [100.2, 100.5, 100.8, 101.0, 100.9, 101.4]
VOLUME = [10, 10, 10, 10, 10, 10]

EXPECTED_VWAP = [100.066667, 100.233333, 100.388889, 100.525, 100.613333, 100.719444]
EXPECTED_EMA9 = [100.2, 100.26, 100.368, 100.4944, 100.57552, 100.740416]


def test_session_vwap_matches_hand_computed_values():
    h = np.array(HIGH); l = np.array(LOW); c = np.array(CLOSE); v = np.array(VOLUME, dtype=float)
    typ = (h + l + c) / 3
    vwap = np.cumsum(typ * v) / np.cumsum(v)
    np.testing.assert_allclose(vwap, EXPECTED_VWAP, atol=1e-6)


def test_nine_ema_matches_hand_computed_values():
    c = np.array(CLOSE)
    result = ema(c, 9)
    np.testing.assert_allclose(result, EXPECTED_EMA9, atol=1e-6)
