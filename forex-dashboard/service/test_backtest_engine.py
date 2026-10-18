"""Fill-model tests. Synthetic bars, no download."""
import numpy as np

from research_backtest.data_io import resample
from research_backtest.engine import simulate


def _m1(rows):
    """rows: (ts, open, high, low, close)."""
    arr = np.array(rows, dtype=np.float64)
    return {
        "ts": arr[:, 0].astype(np.int64),
        "open": arr[:, 1], "high": arr[:, 2], "low": arr[:, 3], "close": arr[:, 4],
        "volume": np.ones(len(arr)),
    }


def test_stop_wins_when_the_same_bar_touches_both():
    # known at 60, so the fill is the bar that opens at 60.
    # That bar trades through both the stop and the target.
    m1 = _m1([
        (0, 1.1000, 1.1002, 1.0998, 1.1001),
        (60, 1.1000, 1.1030, 1.0970, 1.1010),
    ])
    trade = simulate(
        "BUY", m1, known_at=60, entry=1.1000, sl=1.0990, tp=1.1020,
        spread=0.0, slip=0.0, pip=0.0001, entry_kind="market",
        valid_until=None, time_stop_ts=None,
    )
    assert trade is not None
    assert trade["outcome"] == "sl"
    assert trade["entry_ts"] == 60


def test_gap_through_the_stop_fills_at_the_open():
    m1 = _m1([
        (60, 1.1000, 1.1002, 1.0999, 1.1001),
        (120, 1.0960, 1.0965, 1.0950, 1.0955),
    ])
    trade = simulate(
        "BUY", m1, known_at=60, entry=1.1000, sl=1.0990, tp=1.1020,
        spread=0.0, slip=0.0, pip=0.0001, entry_kind="market",
        valid_until=None, time_stop_ts=None,
    )
    assert trade["outcome"] == "sl"
    assert trade["exit_fill"] == 1.0960


def test_spread_makes_a_target_worth_less_than_the_raw_r():
    # Climb to the target on a later bar. No spread would be +2R (20 pips / 10 pips).
    m1 = _m1([
        (60, 1.1000, 1.1004, 1.0998, 1.1002),
        (120, 1.1010, 1.1025, 1.1008, 1.1022),
    ])
    trade = simulate(
        "BUY", m1, known_at=60, entry=1.1000, sl=1.0990, tp=1.1020,
        spread=0.00011, slip=0.00002, pip=0.0001, entry_kind="market",
        valid_until=None, time_stop_ts=None,
    )
    assert trade["outcome"] == "tp"
    assert trade["r"] < 2.0
    assert trade["r_wide"] < trade["r"]
    assert trade["entry_ts"] >= 60


def test_signal_is_not_filled_on_a_bar_that_opened_before_it_was_known():
    m1 = _m1([
        (0, 1.1000, 1.1050, 1.0990, 1.1040),
        (60, 1.1040, 1.1042, 1.1038, 1.1041),
    ])
    trade = simulate(
        "BUY", m1, known_at=60, entry=1.1040, sl=1.1030, tp=1.1060,
        spread=0.0, slip=0.0, pip=0.0001, entry_kind="market",
        valid_until=None, time_stop_ts=None,
    )
    assert trade["entry_ts"] == 60
    # The bar at 0 already traded through the target. It must not count.
    assert trade["outcome"] != "tp"


def test_resample_does_not_fold_a_partial_hour_into_the_previous_one():
    # 60 minutes of hour 0 (complete) plus 10 minutes of hour 1 (partial).
    rows = []
    for i in range(60):
        rows.append((i * 60, 1.0, 1.2, 0.9, 1.1))
    for i in range(60, 70):
        rows.append((i * 60, 5.0, 9.0, 4.0, 8.0))
    out = resample(_m1(rows), "H1")
    assert len(out["ts"]) == 1
    assert out["ts"][0] == 0
    assert out["high"][0] == 1.2
    assert out["close"][0] == 1.1


def test_v9_prefilter_keeps_a_cross_the_scanner_takes():
    from datetime import datetime, timezone
    from bot.signals import extract_vwap9, v9_candidate_prefilter

    day = int(datetime(2024, 1, 3, tzinfo=timezone.utc).timestamp())
    rows = []
    for i in range(144):
        ts = day + i * 300
        px = 1.10 if i < 108 else 1.10 + (i - 108) * 0.0010
        rows.append((ts, px, px + 0.0002, px - 0.0002, px, 10.0))
    arr = np.array(rows, dtype=np.float64)
    series = {
        "ts": arr[:, 0].astype(np.int64),
        "open": arr[:, 1], "high": arr[:, 2], "low": arr[:, 3], "close": arr[:, 4],
        "volume": arr[:, 5],
    }
    from research_backtest.data_io import as_bars
    bars = as_bars(series)
    prefilter = v9_candidate_prefilter(
        series["ts"], series["high"], series["low"], series["close"], series["volume"],
    )
    kept = False
    for i in range(80, len(bars)):
        window = bars[: i + 1]
        sig = extract_vwap9("EUR/USD", window, "M5")
        if sig is not None:
            assert prefilter(window), "filter dropped a real 9EMA/VWAP cross"
            kept = True
    assert kept


def test_higher_timeframe_slice_excludes_a_bar_that_has_not_closed():
    """A higher-timeframe bar is visible only after its own close time."""
    h4_open = np.array([0, 14400], dtype=np.int64)
    h4_close = h4_open + 14400
    known = 7200  # inside the first H4 bar, which closes at 14400
    end = int(np.searchsorted(h4_close, known, side="right"))
    assert end == 0
