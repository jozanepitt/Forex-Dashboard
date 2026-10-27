"""The dashboard's one start of day for every session VWAP: 00:00 SAST."""
import datetime as dt

import session_clock as sc


def _utc(*a):
    return int(dt.datetime(*a, tzinfo=dt.timezone.utc).timestamp())


def test_day_starts_at_midnight_sast_which_is_22_utc():
    assert sc.vwap_day_start(_utc(2026, 10, 20, 22, 0)) == _utc(2026, 10, 20, 22, 0)
    assert sc.vwap_day_start(_utc(2026, 10, 21, 7, 0)) == _utc(2026, 10, 20, 22, 0)
    assert sc.vwap_day_start(_utc(2026, 10, 21, 21, 59)) == _utc(2026, 10, 20, 22, 0)
    sast = dt.timezone(dt.timedelta(hours=2))
    start = dt.datetime.fromtimestamp(sc.vwap_day_start(_utc(2026, 10, 21, 7, 0)), sast)
    assert (start.hour, start.minute) == (0, 0)


def test_no_daylight_saving_shift():
    """SAST has no DST: the same UTC hour in January and July."""
    for month in (1, 7):
        t = _utc(2026, month, 15, 12, 0)
        assert dt.datetime.fromtimestamp(sc.vwap_day_start(t), dt.timezone.utc).hour == 22


def test_every_dashboard_vwap_uses_it():
    import ema1350_cross, sweep_strategy, vwap9ema_strategy, vwap_bounce_strategy
    import vwap_mean_reversion_strategy
    t = _utc(2026, 10, 21, 7, 0)
    assert vwap9ema_strategy._forex_day_start_utc(t) == sc.vwap_day_start(t)
    assert vwap_mean_reversion_strategy._forex_day_of(t) == sc.vwap_day_start(t)
    bars = [{"ts_utc": _utc(2026, 10, 20, 21, 45), "open": 9, "high": 9, "low": 9, "close": 9, "volume": 1},
            {"ts_utc": _utc(2026, 10, 20, 22, 0), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
    assert ema1350_cross.daily_vwap(bars) == 1
    assert vwap_bounce_strategy.vwap_series(bars)[-1] == 1
    assert sweep_strategy.session_vwap(bars)[0][-1] == 1
