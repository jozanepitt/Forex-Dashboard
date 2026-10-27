"""BTMM course rules (phase 2/3, 2026-10-27). Synthetic data only."""
from __future__ import annotations

import math
from datetime import datetime, timezone

import btmm_core as core
import btmm_rules as r

PIP = 0.0001


def _ts(y, mo, d, h, mi=0):
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp())


def _bar(i, o, h, l, c, t0=0):
    return {"ts_utc": t0 + i * 900, "open": o, "high": h, "low": l, "close": c}


# ── Sessions (New York time, DST aware) ───────────────────────────────────────

def test_sessions_follow_new_york_time_through_dst():
    # 27 Oct 2026 = EDT (UTC-4); 1 Dec 2026 = EST (UTC-5)
    assert r.session(_ts(2026, 10, 27, 8)) == "London"        # 04:00 EDT
    assert r.session(_ts(2026, 12, 1, 8)) is None              # 03:00 EST = gap before London
    assert r.session(_ts(2026, 12, 1, 9)) == "London"          # 04:00 EST
    assert r.session(_ts(2026, 10, 27, 2)) == "Asian"          # 22:00 EDT the evening before
    assert r.session(_ts(2026, 10, 27, 15)) == "NY"            # 11:00 EDT
    assert r.session(_ts(2026, 10, 27, 21, 30)) is None        # 17:30 EDT = dead gap


def test_kill_zone_is_london_and_ny_only():
    assert r.kill_zone(_ts(2026, 10, 27, 8)) == "London"
    assert r.kill_zone(_ts(2026, 10, 27, 15)) == "NY"
    assert r.kill_zone(_ts(2026, 10, 27, 2)) is None           # Asian = accumulation, no trades
    assert core.active_kill_zone(_ts(2026, 10, 27, 15)) == "NY"


def test_cycle_phase_uses_course_words():
    assert r.cycle_phase(_ts(2026, 10, 27, 2)) == "Accumulation"
    assert r.cycle_phase(_ts(2026, 10, 27, 8)) == "Stop Hunt"
    assert r.cycle_phase(_ts(2026, 10, 27, 15)) == "Trend Move"
    assert r.cycle_phase(_ts(2026, 10, 27, 20)) == "End of Day Reversal"   # 16:00 EDT


def test_trading_day_resets_at_17_ny():
    assert str(r.trading_day(_ts(2026, 10, 27, 20, 45))) == "2026-10-27"   # 16:45 EDT Tue
    assert str(r.trading_day(_ts(2026, 10, 27, 21, 15))) == "2026-10-28"   # 17:15 EDT Tue = Wed's day


# ── Shark Fin / Blood in the Water ────────────────────────────────────────────

def _tdi(fast, slow=60.0, mid=50.0, upper=65.0, lower=35.0):
    n = len(fast)
    return {"fast": list(fast), "slow": [slow] * n if isinstance(slow, float) else list(slow),
            "mid": [mid] * n, "upper": [upper] * n, "lower": [lower] * n}


ASIAN = {"valid": True, "high": 1.1000, "low": 1.0970}


def _fin_bars(n, spike_at, spike_high):
    bars = [_bar(i, 1.0990, 1.0995, 1.0985, 1.0990) for i in range(n)]
    for i in spike_at:
        bars[i] = _bar(i, 1.0995, spike_high, 1.0990, 1.1000)
    return bars


def test_shark_fin_sell_ready_on_signal_cross_at_68_and_25_50_pips_above_asian():
    fast = [50.0] * 24 + [72.0, 75.0, 64.0, 58.0]             # out of the band, back, crosses 60
    sf = r.detect_shark_fin(_fin_bars(28, (24, 25), 1.1030), _tdi(fast), ASIAN, PIP)
    assert sf["direction"] == "bearish" and sf["overbought"] and sf["location_ok"]
    assert sf["location_pips"] == 30.0
    assert sf["entry"] == "Shark Fin" and sf["entry_ready"]
    assert sf["spike_extreme"] == 1.1030


def test_shark_fin_blood_in_the_water_is_the_baseline_cross():
    fast = [50.0] * 24 + [72.0, 75.0, 64.0, 58.0, 49.0]       # then crosses the 50 baseline
    sf = r.detect_shark_fin(_fin_bars(29, (24, 25), 1.1030), _tdi(fast), ASIAN, PIP)
    assert sf["blood_in_water"] and sf["entry"] == "Blood in the Water" and sf["entry_ready"]
    assert sf["confidence"] == "high"


def test_shark_fin_rejected_outside_25_50_pips_or_without_68():
    fast = [50.0] * 24 + [72.0, 75.0, 64.0, 58.0]
    far = r.detect_shark_fin(_fin_bars(28, (24, 25), 1.1060), _tdi(fast), ASIAN, PIP)   # 60 pips
    assert not far["entry_ready"] and "25-50" in far["reason"]
    near = r.detect_shark_fin(_fin_bars(28, (24, 25), 1.1010), _tdi(fast), ASIAN, PIP)  # 10 pips
    assert not near["entry_ready"]
    weak = [50.0] * 24 + [66.0, 67.0, 64.0, 58.0]             # beyond the 65 band but under 68
    sf = r.detect_shark_fin(_fin_bars(28, (24, 25), 1.1030), _tdi(weak), ASIAN, PIP)
    assert not sf["overbought"] and not sf["entry_ready"]


def test_shark_fin_buy_is_the_mirror():
    fast = [50.0] * 24 + [28.0, 25.0, 36.0, 42.0]
    bars = [_bar(i, 1.0980, 1.0985, 1.0975, 1.0980) for i in range(28)]
    for i in (24, 25):
        bars[i] = _bar(i, 1.0975, 1.0978, 1.0935, 1.0970)    # 35 pips under the 1.0970 Asian low
    sf = r.detect_shark_fin(bars, _tdi(fast, slow=40.0), ASIAN, PIP)
    assert sf["direction"] == "bullish" and sf["entry_ready"] and sf["location_pips"] == 35.0


def test_shark_fin_only_ready_with_the_800_ema_bias():
    fast = [50.0] * 24 + [72.0, 75.0, 64.0, 58.0]
    bars = _fin_bars(28, (24, 25), 1.1030)
    against = r.detect_shark_fin(bars, _tdi(fast), ASIAN, PIP, bias_bullish=True)   # sell vs bullish bias
    assert not against["entry_ready"] and against["reason"] == "against the 800 EMA bias"
    assert r.detect_shark_fin(bars, _tdi(fast), ASIAN, PIP, bias_bullish=False)["entry_ready"]


def test_shark_fin_plan_holds_without_the_13_exit():
    p = r.trade_plan(1.1000, "bearish", 1.1010, PIP, adr=60 * PIP, exit_13=False)
    m = r.manage_trade(p, "bearish", [_bar(0, 1.0998, 1.1004, 1.0996, 1.1003)], [1.1001])
    assert m["outcome"] == "open" and "hold to the stop or TP1" in p["exit_rules"]


def test_confirmed_mw_counts_half_in_the_score():
    items = [False] * 13
    items[9] = True                                            # only the M/W item
    assert core._weighted_checklist(items) < 1 and core._weighted_checklist([True] * 13) == 13


def test_shark_fin_needs_a_valid_asian_range():
    fast = [50.0] * 24 + [72.0, 75.0, 64.0, 58.0]
    sf = r.detect_shark_fin(_fin_bars(28, (24, 25), 1.1030), _tdi(fast), {"valid": False}, PIP)
    assert not sf["detected"]


def test_tdi_confirms_needs_baseline_side_and_signal_cross():
    assert r.tdi_confirms(_tdi([55.0, 55.0, 48.0, 45.0], slow=50.0, mid=52.0), "bearish")
    assert not r.tdi_confirms(_tdi([55.0, 55.0, 51.0, 51.0], slow=52.0, mid=50.0), "bearish")  # above MBL


def _rr_sell_bars(n=5):
    """Ends in a bearish RR (railroad tracks): an up candle, then an equal down candle."""
    bars = [_bar(i, 1.10, 1.101, 1.099, 1.0995) for i in range(n - 2)]
    bars.append(_bar(n - 2, 1.0985, 1.1001, 1.0984, 1.0999))     # up 14 pips
    bars.append(_bar(n - 1, 1.0999, 1.1000, 1.0983, 1.0985))     # down 14 pips
    return bars


def test_confirmed_mw_needs_close_beyond_13_tdi_and_a_2nd_leg_candle():
    ts = _tdi([55.0, 55.0, 48.0, 45.0, 44.0], slow=50.0, mid=52.0)
    ok = r.confirm_mw(_rr_sell_bars(), {"detected": True, "pattern": "M"}, [1.1000] * 5, ts)
    assert ok["confirmed"] and ok["candle"] == "RRT"
    above = r.confirm_mw(_rr_sell_bars(), {"detected": True, "pattern": "M"}, [1.0980] * 5, ts)
    assert not above["confirmed"] and not above["close_beyond_13"]
    plain = [_bar(i, 1.10, 1.101, 1.099, 1.0995) for i in range(5)]   # no RR / star / COW
    no_candle = r.confirm_mw(plain, {"detected": True, "pattern": "M"}, [1.1000] * 5, ts)
    assert not no_candle["confirmed"] and no_candle["candle"] is None


def test_mayo_needs_level_2_a_200_test_and_the_m_w_entry():
    ts = _tdi([55.0] * 22 + [45.0, 44.0], slow=50.0, mid=52.0)
    bars = [_bar(i, 1.0990, 1.0995, 1.0985, 1.0990) for i in range(22)] + _rr_sell_bars(2)[-2:]
    bars[10] = _bar(10, 1.0995, 1.1009, 1.0990, 1.0996)          # tests the 200 EMA at 1.1010
    e13, e200 = [1.1000] * 24, [1.1010] * 24
    lv2 = {"direction": "bearish", "level": 2}
    mayo = r.detect_mayo_mw(bars, {"detected": True, "pattern": "M"}, e13, e200, ts, lv2, PIP)
    assert mayo["detected"] and mayo["direction"] == "bearish"
    lv1 = {"direction": "bearish", "level": 1}                    # never trade back an anchor
    assert not r.detect_mayo_mw(bars, {"detected": True, "pattern": "M"}, e13, e200, ts, lv1, PIP)["detected"]
    far = [1.1040] * 24                                           # 200 EMA never tested
    assert not r.detect_mayo_mw(bars, {"detected": True, "pattern": "M"}, e13, far, ts, lv2, PIP)["detected"]


def _safety_case(level=1, leg=2, rrt=True):
    """M: high at bar 5, then the 13 crosses under the 50 at bar 20 (the lock)."""
    n = 40
    bars = [_bar(i, 1.0996, 1.1005, 1.0995, 1.1000) for i in range(n)]   # plain up candles, not dojis
    bars[5] = _bar(5, 1.1000, 1.1030, 1.0998, 1.1010)          # leg 1 high
    if rrt:
        bars[-2] = _bar(n - 2, 1.0985, 1.1001, 1.0984, 1.0999)
        bars[-1] = _bar(n - 1, 1.0999, 1.1000, 1.0983, 1.0985)
    e13 = [1.1010] * 20 + [1.0990] * 20
    e50, e200 = [1.1000] * n, [1.0950] * n
    lv = {"level": level, "direction": "bearish"}
    tl = {"leg": leg, "direction": "bearish"}
    return r.detect_safety_trade(bars, {"detected": True, "pattern": "M"}, lv, tl, e13, e50, e200, PIP, 0.0060)


def test_safety_trade_needs_all_five_course_checks():
    ok = _safety_case()
    assert ok["active"] and ok["direction"] == "bearish" and ok["gatesPassed"] == 5
    assert ok["sl"] > ok["entry"] > ok["tp1"]
    assert not _safety_case(level=2)["active"]                  # Level 1 only
    assert not _safety_case(leg=1)["active"]                    # TDI 2nd leg back inside the band
    assert not _safety_case(rrt=False)["active"]                # nameable 2nd leg
    names = [g["name"] for g in ok["gates"]]
    assert not any("Kill Zone" in g or "Asian" in g or "H1" in g for g in names)


def test_straightaway_is_a_level_1_consolidation_break():
    box = [_bar(i, 1.1000, 1.1005, 1.0995, 1.1001) for i in range(12)]
    brk = box + [_bar(12, 1.1002, 1.1012, 1.1001, 1.1010)]
    lv1 = {"level": 1, "direction": "bullish", "origin_ts": 0}
    sa = r.detect_straightaway(brk, lv1, [1.1000] * 13, adr=0.0060)
    assert sa["detected"] and sa["direction"] == "bullish"
    assert not r.detect_straightaway(brk, {**lv1, "level": 2}, [1.1000] * 13, adr=0.0060)["detected"]
    wide = [_bar(i, 1.1000, 1.1030, 1.0970, 1.1001) for i in range(12)] + [brk[-1]]
    assert not r.detect_straightaway(wide, lv1, [1.1000] * 13, adr=0.0060)["detected"]


def test_nyc_reversal_at_the_hod_in_the_first_3_ny_hours():
    t_end = _ts(2026, 10, 27, 15)                                 # 11:00 New York
    t0 = t_end - 39 * 900
    bars = [_bar(i, 1.0990 + i * 0.00005, 1.0993 + i * 0.00005, 1.0988 + i * 0.00005, 1.0991 + i * 0.00005, t0)
            for i in range(38)]
    bars.append(_bar(38, 1.1005, 1.1022, 1.1004, 1.1020, t0))     # up to the day's high
    bars.append(_bar(39, 1.1020, 1.1021, 1.1007, 1.1008, t0))     # similar down candle = bearish RR
    nyc = r.detect_nyc_reversal(bars, adr=0.0040, unit=PIP)
    assert nyc["detected"] and nyc["direction"] == "bearish"
    late = [dict(b, ts_utc=b["ts_utc"] + 3 * 3600) for b in bars]  # 14:00 New York: too late
    assert not r.detect_nyc_reversal(late, adr=0.0040, unit=PIP)["detected"]


def test_weekly_cycle_m_after_an_early_week_high():
    t0 = _ts(2026, 10, 25, 21, 15)                                # Monday's trading day starts Sun 17:00 ET
    bars = []
    for i in range(3 * 96 + 40):                                  # Mon up, then down into Wednesday
        p = 1.1000 + (0.00004 * i if i < 96 else 0.00384 - 0.00005 * (i - 96))
        bars.append(_bar(i, p, p + 0.0002, p - 0.0002, p, t0))
    wk = r.detect_weekly_cycle(bars)
    assert wk["detected"] and wk["pattern"] == "M" and wk["direction"] == "bearish"
    assert not r.detect_weekly_cycle(bars[:150])["detected"]      # Monday/Tuesday: too early


# ── Advanced M / W ────────────────────────────────────────────────────────────

def _adv_case(leg2_high=1.1015, consol=10):
    """M: `consol` closes above the 13 EMA (1.1000) with the RSI line out of
    the band, a shift close below, a retest back over the 13, then a close
    back below it with TDI under the MBL crossing the signal line."""
    bars = [_bar(i, 1.1004, 1.1012, 1.1001, 1.1005) for i in range(consol)]
    bars[consol // 2] = _bar(consol // 2, 1.1005, 1.1020, 1.1003, 1.1006)     # leg 1 high
    s = consol
    bars.append(_bar(s, 1.1004, 1.1006, 1.0993, 1.0995))                       # shift
    bars.append(_bar(s + 1, 1.0995, 1.1000, 1.0992, 1.0998))
    bars.append(_bar(s + 2, 1.0998, leg2_high, 1.0997, 1.1002))               # retest over the 13
    bars.append(_bar(s + 3, 1.1002, 1.1004, 1.0996, 1.0999))
    bars.append(_bar(s + 4, 1.0999, 1.1000, 1.0988, 1.0990))                   # closes back below
    n = len(bars)
    fast = [55.0] * n
    fast[consol // 2] = 75.0
    fast[-2], fast[-1] = 55.0, 45.0
    return bars, [1.1000] * n, _tdi(fast, slow=50.0, mid=52.0)


def test_advanced_m_detected():
    am = r.detect_advanced_mw(*_adv_case())
    assert am["detected"] and am["pattern"] == "M" and am["direction"] == "bearish"
    assert am["leg1"] == 1.1020 and am["consolidation_bars"] >= 8


def test_advanced_m_rejected_when_leg2_beats_leg1():
    assert not r.detect_advanced_mw(*_adv_case(leg2_high=1.1030))["detected"]


def test_advanced_m_needs_8_candles_of_consolidation():
    assert not r.detect_advanced_mw(*_adv_case(consol=5))["detected"]


# ── London patterns ───────────────────────────────────────────────────────────

LONDON_T = _ts(2026, 10, 27, 8)          # 04:00 New York = London session


def _london_bars(high, close=1.0985, n=12, t_end=LONDON_T):
    t0 = t_end - (n - 1) * 900
    return [_bar(i, close, high, close - 0.0005, close, t0) for i in range(n)]


def test_london_type1_and_type2():
    asian = {"valid": True, "high": 1.1000, "low": 1.0970}
    ts = _tdi([50.0] * 12)
    t1 = r.detect_london_pattern(_london_bars(1.1010), [1.0990] * 12, ts, asian, PIP,
                                 mw={"pattern": "M", "confirmed": True})
    assert t1["detected"] and t1["type"] == 1 and t1["direction"] == "bearish"
    t2 = r.detect_london_pattern(_london_bars(1.0990), [1.0990] * 12, ts, asian, PIP,
                                 mw={"pattern": "M", "confirmed": True})
    assert t2["type"] == 2


def test_london_type3_fifty_fifty_fifty():
    asian = {"valid": True, "high": 1.1000, "low": 1.0970}
    bars = _london_bars(1.0986, close=1.0984)
    bars[-2] = _bar(10, 1.0985, 1.0989, 1.0982, 1.0984, bars[0]["ts_utc"])   # tags the 50 EMA, closes under
    ts = _tdi([50.0] * 10 + [47.0, 45.0], slow=[52.0] * 10 + [51.0, 49.0], mid=50.0)
    t3 = r.detect_london_pattern(bars, [1.0990] * 12, ts, asian, PIP)
    assert t3["detected"] and t3["type"] == 3 and t3["direction"] == "bearish"


def test_london_pattern_only_in_london():
    asian = {"valid": True, "high": 1.1000, "low": 1.0970}
    ny = _london_bars(1.1010, t_end=_ts(2026, 10, 27, 15))
    assert not r.detect_london_pattern(ny, [1.0990] * 12, _tdi([50.0] * 12), asian, PIP,
                                       mw={"pattern": "M", "confirmed": True})["detected"]


# ── Level context, Fibonacci exit, H1 ─────────────────────────────────────────

def test_level_context_asian_stack_and_adr_legs():
    t0 = _ts(2026, 10, 19, 21)
    bars = []
    for i in range(5 * 96):
        p = 1.1000 + 0.0010 * (i // 96) + 0.00001 * (i % 96)
        bars.append(_bar(i, p, p + 0.0002, p - 0.0002, p, t0))
    ctx = r.level_context(bars, adr=0.0010, direction="bullish", origin_ts=t0)
    assert ctx["asian_stack"] >= 3
    assert ctx["adr_legs"] >= 4
    assert r.level_context(bars, adr=0.0010, direction="bearish", origin_ts=t0)["asian_stack"] == 0


def test_fib_exit_gives_back_more_than_61_8():
    p = {"entry": 1.1000, "sl": 1.1020, "tp1": 1.0960, "break_even_at": 1.0980, "exit_13": False}
    after = [_bar(0, 1.0995, 1.0999, 1.0975, 1.0985), _bar(1, 1.0985, 1.0998, 1.0984, 1.0995)]
    m = r.manage_trade(p, "bearish", after, [], fib=0.618)
    assert m["outcome"] == "exit_fib" and m["exit"] == 1.0995 and m["bars"] == 2
    assert r.manage_trade(p, "bearish", after, [], fib=0.382)["bars"] == 1
    assert r.manage_trade(p, "bearish", after, [])["outcome"] == "open"


def test_h1_gate_on_22_trade_and_three_drive():
    assert core._h1_aligned({"valid": True, "trend": "bearish"}, "bearish")
    assert core._h1_aligned({"valid": True, "trend": "neutral"}, "bullish")
    assert not core._h1_aligned({"valid": True, "trend": "bearish"}, "bullish")
    bars = [_bar(i, 1.1, 1.1, 1.1, 1.1) for i in range(5)]
    td = core.detect_three_drive_pattern(bars, {"detected": True, "direction": "bullish"}, {"detected": False},
                                         {"detected": False}, "EUR/USD", {"valid": True, "trend": "bearish"})
    gate = next(g for g in td["gates"] if g["name"] == "H1 MTF Aligned")
    assert gate["pass"] is False


def test_h1_trend_reads_the_h1_stack():
    bars = [_bar(i, 1 + i * 1e-5, 1 + i * 1e-5, 1 + i * 1e-5, 1 + i * 1e-5) for i in range(1200)]
    assert r.h1_trend(bars) == "bullish"


# ── Half Batman Type 1 / Type 2 ───────────────────────────────────────────────

def _hb_case(type1: bool, retest_high=1.1038):
    """Sell structure: leg 1 high 1.1050 at bar 5, 13 EMA 1.1030, 50 EMA 1.1010."""
    bars = [_bar(i, 1.1035, 1.1040, 1.1032, 1.1036) for i in range(5)]
    bars.append(_bar(5, 1.1036, 1.1050, 1.1034, 1.1040))      # leg 1
    bars.append(_bar(6, 1.1035, 1.1036, 1.1022, 1.1025))      # closes below the 13
    for i in range(7, 39):
        bars.append(_bar(i, 1.1022, 1.1028, 1.1015, 1.1020))
    if type1:
        bars[10] = _bar(10, 1.1025, retest_high, 1.1024, 1.1033)   # back over the 13, lower high
    else:
        bars[12] = _bar(12, 1.1020, 1.1026, 1.1011, 1.1018)        # trapped off the 50
    bars.append(_bar(39, 1.1020, 1.1022, 1.1004, 1.1005))     # 18-pip shift candle
    e13, e50 = [1.1030] * 40, [1.1010] * 40
    ts = _tdi([50.0] * 39 + [40.0], slow=45.0, mid=50.0)
    return bars, e13, e50, ts


def test_half_batman_type1_sell():
    hb = r.detect_half_batman(*_hb_case(True), PIP)
    assert hb["detected"] and hb["type"] == 1 and hb["direction"] == "bearish"
    assert hb["shift_extreme"] == 1.1022


def test_half_batman_type2_sell():
    hb = r.detect_half_batman(*_hb_case(False), PIP)
    assert hb["detected"] and hb["type"] == 2


def test_half_batman_type1_rejected_when_retest_nearly_reaches_leg1():
    hb = r.detect_half_batman(*_hb_case(True, retest_high=1.1045), PIP)   # only 5 pips short
    assert not hb["detected"]


def test_half_batman_needs_tdi_beyond_signal_and_mbl():
    bars, e13, e50, _ = _hb_case(False)
    hb = r.detect_half_batman(bars, e13, e50, _tdi([50.0] * 40, slow=45.0, mid=50.0), PIP)
    assert not hb["detected"]


# ── Market-maker levels and resets ────────────────────────────────────────────

def _reset_case(dip_bars):
    pre = 7
    n = pre + dip_bars + 3
    e800, e200 = [1.04] * n, [1.02] * n
    e50 = [1.00] * 5 + [1.03] * (n - 5)                        # 50 over 200 at bar 5 = Level 2
    e13 = [0.99, 0.99, 1.01, 1.01, 1.01, 1.05, 1.05] + [1.02] * dip_bars + [1.06] * 3
    highs = [1.0, 1.0, 1.01, 1.02, 1.03, 1.05, 1.06] + [1.04] * dip_bars + [1.05, 1.07, 1.08]
    bars = [_bar(i, h, h, h - 0.01, h) for i, h in enumerate(highs)]
    return r.detect_mm_levels(bars, e13, e50, e200, e800)


def test_mm_levels_count_crosses_and_a_confirmed_reset(monkeypatch):
    monkeypatch.setattr(r, "RESET_MIN_SPACING_BARS", 0)
    lv = _reset_case(dip_bars=r.RESET_MIN_BARS)                # 2h pullback, then trend resumes
    assert lv["direction"] == "bullish" and lv["base_level"] == 2
    assert lv["resets"] == 1 and lv["level"] == 3 and lv["last_reset_confirmed"] is True


def test_mm_levels_ignore_a_one_bar_whipsaw(monkeypatch):
    monkeypatch.setattr(r, "RESET_MIN_SPACING_BARS", 0)
    lv = _reset_case(dip_bars=1)
    assert lv["resets"] == 0 and lv["level"] == 2


def test_mm_levels_ignore_a_reset_too_soon_after_the_last_level():
    lv = _reset_case(dip_bars=r.RESET_MIN_BARS)                # reset ~10 bars after Level 2
    assert lv["resets"] == 0


def _mw_bars(highs, lows):
    return [_bar(i, (h + l) / 2, h, l, (h + l) / 2) for i, (h, l) in enumerate(zip(highs, lows))]


def test_mw_picks_the_most_recent_pattern_not_always_m():
    # an M early in the window, then a W whose 2nd low is the latest swing
    hi = [1.10, 1.11, 1.13, 1.11, 1.10, 1.11, 1.125, 1.11, 1.10, 1.09, 1.08, 1.09, 1.10, 1.09, 1.085, 1.09, 1.10, 1.105]
    lo = [h - 0.01 for h in hi]
    hi = [1.10] * 22 + hi
    lo = [1.09] * 22 + lo
    mw = core.detect_mw_pattern(_mw_bars(hi, lo))
    assert mw["pattern"] == "W"


def test_m_rejects_a_clearly_higher_second_high():
    hi = [1.10] * 30 + [1.10, 1.11, 1.120, 1.11, 1.10, 1.11, 1.1215, 1.11, 1.10, 1.10]   # 2nd high +15 pips
    lo = [h - 0.005 for h in hi]
    mw = core.detect_mw_pattern(_mw_bars(hi, lo))
    assert mw["pattern"] != "M"


def test_mm_level_3_on_50_over_800():
    n = 8
    lv = r.detect_mm_levels([_bar(i, 1, 1, 1, 1) for i in range(n)],
                            [0.9, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1],
                            [1.0] * 4 + [1.06] * 4, [1.05] * n, [1.055] * n)
    assert lv["base_level"] == 3 and lv["resets"] == 0


# ── Trade plan and management ─────────────────────────────────────────────────

def test_trade_plan_sell_stop_5_pips_beyond_anchor_tp_20_and_200_ema():
    p = r.trade_plan(1.1000, "bearish", 1.1010, PIP, adr=60 * PIP, e200=1.0950)
    assert math.isclose(p["sl"], 1.1015) and math.isclose(p["tp1"], 1.0980)
    assert math.isclose(p["break_even_at"], 1.0985) and p["tp2"] == 1.0950
    assert p["rr_tp1"] >= 1.0


def test_trade_plan_never_under_1_to_1_and_30_pips_when_adr_over_100():
    p = r.trade_plan(1.1000, "bullish", 1.0960, PIP, adr=120 * PIP)   # 45-pip risk
    assert math.isclose(p["tp1"] - p["entry"], p["entry"] - p["sl"])
    q = r.trade_plan(1.1000, "bullish", 1.0995, PIP, adr=120 * PIP)
    assert math.isclose(q["tp1"], 1.1030)


def test_manage_trade_break_even_then_stopped_flat():
    p = r.trade_plan(1.1000, "bearish", 1.1010, PIP, adr=60 * PIP)
    after = [_bar(0, 1.0995, 1.0998, 1.0984, 1.0990), _bar(1, 1.0990, 1.1001, 1.0988, 1.0999)]
    m = r.manage_trade(p, "bearish", after, [1.1010, 1.1010])
    assert m["outcome"] == "breakeven" and m["exit"] == 1.1000


def test_manage_trade_exits_on_a_close_back_over_the_13():
    p = r.trade_plan(1.1000, "bearish", 1.1010, PIP, adr=60 * PIP)
    m = r.manage_trade(p, "bearish", [_bar(0, 1.0998, 1.1004, 1.0996, 1.1003)], [1.1001])
    assert m["outcome"] == "exit_13"


def test_manage_trade_tp1():
    p = r.trade_plan(1.1000, "bullish", 1.0990, PIP, adr=60 * PIP)
    m = r.manage_trade(p, "bullish", [_bar(0, 1.1000, 1.1025, 1.0998, 1.1020)], [1.0990])
    assert m["outcome"] == "tp1"


# ── Wiring ────────────────────────────────────────────────────────────────────

def test_three_drive_keeps_shark_fin_trade_direction():
    bars = [_bar(i, 1.1, 1.1, 1.1, 1.1) for i in range(5)]
    res = core.detect_three_drive_pattern(bars, {"detected": True, "direction": "bearish"},
                                          {"detected": False}, {"detected": False}, "EUR/USD")
    assert res["direction"] == "bearish"


def test_analyze_returns_course_fields_and_a_sane_plan():
    t0 = _ts(2026, 10, 26, 0)
    bars, p = [], 1.1000
    for i in range(900):
        p += 0.0004 * math.sin(i / 9.0) + 0.00002
        bars.append(_bar(i, p, p + 0.0006, p - 0.0006, p + 0.0001, t0))
    a = core.analyze(bars, symbol="EUR/USD")
    plan = a["trade_plan"]
    assert {"mm_levels", "session", "cycle"} <= a.keys()
    sell = not a["setup_bullish"]
    assert (plan["sl"] > plan["entry"] > plan["tp1"]) if sell else (plan["sl"] < plan["entry"] < plan["tp1"])
    assert a["kz"] in (None, "London", "NY")
