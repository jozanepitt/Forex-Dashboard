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


def test_confirmed_mw_needs_close_beyond_13_and_tdi():
    bars = [_bar(i, 1.10, 1.101, 1.099, 1.0995) for i in range(5)]
    e13 = [1.1000] * 5
    ts = _tdi([55.0, 55.0, 48.0, 45.0, 44.0], slow=50.0, mid=52.0)
    ok = r.confirm_mw(bars, {"detected": True, "pattern": "M"}, e13, ts)
    assert ok["confirmed"]
    above = r.confirm_mw(bars, {"detected": True, "pattern": "M"}, [1.0990] * 5, ts)
    assert not above["confirmed"] and not above["close_beyond_13"]


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
