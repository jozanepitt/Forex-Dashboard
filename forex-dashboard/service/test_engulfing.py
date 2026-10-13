"""Tests for the trend-following Engulfing scanner (engulfing_strategy.py).

Rules being pinned (from the user's rule cards, 2026-10-13):
  * an engulfing is two candles of opposite colour where the second's body
    completely covers the first's (a bigger candle swallowing a smaller one)
  * Rule 1 follow the trend - buy bullish engulfings in an uptrend, sell
    bearish ones in a downtrend
  * Rule 2 the trend comes from the higher timeframe (H4)
  * Rule 3 stop at the wick of the engulfing candle
  * Rule 4 entry at the next candle's open, or 25% / 50% back into the candle
  * Rule 5 an engulfing AGAINST the trend needs a second one to confirm it
"""
from __future__ import annotations

import engulfing_strategy as e

STEP = 900
T0 = 1_790_000_000 - 1_790_000_000 % 900


def _bar(i, o, h, l, c, v=100.0):
    return {"ts_utc": T0 + i * STEP, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _trend_bars(n, start, step):
    """n steadily trending bars (used for the H4 trend and as M15 lead-in)."""
    out, p = [], start
    for i in range(n):
        o, c = p, p + step
        out.append(_bar(i, o, max(o, c) + abs(step) * 0.3, min(o, c) - abs(step) * 0.3, c))
        p = c
    return out


def _lead_in(n=60, start=1.1000, step=0.0002):
    return _trend_bars(n, start, step)


def _with_pattern(lead, prev, cur):
    """Append a (prev, cur) candle pair, given as (open, high, low, close)."""
    i = len(lead)
    return lead + [_bar(i, *prev), _bar(i + 1, *cur)]


UP_H4 = _trend_bars(120, 1.0500, 0.0010)
DOWN_H4 = _trend_bars(120, 1.2500, -0.0010)
FLAT_H4 = [_bar(i, 1.1000, 1.1006, 1.0994, 1.1000 + 0.0002 * (i % 2)) for i in range(120)]

# small red candle, then a larger green one whose body covers it completely
BULL_PREV = (1.1120, 1.1123, 1.1108, 1.1110)
BULL_CUR = (1.1110, 1.1136, 1.1104, 1.1132)
# the mirror image
BEAR_PREV = (1.1110, 1.1122, 1.1107, 1.1120)
BEAR_CUR = (1.1120, 1.1126, 1.1094, 1.1098)


# ── the pattern itself ───────────────────────────────────────────────────────

def test_bullish_engulfing_is_recognised():
    assert e.engulfing_kind(_bar(0, *BULL_PREV), _bar(1, *BULL_CUR)) == "bullish"


def test_bearish_engulfing_is_recognised():
    assert e.engulfing_kind(_bar(0, *BEAR_PREV), _bar(1, *BEAR_CUR)) == "bearish"


def test_same_colour_candles_are_not_an_engulfing():
    assert e.engulfing_kind(_bar(0, 1.1100, 1.1112, 1.1098, 1.1110), _bar(1, *BULL_CUR)) is None


def test_second_candle_must_cover_the_whole_first_body():
    partial = (1.1110, 1.1120, 1.1104, 1.1116)             # closes inside the red body (open 1.1120)
    assert e.engulfing_kind(_bar(0, *BULL_PREV), _bar(1, *partial)) is None


def test_a_doji_first_candle_is_not_engulfed():
    doji = (1.1110, 1.1118, 1.1102, 1.1110)
    assert e.engulfing_kind(_bar(0, *doji), _bar(1, *BULL_CUR)) is None


def test_full_engulf_flag_means_wicks_are_covered_too():
    assert e.covers_wicks(_bar(0, *BULL_PREV), _bar(1, *BULL_CUR)) is True
    wide_prev = (1.1120, 1.1140, 1.1100, 1.1110)           # long wicks poke outside the green candle
    assert e.engulfing_kind(_bar(0, *wide_prev), _bar(1, *BULL_CUR)) == "bullish"
    assert e.covers_wicks(_bar(0, *wide_prev), _bar(1, *BULL_CUR)) is False


# ── trade rules ──────────────────────────────────────────────────────────────

def test_bullish_engulfing_in_an_uptrend_is_a_buy():
    bars = _with_pattern(_lead_in(), BULL_PREV, BULL_CUR)
    row = e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="M15")
    assert row["setup"] == "BUY" and row["with_trend"] is True
    assert row["trend"] == "bullish"
    assert row["entry"] == 1.1132                           # option 1: the new candle opens here
    assert row["sl"] < 1.1104                               # below the engulfing candle's wick
    # Rule 4 card: 25% and 50% are measured on the whole engulfing candle
    # (high 1.1136 to low 1.1104), down from its top.
    assert abs(row["entry_25"] - (1.1136 - 0.25 * (1.1136 - 1.1104))) < 1e-9
    assert abs(row["entry_50"] - (1.1136 - 0.50 * (1.1136 - 1.1104))) < 1e-9
    risk = row["entry"] - row["sl"]
    assert abs(row["tp1"] - (row["entry"] + e.TARGET_RR * risk)) < 1e-9
    assert row["signal_ts"] == bars[-1]["ts_utc"]
    assert row["validated"] is False


def test_bearish_engulfing_in_a_downtrend_is_a_sell():
    bars = _with_pattern(_lead_in(step=-0.0002, start=1.1240), BEAR_PREV, BEAR_CUR)
    row = e.analyze_timeframe("EUR/USD", bars, DOWN_H4, timeframe="M15")
    assert row["setup"] == "SELL" and row["with_trend"] is True
    assert row["sl"] > 1.1126                               # above the engulfing candle's wick
    assert row["tp1"] < row["entry"] < row["sl"]
    assert row["entry_25"] > row["entry"] and row["entry_50"] > row["entry_25"]


def test_engulfing_against_the_trend_waits_for_a_second_one():
    bars = _with_pattern(_lead_in(), BEAR_PREV, BEAR_CUR)
    row = e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="M15")
    assert row["setup"] == "NO-TRADE"
    assert row["state"] == "WAIT-CONFIRM"
    assert row["pattern"] == "bearish"


def test_second_engulfing_against_the_trend_confirms_the_first():
    bars = _with_pattern(_lead_in(), BEAR_PREV, BEAR_CUR)
    # a few quiet bars, then a second bearish engulfing lower down
    p = bars[-1]["close"]
    for k in range(3):
        i = len(bars)
        bars.append(_bar(i, p, p + 0.0003, p - 0.0003, p + 0.0001))      # small green drift
        p = bars[-1]["close"]
    bars = _with_pattern(bars, (p, p + 0.0011, p - 0.0002, p + 0.0009), (p + 0.0009, p + 0.0013, p - 0.0018, p - 0.0014))
    row = e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="M15")
    assert row["setup"] == "SELL"
    assert row["with_trend"] is False and row["confirmed_counter"] is True
    assert row["grade"] != "A"


def test_no_clear_higher_timeframe_trend_is_no_trade(monkeypatch):
    monkeypatch.setattr(e, "_htf_bias", lambda h4: "neutral")
    bars = _with_pattern(_lead_in(), BULL_PREV, BULL_CUR)
    row = e.analyze_timeframe("EUR/USD", bars, FLAT_H4, timeframe="M15")
    assert row["setup"] == "NO-TRADE"
    assert row["state"] == "NO-TREND"


def test_no_pattern_on_the_newest_candle_is_no_trade():
    row = e.analyze_timeframe("EUR/USD", _lead_in(), UP_H4, timeframe="M15")
    assert row["setup"] == "NO-TRADE" and row["state"] == "NO-PATTERN"
    assert row["trend"] == "bullish"


def test_only_the_newest_closed_candle_counts():
    bars = _with_pattern(_lead_in(), BULL_PREV, BULL_CUR)
    p = bars[-1]["close"]
    bars.append(_bar(len(bars), p, p + 0.0003, p - 0.0002, p + 0.0002))
    assert e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="M15")["setup"] == "NO-TRADE"


def test_a_tiny_engulfing_candle_is_ignored():
    prev = (1.11200, 1.11203, 1.11196, 1.11198)
    cur = (1.11198, 1.11206, 1.11195, 1.11204)             # under a pip of body in a 6-pip-range market
    bars = _with_pattern(_lead_in(), prev, cur)
    row = e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="M15")
    assert row["setup"] == "NO-TRADE"


def test_insufficient_data_is_no_trade():
    assert e.analyze_timeframe("EUR/USD", _lead_in(5), UP_H4, timeframe="M15")["state"] == "NO-DATA"
    assert e.analyze_timeframe("EUR/USD", [], None, timeframe="M15")["setup"] == "NO-TRADE"


def _pullback_lead():
    """40 quiet candles near 1.1130, then four red ones stepping down to 1.1120:
    the engulfing pair that follows makes the lowest low of the last 20 candles."""
    bars = [_bar(i, 1.1130, 1.1133, 1.1127, 1.1131 if i % 2 else 1.1129) for i in range(40)]
    p = 1.1130
    for c in (1.1126, 1.1123, 1.1121, 1.1120):
        bars.append(_bar(len(bars), p, p + 0.0001, c - 0.0002, c))
        p = c
    return bars


def test_strength_follows_where_the_candle_closes():
    """From the user's strong/weak cards: weak closes inside the previous
    candle's wick, strong closes beyond it, very strong has a body that covers
    the previous candle wick to wick."""
    prev = _bar(0, *BULL_PREV)                                 # high 1.1123, low 1.1108
    assert e.engulf_strength(prev, _bar(1, 1.1110, 1.1124, 1.1104, 1.1122)) == "weak"
    assert e.engulf_strength(prev, _bar(1, *BULL_CUR)) == "strong"          # closes 1.1132 > 1.1123
    assert e.engulf_strength(prev, _bar(1, 1.1107, 1.1136, 1.1104, 1.1132)) == "very_strong"
    bprev = _bar(0, *BEAR_PREV)                                # high 1.1122, low 1.1107
    assert e.engulf_strength(bprev, _bar(1, 1.1120, 1.1126, 1.1106, 1.1108)) == "weak"
    assert e.engulf_strength(bprev, _bar(1, *BEAR_CUR)) == "strong"         # closes 1.1098 < 1.1107


def test_swing_extreme_means_the_candle_made_the_20_bar_low_or_high():
    bars = _with_pattern(_pullback_lead(), BULL_PREV, BULL_CUR)
    assert e.at_swing_extreme(bars, "bullish") is True
    assert e.at_swing_extreme(_with_pattern(_lead_in(), BULL_PREV, BULL_CUR), "bullish") is False
    down = _with_pattern(_lead_in(step=-0.0002, start=1.1240), BEAR_PREV, BEAR_CUR)
    assert e.at_swing_extreme(down, "bearish") is False          # falling market: not the 20-candle high


def test_grade_a_is_trend_plus_strong_close_plus_swing_extreme():
    """User decision 2026-10-13. The one variant that tested above zero:
    H1 +0.07R over 259, H4 +0.11R over 191 (gross)."""
    a = e.analyze_timeframe("EUR/USD", _with_pattern(_pullback_lead(), BULL_PREV, BULL_CUR), UP_H4, timeframe="H1")
    assert a["setup"] == "BUY" and a["grade"] == "A"
    assert a["strength"] == "strong" and a["at_extreme"] is True

    # strong close, with the trend, but in the middle of a run: not at a swing low
    b = e.analyze_timeframe("EUR/USD", _with_pattern(_lead_in(), BULL_PREV, BULL_CUR), UP_H4, timeframe="H1")
    assert b["setup"] == "BUY" and b["grade"] == "B" and b["at_extreme"] is False

    # at the swing low and with the trend, but the close stays inside the previous wick
    weak_cur = (1.1110, 1.1124, 1.1104, 1.1122)
    c = e.analyze_timeframe("EUR/USD", _with_pattern(_pullback_lead(), BULL_PREV, weak_cur), UP_H4, timeframe="H1")
    assert c["setup"] == "BUY" and c["grade"] == "B" and c["strength"] == "weak"


# ── pair / universe ──────────────────────────────────────────────────────────

UP_D1 = _trend_bars(120, 1.0000, 0.0030)


def test_analyze_pair_runs_h1_against_h4_and_h4_against_daily():
    h1 = _with_pattern(_lead_in(), BULL_PREV, BULL_CUR)
    row = e.analyze_pair("EUR/USD", h1, h4_candles=UP_H4, d1_candles=UP_D1)
    assert row["symbol"] == "EUR/USD"
    assert row["setup"] == "BUY" and row["timeframe"] == "H1"
    assert set(row["legs"]) == {"H1", "H4"}
    assert row["legs"]["H1"]["setup"] == "BUY" and "H4 bullish trend" in row["legs"]["H1"]["notes"]
    assert row["legs"]["H4"]["setup"] == "NO-TRADE" and row["legs"]["H4"]["trend"] == "bullish"


def test_h4_leg_signals_with_the_daily_trend():
    h4 = _with_pattern(_trend_bars(80, 1.1000, 0.0002), BULL_PREV, BULL_CUR)
    row = e.analyze_pair("EUR/USD", _lead_in(), h4_candles=h4, d1_candles=UP_D1)
    assert row["legs"]["H1"]["setup"] == "NO-TRADE"
    assert row["legs"]["H4"]["setup"] == "BUY"
    assert row["setup"] == "BUY" and row["timeframe"] == "H4"          # surfaced when H1 has nothing
    assert "D1 bullish trend" in row["legs"]["H4"]["notes"]


def test_analyze_universe_shape():
    h1 = _with_pattern(_lead_in(), BULL_PREV, BULL_CUR)
    res = e.analyze_universe({"EUR/USD": {"1h": h1, "4h": UP_H4, "1d": UP_D1}})
    assert len(res["pairs"]) == len(e.ENGULF_UNIVERSE)
    assert res["buys"] == 1 and res["sells"] == 0
    assert all("setup" in p for p in res["pairs"])


def test_universe_is_every_dashboard_pair_plus_oil():
    from config import PRIORITY_PAIRS
    u = e.ENGULF_UNIVERSE
    assert u[:len(PRIORITY_PAIRS)] == list(PRIORITY_PAIRS)   # forex, metals, indices, crypto
    assert "XAU/USD" in u and "XAG/USD" in u
    for back in ("US30", "USTEC", "DE30", "DXY", "BTC/USD", "ETH/USD"):
        assert back in u
    assert u[-2:] == ["USOIL", "UKOIL"]
    assert len(u) == len(set(u)) == len(PRIORITY_PAIRS) + 2


def test_oil_symbols_have_instrument_specs():
    import instruments
    for sym in e.EXTRA_SYMBOLS:
        assert instruments.spec(sym) is not None
        assert instruments.asset_class(sym) == "energy"
        assert instruments.pip_size(sym, 70.0) == 0.01


# ── Discord alert gate ───────────────────────────────────────────────────────

import alerts  # noqa: E402


def _alert_harness(monkeypatch, enabled=True, a_only=True):
    sent = []
    monkeypatch.setattr(alerts, "ENGULF_ALERTS_ENABLED", enabled)
    monkeypatch.setattr(alerts, "ENGULF_GRADE_A_ONLY", a_only)
    monkeypatch.setattr(alerts, "ENGULF_NEWS_FILTER", False)
    monkeypatch.setattr(alerts, "_post_discord", lambda embed: (sent.append(embed), True)[1])
    monkeypatch.setattr(alerts, "_is_throttled", lambda pair, rule: False)
    monkeypatch.setattr(alerts, "_mark_sent", lambda pair, rule: None)
    return sent


def _pair_row(prev=BULL_PREV, cur=BULL_CUR):
    return e.analyze_pair("EUR/USD", _with_pattern(_pullback_lead(), prev, cur), h4_candles=UP_H4, d1_candles=UP_D1)


def test_alerts_default_on_and_grade_a_only_in_config():
    """User decision 2026-10-13: send grade A engulfing setups to Discord."""
    import config
    src = open(config.__file__, encoding="utf-8").read()
    assert 'os.environ.get("ENGULF_ALERTS_ENABLED", "true")' in src
    assert 'os.environ.get("ENGULF_GRADE_A_ONLY", "true")' in src


def test_no_alert_when_disabled(monkeypatch):
    sent = _alert_harness(monkeypatch, enabled=False)
    alerts.alert_engulfing_setup("EUR/USD", _pair_row())
    assert sent == []


def test_grade_a_alerts_and_says_unvalidated(monkeypatch):
    sent = _alert_harness(monkeypatch)
    row = _pair_row()
    assert row["grade"] == "A"
    alerts.alert_engulfing_setup("EUR/USD", row)
    assert len(sent) == 1
    assert "Engulfing [H1] BUY" in sent[0]["title"]
    assert "unvalidated" in sent[0]["footer"]["text"].lower()


def test_grade_b_blocked_in_a_only_mode(monkeypatch):
    sent = _alert_harness(monkeypatch)
    row = _pair_row(prev=(1.1120, 1.1140, 1.1100, 1.1110))
    assert row["grade"] == "B"
    alerts.alert_engulfing_setup("EUR/USD", row)
    assert sent == []


def test_no_trade_row_never_alerts(monkeypatch):
    sent = _alert_harness(monkeypatch)
    alerts.alert_engulfing_setup("EUR/USD", e.analyze_pair("EUR/USD", _lead_in(), h4_candles=UP_H4, d1_candles=UP_D1))
    assert sent == []


def test_retrace_entries_never_sit_beyond_the_close():
    """A long upper wick would put the 25% line above where price closed; a buy
    limit there is just a market order, so it is capped at the close."""
    cur = (1.1110, 1.1160, 1.1104, 1.1132)                  # big upper wick
    bars = _with_pattern(_lead_in(), BULL_PREV, cur)
    row = e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="M15")
    assert row["setup"] == "BUY"
    assert row["entry_25"] == row["entry"] == 1.1132
    assert row["entry_50"] == 1.1132                          # midpoint 1.1132 = the close
    assert row["sl"] < row["entry_50"]


# ── Rule 2, first half: the higher timeframe shows an engulfing too ──────────
# Card 5: "Higher timeframe (H4 or Daily): find the trend AND look for an
# engulfing setup. Lower timeframe: then look for a precise entry in the same
# direction." Reported as row["htf_engulfing"].

def _h4_with_engulfing(kind, bars_ago=1):
    h4 = [dict(b) for b in UP_H4] if kind == "bullish" else [dict(b) for b in DOWN_H4]
    i = len(h4) - 1 - bars_ago
    p = h4[i - 1]["close"]
    if kind == "bullish":
        h4[i - 1].update(open=p + 0.0010, high=p + 0.0012, low=p - 0.0002, close=p)          # red
        h4[i].update(open=p, high=p + 0.0026, low=p - 0.0004, close=p + 0.0024)              # green, covers it
    else:
        h4[i - 1].update(open=p - 0.0010, high=p + 0.0002, low=p - 0.0012, close=p)
        h4[i].update(open=p, high=p + 0.0004, low=p - 0.0026, close=p - 0.0024)
    return h4


def test_recent_h4_engulfing_is_detected():
    assert e.recent_htf_engulfing(_h4_with_engulfing("bullish", bars_ago=0), "bullish") is True
    assert e.recent_htf_engulfing(_h4_with_engulfing("bullish", bars_ago=2), "bullish") is True
    assert e.recent_htf_engulfing(_h4_with_engulfing("bullish", bars_ago=2), "bearish") is False
    assert e.recent_htf_engulfing(UP_H4, "bullish") is False                  # steady trend, no engulfing
    assert e.recent_htf_engulfing(_h4_with_engulfing("bullish", bars_ago=e.HTF_ENGULF_LOOKBACK + 2), "bullish") is False
    assert e.recent_htf_engulfing(None, "bullish") is False


def test_row_reports_whether_the_h4_had_its_own_engulfing():
    bars = _with_pattern(_lead_in(), BULL_PREV, BULL_CUR)
    assert e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="M15")["htf_engulfing"] is False
    row = e.analyze_timeframe("EUR/USD", bars, _h4_with_engulfing("bullish"), timeframe="M15")
    assert row["setup"] == "BUY" and row["htf_engulfing"] is True


def test_the_same_candle_never_alerts_twice(monkeypatch):
    """An H4 signal sits in the cache for four hours and the scanner runs every
    15 minutes; the one-hour throttle alone would re-send it."""
    sent = _alert_harness(monkeypatch)
    alerts._engulf_last_alerted.clear()
    row = _pair_row()
    alerts.alert_engulfing_setup("EUR/USD", row)
    alerts.alert_engulfing_setup("EUR/USD", row)
    assert len(sent) == 1
    alerts._engulf_last_alerted.clear()


# ── Rule 5 must look like the rule card (2026-10-13) ─────────────────────────
# Card 8 draws the two opposite engulfings close together at the turn: the
# first, a small bounce, then the second. Seen live: USTEC H4 "confirmed" a
# sell with a first engulfing two days and 600 points earlier, and gold H1
# "confirmed" one although price had since made a new high above the first.

def _counter_series(gap_bars, drift, second_shift=0.0):
    """Uptrend lead-in, a bearish engulfing, `gap_bars` small candles moving
    `drift` each, then a second bearish engulfing."""
    bars = _with_pattern(_lead_in(), BEAR_PREV, BEAR_CUR)
    p = bars[-1]["close"]
    for _ in range(gap_bars):
        i = len(bars)
        c = p + drift
        o = p
        if drift < 0:                      # keep falling bars from engulfing anything: tiny equal bodies
            bars.append(_bar(i, o, o + 0.0002, c - 0.0002, c))
        else:
            bars.append(_bar(i, o, c + 0.0002, o - 0.0002, c))
        p = c
    p += second_shift
    return _with_pattern(bars, (p, p + 0.0011, p - 0.0002, p + 0.0009), (p + 0.0009, p + 0.0013, p - 0.0018, p - 0.0014))


def test_second_engulfing_close_to_the_first_confirms():
    row = e.analyze_timeframe("EUR/USD", _counter_series(3, +0.0001), UP_H4, timeframe="H1")
    assert row["setup"] == "SELL" and row["confirmed_counter"] is True


def test_second_engulfing_after_price_already_ran_away_does_not_confirm():
    # eight candles sliding 15 pips each: the second pattern forms 120 pips below the first
    bars = _counter_series(5, -0.0015)
    row = e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="H1")
    assert row["setup"] == "NO-TRADE" and row["state"] == "WAIT-CONFIRM"


def test_first_engulfing_is_void_once_price_trades_back_above_it():
    # price climbs back over the first engulfing candle's high (1.1126) before the second forms
    bars = _counter_series(4, +0.0012)
    row = e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="H1")
    assert row["setup"] == "NO-TRADE" and row["state"] == "WAIT-CONFIRM"


def test_first_engulfing_too_long_ago_does_not_confirm():
    bars = _counter_series(e.CONFIRM_LOOKBACK + 2, +0.00002)
    row = e.analyze_timeframe("EUR/USD", bars, UP_H4, timeframe="H1")
    assert row["setup"] == "NO-TRADE" and row["state"] == "WAIT-CONFIRM"


def test_confirm_lookback_matches_the_card():
    assert e.CONFIRM_LOOKBACK == 8
