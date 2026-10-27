"""Position sizing and the hard risk limits. No broker, no network."""
from bot.filters import in_session, news_blocks
from bot.risk import (
    RiskConfig,
    RiskSnapshot,
    day_anchor_equity,
    evaluate_risk,
    exposure_slots,
    lots_from_risk,
    pending_expiration,
    spread_allows,
    trading_mode_allowed,
)


def test_eurusd_lots_floor_to_step_and_match_half_percent():
    # 1.0 lot, tick 0.00001, tick value $1 => $10 per pip (0.0001).
    # Stop 20 pips = 0.0020. Loss per lot = 0.0020/0.00001 * 1 = $200.
    # Equity 10_000, 0.5% = $50. Lots = 0.25.
    lots = lots_from_risk(
        equity=10_000, risk_pct=0.5, sl_distance=0.0020,
        tick_size=0.00001, tick_value=1.0,
        volume_min=0.01, volume_max=100, volume_step=0.01,
    )
    assert lots == 0.25


def test_lots_never_round_up_through_the_risk_budget():
    # Raw size would be 0.257 lots. Floor to 0.25, not 0.26.
    lots = lots_from_risk(
        equity=10_280, risk_pct=0.5, sl_distance=0.0020,
        tick_size=0.00001, tick_value=1.0,
        volume_min=0.01, volume_max=100, volume_step=0.01,
    )
    assert lots == 0.25


def test_below_minimum_lot_is_skipped():
    lots = lots_from_risk(
        equity=100, risk_pct=0.5, sl_distance=0.0020,
        tick_size=0.00001, tick_value=1.0,
        volume_min=0.01, volume_max=100, volume_step=0.01,
    )
    assert lots == 0.0


def test_usdjpy_uses_tick_value_not_a_fx_pip_guess():
    # Stop of 0.20 (20 pips). Tick 0.001 worth $0.67. Loss per lot = 200 * 0.67.
    lots = lots_from_risk(
        equity=10_000, risk_pct=0.5, sl_distance=0.20,
        tick_size=0.001, tick_value=0.67,
        volume_min=0.01, volume_max=50, volume_step=0.01,
    )
    assert lots == 0.37  # 50 / 134 = 0.373, floored


def test_gold_contract_via_tick_value():
    # $5 stop. Tick 0.01 worth $1 (100 oz * $0.01). Loss per lot = $500.
    # $50 risk -> 0.10 lots.
    lots = lots_from_risk(
        equity=10_000, risk_pct=0.5, sl_distance=5.0,
        tick_size=0.01, tick_value=1.0,
        volume_min=0.01, volume_max=20, volume_step=0.01,
    )
    assert lots == 0.10


def test_demo_allowed_without_live_flags_real_is_not():
    ok, _ = trading_mode_allowed("demo", live=False, confirm_live=False)
    assert ok
    ok, _ = trading_mode_allowed("contest", live=False, confirm_live=False)
    assert ok
    ok, reason = trading_mode_allowed("real", live=False, confirm_live=False)
    assert not ok and "real" in reason
    ok, reason = trading_mode_allowed("real", live=True, confirm_live=False)
    assert not ok and "CONFIRM" in reason
    ok, _ = trading_mode_allowed("real", live=True, confirm_live=True)
    assert ok


def _snap(**kwargs) -> RiskSnapshot:
    base = dict(
        now_ts=1_700_000_000, equity=10_000, day_start_equity=10_000,
        realized_today=0, floating=0, open_positions=0, trades_today=0,
        kill_switch=False, halted_day="",
    )
    base.update(kwargs)
    return RiskSnapshot(**base)


def test_daily_loss_latches_for_the_rest_of_the_utc_day():
    cfg = RiskConfig(risk_pct=0.5, max_open_positions=3, daily_loss_pct=2.0, max_trades_per_day=5)
    # 2% of 10_000 = 200. Floating -200 trips it.
    ok, reason, halted = evaluate_risk(cfg, _snap(floating=-200))
    assert not ok and halted
    ok, reason, halted = evaluate_risk(cfg, _snap(floating=0, halted_day=halted))
    assert not ok and "already" in reason


def test_max_positions_and_max_trades():
    cfg = RiskConfig(0.5, 3, 2.0, 5)
    ok, reason, _ = evaluate_risk(cfg, _snap(open_positions=3))
    assert not ok and "positions" in reason
    ok, reason, _ = evaluate_risk(cfg, _snap(trades_today=5))
    assert not ok and "trades" in reason


def test_kill_switch_blocks_new_trades():
    cfg = RiskConfig(0.5, 3, 2.0, 5)
    ok, reason, _ = evaluate_risk(cfg, _snap(kill_switch=True))
    assert not ok and "kill" in reason


def test_risk_above_two_percent_is_cut_to_two():
    # 20 pip stop, $200 loss per lot. 2% of 10_000 is $200, so 1.00 lot.
    # A requested 5% would have been 2.50 lots.
    hot = lots_from_risk(
        10_000, 5.0, 0.0020, 0.00001, 1.0, 0.01, 100, 0.01,
    )
    capped = lots_from_risk(
        10_000, 2.0, 0.0020, 0.00001, 1.0, 0.01, 100, 0.01,
    )
    assert hot == capped == 1.0
    assert RiskConfig(risk_pct=5).risk_pct == 2.0


def test_daily_loss_stays_inside_two_to_six_percent():
    loose = RiskConfig(daily_loss_pct=10)
    assert loose.daily_loss_pct == 6.0
    ok, _, _ = evaluate_risk(loose, _snap(floating=-599))
    assert ok
    ok, reason, _ = evaluate_risk(loose, _snap(floating=-600))
    assert not ok and "daily" in reason
    tight = RiskConfig(daily_loss_pct=0.5)
    assert tight.daily_loss_pct == 2.0
    ok, _, _ = evaluate_risk(tight, _snap(floating=-100))
    assert ok


def test_three_pip_stop_includes_spread_and_commission():
    # 3 pips = 0.00030. Spread 1.1 + slip 0.2 = 0.00013. Distance 0.00043.
    # $1 per 0.00001 tick => $43 per lot. 0.5% of 10_000 = $50. Lots = 1.16.
    lots = lots_from_risk(
        10_000, 0.5, 0.00030, 0.00001, 1.0, 0.01, 100, 0.01,
        extra_price=0.00013,
    )
    assert lots == 1.16
    naive = lots_from_risk(
        10_000, 0.5, 0.00030, 0.00001, 1.0, 0.01, 100, 0.01,
    )
    assert naive == 1.66
    # The old size loses 1.66 lots * $43 = $71, about 40% over the $50 budget.
    assert naive * 43 > 50 * 1.4
    # $10 round-turn commission on top of the 20-pip stop ($200) => $210/lot.
    # $50 / 210 = 0.238, floored to 0.23.
    with_commission = lots_from_risk(
        10_000, 0.5, 0.0020, 0.00001, 1.0, 0.01, 100, 0.01,
        commission_per_lot=10.0,
    )
    assert with_commission == 0.23


def test_pending_orders_use_a_slot_and_expire():
    assert exposure_slots(2, 1) == 3
    cfg = RiskConfig(0.5, 3, 2.0, 5)
    ok, reason, _ = evaluate_risk(cfg, _snap(open_positions=exposure_slots(2, 1)))
    assert not ok and "positions" in reason
    assert pending_expiration(1_000, 5_000) == 5_000
    assert pending_expiration(1_000, 500) == 1_000 + 8 * 3600
    assert pending_expiration(1_000, None) == 1_000 + 8 * 3600


def test_day_anchor_is_equity_not_balance():
    assert day_anchor_equity(9_800) == 9_800


def test_config_ceilings_and_defaults(monkeypatch):
    from bot.config import load_config
    monkeypatch.setenv("BOT_RISK_PCT", "5")
    monkeypatch.setenv("BOT_DAILY_LOSS_PCT", "10")
    cfg = load_config()
    assert cfg.risk_pct == 2.0
    assert cfg.daily_loss_pct == 6.0
    monkeypatch.setenv("BOT_DAILY_LOSS_PCT", "1")
    cfg = load_config()
    assert cfg.daily_loss_pct == 2.0
    monkeypatch.delenv("BOT_RISK_PCT", raising=False)
    monkeypatch.delenv("BOT_DAILY_LOSS_PCT", raising=False)
    cfg = load_config()
    assert cfg.risk_pct == 2.0
    assert cfg.daily_loss_pct == 6.0


def test_spread_filter_and_session_and_news():
    assert spread_allows(0.00015, 0.00011, 2.0)
    assert not spread_allows(0.00030, 0.00011, 2.0)
    from datetime import datetime, timezone
    noon = int(datetime(2024, 6, 3, 12, 0, tzinfo=timezone.utc).timestamp())
    assert in_session(noon, 7, 20)
    night = int(datetime(2024, 6, 3, 22, 0, tzinfo=timezone.utc).timestamp())
    assert not in_session(night, 7, 20)
    events = [{"ts": noon, "currency": "USD", "impact": "high"}]
    assert news_blocks("EUR/USD", noon, events, 30)
    assert not news_blocks("EUR/GBP", noon, events, 30)
    assert not news_blocks("EUR/USD", noon, [{"ts": noon, "currency": "USD", "impact": "low"}], 30)
