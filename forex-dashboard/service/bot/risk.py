"""Position sizing and hard risk limits. No broker calls.

Sizing uses the terminal's tick value so the same function works for FX,
gold, and indices. Lots are floored to the volume step so the order never
risks more than the configured percent.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


# Hard ceilings. load_config and these functions both apply them, so a
# hand-built RiskConfig cannot outrun the cap either.
MAX_RISK_PCT = 2.0
DEFAULT_RISK_PCT = 2.0
MIN_DAILY_LOSS_PCT = 2.0
MAX_DAILY_LOSS_PCT = 6.0
DEFAULT_DAILY_LOSS_PCT = 6.0
PENDING_TTL_SECS = 8 * 3600


def bounded_risk_pct(risk_pct: float) -> float:
    """Risk per trade cannot exceed 2%. A smaller setting is left alone."""
    try:
        value = float(risk_pct)
    except (TypeError, ValueError):
        return 0.0
    if value <= 0:
        return 0.0
    return min(value, MAX_RISK_PCT)


def bounded_daily_loss_pct(pct: float) -> float:
    """Daily loss stays inside 2% to 6% of the day's starting equity."""
    try:
        value = float(pct)
    except (TypeError, ValueError):
        return DEFAULT_DAILY_LOSS_PCT
    return min(MAX_DAILY_LOSS_PCT, max(MIN_DAILY_LOSS_PCT, value))


def exposure_slots(open_positions: int, pending_orders: int) -> int:
    """Resting orders use a slot. A pending buy is not free capacity."""
    return max(0, int(open_positions)) + max(0, int(pending_orders))


def pending_expiration(now_ts: int, valid_until_ts: Optional[int]) -> int:
    """Unix time when a resting order should die.

    The signal's own validity wins when it is still in the future.
    Otherwise the order lives ``PENDING_TTL_SECS`` (8 hours).
    """
    if valid_until_ts is not None and int(valid_until_ts) > int(now_ts):
        return int(valid_until_ts)
    return int(now_ts) + PENDING_TTL_SECS


def day_anchor_equity(equity: float) -> float:
    """The daily-loss baseline is equity at the first look of the UTC day.

    Balance ignores open profit and loss, so a day that starts underwater
    would measure the next loss from a higher number than the account can
    actually spend.
    """
    return float(equity)


def lots_from_risk(
    equity: float,
    risk_pct: float,
    sl_distance: float,
    tick_size: float,
    tick_value: float,
    volume_min: float,
    volume_max: float,
    volume_step: float,
    extra_price: float = 0.0,
    commission_per_lot: float = 0.0,
) -> float:
    """Lots so a stop-out loses about ``equity * risk_pct / 100``.

    ``tick_value`` is account-currency P&L for a one-tick move on 1.0 lot.
    ``extra_price`` is spread plus slippage, added to the stop distance,
    because a buy filled on the ask and stopped on the bid loses that
    much more than the scanner's entry-to-stop gap. ``commission_per_lot``
    is account currency for a round turn on 1.0 lot (0 on Exness Standard).

    Returns 0 when the size would be below the broker minimum (the trade is
    skipped rather than rounded up through the risk budget). Risk above 2%
    is cut to 2% here, not only in the env parser.
    """
    risk_pct = bounded_risk_pct(risk_pct)
    if equity <= 0 or risk_pct <= 0 or sl_distance <= 0:
        return 0.0
    if tick_size <= 0 or tick_value <= 0 or volume_step <= 0:
        return 0.0
    distance = sl_distance + max(float(extra_price), 0.0)
    risk_amount = equity * (risk_pct / 100.0)
    loss_per_lot = (distance / tick_size) * tick_value + max(float(commission_per_lot), 0.0)
    if loss_per_lot <= 0:
        return 0.0
    raw = risk_amount / loss_per_lot
    steps = math.floor((raw / volume_step) + 1e-9)
    lots = steps * volume_step
    # Guard binary dust (0.30000000000000004).
    lots = round(lots, 8)
    if lots < volume_min - 1e-12:
        return 0.0
    if lots > volume_max:
        lots = math.floor((volume_max / volume_step) + 1e-9) * volume_step
        lots = round(lots, 8)
    return lots


def trading_mode_allowed(trade_mode: str, live: bool, confirm_live: bool) -> tuple[bool, str]:
    """Demo and contest accounts run without the live flags.

    A real account, or an explicit live request, needs both
    ``MT5_BOT_LIVE`` and ``MT5_BOT_CONFIRM_LIVE``.
    """
    mode = (trade_mode or "").strip().lower()
    if live and not confirm_live:
        return False, "MT5_BOT_LIVE is set but MT5_BOT_CONFIRM_LIVE is not"
    if mode in ("demo", "contest"):
        if live and confirm_live:
            return True, "live flags set on a demo/contest account; orders still go to that account"
        return True, "demo mode"
    if mode == "real":
        if live and confirm_live:
            return True, "LIVE account: both flags set"
        return False, "real account blocked; set MT5_BOT_LIVE=true and MT5_BOT_CONFIRM_LIVE=true to allow it"
    return False, f"unknown account trade mode {trade_mode!r}"


@dataclass
class RiskSnapshot:
    """Point-in-time inputs for the halt decision. All money in account currency."""

    now_ts: int
    equity: float
    day_start_equity: float
    realized_today: float
    floating: float
    open_positions: int
    trades_today: int
    kill_switch: bool
    halted_day: str  # UTC date already latched, or ""


@dataclass
class RiskConfig:
    risk_pct: float = DEFAULT_RISK_PCT
    max_open_positions: int = 3
    daily_loss_pct: float = DEFAULT_DAILY_LOSS_PCT
    max_trades_per_day: int = 5

    def __post_init__(self) -> None:
        self.risk_pct = bounded_risk_pct(self.risk_pct)
        self.daily_loss_pct = bounded_daily_loss_pct(self.daily_loss_pct)


def utc_day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def evaluate_risk(cfg: RiskConfig, snap: RiskSnapshot) -> tuple[bool, str, str]:
    """Return (allow_new_trade, reason, halted_day).

    ``halted_day`` is the UTC date the daily-loss latch is on. Once the
    day's loss limit is hit it stays hit until the next UTC date, even if
    floating P&L recovers. The kill switch blocks new trades and does not
    by itself flatten positions.
    """
    today = utc_day(snap.now_ts)
    halted = snap.halted_day
    if halted and halted != today:
        halted = ""
    if snap.kill_switch:
        return False, "kill switch", halted
    if halted == today:
        return False, "daily loss limit already hit", halted
    day_base = snap.day_start_equity if snap.day_start_equity > 0 else snap.equity
    if day_base <= 0:
        return False, "no equity", halted
    pnl_today = snap.realized_today + snap.floating
    limit = -abs(day_base * (cfg.daily_loss_pct / 100.0))
    if pnl_today <= limit:
        return False, "daily loss limit hit", today
    if snap.open_positions >= cfg.max_open_positions:
        return False, "max open positions", halted
    if snap.trades_today >= cfg.max_trades_per_day:
        return False, "max trades today", halted
    return True, "ok", halted


def spread_allows(spread_price: float, typical_spread: float, mult: float) -> bool:
    """True when the live spread is within ``mult`` times the assumed typical."""
    if spread_price < 0 or typical_spread <= 0 or mult <= 0:
        return False
    return spread_price <= typical_spread * mult + 1e-12
