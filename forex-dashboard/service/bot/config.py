"""Environment configuration for the execution bot.

Reads ``service/.env`` when it exists. Real credentials stay there and are
gitignored. See ``service/.env.example`` for the keys.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

SERVICE_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(SERVICE_ROOT / ".env")


def _flag(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


@dataclass
class BotConfig:
    login: str
    password: str
    server: str
    symbol_suffix: str
    live: bool
    confirm_live: bool
    strategy: str
    timeframe: str
    entry_mode: str
    symbols: list[str]
    grades: str
    risk_pct: float
    max_open_positions: int
    daily_loss_pct: float
    max_trades_per_day: int
    max_spread_mult: float
    session_filter: bool
    session_start_utc: int
    session_end_utc: int
    news_filter: bool
    news_window_min: int
    kill_switch: bool
    kill_close: bool
    magic: int
    deviation_points: int
    poll_seconds: int
    log_path: Path
    state_path: Path
    kill_path: Path

    def kill_tripped(self) -> bool:
        return self.kill_switch or self.kill_path.exists()


def load_config() -> BotConfig:
    symbols = [s.strip() for s in os.environ.get("BOT_SYMBOLS", "EUR/USD").split(",") if s.strip()]
    return BotConfig(
        login=os.environ.get("MT5_LOGIN", "").strip(),
        password=os.environ.get("MT5_PASSWORD", "").strip(),
        server=os.environ.get("MT5_SERVER", "").strip(),
        symbol_suffix=os.environ.get("MT5_SYMBOL_SUFFIX", "m").strip(),
        live=_flag("MT5_BOT_LIVE"),
        confirm_live=_flag("MT5_BOT_CONFIRM_LIVE"),
        strategy=os.environ.get("BOT_STRATEGY", "").strip(),
        timeframe=os.environ.get("BOT_TIMEFRAME", "M15").strip().upper(),
        entry_mode=os.environ.get("BOT_ENTRY", "close").strip().lower(),
        symbols=symbols,
        grades=os.environ.get("BOT_GRADES", "alert").strip(),
        risk_pct=_float("BOT_RISK_PCT", 0.5),
        max_open_positions=_int("BOT_MAX_OPEN_POSITIONS", 3),
        daily_loss_pct=_float("BOT_DAILY_LOSS_PCT", 2.0),
        max_trades_per_day=_int("BOT_MAX_TRADES_PER_DAY", 5),
        max_spread_mult=_float("BOT_MAX_SPREAD_MULT", 2.0),
        session_filter=_flag("BOT_SESSION_FILTER", "true"),
        session_start_utc=_int("BOT_SESSION_START_UTC", 7),
        session_end_utc=_int("BOT_SESSION_END_UTC", 20),
        news_filter=_flag("BOT_NEWS_FILTER", "false"),
        news_window_min=_int("BOT_NEWS_WINDOW_MIN", 30),
        kill_switch=_flag("BOT_KILL_SWITCH", "false"),
        kill_close=_flag("BOT_KILL_CLOSE", "false"),
        magic=_int("BOT_MAGIC", 2601801),
        deviation_points=_int("BOT_DEVIATION_POINTS", 30),
        poll_seconds=_int("BOT_POLL_SECONDS", 20),
        log_path=SERVICE_ROOT / "bot" / "logs" / "trades.jsonl",
        state_path=SERVICE_ROOT / "bot" / "state.json",
        kill_path=SERVICE_ROOT / "bot" / "KILL_SWITCH",
    )
