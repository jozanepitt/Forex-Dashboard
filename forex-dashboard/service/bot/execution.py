"""MetaTrader 5 order gateway.

Imported only when the bot runs. The MetaTrader5 package exists on Windows
with the terminal installed; this module fails with a clear error anywhere
else. Login, password, and server come from the environment and are never
written to the trade log.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from bot.config import BotConfig

log = logging.getLogger("bot.execution")

_TF = {
    "M1": "1min", "M5": "5min", "M15": "15min",
    "H1": "1h", "H4": "4h", "D1": "1day",
}


class MT5Gateway:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.mt5 = None
        self.connected = False

    def connect(self) -> dict:
        try:
            import MetaTrader5 as mt5
        except ImportError as exc:
            raise RuntimeError(
                "MetaTrader5 is not installed. The bot runs on Windows, in the "
                "Python that can see the open Exness MT5 terminal. "
                "pip install MetaTrader5"
            ) from exc
        self.mt5 = mt5
        if self.cfg.login and self.cfg.password and self.cfg.server:
            ok = mt5.initialize(
                login=int(self.cfg.login),
                password=self.cfg.password,
                server=self.cfg.server,
            )
        else:
            ok = mt5.initialize()
        if not ok:
            raise RuntimeError(f"mt5.initialize failed: {mt5.last_error()}")
        self.connected = True
        info = mt5.account_info()
        if info is None:
            raise RuntimeError(f"no account info: {mt5.last_error()}")
        mode = {0: "demo", 1: "contest", 2: "real"}.get(int(info.trade_mode), str(info.trade_mode))
        log.info("connected login=%s server=%s mode=%s", info.login, info.server, mode)
        return {
            "login": info.login,
            "server": info.server,
            "trade_mode": mode,
            "balance": float(info.balance),
            "equity": float(info.equity),
            "currency": info.currency,
        }

    def shutdown(self) -> None:
        if self.connected and self.mt5 is not None:
            self.mt5.shutdown()
            self.connected = False

    def account(self) -> dict:
        info = self.mt5.account_info()
        return {"balance": float(info.balance), "equity": float(info.equity)}

    def broker_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "").upper() + self.cfg.symbol_suffix

    def fetch(self, symbol: str, timeframe: str, count: int) -> list[dict]:
        mt5 = self.mt5
        tf_name = _TF[timeframe]
        from providers.exness_mt5 import _INTERVAL_SECS, _INTERVAL_TO_TIMEFRAME

        # The provider builds the map at import. Rebuild if this process
        # imported it before MetaTrader5 existed — it does exist now.
        if not _INTERVAL_TO_TIMEFRAME:
            from providers import exness_mt5
            exness_mt5._build_interval_map()
            from providers.exness_mt5 import _INTERVAL_TO_TIMEFRAME as tf_map
        else:
            tf_map = _INTERVAL_TO_TIMEFRAME
        broker = self.broker_symbol(symbol)
        if not mt5.symbol_select(broker, True):
            raise RuntimeError(f"cannot select {broker}: {mt5.last_error()}")
        rates = mt5.copy_rates_from_pos(broker, tf_map[tf_name], 0, count)
        if rates is None or len(rates) == 0:
            raise RuntimeError(f"no rates for {broker} {timeframe}: {mt5.last_error()}")
        secs = _INTERVAL_SECS[tf_name]
        now = int(datetime.now(tz=timezone.utc).timestamp())
        bars = []
        for row in rates:
            ts = int(row["time"])
            if ts + secs > now:
                continue
            bars.append({
                "ts_utc": ts,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["tick_volume"]),
            })
        bars.sort(key=lambda b: b["ts_utc"])
        return bars

    def symbol_spec(self, symbol: str) -> dict:
        mt5 = self.mt5
        broker = self.broker_symbol(symbol)
        mt5.symbol_select(broker, True)
        info = mt5.symbol_info(broker)
        if info is None:
            raise RuntimeError(f"no symbol info for {broker}")
        tick = mt5.symbol_info_tick(broker)
        spread_price = float(info.spread) * float(info.point)
        return {
            "broker": broker,
            "digits": int(info.digits),
            "point": float(info.point),
            "tick_size": float(info.trade_tick_size),
            "tick_value": float(info.trade_tick_value),
            "volume_min": float(info.volume_min),
            "volume_max": float(info.volume_max),
            "volume_step": float(info.volume_step),
            "stops_level": int(info.trade_stops_level),
            "spread_price": spread_price,
            "bid": float(tick.bid) if tick else None,
            "ask": float(tick.ask) if tick else None,
            "filling_mode": int(info.filling_mode),
        }

    def positions(self) -> list[dict]:
        rows = self.mt5.positions_get() or []
        return [{
            "ticket": int(p.ticket),
            "symbol": p.symbol,
            "magic": int(p.magic),
            "type": int(p.type),
            "volume": float(p.volume),
            "profit": float(p.profit),
            "swap": float(p.swap),
            "price_open": float(p.price_open),
            "sl": float(p.sl),
            "tp": float(p.tp),
        } for p in rows]

    def realized_today(self, now_ts: int) -> tuple[float, int]:
        """P&L of this bot's deals since UTC midnight, and entry count."""
        mt5 = self.mt5
        day = datetime.fromtimestamp(now_ts, tz=timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0,
        )
        deals = mt5.history_deals_get(day, datetime.fromtimestamp(now_ts, tz=timezone.utc)) or []
        pnl = 0.0
        entries = 0
        entry_in = getattr(mt5, "DEAL_ENTRY_IN", 0)
        for deal in deals:
            if int(deal.magic) != self.cfg.magic:
                continue
            pnl += float(deal.profit) + float(deal.commission) + float(deal.swap)
            if int(deal.entry) == entry_in:
                entries += 1
        return pnl, entries

    def pending_orders(self) -> list[dict]:
        rows = self.mt5.orders_get() or []
        return [{
            "ticket": int(o.ticket),
            "symbol": o.symbol,
            "magic": int(o.magic),
            "volume": float(o.volume_current),
        } for o in rows]

    def send(self, broker: str, side: str, lots: float, sl: float, tp: float,
             spec: dict, limit_price: Optional[float] = None,
             expiration: Optional[int] = None) -> dict:
        mt5 = self.mt5
        price = limit_price if limit_price is not None else (spec["ask"] if side == "BUY" else spec["bid"])
        if price is None:
            return {"ok": False, "retcode": None, "comment": "no quote"}
        digits = spec["digits"]
        price, sl, tp = round(price, digits), round(sl, digits), round(tp, digits)
        if side == "BUY":
            order_type = mt5.ORDER_TYPE_BUY_LIMIT if limit_price is not None else mt5.ORDER_TYPE_BUY
        else:
            order_type = mt5.ORDER_TYPE_SELL_LIMIT if limit_price is not None else mt5.ORDER_TYPE_SELL
        action = mt5.TRADE_ACTION_PENDING if limit_price is not None else mt5.TRADE_ACTION_DEAL
        request = {
            "action": action,
            "symbol": broker,
            "volume": float(lots),
            "type": order_type,
            "price": price,
            "sl": sl,
            "tp": tp,
            "deviation": int(self.cfg.deviation_points),
            "magic": int(self.cfg.magic),
            "comment": "dash-bot",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": _filling(mt5, spec["filling_mode"]),
        }
        if expiration is not None and limit_price is not None:
            request["type_time"] = getattr(mt5, "ORDER_TIME_SPECIFIED", 2)
            request["expiration"] = int(expiration)
        result = mt5.order_send(request)
        if result is None:
            return {"ok": False, "retcode": None, "comment": str(mt5.last_error())}
        done = int(result.retcode) in (
            getattr(mt5, "TRADE_RETCODE_DONE", 10009),
            getattr(mt5, "TRADE_RETCODE_PLACED", 10008),
        )
        return {
            "ok": done,
            "retcode": int(result.retcode),
            "comment": str(result.comment),
            "order": int(result.order),
            "deal": int(getattr(result, "deal", 0) or 0),
            "price": float(result.price) if result.price else price,
            "request_price": price,
            "sl": sl,
            "tp": tp,
            "lots": float(lots),
        }

    def close_all(self) -> int:
        """Flatten this bot's positions. Used only when BOT_KILL_CLOSE is set."""
        mt5 = self.mt5
        closed = 0
        for pos in self.positions():
            if pos["magic"] != self.cfg.magic:
                continue
            broker = pos["symbol"]
            tick = mt5.symbol_info_tick(broker)
            if tick is None:
                continue
            # POSITION_TYPE_BUY = 0
            if pos["type"] == 0:
                order_type = mt5.ORDER_TYPE_SELL
                price = float(tick.bid)
            else:
                order_type = mt5.ORDER_TYPE_BUY
                price = float(tick.ask)
            info = mt5.symbol_info(broker)
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": broker,
                "volume": pos["volume"],
                "type": order_type,
                "position": pos["ticket"],
                "price": price,
                "deviation": int(self.cfg.deviation_points),
                "magic": int(self.cfg.magic),
                "comment": "dash-bot-kill",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": _filling(mt5, int(info.filling_mode) if info else 0),
            }
            result = mt5.order_send(request)
            if result is not None and int(result.retcode) == getattr(mt5, "TRADE_RETCODE_DONE", 10009):
                closed += 1
        return closed

    def cancel_pending(self) -> int:
        """Cancel this bot's resting orders. Used with the kill-close flag."""
        mt5 = self.mt5
        cancelled = 0
        for order in self.pending_orders():
            if order["magic"] != self.cfg.magic:
                continue
            request = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": order["ticket"],
                "magic": int(self.cfg.magic),
                "comment": "dash-bot-kill",
            }
            result = mt5.order_send(request)
            if result is not None and int(result.retcode) in (
                getattr(mt5, "TRADE_RETCODE_DONE", 10009),
                getattr(mt5, "TRADE_RETCODE_PLACED", 10008),
            ):
                cancelled += 1
        return cancelled


def _filling(mt5, filling_mode: int) -> int:
    # SYMBOL_FILLING_FOK = 1, IOC = 2. ORDER_FILLING_* is a separate enum.
    if filling_mode & 1:
        return mt5.ORDER_FILLING_FOK
    if filling_mode & 2:
        return mt5.ORDER_FILLING_IOC
    return getattr(mt5, "ORDER_FILLING_RETURN", mt5.ORDER_FILLING_IOC)
