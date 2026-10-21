"""Poll closed candles and send orders for one configured strategy.

Demo accounts are the default. A real account is refused unless both
MT5_BOT_LIVE and MT5_BOT_CONFIRM_LIVE are set. An empty BOT_STRATEGY
does not trade.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

from bot.config import BotConfig, load_config
from bot.costs import cost_model, spread_price
from bot.execution import MT5Gateway
from bot.filters import in_session, news_blocks
from bot.risk import (
    RiskConfig, RiskSnapshot, day_anchor_equity, evaluate_risk, exposure_slots,
    lots_from_risk, pending_expiration, trading_mode_allowed, utc_day,
)
from bot.signals import LOOKBACK, current_signal

log = logging.getLogger("bot")


def _load_state(cfg: BotConfig) -> dict:
    if cfg.state_path.exists():
        try:
            return json.loads(cfg.state_path.read_text())
        except json.JSONDecodeError:
            log.warning("state file unreadable, starting fresh")
    return {"day": "", "day_start_equity": 0.0, "halted_day": "", "dedup": []}


def _save_state(cfg: BotConfig, state: dict) -> None:
    cfg.state_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.state_path.write_text(json.dumps(state))


def _log_event(cfg: BotConfig, record: dict) -> None:
    cfg.log_path.parent.mkdir(parents=True, exist_ok=True)
    record = {"ts": datetime.now(timezone.utc).isoformat(), **record}
    with cfg.log_path.open("a") as handle:
        handle.write(json.dumps(record) + "\n")


def _grade_ok(cfg: BotConfig, grade: str, alert: bool) -> bool:
    mode = cfg.grades.strip().lower()
    if mode == "all":
        return True
    if mode == "alert":
        return alert
    allowed = {part.strip().upper() for part in cfg.grades.split(",") if part.strip()}
    return grade.upper() in allowed


def _timeframes(strategy: str, timeframe: str) -> list[str]:
    if strategy in ("crt_5am", "crt_1am"):
        return ["M15"]
    lookback, htf, _ = LOOKBACK[(strategy, timeframe)]
    out = [timeframe]
    if htf and htf not in out:
        out.append(htf)
    if strategy == "tdi123" and "D1" not in out:
        out.append("D1")
    return out


def _fetch_count(strategy: str, timeframe: str, name: str) -> int:
    if strategy in ("crt_5am", "crt_1am"):
        return 800
    lookback, _, htf_lb = LOOKBACK[(strategy, timeframe)]
    if name == timeframe:
        return lookback + 5
    if name == "D1":
        return 40
    return htf_lb + 5


def run_once(cfg: BotConfig, gateway: MT5Gateway, state: dict) -> dict:
    """One poll. Returns the updated state. Does not sleep."""
    now_ts = int(time.time())
    today = utc_day(now_ts)
    account = gateway.account()
    if state.get("day") != today or not state.get("day_start_equity"):
        state["day"] = today
        state["day_start_equity"] = day_anchor_equity(account["equity"])
        state["halted_day"] = "" if state.get("halted_day") != today else state.get("halted_day", "")
        log.info("UTC day %s starting equity %.2f", today, state["day_start_equity"])

    if cfg.kill_tripped():
        _log_event(cfg, {"event": "kill_switch"})
        if cfg.kill_close:
            closed = gateway.close_all()
            cancelled = gateway.cancel_pending()
            log.warning("kill switch closed %s bot positions and cancelled %s pending orders", closed, cancelled)
        else:
            log.warning("kill switch set: no new trades")
        state["halted_day"] = state.get("halted_day") or ""
        return state

    positions = gateway.positions()
    pendings = gateway.pending_orders()
    floating = sum(p["profit"] + p["swap"] for p in positions)
    realized, entries = gateway.realized_today(now_ts)
    snap = RiskSnapshot(
        now_ts=now_ts,
        equity=account["equity"],
        day_start_equity=float(state["day_start_equity"]),
        realized_today=realized,
        floating=floating,
        open_positions=exposure_slots(len(positions), len(pendings)),
        trades_today=entries,
        kill_switch=False,
        halted_day=state.get("halted_day") or "",
    )
    risk_cfg = RiskConfig(
        cfg.risk_pct, cfg.max_open_positions, cfg.daily_loss_pct, cfg.max_trades_per_day,
    )
    allowed, reason, halted = evaluate_risk(risk_cfg, snap)
    state["halted_day"] = halted
    if not allowed:
        log.info("not trading: %s", reason)
        return state

    events: list[dict] = []
    if cfg.news_filter:
        try:
            from providers.forexfactory import get_events
            events = get_events()
        except Exception as exc:  # noqa: BLE001
            log.warning("news feed failed (%s); skipping new entries this pass", exc)
            return state

    held_symbols = {p["symbol"] for p in positions} | {p["symbol"] for p in pendings}
    dedup = set(state.get("dedup") or [])
    tfs = _timeframes(cfg.strategy, cfg.timeframe)

    for symbol in cfg.symbols:
        broker = gateway.broker_symbol(symbol)
        if broker in held_symbols:
            continue
        if cfg.session_filter and not in_session(now_ts, cfg.session_start_utc, cfg.session_end_utc):
            continue
        if cfg.news_filter and news_blocks(symbol, now_ts, events, cfg.news_window_min):
            log.info("%s skipped: high-impact news", symbol)
            continue
        try:
            spec = gateway.symbol_spec(symbol)
        except Exception as exc:  # noqa: BLE001
            log.warning("%s symbol spec failed: %s", symbol, exc)
            continue
        typical = spread_price(symbol)
        if typical is None:
            log.warning("%s has no spread assumption; skipped", symbol)
            continue
        if not (spec["spread_price"] <= typical * cfg.max_spread_mult + 1e-9):
            log.info("%s spread %.6f wider than %.1fx typical", symbol, spec["spread_price"], cfg.max_spread_mult)
            continue
        candles = {}
        try:
            for name in tfs:
                candles[name] = gateway.fetch(symbol, name, _fetch_count(cfg.strategy, cfg.timeframe, name))
            partner = None
            if cfg.strategy in ("crt_5am", "crt_1am") and symbol in ("EUR/USD", "GBP/USD"):
                other = "GBP/USD" if symbol == "EUR/USD" else "EUR/USD"
                partner = gateway.fetch(other, "M15", 800)
            sig = current_signal(cfg.strategy, symbol, cfg.timeframe, candles, partner, cfg.entry_mode)
        except Exception as exc:  # noqa: BLE001
            log.warning("%s signal failed: %s", symbol, exc)
            continue
        if sig is None:
            continue
        if not _grade_ok(cfg, sig.grade, sig.alert):
            continue
        if sig.dedup_key in dedup:
            continue
        sl_distance = abs(sig.entry - sig.sl)
        model = cost_model(symbol)
        typical = model[0] if model else 0.0
        slip = model[1] if model else 0.0
        live_spread = float(spec["spread_price"] or 0.0)
        # Size on the worse of the live quote and the assumed typical, plus
        # slippage. Commission is 0 on Exness Standard; the argument is here
        # so a commission account cannot forget it.
        lots = lots_from_risk(
            account["equity"], cfg.risk_pct, sl_distance,
            spec["tick_size"], spec["tick_value"],
            spec["volume_min"], spec["volume_max"], spec["volume_step"],
            extra_price=max(live_spread, typical) + slip,
            commission_per_lot=0.0,
        )
        if lots <= 0:
            _log_event(cfg, {"event": "skip", "symbol": symbol, "reason": "size below minimum",
                             "equity": account["equity"], "sl_distance": sl_distance})
            continue
        limit = sig.entry if sig.entry_kind == "limit" else None
        expiration = pending_expiration(now_ts, sig.valid_until_ts) if limit is not None else None
        result = gateway.send(
            spec["broker"], sig.side, lots, sig.sl, sig.tp, spec,
            limit_price=limit, expiration=expiration,
        )
        _log_event(cfg, {
            "event": "order",
            "symbol": symbol,
            "broker": spec["broker"],
            "side": sig.side,
            "grade": sig.grade,
            "strategy": cfg.strategy,
            "timeframe": cfg.timeframe,
            "lots": lots,
            "sl": sig.sl,
            "tp": sig.tp,
            "entry": sig.entry,
            "risk_pct": cfg.risk_pct,
            "equity": account["equity"],
            "ok": result["ok"],
            "retcode": result.get("retcode"),
            "comment": result.get("comment"),
            "dedup": sig.dedup_key,
        })
        if result["ok"]:
            dedup.add(sig.dedup_key)
            entries += 1
            snap.trades_today = entries
            snap.open_positions += 1
            allowed, reason, halted = evaluate_risk(risk_cfg, snap)
            state["halted_day"] = halted
            if not allowed:
                log.info("stopping this pass: %s", reason)
                break
    state["dedup"] = list(dedup)[-500:]
    return state


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    once = argv is not None and "--once" in argv
    if not cfg.strategy:
        log.error(
            "BOT_STRATEGY is empty, so the bot will not trade. Read BACKTEST_RESULTS.md "
            "and BOT_README.md, then set BOT_STRATEGY and BOT_TIMEFRAME in service/.env."
        )
        return 0
    if cfg.strategy not in ("crt_5am", "crt_1am") and (cfg.strategy, cfg.timeframe) not in LOOKBACK:
        log.error("no scanner for %s %s", cfg.strategy, cfg.timeframe)
        return 2
    gateway = MT5Gateway(cfg)
    try:
        account = gateway.connect()
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 2
    ok, reason = trading_mode_allowed(account["trade_mode"], cfg.live, cfg.confirm_live)
    log.info("%s", reason)
    if not ok:
        gateway.shutdown()
        return 2
    state = _load_state(cfg)
    try:
        while True:
            state = run_once(cfg, gateway, state)
            _save_state(cfg, state)
            if once or cfg.kill_tripped():
                break
            time.sleep(cfg.poll_seconds)
    except KeyboardInterrupt:
        log.info("stopped")
    finally:
        _save_state(cfg, state)
        gateway.shutdown()
    return 0
