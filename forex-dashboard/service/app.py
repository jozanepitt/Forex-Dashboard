"""Flask HTTP service serving cached candles to the dashboard."""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from urllib.parse import unquote

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO

import alerts
import backtest as bt
import cache
import crt_strategy
import snr_strategy
import snr_m15_strategy
import tdi_cycle_123
import fetcher
import scheduler
from providers import forexfactory
from config import (
    DEFAULT_BACKFILL,
    DEFAULT_INTERVAL,
    INTERVAL_SECS,
    PRIORITY_PAIRS,
    SERVICE_HOST,
    SERVICE_PORT,
    SERVICE_ROOT,
)

# Log to a rotating file (in addition to stdout) so stalls/errors are
# diagnosable even when the service runs headless under pythonw (no console).
from logging.handlers import RotatingFileHandler  # noqa: E402

_log_fmt = "%(asctime)s %(name)s %(levelname)s: %(message)s"
_file_handler = RotatingFileHandler(
    SERVICE_ROOT / "service.log", maxBytes=2_000_000, backupCount=5, encoding="utf-8"
)
_file_handler.setFormatter(logging.Formatter(_log_fmt))
logging.basicConfig(
    level=logging.INFO,
    format=_log_fmt,
    handlers=[logging.StreamHandler(), _file_handler],
)
log = logging.getLogger("app")

import os as _os
DASHBOARD_DIR = str(Path(_os.path.abspath(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))))

app = Flask(__name__, static_folder=DASHBOARD_DIR, static_url_path="")
CORS(app)


@app.get("/")
def dashboard():
    return send_from_directory(DASHBOARD_DIR, "index.html")
sio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=False, engineio_logger=False)


def emit_refresh_complete(pairs_updated: int):
    """Called by scheduler after each successful refresh cycle. Pushes to all subscribers."""
    try:
        sio.emit("refresh-complete", {"ts": int(time.time()), "pairs_updated": pairs_updated})
    except Exception as e:
        log.warning("emit_refresh_complete failed: %s", e)


@sio.on("connect")
def _on_connect():
    log.info("websocket client connected")


@sio.on("disconnect")
def _on_disconnect():
    log.info("websocket client disconnected")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/news")
def news():
    try:
        minutes = int(request.args.get("minutes", 60))
    except ValueError:
        return jsonify({"error": "minutes must be integer"}), 400
    impact = request.args.get("impact")  # optional: "high" | "medium" | "low"
    force = request.args.get("refresh", "").lower() in ("1", "true")
    if force:
        forexfactory.get_events(force_refresh=True)
    events = forexfactory.upcoming(minutes=minutes, impact_filter=impact or None)
    return jsonify({"count": len(events), "events": events})


@app.get("/status")
def status():
    return jsonify(fetcher.status_summary())


@app.get("/candles/<path:symbol>")
def candles(symbol: str):
    symbol = unquote(symbol).upper()
    if "/" not in symbol and len(symbol) == 6:
        symbol = f"{symbol[:3]}/{symbol[3:]}"

    interval = request.args.get("interval", DEFAULT_INTERVAL)
    if interval not in INTERVAL_SECS:
        return jsonify({"error": f"unknown interval '{interval}'"}), 400

    try:
        limit = int(request.args.get("limit", DEFAULT_BACKFILL))
    except ValueError:
        return jsonify({"error": "limit must be integer"}), 400
    limit = max(1, min(limit, DEFAULT_BACKFILL))

    bars, stale = fetcher.get_candles(symbol, interval, limit)
    return jsonify({
        "symbol":   symbol,
        "interval": interval,
        "count":    len(bars),
        "stale":    stale,
        "candles":  bars,
    })


# 5-minute TTL cache for /crt — analyses are computed against H4-aligned candles
# that only change every 4 hours. 5 min keeps tab-clicks instant, shields MT5 from
# refresh storms, and avoids long blocking fetches when multiple tabs click rapidly.
# The 15-min scheduler refresh keeps data fresh anyway.
_CRT_CACHE: dict[str, object] = {"ts": 0.0, "payload": None}
_CRT_TTL_SECS = 300


@app.get("/crt")
def crt():
    """1AM CRT scanner across the universe.

    Uses `fetcher.get_candles()` (live MT5 with TwelveData/Stooq fallback) for the
    same data path as the BTMM tab. Fetches run in parallel
    (ThreadPoolExecutor, 8 workers); inside the MT5 client a per-instance lock
    serializes the actual IPC calls so concurrent symbols don't race on
    `symbol_select`. Result memoized for 30s.
    """
    import time as _t
    from concurrent.futures import ThreadPoolExecutor

    now = _t.time()
    if _CRT_CACHE["payload"] is not None and (now - _CRT_CACHE["ts"]) < _CRT_TTL_SECS:
        return jsonify(_CRT_CACHE["payload"])

    universe = crt_strategy.CRT_UNIVERSE
    jobs: list[tuple[str, str]] = [(sym, "15min") for sym in universe]

    def _fetch(job):
        sym, iv = job
        bars, stale = fetcher.get_candles(sym, iv, limit=400)
        return sym, bars, stale

    candles_by_pair: dict[str, dict] = {sym: {"m15": []} for sym in universe}
    stale_set: set[str] = set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        for sym, bars, stale in pool.map(_fetch, jobs):
            candles_by_pair[sym]["m15"] = bars
            if stale:
                stale_set.add(sym)

    result = crt_strategy.analyze_universe(candles_by_pair)
    result["stale_pairs"] = sorted(stale_set)
    result["cached_at"] = int(now)
    _CRT_CACHE["payload"] = result
    _CRT_CACHE["ts"] = now
    return jsonify(result)


# ── SNR endpoint (Malaysian SNR Emperor) ─────────────────────────────────────────────────────────────
_SNR_CACHE: dict[str, object] = {"ts": 0.0, "payload": None}
_SNR_TTL_SECS = 300


@app.get("/snr")
def snr():
    """SNR (Support & Resistance) scanner across the universe."""
    import time as _t
    from concurrent.futures import ThreadPoolExecutor

    now = _t.time()
    if _SNR_CACHE["payload"] is not None and (now - _SNR_CACHE["ts"]) < _SNR_TTL_SECS:
        return jsonify(_SNR_CACHE["payload"])

    universe = snr_strategy.SNR_UNIVERSE
    jobs: list[tuple[str, str]] = [(sym, iv) for sym in universe for iv in ("15min", "1day")]

    def _fetch(job):
        sym, iv = job
        bars, stale = fetcher.get_candles(sym, iv, limit=(400 if iv == "15min" else 60))
        return sym, iv, bars, stale

    candles_by_pair: dict[str, dict] = {sym: {"m15": [], "1d": []} for sym in universe}
    stale_set: set[str] = set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        for sym, iv, bars, stale in pool.map(_fetch, jobs):
            candles_by_pair[sym]["m15" if iv == "15min" else "1d"] = bars
            if stale:
                stale_set.add(sym)

    result = snr_strategy.analyze_universe(candles_by_pair)
    result["stale_pairs"] = sorted(stale_set)
    result["cached_at"] = int(now)
    _SNR_CACHE["payload"] = result
    _SNR_CACHE["ts"] = now
    return jsonify(result)


# ── SNR M15 Fast Scanner endpoint ─────────────────────────────────────────────────────────────────────
_SNR_M15_CACHE: dict[str, object] = {"ts": 0.0, "payload": None}
_SNR_M15_TTL_SECS = 120  # 2-min cache — fast scanner needs fresher data


@app.get("/snr-m15")
def snr_m15():
    """SNR M15 Fast Scanner — same Emperor methodology, lower timeframes.

    Fetches H1 (for level marking) + M15 (for breakout/engulfing).
    Catches setups hours earlier than the H4 scanner.
    """
    import time as _t
    from concurrent.futures import ThreadPoolExecutor

    now = _t.time()
    if _SNR_M15_CACHE["payload"] is not None and (now - _SNR_M15_CACHE["ts"]) < _SNR_M15_TTL_SECS:
        return jsonify(_SNR_M15_CACHE["payload"])

    universe = snr_m15_strategy.SNR_M15_UNIVERSE
    # Fetch H1 (for level marking) + M15 (for breakout/engulfing confirmation)
    jobs: list[tuple[str, str]] = [(sym, iv) for sym in universe for iv in ("15min", "1h")]

    def _fetch(job):
        sym, iv = job
        limit = 400 if iv == "15min" else 200  # ~8 days H1, ~4 days M15
        bars, stale = fetcher.get_candles(sym, iv, limit=limit)
        return sym, iv, bars, stale

    candles_by_pair: dict[str, dict] = {sym: {"1h": [], "m15": []} for sym in universe}
    stale_set: set[str] = set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        for sym, iv, bars, stale in pool.map(_fetch, jobs):
            candles_by_pair[sym]["1h" if iv == "1h" else "m15"] = bars
            if stale:
                stale_set.add(sym)

    result = snr_m15_strategy.analyze_universe(candles_by_pair)
    result["stale_pairs"] = sorted(stale_set)
    result["cached_at"] = int(now)
    _SNR_M15_CACHE["payload"] = result
    _SNR_M15_CACHE["ts"] = now
    return jsonify(result)


# ── TDI Cycle 123 endpoint ──────────────────────────────────────────────────
# Improvements-on-BTMM scanner: FSO_TDI + 123 Peak Formation + divergence.
_TDI123_CACHE: dict[str, object] = {"ts": 0.0, "payload": None}
_TDI123_TTL_SECS = 180  # 3-min cache — H1 primary TF, so slightly fresher than SNR/CRT


@app.get("/tdi123")
def tdi123():
    """TDI Cycle 123 Reversal scanner across the universe.

    Fetches H1 (primary), H4 (HTF bias), and D1 (context). Reuses the same
    parallel-fetch pattern as /snr so per-symbol IPC to MT5 is serialised inside
    the client but symbols are fetched concurrently.
    """
    import time as _t
    from concurrent.futures import ThreadPoolExecutor

    now = _t.time()
    if _TDI123_CACHE["payload"] is not None and (now - _TDI123_CACHE["ts"]) < _TDI123_TTL_SECS:
        return jsonify(_TDI123_CACHE["payload"])

    universe = tdi_cycle_123.TDI123_UNIVERSE
    jobs: list[tuple[str, str]] = [(sym, iv) for sym in universe
                                   for iv in ("1h", "4h", "1day", "15min")]

    def _fetch(job):
        sym, iv = job
        # H1 needs >=800 bars for a real EMA-800 (calc_ema falls back to a flat
        # line at current price otherwise) — match DEFAULT_BACKFILL so it's
        # actually converged, not just past the bare minimum.
        # M15 also needs deep history for accurate EMA convergence.
        limits = {"1h": DEFAULT_BACKFILL, "4h": 200, "1day": 60, "15min": DEFAULT_BACKFILL}
        bars, stale = fetcher.get_candles(sym, iv, limit=limits[iv])
        return sym, iv, bars, stale

    candles_by_pair: dict[str, dict] = {sym: {"1h": [], "4h": [], "1d": [], "m15": []}
                                        for sym in universe}
    stale_set: set[str] = set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        for sym, iv, bars, stale in pool.map(_fetch, jobs):
            key = {"1h": "1h", "4h": "4h", "1day": "1d", "15min": "m15"}[iv]
            candles_by_pair[sym][key] = bars
            if stale and iv == "1h":  # only flag stale if H1 is stale (primary timeframe)
                stale_set.add(sym)

    result = tdi_cycle_123.analyze_universe(candles_by_pair)
    result["stale_pairs"] = sorted(stale_set)
    result["cached_at"] = int(now)
    _TDI123_CACHE["payload"] = result
    _TDI123_CACHE["ts"] = now
    return jsonify(result)


TDI123_CHART_DISPLAY_BARS = 400  # candles actually drawn on the chart (readability + payload size)


@app.get("/tdi123/detail")
def tdi123_detail():
    """Per-pair detail for the TDI Cycle 123 chart overlay.

    Returns the raw H1 candles + full TDI series + swing markers + pattern points
    the frontend needs to draw 1/2/3 markers and the divergence line.

    Pattern/EMA detection runs on the FULL DEFAULT_BACKFILL history (so EMA-800
    is a real converged average, not the calc_ema() flat-line fallback), but the
    chart only ever displays the most recent TDI123_CHART_DISPLAY_BARS candles —
    so every per-bar array (and the p1/p2/p3 pattern indices) is sliced down to
    that display window before being sent to the frontend.
    """
    symbol = request.args.get("symbol", "").upper().replace("_", "/")
    if not symbol:
        return jsonify({"error": "symbol required"}), 400
    if "/" not in symbol and len(symbol) == 6:
        symbol = f"{symbol[:3]}/{symbol[3:]}"

    h1_bars, h1_stale = fetcher.get_candles(symbol, "1h", limit=DEFAULT_BACKFILL)
    h4_bars, h4_stale = fetcher.get_candles(symbol, "4h", limit=200)
    d1_bars, d1_stale = fetcher.get_candles(symbol, "1day", limit=60)
    m15_bars, m15_stale = fetcher.get_candles(symbol, "15min", limit=DEFAULT_BACKFILL)

    row = tdi_cycle_123.analyze_pair(symbol, h1_bars, h4_candles=h4_bars, d1_candles=d1_bars, m15_candles=m15_bars)

    # Build TDI + baseline series for chart overlay (full history, for accuracy)
    from btmm_core import _rsi, _sma
    closes = [b["close"] for b in h1_bars]
    rsi_series = _rsi(closes, 13) if len(closes) >= 14 else [50.0] * len(closes)
    fast_arr = _sma(rsi_series, 2)
    slow_arr = _sma(rsi_series, 7)
    baseline = tdi_cycle_123._baseline_series(rsi_series, 34)

    # EMAs for the price pane (full history, so EMA-800 actually converges).
    # Convergence thresholds (seed influence < 1%) match tdi_cycle_123._ema_targets
    # and BTMM's own ema200Warm/ema800Warm standard elsewhere in index.html —
    # below these, calc_ema() is either flat-lined at current price (below its
    # period) or too seed-biased to draw as if it were a real average.
    from btmm_core import calc_ema
    ema50 = calc_ema(closes, 50) if len(closes) >= 50 else [None] * len(closes)
    ema200 = calc_ema(closes, 200) if len(closes) >= 300 else [None] * len(closes)
    ema800 = calc_ema(closes, 800) if len(closes) >= 2400 else [None] * len(closes)

    # Slice everything down to the display window. Guard against a pattern
    # swing landing before the window (shouldn't happen — p1/p2/p3 are always
    # among the most recent swings — but widen the window rather than emit an
    # out-of-range index if it ever does).
    display_n = TDI123_CHART_DISPLAY_BARS
    pattern = (row.get("pattern") or {})
    swing_idxs = [pattern[k]["idx"] for k in ("p1", "p2", "p3") if k in pattern]
    if swing_idxs:
        min_swing_idx = min(swing_idxs)
        display_n = max(display_n, len(h1_bars) - min_swing_idx + 5)
    display_n = min(display_n, len(h1_bars))
    offset = len(h1_bars) - display_n

    if offset > 0 and pattern:
        row = dict(row)
        row["pattern"] = dict(pattern)
        for k in ("p1", "p2", "p3"):
            if k in pattern:
                pt = dict(pattern[k])
                pt["idx"] = pt["idx"] - offset
                row["pattern"][k] = pt

    def _tail(arr):
        return arr[offset:] if offset > 0 else arr

    return jsonify({
        "symbol": symbol,
        "stale": bool(h1_stale or h4_stale or d1_stale),
        "row": row,
        "candles": _tail(h1_bars),
        "ema50": _tail(ema50),
        "ema200": _tail(ema200),
        "ema800": _tail(ema800),
        "tdi": {
            "rsi": _tail(rsi_series),
            "fast": _tail(fast_arr),
            "slow": _tail(slow_arr),
            "baseline": _tail(baseline),
        },
    })


@app.get("/alerts/config")
def alerts_config():
    return jsonify({
        "webhook_set": bool(alerts.WEBHOOK_URL),
        "rate_limit_secs": alerts.RATE_LIMIT_SECS,
        "tracked_pairs": len(PRIORITY_PAIRS),
    })


@app.post("/alerts/test")
def alerts_test():
    """Send a test embed to Discord to verify the webhook works."""
    if not alerts.WEBHOOK_URL:
        return jsonify({"error": "DISCORD_WEBHOOK_URL not set in .env"}), 400
    ok = alerts._post_discord({
        "title": "✅ BTMM Dashboard — Test Alert",
        "description": "Discord webhook is connected. Alerts are live.",
        "color": 0x00E676,
        "footer": {"text": "BTMM Dashboard alert system"},
    })
    return jsonify({"sent": ok})


@app.post("/journal/trade")
def journal_open():
    body = request.get_json(silent=True) or {}
    required = ("pair", "direction", "entry", "sl", "tp1")
    missing = [f for f in required if f not in body]
    if missing:
        return jsonify({"error": f"missing fields: {missing}"}), 400
    try:
        trade_id = cache.open_trade(
            pair=body["pair"].upper(),
            direction=body["direction"],
            entry=float(body["entry"]),
            sl=float(body["sl"]),
            tp1=float(body["tp1"]),
            tp2=float(body["tp2"]) if body.get("tp2") else None,
            setup=body.get("setup"),
            signal=body.get("signal"),
            signal_score=float(body["signal_score"]) if body.get("signal_score") else None,
            gates_json=json.dumps(body["gates"]) if body.get("gates") else None,
            notes=body.get("notes"),
        )
        return jsonify({"id": trade_id, "status": "open"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.patch("/journal/trade/<int:trade_id>")
def journal_close(trade_id: int):
    body = request.get_json(silent=True) or {}
    if "exit_price" not in body or "result" not in body:
        return jsonify({"error": "exit_price and result required"}), 400
    if body["result"] not in ("win", "loss", "be"):
        return jsonify({"error": "result must be win|loss|be"}), 400
    ok = cache.close_trade(
        trade_id=trade_id,
        exit_price=float(body["exit_price"]),
        result=body["result"],
        pl_pips=float(body["pl_pips"]) if body.get("pl_pips") is not None else None,
        pl_dollars=float(body["pl_dollars"]) if body.get("pl_dollars") is not None else None,
        notes=body.get("notes"),
    )
    if not ok:
        return jsonify({"error": "trade not found"}), 404
    return jsonify({"id": trade_id, "status": body["result"]})


@app.get("/journal/trades")
def journal_list():
    try:
        limit = int(request.args.get("limit", 50))
    except ValueError:
        limit = 50
    trades = cache.get_trades(
        limit=limit,
        pair=request.args.get("pair"),
        setup=request.args.get("setup"),
    )
    return jsonify({"count": len(trades), "trades": trades})


@app.get("/journal/stats")
def journal_stats():
    stats = cache.get_journal_stats(
        setup=request.args.get("setup"),
        pair=request.args.get("pair"),
    )
    return jsonify(stats)


@app.post("/backtest")
def run_backtest():
    body = request.get_json(silent=True) or {}
    pair  = (body.get("pair") or "EUR/USD").upper()
    start = body.get("start")
    end   = body.get("end")
    if not start or not end:
        return jsonify({"error": "start and end timestamps required"}), 400
    try:
        result = bt.run(
            pair=pair,
            start_ts=int(start),
            end_ts=int(end),
            setups=body.get("setups", ["safety"]),
            min_gates=int(body.get("min_gates", 5)),
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(result)


@app.post("/refresh")
def refresh():
    only = request.args.get("symbol")
    symbols = [only] if only else PRIORITY_PAIRS
    results = []
    for sym in symbols:
        try:
            _, stale = fetcher.get_candles(sym, DEFAULT_INTERVAL)
            results.append({"symbol": sym, "stale": stale})
        except Exception as e:
            results.append({"symbol": sym, "error": str(e)})
    return jsonify({"refreshed": len(results), "results": results})


def main():
    cache.init_db()
    log.info("DB initialised at %s", cache.DB_PATH if False else "candles.db")
    try:
        forexfactory.get_events()
    except Exception as e:
        log.warning("news prefetch failed: %s", e)
    scheduler.start()
    log.info("listening on http://%s:%d (with WebSocket)", SERVICE_HOST, SERVICE_PORT)
    sio.run(app, host=SERVICE_HOST, port=SERVICE_PORT, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    main()
