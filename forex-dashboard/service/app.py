"""Flask HTTP service serving cached candles to the dashboard."""
from __future__ import annotations

import logging
from urllib.parse import unquote

from flask import Flask, jsonify, request
from flask_cors import CORS

import cache
import fetcher
import scheduler
from config import (
    DEFAULT_BACKFILL,
    DEFAULT_INTERVAL,
    INTERVAL_SECS,
    PRIORITY_PAIRS,
    SERVICE_HOST,
    SERVICE_PORT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
log = logging.getLogger("app")

app = Flask(__name__)
CORS(app)


@app.get("/health")
def health():
    return {"status": "ok"}


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
    scheduler.start()
    log.info("listening on http://%s:%d", SERVICE_HOST, SERVICE_PORT)
    app.run(host=SERVICE_HOST, port=SERVICE_PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
