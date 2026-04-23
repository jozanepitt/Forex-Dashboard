# Forex Dashboard — Local Fetcher Service

Small Flask service that caches Twelve Data candles in SQLite, fetches
incrementally (only new closed bars), and exposes them to the dashboard via
a local HTTP endpoint. Removes API keys from the browser.

## Quick start

```bash
cd service
python -m pip install -r requirements.txt
copy .env.example .env          # then edit keys if they change
python app.py
```

Service listens on `http://127.0.0.1:3002`.

## Endpoints

| Method | Path                      | Purpose                              |
| ------ | ------------------------- | ------------------------------------ |
| GET    | `/health`                 | Liveness check                       |
| GET    | `/status`                 | Per-key credit usage + per-pair freshness |
| GET    | `/candles/<SYMBOL>`       | `?interval=15min&limit=800`          |
| POST   | `/refresh`                | Manually trigger refresh (?symbol=…) |

## Budget math

Free Twelve Data plan: **1 credit per candle**, **800 credits/day/key**, 4 keys
= **3,200 credits/day** budget.

- Initial backfill (one-time): 15 pairs × 800 = **12,000 credits** (spread
  across 4 keys = ~3,000 each, fits the daily budget).
- Steady state: 15 pairs × ~2 credits per refresh × 48 refreshes/day
  = **~1,440 credits/day** (≈45% utilisation).
- Headroom exists for dropping the cadence to 15-min (2,880/day) later.

## Files

```
app.py            Flask app + endpoints
fetcher.py        Cache-first, incremental fetch orchestration + Stooq failover
scheduler.py      APScheduler job at :02, :17, :32, :47 UTC
cache.py          SQLite helpers
config.py         Pairs, keys loader, constants
providers/
  twelvedata.py   TD client with key rotation + quota
  stooq.py        CSV fallback (needs STOOQ_APIKEY — see below)
candles.db        Created on first run; safe to delete to re-backfill
.env              API keys (gitignored)
```

## Stooq fallback

Triggered automatically when all Twelve Data keys are over quota. Stooq's
historical CSV endpoint now requires an apikey (captcha-gated). To enable:

1. Visit https://stooq.com/q/d/?s=eurusd&get_apikey and solve the captcha
2. Copy the apikey from the download link
3. Add to `.env`: `STOOQ_APIKEY=<your key>`

Without `STOOQ_APIKEY` set, the provider short-circuits with a clear
"needs apikey" log message and the dashboard keeps serving cached candles
with a `stale:true` flag (shown as "Cached (N stale)" in the header).
