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

Free Twelve Data plan: **1 credit per candle**, **800 credits/day/key**, 6 keys
= **4,800 credits/day** budget.

- Initial backfill (one-time): 15 pairs × 800 = **12,000 credits** (spread
  across 6 keys = ~2,000 each, fits the daily budget).
- Steady state: 15 pairs × ~2 credits per refresh × 96 refreshes/day (15-min)
  = **~2,880 credits/day** (≈60% utilisation with 6 keys).
- Keys rotate automatically via `TWELVEDATA_KEY_1..N` in `.env`.

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

## HOD/LOB and LOD/HOB

Setup scanner only. It does not place trades. Grade A and B posts go to the
existing Discord webhook (`HODLOB` on `DISCORD_SIGNALS`). Each post is also
appended to `hodlob_alerts.jsonl` next to this service (gitignored, so it
does not land in the OneDrive repo copy as a generated file). The same setup is sent once per symbol, direction, trading day and timeframe
(`hodlob_seen.json`).

M15 is the main timeframe. M5 is scanned for the main book, where 5-minute
bars are already fetched. An M5 alert and an M15 alert for the same day are
separate posts, so the M5 one cannot block the M15 one. Exotics (ZAR, TRY,
MXN, SEK, NOK, PLN, HKD, SGD) and the added crosses (CHF/JPY, NZD/JPY,
GBP/CAD, EUR/NZD, GBP/NZD, NZD/CAD, NZD/CHF, CAD/CHF, GBP/NOK, GBP/SEK) are
fetched on M15 and skipped when the broker has no symbol.

The videos leave several levels unmeasured. The choices, and why, are the
constants at the top of `hodlob_strategy.py`:

- Price Bollinger 20 / 2.0 on the close. TDI is the chart's 13, 34, 2, 7. Divergence uses the green SMA(2) of RSI(13), compared with the previous trading day.
- Hug: Asia close in the outer 25% of the prior-day + Asia box. Above 60% or below 40% is a directional lean (a weak hug can be the one miss on a B). The middle 20%, from 40% to 60% including the ends, is not a setup.
- Squeeze: Asia range at or under the median of the previous Asia sessions (up to 20, at least 8). Up to 1.25× that median is a weak squeeze and fails the check.
- Clock, all SAST (UTC+2, no daylight saving). Jo-zane, 2026-11-12: 7-8-9 is his clock. Asia is 00:00–06:00. Spikes are searched 06:00–11:00 (he gives up at 11). The preferred window, which affects the grade and does not delete a setup inside that search, is 07:00–09:45. The hours live in one place: `ASIA_END_HM`, `SEARCH_START_HM`, `SEARCH_END_HM`, `PREFER_START_HM`, `PREFER_END_HM` in `hodlob_strategy.py`.
- Re-entry is the first close back inside the band, within 8 bars and with that candle closed by 11:00 SAST, whichever is earlier. The alert time is the re-entry candle's close. The stop is beyond the spike extreme. The target reference is the opposite band, with the far side of the Asia range beside it.
- Monday's prior day is the last session with at least 40 bars. The Sunday 21:00 UTC reopen is not yesterday.
- Grade A needs the hug, the squeeze, the band spike, the Asia break, the TDI divergence, the re-entry and the 07:00–09:45 SAST window. Grade B is that core (spike, Asia break, re-entry) with exactly one of the soft checks missing, and it still needs a directional lean. Anything weaker is not alerted.

## BTMM 12 / 21 / 22 anchors

Setup alerts only, on the same M15 book as HOD/LOB. Discord key `BTMM_ANCHOR`
(on the default `DISCORD_SIGNALS` list). FULL posts. FORMING stays on the
dashboard unless `BTMM_ANCHOR_FULL_ONLY=false`. Each post is also appended to
`btmm_anchor_alerts.jsonl` (gitignored). One post per symbol, label and day.

The digits are Kar's: 2 is an M, W or head and shoulders, 1 is a one-leg
half-bat. The first digit is yesterday, the second is today's test of
yesterday's high (sell) or low (buy). A peak needs 3 to 5 days, at least
3× ADR, fanned EMAs and TDI overbought or oversold. The trigger is a close
back through the 13 EMA with the TDI cross or a shark fin. Closer than 10
pips to the 50 EMA waits. A later close back through the 13, 50 or 200, or
beyond the extreme, kills it.

Unknowns, and the defaults (`btmm_anchor_strategy.py`):

- No time-of-day filter. Kar never gave one. The day starts at 00:00 SAST.
- A test of the tracer can stop 8 pips short or run through it (`NEAR_PIPS`).
- "Too close to the water" is 10 pips (`WATER_PIPS`).
- The stop idea is 5 pips beyond the extreme, in that symbol's pip (`STOP_PIPS`).
- The target idea is the 200 EMA when it is on the trade side, otherwise the far side of yesterday.
- H1 is not scanned.

## Stooq fallback

Triggered automatically when all Twelve Data keys are over quota. Stooq's
historical CSV endpoint now requires an apikey (captcha-gated). To enable:

1. Visit https://stooq.com/q/d/?s=eurusd&get_apikey and solve the captcha
2. Copy the apikey from the download link
3. Add to `.env`: `STOOQ_APIKEY=<your key>`

Without `STOOQ_APIKEY` set, the provider short-circuits with a clear
"needs apikey" log message and the dashboard keeps serving cached candles
with a `stale:true` flag (shown as "Cached (N stale)" in the header).
