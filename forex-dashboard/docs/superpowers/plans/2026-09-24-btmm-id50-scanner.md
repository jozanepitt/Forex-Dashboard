# Implementation Plan: BTMM ID50 Scanner (M15) — 2026-09-24

Spec: `docs/superpowers/specs/2026-09-24-btmm-id50-scanner-design.md` (fully approved in
conversation — sections 1–4 "yes / yes go").

This plan is `git bash`-agnostic, Windows/PowerShell safe, and every step is verifiable.
Run everything from `service/`. Python is `python` (Python 3.12 at
`C:\Users\jzpit\AppData\Local\Programs\Python\Python312`).

## Overview

A new **M15** trend-continuation scanner ("ID50": buys retests of the EMA-50 continuation
after the 13/50 cross leg-out), scored 0–26 with grades A+/A/B/C/NO-TRADE, a concrete
trade plan, dashboard tab, alert pipeline, and a cached-candles walk-forward backtest.

**Mottos that pinned the design:**
- No `notify/alert` unless the backtest earns it. `BTMM_ID50_ALERTS_ENABLED=false` default.
- Grade A+ (22–26) is the only tier allowed to auto-discord under the shipped defaults
  (`BTMM_ID50_GRADE_A_ONLY=true`), same evidence-first posture as BTMM123/VWAP-MR.
- NEVER mutate `tdi_cycle_123.py`, `btmm_core.py`, or any existing live path — import and reuse.

**Wiring model copied from btmm_123.py** (read in full; this plan references its exact patterns):
- Module exposes `analyze_pair(symbol, m15_candles, h1_candles=None)` and
  `analyze_universe(candles_by_pair)` returning `{"universe","buys","sells","grade_a",
  "grade_aplus","grade_b","pairs"}`.
- Alerts/`_should_alert`/watch live in `alerts.py` (session + news + throttle gates).
- `/id50` + `/id50/detail` in `app.py` (180s TTL cache, stale-pairs, chart detail).
- `_run_id50_alerts()` in `scheduler.py`, called from `refresh_all()` after `_run_btmm123_alerts()`.
- Healthcheck adds `/id50` to the dashboard-endpoint loop and a `btmm_id50` drive to the
  pipeline dry-run.

## Verified codebase facts this plan depends on (all confirmed by reading source)

- `btmm_core.calc_ema(closes, period) -> list[float]` — front-pads so list INDEX ALIGNS with
  `closes` (critical: `cross_idx` from `_last_ema_cross` indexes `closes`).
- `btmm_core._last_ema_cross(fast, slow, start=0) -> Optional[tuple[int, str]]` —
  last cross bar index + direction ("bullish"/"bearish").
- `btmm_core.detect_nameable_candle(bars) -> {"found","name","direction"}` — names are
  string-exact: `Doji`, `Hammer`, `Shooting Star`, `Bullish Engulfing`, `Bearish Engulfing`,
  `RRT`, `COW`, `None`. Doji → `found=True, name="Doji", direction="neutral"`.
- `btmm_core.detect_rsi_signal_cross(tdi, lookback=5) -> {"crossed","direction"}`;
  `btmm_core.calc_tdi(closes) -> dict` with `fast_arr`/`slow_arr`/`bb_upper`/`bb_lower`.
- `btmm_core.detect_asian_range(bars, symbol) -> {"valid","high","low","mid","range_pips"}`.
- `btmm_core.detect_stop_hunt(bars, lookback=20) -> {"active","prev_high","prev_low"}`.
- `tdi_cycle_123._atr(bars, period=ATR_PERIOD) -> Optional[float]` (True Range mean, same
  convention as BTMM123/TDI123).
- `tdi_cycle_123._session_label(ts_utc)` and `_in_active_session(ts_utc)`.
- `tdi_cycle_123._htf_bias(bars) -> Optional[str]` ("bullish"/"bearish"/None).
- `tdi_cycle_123._prev_week_hlc(bars, as_of_ts)` + `_weekly_fib_pivots(hlc)` +
  `_location(price, direction, pivots) -> {"zone","quality","ok",...}`.
- `instruments.pip_size(symbol, price)`, `instruments.min_sl_distance(symbol, price)`,
  `instruments.asset_class(symbol)`, `instruments.fmt_price(symbol, price)`.
- `config.py`: `PRIORITY_PAIRS` (28), `DEFAULT_BACKFILL=3200`, env-flag pattern
  `os.environ.get("X", "false").lower() in ("1","true","yes")`, 15min/1h in `INTERVAL_SECS`.
- `cache.read_candles(symbol, interval, limit)` and `cache.max_ts(symbol, interval)`.
- `alerts.py`: `_is_throttled/_mark_sent/_post_discord/_fmt_price/_check_rr(entry,sl,tp1,
  dir_str,min_rr,symbol)`, `_news_blocks_pair(pair, blocked)`, `_pair_currencies(pair)`,
  `forexfactory.currencies_in_window(60, high_only=True)`, `_now_utc_str()/ _now_sast_str()`,
  `_COLOURS["watch"]`. Throttle rule format e.g. `btmm123_{tf}_{setup_type}_{dir}_{grade}`.
- `scheduler.py`: `_run_btmm123_alerts()` pattern at lines 304–325; `refresh_all()` calls it
  at 146–151 inside a try/except. Add ID50 as the next sibling block.
- `healthcheck.py`: endpoint loop at line 90, `_drive()` pipeline dry-run at 115–139.
- `app.py`: `/btmm123` (line 378) 180s cache, `_BTMM123_CACHE`, stale via
  `cache.max_ts(sym, "1h") + 2*INTERVAL_SECS["1h"] + 60`; `/btmm123/detail` (line 410) rebuilds
  `calc_ema` arrays + re-indexes pattern markers to the display window.
- `tdi123_backtest.py`: the reference engine — per-bar worst-case tie-break (SL wins on same
  bar), open-trade exclusion from win/loss stats, cached-only reads.
- Tests live in `service/` as `test_*.py` (pytest). `test_btmm_123.py` has an
  `_should_alert_btmm123` gate matrix to mirror at its line ~183.

## File map

| File | Action |
|---|---|
| `service/btmm_id50.py` | CREATE — detector + score/grade + trade plan + analyze_pair/universe |
| `service/test_btmm_id50.py` | CREATE — unit + boundary + plan + gate tests (TDD) |
| `service/btmm_id50_backtest.py` | CREATE — cached walk-forward + filter-contribution + sweep |
| `service/config.py` | EDIT — `BTMM_ID50_*` flags (block after BTMM123 flags) |
| `service/app.py` | EDIT — `/id50` + `/id50/detail` endpoints |
| `service/alerts.py` | EDIT — `_should_alert_id50`, `alert_id50_setup`, `_id50_watch_reasons`, `alert_id50_watch` |
| `service/scheduler.py` | EDIT — `_run_id50_alerts()` + call in `refresh_all()` |
| `service/healthcheck.py` | EDIT — `/id50` endpoint + `btmm_id50` pipeline drive |
| `index.html` | EDIT — `id50` mode-tab, container/table, `loadId50/renderId50Table/openId50Chart` |
| `docs/superpowers/specs/2026-09-24-...md` | already exists (approved) |

## Step 0 — config.py flags (do first, tiny)

Add after the BTMM123 block (line 119), same comment conventions. All OFF-ish:

```python
# BTMM ID50 — M15 trend-continuation retest scanner (13/50 cross → EMA-50 retest).
# NEW strategy, zero track record: ALERTS default FALSE (same evidence-first
# posture as BTMM123). Dashboard-visible immediately; Discord only after the
# walk-forward backtest (btmm_id50_backtest.py) shows a real edge.
BTMM_ID50_ALERTS_ENABLED = os.environ.get("BTMM_ID50_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")
BTMM_ID50_SESSION_FILTER = os.environ.get("BTMM_ID50_SESSION_FILTER", "true").lower() in ("1", "true", "yes")
BTMM_ID50_NEWS_FILTER = os.environ.get("BTMM_ID50_NEWS_FILTER", "true").lower() in ("1", "true", "yes")
BTMM_ID50_GRADE_A_ONLY = os.environ.get("BTMM_ID50_GRADE_A_ONLY", "true").lower() in ("1", "true", "yes")
BTMM_ID50_WATCH_ALERTS_ENABLED = os.environ.get("BTMM_ID50_WATCH_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")
```

## Step 1 — btmm_id50.py (TDD: write test_btmm_id50.py first)

### 1.1 Module constants (match spec exactly)

```python
ATR_PERIOD = 14
MOVE_AWAY_ATR_MULT = 0.75        # move-away must clear ≥ 0.75 × ATR from cross bar
RETEST_TOL_PIPS = 2.0            # EMA-trap tolerance floor (pips)
RETEST_TOL_ATR = 0.35            # EMA-trap tolerance multiple of ATR
TOL_ATR_MULT = 1.5               # asian tightness: < min(50, 1.5×M15 ATR) pips
CROSS_OK_BARS_FULL = 10          # 13/50 cross age: ≤10 bars = full credit
CROSS_OK_BARS_PARTIAL = 20       # ≤20 = partial credit, >20 = grade floored to C
RETRACE_MIN_PCT = 0.25           # retest must retrace ≥ 25% of the move-away leg
BARRIER_BARS = 96                # backtest outcome window (1 M15 day), scratch after
```

Add module-level `ID50_PARAMS` dict (defaults mirror the above) so the backtest's sweep can
override per-run without touching live defaults; `_analyze_id50(..., params=None)` merges
`{**ID50_PARAMS, **(params or {})}` before use. **Do not change the public
`analyze_pair(symbol, m15_candles, h1_candles=None)` signature.**

### 1.2 Detector helpers (each pure, dict-returning, unit-testable)

Define (names and shapes for the plan; exact math is in the spec):

- `_cross_state(closes)` → `{"idx": int|None, "direction": str|None, "bars_ago": int|None}`
  via `_last_ema_cross(ema_stack(closes)["ema13"], closes EMA-50 ...)`. Use
  `calc_ema(closes,13)` and `calc_ema(closes,50)`; `_last_ema_cross` returns the LAST
  (most recent) cross in the series. `bars_ago = len(closes) - 1 - idx`.
- `_anchor_fractal(bars, cross_idx, direction)` → `{"anchor": float, "idx": int}` — nearest
  swing fractal at/before the cross (use the swing list from
  `tdi_cycle_123._find_swings(bars)`; pick highest swing low for BUY / lowest swing high for
  SELL with `idx <= cross_idx`, tie → closest to cross).
- `_move_away(bars, closes, cross_state, anchor, atr)` → `{"peak": float, "ok": bool,
  "magnitude_atr": float}`. BUY: `peak = max(high[ cross_idx .. ])`; `ok` iff
  `(peak - max(close at cross, anchor)) >= MOVE_AWAY_ATR_MULT * atr`. SELL mirrors.
- `_first_retest(bars, closes, peak_idx, direction)` → `{"retest_low": float, "retest_idx":
  int, "ok": bool, "retrace_pct": float}`. First retest = NO prior bar between `peak_idx+1`
  and last bar closes through (i.e. for BUY: no prior bar `close < peak` after the peak bar
  except this current one). Retracement from peak toward anchor must be ≥
  `RETRACE_MIN_PCT` of the move-away leg.
- `_ema_trap(bars, closes, direction)` → `{"trap": bool, "rejection": bool, "tol": float}`.
  tol = `max(RETEST_TOL_PIPS * pip, RETEST_TOL_ATR * atr)`; current bar body overlaps
  `e50 ± tol` AND closes back on the trade side.
- `_entry_candle(bars, direction, atr)` → `{"ok": bool, "reason": str}`. Use
  `detect_nameable_candle(bars)`: ok if name ∈ {Hammer, Bullish Engulfing, RRT, COW} (BUY)
  / {Shooting Star, Bearish Engulfing, RRT, COW} (SELL) with matching `direction`; else
  outside-bar close-through (close beyond prev high/low); else body ≥ 1.2 × ATR closing
  trade-side. `Doji` → hard fail.
- `_tdi_confirmation(closes)` → `{"ok": bool}` = `detect_rsi_signal_cross(calc_tdi(closes),
  lookback=5)["crossed"]`.
- `_move_away_room(bars, closes, direction)` → `{"room_pips": float, "ok": bool}` —
  `room_pips = |peak − e50| / pip`; `ok` iff `room_pips >= 2 * sl_risk_pips` (feed SL risk in
  or recompute).

### 1.3 Hard-rule gate (H1–H8) — all must pass or `setup="NO-TRADE"`, `grade="NO-TRADE"`

Order exactly as spec section 1. Fail returns a row with `reason` naming the failed rule.
Needs ≥ 220 M15 candles (13, 50 cross needs ~60; EMA-50/200 stack needs 200; `len < 220 →
{"reason": "need >=220 M15 candles"}`).

1. M15 range check (each bar `high > low`), stop if violated.
2. Cross direction matches trade direction AND `bars_ago ≤ CROSS_OK_BARS_PARTIAL`.
3. Trend stack: BUY → `close > e50 > e200`; SELL → `close < e50 < e200`.
4. `_move_away().ok`.
5. `_first_retest().ok`.
6. `_ema_trap()` → trap signal allowed only when rejection/close-through confirms (spec: the
   trap itself is the entry condition — bar must be ON the trade side per H2, so trap + close
   trade-side = ok; a close on the wrong side fails).
7. `_entry_candle().ok` (Doji → fail).
8. Anchor sanity: anchor must be on the correct side of current price for the trade.

### 1.4 Scoring (26 max) — exact table in spec section 2

| Cond | pts |
|---|---|
| Anchor present (H8) | 2 |
| 13/50 cross ≤10 bars | 2 |
| 13/50 cross 11–20 bars | 1 |
| cross >20 bars | fl→C |
| Trend stack aligned (H3) | 2 |
| Move-away ≥ 0.75×ATR | 2 |
| First retest (H5) | 3 |
| EMA-trap + rejection (H6) | 2 |
| TDI RSI/signal cross ≤5 bars | 2 |
| H1 bias aligned (via `_htf_bias(h1_candles)`) | 2 (neutral 0 / counter −2) |
| e50/e200 aligned + trending right way | 2 (`+1` if crossing right way, `−2` if crossed counter) |
| Asian < min(50, 1.5×M15 ATR) pips | 1 |
| Active session (kz) | 1 |
| Stop hunt/reclaim at retest | 2 (or −1 against) |
| Pivot location good (via `_location`) | 1 (poor/wrong → cap to C) |
| R:R room +(`room ≥ 2×SL risk`) | 2 |
| **Total** | **26** |

`score < 12 → grade="NO-TRADE"`. R:R room `< 1.0 → floor to C`. Pivot poor/wrongside caps
any A/B to C (same as BTMM123 lines 198–200).

### 1.5 Grades

`_grade_from_score`: `>=22 → "A+"`, `>=19 → "A"`, `>=15 → "B"`, `>=12 → "C"`, else `"NO-TRADE"`.

### 1.6 Trade plan

- `entry = current close`.
- `sl = min(anchor, retest_low) − 0.5 × ATR` (BUY); floored: `sl_dist = max(entry − sl,
  instruments.min_sl_distance(symbol, price))`, then **2-pip clamp** `sl_dist =
  max(sl_dist, 2 × pip)` (floor first, then clamp — order per spec).
- `tp1 = move_away peak`; `tp2/tp3 = 1.5× / 2× projections` of the leg.
- Emit `room_pips`, `rr_to_target` (SL risk in pips), `sl_pips`, `rr1`.

### 1.7 analyze_pair / analyze_universe

`analyze_pair(symbol, m15_candles, h1_candles=None) -> row`; attach `row["h1_bias"]`,
`row["session"]`, `row["in_active_session"]`, `row["location"]`, `row["trade_plan"]`,
`row["score"]`, `row["grade"]`, `row["notes"]`, `row["setup"]` (BUY/SELL/NO-TRADE),
`row["timeframe"] = "M15"`, `row["direction"]`. Row must be JSON-serialisable (no arrays
bigger than needed; detail markers come from `/id50/detail`).

`analyze_universe(candles_by_pair)` iterates `BTMM_ID50_UNIVERSE = list(PRIORITY_PAIRS)`,
looks up `"m15"` and `"1h"` (case-tolerant), try/except per pair, returns the aggregate
dict incl. `grade_aplus` count.

## Step 2 — test_btmm_id50.py (write/run first for each 1.x chunk)

Mirror `test_btmm_123.py` conventions. Use small synthetic candle builders. Cover:
- `_grade_from_score` boundaries: 22→A+, 21→A, 19→A, 18→B, 15→B, 14→C, 12→C, 11→NO-TRADE.
- Hard-rule failures produce `NO-TRADE` with specific `reason` (too-few bars; second retest;
  Doji entry; wrong-side stack; cross too old).
- Scoring: each contributor flips the point count by its exact amount (build a fixture where
  exactly one condition toggles per test).
- Trade-plan math: SL floor + 2-pip clamp engage (`entry − sl < min_sl_distance`),
  TP1 == peak, `rr_to_target` computed.
- Gate matrix test mirroring `test_btmm_123.py` (grade A+ / A / B-with-A-ONLY / session
  False / NO-TRADE) for `_should_alert_id50`. You may need `monkeypatch` on the config bools
  (check how test_btmm_123.py does it — replicate).
- `analyze_universe` smoke: one BUY, one SELL, one NO-TRADE pair → counts correct.

## Step 3 — alerts.py

- `_should_alert_id50(row)`: grade in (A+, A, B) else False; `BTMM_ID50_GRADE_A_ONLY and
  grade == "B"` → False (A+ and A always pass); `BTMM_ID50_SESSION_FILTER and
  in_active_session is False` → False. (None never hard-fails — mirror BTMM123 line 1058.)
- `alert_id50_setup(pair, row)`: mirror `alert_btmm123_setup` order: enabled flag → setup in
  (BUY, SELL) → `_should_alert_id50` → plan complete → `_check_rr(min_rr=0.8)` → news gate
  (`_news_blocks_pair`, fail-open) → throttle rule `f"id50_{tf.lower()}_{setup.lower()}_{grade}"` →
  embed. No m15-nesting (single timeframe). Fields: Direction, **Grade (X/26)**, Setup,
  Entry, Stop Loss (+sl_pips), R:R (1 : rr1), **Structure** (cross age + anchor), **TDI**
  cross ok, **H1 bias**, Retest (retrace %), Move-away (magnitude ATR), Asian (range pips),
  Session, Trap/Entry-candle. Color: `0x34D399` (emerald) — distinct from BTMM123 gold;
  footer "BTMM ID50 · M15".
- `_id50_watch_reasons(pair, row)`: mirror `_btmm123_watch_reasons` gate order and formatting.
- `alert_id50_watch(pair, row)`: mirror `alert_btmm123_watch` — guard `BTMM_ID50_WATCH_
  ALERTS_ENABLED`, grades (A+, A, B), skip when real alert live and all checks pass, throttle
  key `f"id50watch_{tf.lower()}_{setup.lower()}_{grade}"`.

## Step 4 — app.py

- `_ID50_CACHE`/`_ID50_TTL_SECS = 180` right after the BTMM123 cache block (line 375).
- `@app.get("/id50")`: exact mirror of `/btmm123` (378–407) with candles_by_pair `{"m15":
  ("15min", DEFAULT_BACKFILL), "1h": ("1h", DEFAULT_BACKFILL)}`; stale check off `cache.max_ts(
  sym, "15min") + 2 * INTERVAL_SECS["15min"] + 60`; attach `stale_pairs` + `cached_at`.
- `@app.get("/id50/detail")`: mirror `/btmm123/detail` (410–…): `?symbol=` + `?timeframe=M15`
  only (default M15); build `ema50 = calc_ema(closes, 50)` and `ema200 = calc_ema(closes, 200)`
  on M15 closes; attach `ema50`/`ema200` tails and markers `{"cross": idx, "anchor": idx,
  "peak": idx, "retest": idx}` (re-indexed to the `offset` display window like lines 462–469);
  return row + bars + ema arrays. Guard `min(len(entry_bars), ...)` for short series.

## Step 5 — scheduler.py

- Add `_run_id50_alerts()` directly after `_run_btmm123_alerts()` (after line 325), exact
  mirror: read `"m15"` + `"1h"` (`DEFAULT_BACKFILL`), `analyze_universe`, loop rows →
  `alerts.alert_id50_setup` + `alerts.alert_id50_watch`, each in its own try/except.
- In `refresh_all()` after the BTMM123 block (line 151), add the sibling try/except:
  note "M15-based ID50 scanner — dashboard-visible; Discord stays silent until
  BTMM_ID50_ALERTS_ENABLED is on post-backtest."

## Step 6 — healthcheck.py

- Add `"/id50"` to the endpoint loop (`for ep in ("/btmm123", ...)`), line 90.
- Add to dry-run: `import btmm_id50` and a `_drive(btmm_id50.BTMM_ID50_UNIVERSE,
  {"m15": ("15min", DEFAULT_BACKFILL), "1h": ("1h", DEFAULT_BACKFILL)},
  (btmm_id50.analyze_universe, alerts.alert_id50_setup))` alongside the btmm_123 drive.

## Step 7 — index.html

Copy the BTMM123 tab wholesale and adapt (`id50` variant). Exact items (all verified to exist):
- CSS: add `.mode-tab[data-mode="id50"].active` style (emerald `#34d399` family, mirroring
  the tdi123/btmm123 rules at lines 330–333). Add `.id50-chart-modal .modal` selectors
  mirroring line 335. `.id50-summary-tile` mirroring 346–348.
- Mode bar (line ~434): `<button class="mode-tab" data-mode="id50">🎯 ID50</button>` after
  BTMM 123.
- Container block after `#btmm123Container` (after line 888): `id50Container` with the
  approved columns **Pair·TF·Dir·Grade(X/26)·Structure·TDI·H1·Retest·Move-away·Asian·Session·
  Entry/SL/TP1·R:R·View**. Status line "M15 · 13/50 cross → EMA-50 retest · 26-pt scale ·
  A+/A/B/C".
- JS: `id50Filter` state, `id50Data`, `loadId50()`, `renderId50Table(data)` (mirror
  `renderBtmm123Table` 5740–5918: filter tiles `all/ab/a/b`, summary counts incl. A+),
  `openId50Chart(symbol, tf)` hitting `/id50/detail` and drawing the chart with the SAME
  candle/EMA/marker code as `openBtmm123Chart` (5929–…) — EMA50/EMA200 + cross/anchor/peak/
  retest markers + entry/SL/TP lines, using `btmm123-chart-modal`-equivalent classes or the
  shared modal.
- Wire into the tab system: add `'id50'` to `isScanner` set (line 4982), the container
  display toggle (5003–5016), and `if (mode === 'id50') loadId50();`. Add refresh-btn handler
  + `data-mode` filter behaviour identical to BTMM123.

## Step 8 — btmm_id50_backtest.py

CLI: `python btmm_id50_backtest.py [--pair EUR/USD GBP/CHF ...] [--sweep] [--grades A+|A|B|ALL]`.
- Read-only on cache: `cache.read_candles(pair, "15min", limit=DEFAULT_BACKFILL)` +
  `"1h"` `DEFAULT_BACKFILL`. No live fetch.
- Walk-forward per pair exactly in the spirit of `tdi123_backtest._run_one`:
  - warmup = 220 bars; for `i` in `[warmup, n)`: window `bars[max(0,i−warmup):i+1]`,
    h1 window = all `1h` bars with `ts_utc <= bars[i].ts_utc` (rolling pointer).
  - call `analyze_pair(pair, entry_window, h1_bars_window or None)`; keep open trade only
    when setup in (BUY,SELL), grade passes `--grades`, and trade_plan complete.
  - outcome loop: warn-close if SL or TP1 hit on a bar (worst-case tie-break: SL wins);
    **barrier cap**: if still open after `BARRIER_BARS` (96) → `scratch` (0 pips, own bucket,
    excluded from expectancy/win-rate like tdi123's `be`).
- Aggregate exports (writes `btmm_id50_backtest_results.txt` + `.json` in `service/`):
  overall + by-class + per-pair (mirror `_aggregate` incl. pips math),
  **filter-contribution report**: for each score/hard-rule condition, pass vs fail → signals,
  win rate, expectancy lift; and a **grade-tier breakdown** (A+/A/B/C: n, wr, exp).
- `--sweep`: grid `(MOVE_AWAY_ATR_MULT: {0.5,0.75,1.0,1.25}) × (CROSS_OK_BARS_FULL:
  {10,20}) × (RETEST_TOL_ATR: {0.25,0.5,0.75})`, reusing `ID50_PARAMS` override per run;
  print a ranked table by expectancy (best first).
- Single-trade-per-series-per-config open state (mirror tdi123_backtest).

## Step 9 — end-to-end verification (Definition of Done)

1. `cd service` then: `python -m pytest test_btmm_id50.py -q` → all pass (new tests).
2. Full suite: `python -m pytest -q` → no regressions (existing 300+ tests stay green).
3. `python -m py_compile btmm_id50.py btmm_id50_backtest.py app.py alerts.py scheduler.py
   healthcheck.py config.py` → clean.
4. `import app` (or `python app.py` import path) → `/id50` returns JSON with 28 pairs,
   `grade_aplus`, `stale_pairs`, `cached_at`.
5. `python btmm_id50_backtest.py --pair EUR/USD GBP/CHF` → writes results files, prints
   overall + filter contribution (no exception).
6. `python healthcheck.py` → all dashboard endpoints incl. `/id50` pass.
7. `index.html` tab renders: flip to 🎯 ID50, refresh, table + summary tiles + chart modal
   work.
8. Unchanged-byte check: `git diff --stat` shows ONLY config.py, app.py, alerts.py,
   scheduler.py, healthcheck.py, index.html + new files. `tdi_cycle_123.py`, `btmm_core.py`,
   `btmm_123.py` untouched.
9. Do NOT commit unless the user explicitly asks.

## Rollback

- **Config/runtime**: flip `BTMM_ID50_ALERTS_ENABLED=false` (already default) → Discord
  silent. Remove the `_run_id50_alerts()` call in `refresh_all()` (one 3-line block) → no
  scheduler involvement. Dashboard `/id50` + tab remain harmless read-only.
- **Full revert**: delete the 3 new files + `git revert`-style strip of the 5 edited lines/edits
  (all additive, no existing logic altered) → zero residue.
- **Every change is additive**: no existing function signature or behaviour is modified.

## Devil's advocate (what could make this plan fail — check before claiming done)

- `_last_ema_cross` returns the LATEST cross anywhere in the series — if a fresh cross of the
  13/50 happened at the current bar, `bars_ago=0` is fine; but a cross far back paired with a
  retracement could interleave two crosses. Confirm the chosen cross is the one defining the
  move-away anchor (tests must pin a 2-cross fixture).
- `detect_nameable_candle` may not flag the trap bar as a nameable entry — the outside-bar +
  1.2×ATR fallbacks are REQUIRED for the gate to fire realistically. Test must cover the
  fallback path, not just the name path.
- `calc_ema` front-pads to align indices — but `ema_stack` (used by BTMM123) may index
  differently; this module uses `calc_ema` directly for `e50`/`e200` to keep alignment with
  `closes` (verified `calc_ema` pads `[out[0]]*period + out[1:]`).
- `min_sl_distance` for XAU/USD (60 pips = 0.60 price) is huge on M15 — the 2-pip clamp is the
  floor on TOP of that; verify the SL is not *so wide it fails `_check_rr` min 0.8* on TP1 for
  metals (alert stays silent, which is the correct conservative outcome, but the dashboard
  should still render the plan).
- Backtest scratch-vs-expiry semantics: design says win/loss/scratch; make scratch exclusion
  explicit in the printed report so reviewers can't misread it as a loss.
- `refresh_all()` ordering: ID50 is M15-fed; run AFTER `_fetch_guarded(sym, DEFAULT_INTERVAL,
  ...)` (already true since it fetches before any alert runner).