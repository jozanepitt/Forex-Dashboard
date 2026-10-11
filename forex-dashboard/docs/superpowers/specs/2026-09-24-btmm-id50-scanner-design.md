# BTMM ID50 — M15 Trend-Continuation Scanner Design

**Date:** 2026-09-24
**Scope:** New `btmm_id50` detector + scanner + dashboard tab + alerts + filter-contribution backtest in `forex-dashboard`, mirroring the `btmm_123` architecture.
**Status:** Approved in conversation (sections 1–4) — implementing.

---

## 1. Context

The trader's reference material (BTMM Patterns & Setups, Steve Mauro's doctrine) describes **ID50** as an **M15 intraday 50 EMA bounce**: an anchor/peak formation exists, the 13 EMA crosses the 50 EMA, price makes a meaningful move away from the 50 EMA, then returns to test the 50 EMA for the **first quality bounce** — with a rejection/"trap" trigger and TDI confirmation. It is explicitly NOT "price merely touching the 50 EMA".

The dashboard already ships a legacy 5-gate `detect_fifty_fifty_bounce` (labelled "50/50 Bounce == ID 50") inside the H1-shape `btmm_core.analyze()`. This design adds the **authoritative** M15 ID50 as a dedicated scanner. `bounce5050` stays untouched and is documented as legacy, scheduled for retirement once ID50 shows edge.

### Decisions locked with the trader
- Build scanner + backtest harness in the same iteration.
- `bounce5050` kept parallel (no behaviour change to audited pipeline).
- Backtest runs over **cached** M15 candles (~170 days, 28 pairs) — zero API cost, reproducible from `candles.db`.
- Architecture: standalone module mirroring `btmm_123`.

---

## 2. Detection pipeline (`btmm_id50.py`)

### 2.1 Inputs
- M15 bars (primary), H1 bars (bias only), symbol. Missing H1 degrades bias to unknown — never a hard fail.
- Reused untouched helpers from `btmm_core` (`calc_ema`, `detect_level_count`, `detect_asian_range`, `detect_stop_hunt`, `detect_nameable_candle`, `calc_tdi`, `detect_rsi_signal_cross`, `_last_ema_cross`) and `tdi_cycle_123` (`_session_label`, `_in_active_session`, `_atr`, `_weekly_fib_pivots`, `_location`, `_htf_bias`).

### 2.2 Hard rules (all must pass, else NO-TRADE)

| # | Rule | Algorithm |
|---|------|-----------|
| H1 | M15 timeframe | Detection only ever runs on M15 bars |
| H2 | Anchor exists | Last e13/e50 cross index `cx` via `_last_ema_cross`. Scan `[cx−2, now−5]` for most recent swing fractal on the pre-impulse side (BUY: `low[i] ≤ lows[i±2]`). Must exist and be ≥5 bars old |
| H3 | 13/50 cross | Last `e13/e50` cross direction == trade direction |
| H4 | Correct EMA direction | `price > e50 > e200` (BUY) / mirrored |
| H5 | Move away | Since `cx`, max extension past e50 on trade side `≥ MOVE_AWAY_ATR_MULT × ATR(14)`. Default `0.75`. Tracks the peak-away bar (retracement origin) |
| H6 | First retest | After peak-away bar, price returned to e50 with no **prior close-through retest** (a bar that retested with a close back on the trade side = second retest → fail). Retracement depth ≥ 25% of move-away |
| H7 | 50 EMA trap | Current bar range overlaps e50 ± `tol`; wicks into/through e50; **closes back on the trade side**; rejection signature (BUY: lower wick ≥ 0.5×body). `tol = max(RETEST_TOL_PIPS × pip, RETEST_TOL_ATR × ATR)`, defaults 2 pips / 0.35 ATR |
| H8 | Valid entry candle | Latest M15 candle ∈ {Hammer, Engulfing, RRT, COW, Morning Star, Evening Star} matching direction (`detect_nameable_candle` + local `_detect_star`), OR outside bar with close-through-EMA, OR body ≥ 1.2×ATR closing on the trade side. Doji–alone fails |

### 2.3 Soft factors (scored, stacked on hard gates)

| Condition | Pts | Source |
|-----------|----:|--------|
| Valid anchor confirmed | +2 | H2 |
| Recent 13/50 cross | +2 | bars since cross ≤10 → +2; ≤20 → +1; >20 → 0 (grade floored to C) |
| Correct EMA direction | +2 | H4 |
| Meaningful move away | +2 | H5 |
| First 50 EMA retest | +3 | H6 |
| 50 EMA trap/rejection | +2 | H7 |
| TDI confirmation | +2 | `detect_rsi_signal_cross` in trade direction within 5 bars |
| H1 aligned | +2 | H1 bias == direction; neutral 0; counter −2 |
| 50/200 aligned | +2 | e50 vs e200 aligned +2; crossing right way +1; counter −2 |
| Tight Asian range | +1 | `detect_asian_range` valid AND `range_pips < min(50, 1.5×M15 ATR pips)` |
| Session timing | +1 | London/NY/Overlap kill zone at signal bar |
| Stop hunt/reclaim | +2 | `detect_stop_hunt` active with fade == trade; against −1 |
| Good pivot location | +1 | `_location` usable; poor/wrong-side caps grade to C |
| R:R room to target | +2 | measured move (peak−e50 for BUY) ≥ 2×SL risk |

**Maximum = 26.**

### 2.4 Grading → display/alert mapping

| Score | Grade | Display | Alert |
|------:|:-----:|---------|-------|
| 22–26 | A+ | TRADE | Yes (subject to config gates) |
| 19–21 | A | TRADE | Yes |
| 15–18 | B | WATCH | Watch embed only (default OFF) |
| 12–14 | C | WATCH (low) | No |
| <12 | Reject | filtered | No |

- Any hard rule fails → NO-TRADE regardless of score.
- Poor pivot location caps A/B → C. R:R room < 1.0 floors grade to C.

### 2.5 Trade plan
- Entry: current M15 close. SL: `min(anchor, retest_low) − 0.5×ATR` (BUY), floored at `instruments.min_sl_distance`, with a 2-pip clamp (same as `btmm_123`).
- TP1: move-away peak (structural target). TP2/TP3: 1.5× / 2× measured-move projections.
- `room_pips`, `rr_to_target` emitted; `room ≥ 2×SL risk` scores +2.

### 2.6 Tunable constants (module-level, swept by the backtest)
`MOVE_AWAY_ATR_MULT` (0.75), `RETEST_TOL_ATR` (0.35), `RETEST_TOL_PIPS` (2), `CROSS_AGE_GOOD_MAX` (10), `CROSS_AGE_ACCEPT_MAX` (20), `RETRACE_MIN_FRAC` (0.25), grade bands (22/19/15/12), barrier cap (96 bars).

---

## 3. Wiring

### 3.1 Public API
```python
BTMM_ID50_UNIVERSE = list(PRIORITY_PAIRS)
analyze_pair(symbol, m15_candles, h1_candles=None) -> row
analyze_universe(candles_by_pair) -> summary   # {universe, buys, sells, grade_a, grade_aplus, grade_b, pairs[]}
```
Row shape mirrors `btmm_123` + `id50` breakdown dict (`hard_rules[]`, per-condition score table, anchor/cross-age/move-away/retest/trap details).

### 3.2 `config.py` — `BTMM_ID50_*` flags
`ALERTS_ENABLED=false`, `SESSION_FILTER=true`, `NEWS_FILTER=true`, `GRADE_A_ONLY=true`, `WATCH_ALERTS_ENABLED=false` (post-review posture, mirroring BTMM123).

### 3.3 `app.py`
- `/id50` — cached 180s universe scan (`cache.read_candles` M15+H1), `stale_pairs`+`cached_at`.
- `/id50/detail?symbol=&timeframe=M15` — row + raw M15 bars + EMA50/EMA13 arrays + structure markers.

### 3.4 `alerts.py`
- `_should_alert_id50(row)` (grade, session, A-only).
- `alert_id50_setup(pair, row)` — embed: grade/score **X/26**, structure checklist, trade plan, R:R-to-target, session; throttle key includes grade+setup.
- `alert_id50_watch(pair, row)` + `_id50_watch_reasons()` — grey "still forming" mirror.

### 3.5 `scheduler.py`
`_run_id50_alerts()` invoked beside `_run_btmm123_alerts()`, with the standard M15-leg recursion guard.

### 3.6 `healthcheck.py`
Add `(btmm_id50.analyze_universe, alerts.alert_id50_setup)` to the strategy map + `/id50` endpoint probe.

### 3.7 `index.html`
New 🎯 **ID50** tab: mode-tab, container, summary tiles (A+/A/B, BUY/SELL), filter tiles (A+/A/B/all), `loadId50()`/`renderId50Table()`/`openId50Chart()` mirroring the BTMM 123 tab; columns Pair · TF · Dir · Grade (X/26) · Structure · TDI · H1 · Retest · Move-away · Asian · Session · Entry/SL/TP1 · R:R · View; detail modal via `/id50/detail`.

---

## 4. Backtest harness — `btmm_id50_backtest.py`

```
python btmm_id50_backtest.py [--pair EUR/USD ...] [--sweep] [--grades A+|A|B|ALL]
```
- Source: cached M15+H1 (`limit=DEFAULT_BACKFILL`), ~170d × 28 pairs.
- Walk-forward over `bars[:i]`; A+/A/B signals recorded with all scored-condition flags, cross-age, move-away pips, Asian pips, session, stop-hunt.
- Outcome: barrier scan (cap 96 M15 bars); win = TP1 reached first, loss = SL breached first, else scratch. Win rate, avg R, expectancy (R).
- Filter-contribution report: pass/fail splits per condition + grade-tier breakdown.
- `--sweep`: grid over `MOVE_AWAY_ATR_MULT {0.5,0.75,1.0,1.25}`, cross-ok {10,20}, `RETEST_TOL_ATR {0.25,0.5,0.75}`.
- Writes `btmm_id50_backtest_results.txt` + JSON.

---

## 5. Tests — `test_btmm_id50.py`

Per-detector units (cross direction/age, anchor fractal, move-away, first-retest incl. prior-close-through case, trap/rejection, entry candle, TDI scoring, H1/50-200 alignment, pivot cap), grade-band boundaries (22/19/15/12), trade-plan math (SL floor, clamp, room scores), fixture-driven walk-forward smoke (known-good → TRADE; second-retest → NO-TRADE), `_should_alert_id50` gate matrix.

---

## 6. Definition of done

- `pytest test_btmm_id50.py` AND full existing suite pass unchanged.
- `btmm_id50_backtest.py` runs on EUR/USD + GBP/CHF, writes results.
- `import app` succeeds; `/id50` returns JSON; ID50 tab renders in served `index.html`.
- Existing BTMM/H1 pipelines byte-for-byte unchanged (`bounce5050` included).

## 7. Rollback

Purely additive. Revert = delete `btmm_id50.py`, `btmm_id50_backtest.py`, `test_btmm_id50.py`; remove `/id50` endpoints, `_run_id50_alerts`, healthcheck entry, `BTMM_ID50_*` flags, and the ID50 tab block. Nothing existing changes.

## 8. Risks / notes
- The 26-pt scores and grade bands are **engineering choices, not BTMM doctrine** — the backtest decides what the shipped defaults are.
- "First retest" definition (no prior close-through retest) is the main judgement call; the smoke test locks the intended behaviour.
- ~170d cached M15 is a first sample, not a definitive edge proof; harness is data-source-agnostic for a deeper MT5-extended pass later.