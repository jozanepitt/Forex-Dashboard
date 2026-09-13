# VWAP+9EMA — MT5 Honest Backtest Validation — Design Spec

**Date:** 2026-09-13
**Source document:** `C:\Users\jzpit\Downloads\MT5_Execution_Plan.md` (+ duplicate `_1.md`, identical), with `fetch_mt5.py` / `backtest_mt5.py` provided alongside.

## Critical caveat — read this before anything else

This is **not a brand-new strategy**. The dashboard already shipped a "VWAP+9EMA" scanner (`service/vwap9ema_strategy.py` / `vwap9ema_backtest.py`) and retired it on 2026-09-11, logged in this repo's own spec (`2026-09-10-vwap-mean-reversion-strategy-design.md`, line 4) as: **"VWAP+9EMA — 'doesn't work', per user."** Those files were never committed to git (no history recoverable), so the exact old rules and the exact old verdict's evidence are both lost — we don't know if "doesn't work" came from a real backtest, an informal spot-check, or live/demo trading.

**Decision (user, 2026-09-13):** proceed anyway. The entire point of the new MT5 plan is to settle this properly — a broker-realistic backtest (real per-bar spread, out-of-sample split, parameter sensitivity, Monte Carlo drawdown) — rather than relying on whatever informal check produced the earlier verdict. If Phase 3 validation fails, that *confirms* the 2026-09-11 verdict with real evidence this time. If it passes, the earlier verdict was likely based on incomplete data. Either outcome is a legitimate result of doing this properly.

## Scope

**In scope:**
1. Data-fetch tooling: pull years of M5 bars with real spread from MT5 for the validation universe.
2. Backtest + honest validation: in-sample, out-of-sample (last 40%), parameter sensitivity sweep, per-year breakdown, Monte Carlo max-drawdown — exactly as specified in the source plan's Phase 3.
3. **Conditional** dashboard integration (new tab, scanner, Discord alerts) — built **only** for symbol/session combinations that clear the plan's own acceptance criteria on out-of-sample data.

**Explicitly out of scope:**
- Phase 5 (demo forward-testing, `execution.py`, order placement). That's manual trading by the user on a demo account, not something to automate here.
- Any merge into the existing "VWAP Mean Reversion" tab — confirmed with the user this is a **separate strategy**, opposite in nature (trend-continuation pullback vs. that strategy's counter-trend fade), and gets its own tab.
- Backfilling git history for the old, deleted `vwap9ema_strategy.py` (not recoverable).

## Universe & test parameters (user-approved)

- **Symbols:** `USTECm` (primary — the plan's own focus), plus the dashboard's existing 7 USD majors: `EURUSDm`, `GBPUSDm`, `USDJPYm`, `USDCHFm`, `AUDUSDm`, `USDCADm`, `NZDUSDm`.
- **History:** 2021-01-01 → present (~5 years of M5), per the plan's own default.
- **Sessions:** London (server hours 9–17) and NY (15–23) tested **separately** per symbol — a symbol may pass on one session and fail the other; both must be reported.
- **Strategy parameters swept in validation** (per the plan, unchanged): EMA length {7, 9, 13, 21}, R:R {1.5, 2.0, 3.0}, stop buffer 0.10 (fixed, not swept — plan doesn't sweep it either).

## Architecture — Phase 1-3 (backtest tooling)

Adapt the two provided scripts into the repo rather than leaving them as loose files in Downloads, following the placement convention already used for `vwap_mean_reversion_backtest.py`:

- `service/vwap9ema_backtest/fetch_mt5.py` — near-verbatim port of the provided script (connects to the already-running, already-authenticated MT5 terminal via the `MetaTrader5` package; confirmed working on this machine — `mt5.initialize()` returns `True`). Writes `service/vwap9ema_backtest/data/<SYMBOL>_M5.csv`.
- `service/vwap9ema_backtest/backtest_mt5.py` — near-verbatim port of the provided script (session-anchored VWAP, 9-EMA trend filter, pullback-and-reject entry, real per-bar spread cost, `--validate` mode running all four validation checks).
- `service/vwap9ema_backtest/data/` and `service/vwap9ema_backtest/results/` are gitignored — multi-year M5 CSVs and equity-curve PNGs are local artifacts, not committed.
- No changes to the live dashboard, scheduler, or any existing strategy module in this phase. This tooling runs standalone, on demand, from the command line — it does not touch `app.py`, `scheduler.py`, or `alerts.py`.

**Why port instead of running from Downloads:** reproducibility (anyone — including a future session) can re-run the exact validation later) and consistency with how the existing VWAP Mean Reversion backtest tool is already organized inside the repo.

**Data flow:** MT5 terminal (already open, logged into Exness) → `MetaTrader5` Python package → `fetch_mt5.py` (`copy_rates_range`, per-bar spread + tick volume) → CSV → `backtest_mt5.py` (session filter → VWAP/EMA calc → signal/entry/exit simulation with real spread cost) → console report + equity-curve PNG.

## Validation gate (go/no-go — the whole point of this exercise)

Run `backtest_mt5.py --symbol <SYM> --session <London|NY> --validate` for every symbol × session pair in the universe (16 runs: 8 symbols × 2 sessions). For each, report:

- Full-sample and out-of-sample (last 40%) trade count, win rate, expectancy (R), profit factor, net return, max drawdown.
- Parameter sensitivity table (EMA × R:R grid) — flag any combo that only works at one exact setting as curve-fit.
- Per-year expectancy/net-return breakdown.
- Monte Carlo (2000 shuffles) median and 5th-percentile max drawdown.

**A symbol/session combination is a PASS only if, on out-of-sample data:** expectancy > 0 **and** profit factor ≥ 1.2 **and** ≥ 150 trades **and** positive in the majority of tested years. Everything else is a FAIL for that combination — not tuned further, not shipped to the dashboard.

Results (pass/fail per combination, with the actual numbers) get written to `service/vwap9ema_backtest/VALIDATION_RESULTS.md` and reported to the user directly — this is a decision point, not an implementation detail to skip past.

## Architecture — Phase 4 (conditional: only for combinations that pass)

Built **only** if at least one symbol/session combination passes. If none pass, this phase does not happen — the user gets a clear "nothing cleared the bar, here's why" report instead, matching the source plan's own stated "good outcome" of a negative result.

- **New module** `service/vwap9ema_strategy.py` (name reused — the old one is gone and untracked, no collision), mirroring the `analyze_pair()`/`analyze_universe()` convention shared by every other strategy on this dashboard (TDI123, BTMM123, VWAP-MR). Implements the *exact* validated rule per symbol/session, scoped only to combinations that passed.
- **Parameter selection when more than one combination passes for the same symbol/session:** use the passing combination with the highest out-of-sample expectancy (R); tie-break by highest out-of-sample profit factor. This is decided per symbol/session independently — USTECm/NY and EURUSDm/London can end up on different EMA/R:R settings.
- **Grading:** Grade A = live signal matches that selected parameter combination on a symbol/session that passed. Grade B = a looser variant (different EMA/RR that did not itself pass validation) — dashboard-visible, Discord-silent (same convention as BTMM123's own "no track record yet" default). No Grade C.
- **New tab:** "VWAP+9EMA", routes `/vwap9ema` and `/vwap9ema/detail`, table showing entry/stop/target/R:R/session/grade — same shape as the other strategy tables.
- **New config** in `config.py`: `VWAP9EMA_ALERTS_ENABLED` (default `true`, but only for passing combos), `VWAP9EMA_GRADE_A_ONLY` (default `true` — learned directly from the BTMM123/TDI123 signal-fatigue fix earlier today), `VWAP9EMA_WATCH_ALERTS_ENABLED` (default **`false`** — same lesson, no "still forming" embeds on a brand-new strategy), `VWAP9EMA_NEWS_FILTER` (default `true`, reuses the existing ForexFactory infra).
- **Discord alert** `alert_vwap9ema_setup(pair, row)` — Grade A only, same throttle/session/news-gate pattern as every other alert function in `alerts.py`.
- **Scheduler wiring:** `scheduler.py` calls `analyze_universe()` for the validated symbol list only (not the full 8-symbol universe if some failed) on the M5 refresh cycle.

## Error handling

- `fetch_mt5.py`: if `mt5.initialize()` fails or a symbol can't be selected, fail loudly with the exact MT5 error code (already the provided script's behavior) — no silent empty-data fallback, since a backtest on partial/missing data is worse than no backtest.
- `backtest_mt5.py`: zero-trade sessions report `"no trades"` explicitly rather than crashing on empty arrays (already handled in the provided script's `stats()`).
- Live scanner (Phase 4 only): follows the exact same fail-open/fail-closed conventions already established in `tdi_cycle_123.py`/`btmm_123.py` (e.g., a `None` filter flag from unavailable data never hard-fails a signal).

## Testing

- Phase 1-3 tooling: no new unit tests planned — this is offline research tooling run on demand, validated by its own output (the validation report itself is the check). Matches how the existing `vwap_mean_reversion_backtest.py` single-pair replay tool is treated (no dedicated test file).
- Phase 4 (conditional): `service/test_vwap9ema_strategy.py` covering the grading logic and the exact validated entry/stop/target calculation against hand-computed values on a synthetic candle sequence — same bar this dashboard holds every other strategy to (see the VWAP Mean Reversion spec's testing section for the precedent).

## Implementation planning note

This spec covers two cycles, not one. The implementation plan produced from this spec will cover **Phase 1-3 only** (data tooling, backtest engine, validation run, results report) — that work is fully specified above and can be planned and built now. **Phase 4 cannot be planned yet**: which symbol/session combinations qualify and which exact parameter combination wins each one are outputs of Phase 3, not known today. Once Phase 3 produces real numbers, Phase 4 (if anything passed) gets its own short follow-up plan against this same spec's Phase 4 section — no new brainstorming round needed unless the results surface something this spec didn't anticipate.

## Open risk

The two provided scripts are copied close to verbatim from the source document; I have not yet independently re-derived their VWAP/EMA/entry math against a hand-computed example the way the VWAP Mean Reversion spec did for its own formulas. Before trusting Phase 3's numbers, I will spot-check `backtest_mt5.py`'s VWAP and entry logic against a small hand-built candle sequence as part of implementation — flagging this now rather than silently assuming the provided code is correct.
