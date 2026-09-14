# VWAP+9EMA Live Scanner (Replacing 1AM CRT) — Design Spec

**Date:** 2026-09-14
**Source:** `docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md` (the honest backtest this strategy just ran) and `service/vwap9ema_backtest/VALIDATION_RESULTS.md` (the result).

## Critical caveat — read this before anything else

**This strategy failed its own validation gate.** 0 of 48 symbol/session/variant combinations passed the acceptance criteria (OOS expectancy > 0, profit factor ≥ 1.2, ≥150 trades, positive in the majority of tested years). The entire point of that exercise was to decide, with real evidence, whether to build exactly what this spec describes — and the honest answer was no.

**Decision (user, 2026-09-14):** build it anyway, for manual trading with the user's own judgment and risk management — not as an automated, blindly-trusted signal. The user was told the result plainly and confirmed they want to proceed regardless. Every part of this design reflects that: no misleading grade tiers, no claim of validated confidence, a visible "unvalidated" warning on the dashboard tab and every Discord alert, and scope restricted to the only two symbol/session combinations that ever showed positive (if still failing) expectancy.

## Scope

**In scope:**
1. Retire 1AM CRT's dashboard tab, route, scheduler job, Discord alert, and config flag.
2. New live `service/vwap9ema_strategy.py` scanner implementing the `vp-climax` variant (the only one that ever produced positive expectancy) at `ema=9, rr=2.0` — the exact configuration behind the near-miss numbers — reusing the already-tested math from `service/vwap9ema_backtest/volume_profile.py` and `backtest_mt5.py`, not reimplementing it.
3. New dashboard tab "VWAP+9EMA" in 1AM CRT's former slot, with a persistent unvalidated-backtest warning.
4. New Discord alert `alert_vwap9ema_setup()`, same embed conventions as the other strategies, every embed carrying the same warning.

**Explicitly out of scope:**
- 5AM CRT — untouched. It shares `crt_strategy.py` with 1AM CRT but has its own Discord-only alert path and no dashboard tab; nothing here touches it.
- Grade tiers, watch-alerts, or any confidence scoring for the new strategy — there is no validated basis for any of that, and adding it would misrepresent what this signal is.
- Any change to the offline backtest tooling in `service/vwap9ema_backtest/` — that work is done and committed; this spec only concerns building a *live* version of the one rule that came closest to working.

## Retirement — 1AM CRT

- Remove the `/crt` route and its dashboard tab (`crtTableBody` and the corresponding nav button) from `app.py`/`index.html`.
- Remove `_run_crt_alerts()` from `scheduler.py` and `alert_crt_setup()` from `alerts.py`.
- Remove the `CRT_GRADE_A_ONLY` config flag from `config.py`.
- **Do not touch:** `crt_strategy.py` (5AM CRT still calls `analyze_universe_5am()` from it), `_run_crt_5am_alerts()`, `alert_crt_5am_setup()`, `CRT_5AM_GRADE_A_ONLY`. Verify after removal that no remaining code references anything 1AM-CRT-specific that was just deleted (mirroring the verification step already used when the old VWAP9EMA scanner was retired in September).

## Architecture — live scanner

- **New module** `service/vwap9ema_strategy.py`, `analyze_pair(symbol, m5_candles) -> dict` / `analyze_universe(candles_by_pair) -> dict`, matching the convention every other live strategy on this dashboard already follows.
- **Reuses tested math, does not reimplement it:** imports `compute_session_volume_profile` and `price_passes_vp_filter` from `service/vwap9ema_backtest/volume_profile.py`, and the `ema()` function from `service/vwap9ema_backtest/backtest_mt5.py`. Live candle dicts use a `"volume"` key (already the case for every other strategy's M5/M15 candles, sourced from MT5 `tick_volume` via `providers/exness_mt5.py`); the backtest module's functions take plain arrays, so `analyze_pair` extracts `[c["volume"] for c in candles]` etc. before calling them — a thin adapter, not new math.
- **Signal logic**, adapted from `backtest_mt5.py`'s `run_day()` from "simulate a whole historical day of bars" to "is there a signal as of the most recent closed bar" (the same adaptation every other live scanner already makes relative to how a backtest works):
  1. Trend filter: close and 9-EMA both same side of session VWAP.
  2. Pullback/reject: price tags the 9-EMA then closes back through it in the trend direction (identical condition to `run_day()`'s `sig` logic).
  3. Volume-profile filter: the candidate entry price must pass `price_passes_vp_filter()` against the session's volume profile computed so far.
  4. Volume climax: the entry bar's `tick_volume` (`"volume"` key) must be ≥ 1.3× the trailing 20-bar average — identical threshold to the backtest's `vp-climax` variant.
  5. Entry/stop/target: same formulas as `run_day()` (entry at next-bar open ± half spread, stop at swing ± `buf=0.10` × distance, target at `rr=2.0` × risk — the exact same `buf`/`rr` defaults the validated numbers used, not re-tuned). Live spread isn't known in advance the way backtest CSVs recorded it — use the live spread field already available from `providers/exness_mt5.py` for the current bar (the same source every other strategy's R:R display already uses), not a placeholder.
- **Universe:** `USTECm`, `AUDUSDm` only.
- **Session gate:** London only. The backtest defined this as broker server hours 9–17 (assuming Exness ≈ UTC+2, i.e. UTC 7–15) — at implementation time, verify the *current* live broker offset the same way `fetch_mt5.py`'s `detect_server_offset()` already does, rather than hardcoding an assumption that could be stale (broker offsets can shift with DST). Outside this window: no signal, same as every other session-gated strategy's convention (`in_active_session` field, block only on an explicit `False`, never on missing/unknown data).
- **No grading.** A signal either clears all four gates above or it doesn't — there is no A/B/C tier. The row's `setup` field is `BUY`/`SELL`/`NO-TRADE`, matching the shape other strategies use minus the `grade` field (or `grade` can be a constant `"UNVALIDATED"` string purely for UI consistency with existing badge-rendering code — implementer's call, whichever is less code).

## Dashboard UI

- New tab "VWAP+9EMA" in 1AM CRT's former nav position.
- Persistent banner at the top of the tab, not dismissible: *"⚠ Unvalidated — failed the honest MT5 backtest (0/48 combinations passed). Trade at your own judgment."*
- Table: pair, direction, entry/stop/target, session, and a short "why" note (same convention as other strategies' `notes` field) — no grade column, or a grade column that always reads "UNVALIDATED" if that's less code than omitting the column entirely.

## Discord alerts

- `alert_vwap9ema_setup(pair, row)` in `alerts.py`, same throttle (`RATE_LIMIT_SECS`, one alert per pair+rule per hour) and news-gate conventions as every other alert function.
- Every embed includes the line: *"⚠ Unvalidated strategy — failed backtest (0/48). Not a Grade-A signal like your other alerts."*
- **Config:** `VWAP9EMA_ALERTS_ENABLED` (default `true`). No grade-only flag (nothing to gate by), no watch-alerts flag (none built — deliberately, given everything learned today about premature "still forming" signals being the exact wrong thing to add to a strategy with zero validated edge).
- **Scheduler wiring:** new `_run_vwap9ema_alerts()` in `scheduler.py`, called from the same refresh cycle as the other M5/M15 scanners, scoped to just the 2-symbol universe (cheap — far less work than the 8-symbol strategies).

## Error handling

Follows the exact fail-open/fail-closed conventions already established across every other strategy on this dashboard (`tdi_cycle_123.py`, `btmm_123.py`, `vwap_mean_reversion_strategy.py`): a `None`/unavailable session-offset or volume-profile computation never hard-fails a signal into a false alert; it's treated the same as "not in active session" — no signal, not a crash.

## Testing

- `service/test_vwap9ema_strategy.py`: reuses the same hand-verification rigor already applied to `volume_profile.py` and `backtest_mt5.py`'s VWAP/EMA math (no need to re-derive those — they're already tested; this file tests the *live-adaptation* logic: the "most recent bar" signal detection, the live spread lookup, and that a live candle sequence produces the same entry/stop/target the backtest's `run_day()` would produce for an equivalent bar sequence).
- No watch-alert tests needed (none built).
- Verify after 1AM CRT's removal: run the full test suite and confirm no test references removed 1AM CRT code paths (any that do need updating, not silent deletion — check whether they test something 5AM CRT still needs first).

## Open risk

The live spread source (`providers/exness_mt5.py`'s current-bar spread) has not been cross-checked against what the backtest's CSV-recorded `spread_points` actually represented bar-by-bar — they should be the same underlying MT5 field, but this should be confirmed at implementation time rather than assumed, since a systematic spread-source mismatch would make live entries/stops subtly different from what the (already-failing) backtest even tested.
