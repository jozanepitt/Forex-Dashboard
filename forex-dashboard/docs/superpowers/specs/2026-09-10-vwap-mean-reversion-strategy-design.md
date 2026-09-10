# VWAP Mean Reversion Strategy — Design Spec

**Date:** 2026-09-10
**Replaces:** `service/vwap9ema_strategy.py` / `service/vwap9ema_backtest.py` (VWAP+9EMA — "doesn't work", per user)
**Source documents:** `C:\Users\jzpit\Downloads\VWAP_Mean_Reversion_Strategy.md` / `.pdf` (identical content, verified — see Critical Caveat)

## Critical caveat — read this before the rule set below

The source document is **not** a forex strategy and **not** a ready-to-deploy live-trading ruleset. It is a research/backtesting specification written for US equity index futures/ETFs (its own words: *"Start with a single liquid futures contract or ETF... Recommended primary universe: ES or NQ futures, or SPY/QQQ"*), anchored to US market hours (VWAP resets at 09:30 ET, forced flat at 15:55 ET), and explicitly self-described as *"a hypothesis specification, not a validated edge... every rule herein is an untested hypothesis until your own backtest says otherwise."* It recommends building an offline walk-forward validation framework (2018–2026 historical data, kill criteria, held-out test set used once) before trusting any of it, and it references a separate, unrelated project ("BOS+FVG") of the document's original intended reader.

Verified via two independent read-throughs (one of the PDF cross-checked against the MD, one an independent structured summary of the MD) — both confirm the above with no discrepancies. Full PDF/MD text fidelity confirmed (13/13 pages, no missing content).

**Decision (user, 2026-09-10):** adapt the document's mechanical framework (VWAP + sigma bands, efficiency-ratio regime filter, stall/rejection confirmation, ATR-floored stops, T1/T2 targets) onto forex M15 candles as a new live dashboard scanner — explicitly as an *adaptation of an unvalidated hypothesis spec*, not a literal or backtested implementation. No walk-forward validation, kill-criteria enforcement, or multi-year backtest infrastructure is being built. This spec documents every point where the forex adaptation necessarily departs from the document's literal (US-equity-market) wording, with rationale, so "accurate to the document" means "accurate to its mechanics, honestly adapted" rather than a silent rewrite.

## Scope

Full replacement of the VWAP+9EMA strategy: scanner logic, backtest tool, dashboard tab, Discord alerts (real + new watch alert), and config. Same universe as today (7 majors), same M15 timeframe, same integration pattern as every other strategy on this dashboard (TDI123, BTMM123, CRT). No changes to any other strategy, tab, or alert.

## Architecture

New module `service/vwap_mean_reversion_strategy.py`, mirroring the existing `vwap9ema_strategy.py` shape (`analyze_pair()` / `analyze_universe()`), reusing existing shared helpers (`_atr` from `tdi_cycle_123.py`, `instruments.py` for pip size / price formatting / min stop distance). Same data flow as every other scanner: `scheduler.py` reads cached M15 candles → `analyze_universe()` → per-pair row → `app.py` dashboard route → `alerts.py` real + watch alert evaluation.

**Universe:** `EUR/USD, GBP/USD, USD/JPY, USD/CHF, AUD/USD, USD/CAD, NZD/USD` (same as current VWAP+9EMA — forex tick-volume is too sparse on crosses/exotics, and this strategy leans on volume more than the one it replaces).

## Rule set — mapped to the document, deviations flagged

| # | Doc rule (§) | Forex adaptation | Deviation & rationale |
|---|---|---|---|
| 1 | VWAP anchor: 09:30 ET regular-session open (§2.1) | Reset at 00:00 UTC calendar day | **Deviation.** Forex has no single market open. Matches the existing VWAP+9EMA scanner's convention. |
| 2 | Sigma = cumulative volume-weighted stdev, definition (a) (§2.2) | Same formula; tick-volume proxy as the weight (equal-weight fallback when a bar reports 0) | **Deviation.** No consolidated forex volume exists. Same caveat the existing scanner already carries for its VWAP calc. |
| 3 | ATR-normalized cross-check `d_t` (§2.3) | Computed and displayed alongside z-score | No deviation — doc explicitly recommends this exact cross-check when volume is unreliable. |
| 4 | Minimum 15 bars before any signal is valid (§2.4) | Same, bar-count based | No deviation. |
| 5 | Regime gate: Efficiency Ratio ER(20) < 0.35, hard block (§3.2) | Same formula and threshold, computed on M15 closes; **hard gate** — NO-TRADE when ER ≥ 0.35 | No deviation (user's explicit choice). |
| 6 | Daily exclusions: overnight gap > 1.5×ATR20, month/quarter-turn, rebalance days (§3.2) | **Omitted** | Forex has no daily "regular session open" gap concept intraday; the only analogue (weekend gap) doesn't apply to an always-on scanner. Straight omission, not a disguised equivalent. |
| 7 | Extension: `z_t ≥ +2.0` / `≤ −2.0`, bar index ≥ min (§3.3.1) | Identical | No deviation. |
| 8 | Exhaustion: extension-bar volume > 1.5× 20-bar avg (§3.3.2) | Same threshold, **scored, not a hard gate** | **Deviation.** Tick-volume noise makes a hard gate here risk starving the scanner entirely. Same treatment the existing VWAP+9EMA scanner gives its own volume check. |
| 9 | Stall confirmation — any of 3 variants (§3.3.3) | Implemented identically: close back inside band / rejection wick ≥50% of range / two consecutive lower-highs (or higher-lows) | No deviation. |
| 10 | Fading volume on confirmation bar < extension-bar volume (§3.3.4) | Scored, not a hard gate | Same volume-reliability reasoning as #8. |
| 11 | Regime + no news window, both required (§3.3.5) | Regime: scanner-level hard gate (#5). News: applied at the **alert layer** via the existing ForexFactory news-filter, same infrastructure as TDI123/BTMM123 | **Architectural placement deviation, not a rule deviation** — matches this codebase's existing separation of scanner (objective technical read) from alert layer (news/session gating). Net effect (no alert during news) is identical to the doc's intent. |
| 12 | Confirmation timeout: void after 6 bars (§3.3) | Identical | No deviation. |
| 13 | Stop = `max(structural, 0.75 × ATR14)` (§3.4) | Identical formula; structural = extension-bar extreme + 1-tick buffer via `instruments.py` | No deviation. |
| 14 | Position/account limits: 0.5% risk sizing, max 3 trades/session, daily −2R stop (§3.4) | **Omitted** | Account/execution-management rules for someone with a broker connection. No strategy on this dashboard enforces these (TDI123/BTMM123 don't either) — the dashboard shows setups, the user manages their own risk. |
| 15 | T1 = VWAP (z=0), 60% (§3.5) | Same level, shown as an informational TP1 field | **Deviation in representation** — no partial-position simulation; this is a signal display, not a position tracker. |
| 16 | T2 = overshoot z −0.5 to −1.0 (§3.5) | z = −0.75 (midpoint default), shown as informational TP2 | Same representation deviation as #15; midpoint chosen since no backtest will select within the doc's given range. |
| 17 | Move to breakeven after T1; trail 1.0×ATR after T1 (§3.5) | **Omitted** (informational only, no position tracked) | Same reasoning as #14/#15. |
| 18 | Time stop: exit if T1 not reached within N bars (§3.5) | N=20 bars; shown as a "fresh" vs "stale — past time-stop window" badge, not an auto-exit | Representation deviation — no position to exit; badge instead. |
| 19 | Session close 15:55 ET, flat (§3.5) | **Omitted** — replaced with the existing session-bucket scoring (ACTIVE/LONDON/NY-LATE/ASIAN) already used by VWAP+9EMA | Forex trades continuously; no session close exists. Time-of-day still surfaced (per doc's "measure, don't assume" guidance) as informational scoring, not a hard rule. |

## Scoring (score / 10)

Same convention as the strategy it replaces (VWAP+9EMA was also `/10`). Breakdown sums to exactly 10 at maximum:

- Base 3 — confirmed extension + stall confirmation + regime pass (a real setup exists at all)
- +2 — exhaustion volume confirmed (extension-bar tick-volume > 1.5× 20-bar avg)
- +1 — fading volume on confirmation bar
- Confirmation-type quality: **rejection wick ≥50% of range = +2**, **close-back-inside-band = +1**, **two-bar lower-highs/higher-lows = +1** (only the single confirmation type that actually fired counts; max +2)
- Session bucket: **ACTIVE (overlap) = +2**, **LONDON/NY-LATE = +1**, **ASIAN = +0**, reusing existing session tagging

Extension depth (z ≥ 2.5 / ≥ 3.0) is surfaced as a display badge, not added to the score, to keep the max at a clean 10.

Grade: A ≥ 8, B ≥ 6, C ≥ 4, else NO-TRADE (matches existing thresholds). NO-TRADE whenever regime gate fails, extension never reached, confirmation never found, or the 6-bar confirmation window times out.

## Alerts & config

- `alert_vwap_mr_setup(pair, row)` replaces `alert_vwap9ema_setup` — same R:R/direction sanity gate (`_check_rr`) added tonight, same throttle mechanism.
- `alert_vwap_mr_watch(pair, row)` — new, matching tonight's TDI123/BTMM123 watch-alert pattern (grey "still forming" embed listing exactly what's missing: regime status, extension, exhaustion, confirmation, R:R). Independent enable flag, same "skip only if the real alert would also have fired" logic (including tonight's Critical #1 fix — skip only when the real alert's own switch is actually on).
- New config: `VWAP_MR_ALERTS_ENABLED` (default true), `VWAP_MR_WATCH_ALERTS_ENABLED` (default true), `VWAP_MR_GRADE_A_ONLY` (default false), `VWAP_MR_MIN_SCORE` (default 9 — carrying forward the user's recent VWAP9EMA tightening), `VWAP_MR_NEWS_FILTER` (default true, reuses ForexFactory infra).
- Old `VWAP9EMA_*` config flags and `alert_vwap9ema_setup` removed.

## Dashboard UI

Retire the `/vwap9ema` route and tab; replace with `/vwap-mr` and a "VWAP Mean Reversion" tab. New fields shown: VWAP, z-score, ATR-normalized `d_t`, ER (regime), extension/confirmation detail, Entry/SL/TP1(VWAP)/TP2(overshoot), session bucket, freshness badge. Touches the ~22 existing `vwap9ema` references in `index.html`. The single-pair historical-replay tool (`/backtest?strategy=`) gets a `vwap_mean_reversion_backtest.py` replacement — a quick historical eyeball-check like BTMM's existing one, **not** the document's full walk-forward/ablation framework (explicitly out of scope).

## Testing — non-negotiable given the accuracy requirement

The document's own stated first priority: *"compute session VWAP for three sample days and reconcile it to the cent against a reference source... a twenty-minute check that saves weeks"* and *"unit-test ER against hand-computed values."* New `service/test_vwap_mean_reversion_strategy.py` covering, in the document's own prescribed build order:

1. VWAP reconciliation against hand-computed values on a synthetic candle sequence
2. Sigma/z-score reconciliation against hand-computed values
3. ER regime-filter calculation against hand-computed values (known choppy vs. known trending synthetic sequences)
4. Extension threshold detection
5. Each of the 3 confirmation types, individually
6. Stop calculation (`max(structural, 0.75×ATR)`)
7. Full `analyze_pair()` integration case producing a real BUY and a real SELL setup end-to-end
8. Regime-gate rejection case (trending market → NO-TRADE regardless of extension)
9. Confirmation-timeout expiry case

## Explicitly out of scope

- Walk-forward validation, kill-criteria enforcement, multi-year historical backtest, ablation studies, deflated Sharpe ratio (Sections 5–7 of the source document) — this is a live heuristic scanner, not the research framework the document primarily describes.
- Any position/account-level risk management (sizing, max trades/session, daily stop-out) — no strategy on this dashboard does this.
- Any claim that this produces validated edge or "good trades" — the source document does not make that claim even for its intended market, and nothing here changes that.

## Success criteria

- `vwap_mean_reversion_strategy.py` passes all tests in the Testing section, including VWAP/ER reconciliation against hand-computed values.
- Old VWAP9EMA strategy, backtest, routes, alerts, and config fully removed; nothing references them.
- New scanner wired into the scheduler, dashboard tab, and both real + watch Discord alerts, following the exact conventions already used by TDI123/BTMM123.
- A full synthetic Discord smoke test (same method used tonight for the other strategies) confirms the new alert functions post correctly with no exceptions.
- Every deviation in the Rule Set table is reflected in code comments at the point of implementation, not just in this doc.
