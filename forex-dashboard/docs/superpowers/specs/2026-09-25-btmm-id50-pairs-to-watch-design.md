# BTMM ID50 — "Pairs to Watch" Precursor Panel Design

**Date:** 2026-09-25
**Scope:** Additive precursor panel on the ID50 dashboard tab in `forex-dashboard`. Surfaces pairs that are at the earliest formation stage of the M15 ID50 (a fresh 13/50 EMA cross with a confirmed pre-impulse anchor, H3+H2) but have not yet produced a full ID50 signal. Reuses existing, tested detection internals only — the audited signal pipeline is untouched.
**Status:** Approved in conversation — implementing.

---

## 1. Context

The BTMM doctrine for ID50 (see `2026-09-24-btmm-id50-scanner-design.md`, H2/H3) reads:

> Must have anchor present left → 13/50 cross → first pullback to 50 → trap at 50 → entry trigger.

The scanner today only emits rows when the **entire** sequence completes (H2–H8 all pass, emitting BUY/SELL). Pairs that have just formed the anchor and the 13/50 cross — but are still awaiting the pullback/trap/entry — are invisible on the dashboard. This design adds a **"Pairs to Watch"** panel that catches the setup at that earliest stage, so the trader sees what is developing, not just what has completed.

### Decisions locked with the trader
- **Minimum stage:** 13/50 EMA cross **+ confirmed anchor only** (H3+H2). No H4 stack, no move-away requirement — the broad early-screening net.
- **Freshness:** cross must be ≤ `CROSS_AGE_ACCEPT_MAX` (20 M15 bars ≈ 5 hours).
- **Presentation:** one table with a direction column (Bullish/Bearish pill), not split panels.
- **Architecture:** extend the existing `/id50` payload (Approach A) — no new endpoint, one fetch, one consistent candle snapshot.

---

## 2. Watch criteria (btmm_id50.py)

A pair qualifies for the watch list iff **all** of the following hold on its M15 candles:

| # | Criterion | Algorithm | Source |
|---|-----------|-----------|--------|
| W1 | 13/50 cross exists | `_cross_state(closes)` returns a cross (`idx` not None) | existing H3 |
| W2 | Anchor confirmed | `_anchor_fractal(m15, cross_idx, direction)` returns the pre-impulse swing fractal | existing H2 |
| W3 | Cross is fresh | `cross["bars_ago"] <= CROSS_AGE_ACCEPT_MAX` (20 M15 bars ≈ 5h) | existing constant |
| W4 | Not already a live signal | Pair's own `analyze_pair()` row must **not** be BUY when cross is bullish, nor SELL when cross is bearish (per-direction dedupe, so a bearish watch can coexist with a live bullish signal) | new |

Any pair failing W1–W4 is silently skipped. Missing/empty M15 data is skipped. The watch computation is additive: it replays exactly the tested H2/H3 helpers and does not alter `_analyze_id50` or any existing return value.

### Watch-row fields
`symbol`, `direction` (`"bullish"`/`"bearish"`), `cross_age` (int, bars), `anchor` (float, price), `price` (float, last M15 close). Rows sorted by `cross_age` ascending (freshest first).

---

## 3. Data flow

### 3.1 Module (`btmm_id50.py`)
- New helper `_watch_row(symbol, m15_bars, live_row)` → optional watch-row dict implementing W1–W4.
- `analyze_universe(candles_by_pair)`:
  - already loops all pairs calling `analyze_pair` (unchanged);
  - additionally computes a watch row per pair using the same `m15` candles;
  - appends to `result["watches"]` sorted freshest-first;
  - adds `result["watch_count"]` (int).

### 3.2 Endpoint (`app.py`)
- **No change.** `/id50` already returns the `analyze_universe` result on the existing 5-minute cache.

### 3.3 Frontend (`index.html`)
- "Pairs to Watch" panel at the **top** of the ID50 tab, above the signals table.
- Rendered inside the existing `id50Container`; refreshed by the existing `loadId50()` and refresh button.
- Table columns: `Symbol | Direction (pill) | Cross age | Anchor | Price`.
- Empty state: visible text "No pairs currently at the watch stage." when `watches` is empty.
- Styling matches the existing ID50 tab (reuse table classes and pill styling already present for direction).

---

## 4. Testing (TDD — written before implementation, watched to fail)

`test_btmm_id50.py`:

| Test | Asserts |
|------|---------|
| Fresh cross + anchor | pair appears in `watches` with correct direction/cross_age/anchor |
| Stale cross (>20 bars) | pair excluded |
| Cross but no anchor fractal | pair excluded |
| Live BUY pair (bullish cross) | excluded from `watches`; a bearish cross on the same pair is still listed |
| Ordering | freshest cross first in `watches` |
| Universe shape | `watch_count` consistent with `len(watches)` and the signal rows |

Existing tests must remain green (signal logic untouched).

---

## 5. Error handling and rollback

- Empty/missing M15 candles, or unparseable closes → pair skipped (no crash, no `None` rows).
- If `analyze_universe` is ever called with an empty universe → `watches = []`, `watch_count = 0`.
- **Rollback** is two reversible edits: delete the `watches` computation from `analyze_universe` and remove the panel block from `index.html`. No data is written or destroyed. Nothing committed to git.

---

## 6. Definition of done

1. All tests above pass (plus the existing 222).
2. `py_compile` clean on `btmm_id50.py`.
3. Dashboard restarted; `GET /id50` returns `watches` + `watch_count` (non-empty for a qualifying pair, or empty with count 0).
4. ID50 tab shows the "Pairs to Watch" panel, correctly paginated/sorted, empty-state rendered when applicable.
5. A pair drops off the panel when its cross ages past 20 bars or when a same-direction signal fires.

---

## 7. Devil's advocate

- **Broad net by design:** watch rows do not require e50/e200 alignment, so a pair may sit on the panel without ever maturing into a signal. Accepted: the signals table enforces H4–H8; the panel is explicitly an early-screening net.
- **Freshness is hard-capped at 5h:** a valid setup whose pullback takes longer will drop off the panel before entry. Conscious trade-off chosen by the trader (recommended option); revisit only if setups are observed to stall past the window.
- **Snapshot consistency:** watch rows and signal rows derive from the same cached candle bundle in one `/id50` request, so the panel cannot disagree with the signals table.
- **"Still forming" is not guaranteed:** a watch row only requires the anchor + a fresh (≤ 20-bar) 13/50 cross. A pair whose full sequence *completed and was rejected* within that window (e.g. retest/trap/entry failures, or H1–H8 hard gates failing with a score < 12) can therefore sit on the panel labelled "setup forming" when in fact it already failed. This is accepted precursor-screening behaviour (W1–W4 only), and the signals table remains authoritative — but the UI subtitle now reads "setup forming or recently formed (may have failed; signals table is authoritative)" to avoid overstating the stage. A pair that *has* a live BUY/SELL is always excluded from the panel by W4 dedupe, so the panel and signals table can never contradict each other.