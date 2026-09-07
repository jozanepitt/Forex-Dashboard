# StrictlyCorrect "TDI Cycle / 123 Pattern" — Full Doctrine Synthesis

**Source:** Telegram export of the StrictlyCorrectFX channel and its mentee groups
("4hour Protocol Mentorship", "Divergence Protocol Mentorship", the earlier
"BTMM Modifications" channel), plus 27 short voice-clip videos. Covers
16 Jan 2021 – 6 Sep 2026, ~3,236 text messages, 2,806 unique photos (562
sampled across 6 parallel review passes), 27 videos. Researched via 10
parallel agents (4 full-text, 6 stratified image samples) plus local Whisper
transcription of the videos. This document is the merged, deduplicated
findings — see the session transcript for full per-agent detail if needed.

**Why this exists:** the dashboard's `tdi_cycle_123.py` already implements a
partial version of this pattern, sourced from limited earlier exposure to the
same material. This synthesis checks that implementation against the full
corpus and catalogs everything else the source teaches, so future decisions
about what (if anything) to build are made with the complete picture rather
than a partial one.

---

## 1. Identity and structure of the source material

- Product name: **"TDI CYCLE System"**, sold as "TDI CYCLE Education &
  Training" ($35–$100 depending on promotion), bundled with a **"3 Strategies
  in 1"** Critical Areas system. Core teaching document: **"4H SWING Trading
  Protocol.pdf"** (a numbered-rule PDF) plus a booklet **"BUYING LOW AND
  SELLING HIGH WITH TDI — Trading Peak Formations (123 Patterns)"**.
- Built explicitly as a **fix on top of classic BTMM**, not a replacement:
  *"We have lost a large amount of money using the 'BTMM strategy' alone.
  This strategy was missing something."* And: *"PROBLEMS we previously
  experienced with BTMM: [1] rigid Level 1/2/3 and M/W formations are highly
  SUBJECTIVE — two traders analyzing the same chart interpret differently.
  [2] the BTMM template is OLD, market dynamics changed. [3] Market timing —
  you enter too early. Our SOLUTION = the TDI CYCLE — a FIXED area, no
  guessing whether Level 3 was reached."* This is independent, external
  confirmation of this project's own 2026-07-24 audit finding that the
  existing `btmm_core.py` strategy was "most compromised."
- Two channel lineages found: the original **"BTMM Modifications"** broadcast
  (2021–22, uses "Second Leg"/"Weekly Cycle"/"MultiWeek M Pattern"
  terminology — the pre-TDI-Cycle vocabulary) and the current
  **"4hour Protocol Mentorship"/"Divergence Protocol Mentorship"** groups
  (richer — includes live chat, Q&A, real signal cards).

---

## 2. The 123 Pattern / Peak Formation — confirms existing code, adds precision

Matches `tdi_cycle_123.py` closely. Confirmed exactly:
- Point 1 = swing extreme, Point 2 = counter-move, Point 3 = retest of Point 1's
  zone (equal / marginal overshoot = "stop hunt" / shallow shortfall).
- Divergence 1→3 on the TDI oscillator.
- **Exact numeric thresholds match**, verbatim from the source booklet: TDI
  overbought baseline≥63 / RSI≥68, oversold baseline≤37 / RSI≤32.
- *"This pattern may take up to 4 days to be completed"* — corpus examples
  show a 1–5 day range in practice, not a hard cap.
- *"This pattern is usually followed by divergence and reversal patterns (Ms,
  Ws and head & shoulders)"* — one clean corpus example confirms this
  (123+divergence → Head & Shoulders on the JPY Currency Index).

**New precision not currently coded:**
- **Point 2 has a concrete definition in one source**: *"price touching the
  1h 50 EMA."* Currently the code only requires Point 2 to cross the TDI
  midline — this is a candidate second confirmation, not yet required.
- **A formal two-timeframe hierarchy**, found independently in two source
  excerpts of the same PDF: *"[Rule 3] We only look for Reversal/movement
  when the 4H RSI enters an extreme. This is ALWAYS followed by a TDI
  Baseline Extreme + Divergence on the 1H TF."* A "triple confluence" variant
  is also stated: *4h Extreme + Daily Extreme + Divergence.* This is the
  single most concrete, mechanically well-defined rule found that isn't in
  any current implementation — TDI123 and the current `btmm_123.py` both
  treat H4 as an alignment bonus, not a hard precondition-before-trigger gate.
- **A "Straight Away Setup"** — Point 2/3 can be skipped when divergence at
  Point 1 is unusually strong.
- **123 patterns nest fractally across timeframes**, and the rule for which
  one to trade is explicit: *"When EURJPY was at 4h Point 3, another 1h 123
  Reversal Pattern was being formed. We had to WAIT for the 1h Pattern to be
  completed before SELLING."* — the smaller-timeframe pattern is the actual
  trigger, even when it appears at the larger timeframe's own Point 3.
- **A pair's own Point 3 can be "broken" (overshot) and the setup still
  counts as valid**, provided the driving correlated index hasn't reached
  its own Point 3 yet: *"USDCAD has broken our Point 3 but the Setup is still
  VALID... because of USD INDEX. When USD INDEX reaches its Point 3, we will
  execute."* The index is the actual authority; individual-pair geometry is
  secondary.
- **Discipline disclaimer, repeated dozens of times across 2025–2026**: *"We
  don't randomly trade every 123 Reversal Pattern we see... there are STRICT
  RULES... Our TDI CYCLE is the KEY."* The exact rules are never numerically
  disclosed in the free channel (deferred to the paid mentorship) — this
  should be read as "the visible geometry is necessary but not sufficient,"
  not as a fully specified filter we're missing entirely.

---

## 3. The "Reset" — a second, co-equal weekly setup (now partially coded)

*"There are 2 Main Setups that we RELY ON each week: 123 PATTERN & 1h 200ema
false breakout... It's either a Peak Formation week or a market Reset
week."* The Reset is a **false breakout ("Trap") of the 50/200/800 EMA** on
1h/4h/Daily, explicitly a **swing CONTINUATION setup**, not a reversal —
distinct from and complementary to the 123 pattern (they sometimes chain
together on the same chart: a Reset trap right at a 123 pattern's Point 3).

The current `btmm_123.py` already implements a version of this
(`_detect_ema200_false_break`, a 1h/200 EMA pierce-and-reclaim check) as a
fallback path when no 123 geometry is found. Confirmed by the corpus as a
reasonable first cut; the source additionally names Daily 800ema and 4h 800ema
traps as valid variants (not just 1h/200), and ties a formal confluence
checklist to it: *4h RSI EXTREME + 4h Market Baseline Extreme + 4h RSI/
Baseline cross.*

---

## 4. "Critical Areas" — required reversal zone, not yet coded anywhere

Extremely well-attested — roughly a quarter to a third of every sampled image
batch shows this template on its own, independent of the 123 pattern. Three
named trigger types: **(1) a Moving Average trap, (2) a Stophunt (25–50
pips), (3) a previous/multi-day high-low trap.** Explicitly *not* a fixed
grid, not quarter-points, not weekly H/L: *"the lines are drawn by a leading
indicator, projections set at the beginning of the week (no repaint),
adjusted the following week."*

The load-bearing rule: **divergence is explicitly not a standalone signal** —
*"DIVERGENCE is not a stand-alone buy or sell Signal but using it with
CRITICAL AREAS is how you'll see the light... Point 3 must be on a CRITICAL
AREA when trading DIVERGENCE."* This is a genuine gap: neither
`tdi_cycle_123.py` nor `btmm_123.py` requires Point 3 to sit at any kind of
support/resistance zone. `btmm_123.py`'s weekly-Fibonacci-pivot location gate
serves a similar *purpose* (don't trade a reversal in a bad location) but is
mechanically a different concept — Critical Areas are non-repainting
algorithmic S/R lines, not Fibonacci retracements of the prior week's range.

---

## 5. The macro A-B-C-D cycle and Fibonacci roles — not currently coded

A structure larger than the 123 pattern, confirmed extensively (dozens of
chart examples across every image batch, on both individual pairs and the
correlated indices):

- **A** = initial extreme (often an 800ema trap), **B** = pullback,
  **C** = "manipulation" retest (~61.8% Fib of the A-B leg, itself often a
  stophunt beyond A), **D** (sometimes split D1/D2) = final target/reversal
  zone.
- Fibonacci roles named explicitly on-chart: **38.2% = "Reversal 2"**,
  **50.0% = "Reversal 1"**, **61.8% = "Manipulation"**, **100%**,
  **127% = "TP1"**, **161.8% = "TP2" / "Level 3"**.
- A letter-notation alias for the 1-2-3 system recurs constantly:
  **C ≈ Point 1, D ≈ Point 2 (baseline cross), A ≈ Point 3** — used
  interchangeably with numbers, sometimes in non-standard order, and
  sometimes as a *separate* longer-timeframe cycle wrapping several 123
  patterns.
- **Terminology drift over time**: in 2024, "Level 3" meant the 161.8% Fib
  target. By 2026, the channel also uses "Daily Level 3 (T.D.I)" to mean "the
  Daily-timeframe TDI has reached an extreme" — a different concept reusing
  the same name. Any future implementation needs to pick one meaning
  explicitly and not conflate them.

---

## 6. Correlation / basket trading — a standing discipline, not a tip

The best-evidenced piece of doctrine in the whole corpus, stated as a formal
rule as recently as 16 Jul 2026: *"We trade Currencies & Commodities in line
with USD INDEX. We trade JPY Pairs in line with JPY Currency Index. We trade
INDICES in line with the VIX."*

- **USD-INDEX (DXY)**: mandatory directional filter for all currency and
  commodity trades — *"Never trade against US-Index."*
- **VIX**: mandatory, inverse filter for equity indices — *"When VIX goes
  UP, INDICES must go DOWN."*
- **JPY Currency Index**: filter for JPY pairs (GBPJPY, EURJPY, USDJPY).
- **Basket trading**: correlated instruments are screened as a group — US100
  with US30/US500, UK100 with GER30, AUDUSD with NZDUSD, EURUSD with GBPUSD,
  USDZAR with USDMXN — *"If there's no setup on US100, there's a setup on
  US30 or US500."*
- **"Fractional disparity"**: correlated pairs lag the driving index
  slightly; that lag ("delayed pair") is itself treated as tradeable
  information, not noise.

This is genuinely new territory for the dashboard — nothing currently
cross-references DXY/VIX state before firing a signal on a correlated
instrument. It is also the piece of doctrine most directly supported by this
project's *own* prior data: the TDI123 audit already found indices
outperform FX majors for this pattern family (indices net +1.988R vs FX
majors −0.326R over 212 days) — a VIX-driven index filter is a plausible
explanation for why the "good" bucket was good.

---

## 7. Timing structure

- **Weekly**: "Week Beginning" (Mon/Tue) = stophunt/trap window; "MidWeek"
  (Wed–Fri) = the main reversal window. A "Safety Trade" (same-direction
  continuation off a Zero-Line/Critical-Area bounce) typically fires Thu/Fri.
- **Session anchor**: live Zoom sessions and signal timing key off
  09:00–09:30 **New York time**.
- Base-rate data point from the channel's own marketing copy: roughly
  0.7–1 setup (123 or Reset) per week per actively-tracked pair.

---

## 8. Other named concepts (attested, not mechanically detailed in this export)

- **"Golden Cross"** — an EMA crossover swing-trade trigger, exact periods
  not stated (likely one of the 13/50/200/800 EMAs given everything else,
  but unconfirmed).
- **Harmonic patterns (Bat, Butterfly)** as a *third*, explicitly
  interchangeable confirmation method: *"The BTMM side of our system
  includes EXTREME TDI DIVERGENCE, THE PATTERN [Ms&Ws/123] and HARMONIC
  PATTERNS. But you can't use everything. Choose what you're comfortable
  with and master it."* Several corpus charts show Bat/Butterfly patterns
  drawn alongside Critical Area lines.
- **ADR (Average Daily Range) projection levels** (1×/2×/3× ADR) used as a
  target/exhaustion tool, separate from Critical Areas.
- **Risk management**: explicit numeric rule found once — *"DON'T PUT UP
  MORE THAN 20% AT SL. ALWAYS DO R:R of 1:12+."* (The 20% figure's unit is
  ambiguous in context — likely % of account risk budget, not literal SL
  distance.) Signal cards use Entry/SL/TP1/TP2 with an explicit
  trail-to-breakeven-then-trail-further instruction, and an open-ended
  "SWING" target style for continuation trades.
- Video curriculum (title list only, mechanics live in the paid videos which
  weren't in this export): MM Cycle, Test, Higher TF Levels, Micro/Daily
  trends, Resets, 1h anchor patterns, Ms & Ws, Half-Bats, Head & Shoulders,
  Harmonics, Entries, Exits & trade management, TDI, Fractional Disparity,
  ADR & Blue Tracer, "12 21 22", 5050 bounce, Reset safety trades, ID
  Safety/50 with 2nd leg, M/W off 200ma, 2nd-leg halfbat, AR 5050. Several of
  these (Half-Bat, M/W, ID-50/5050) already have counterparts in the
  existing `btmm_core.py` — this list is useful mainly as confirmation that
  the current BTMM strategy's feature set was drawn from the same lineage.

---

## 9. What this means for the dashboard — recommendation

Everything above is now documented. Three things are worth distinguishing:

1. **Already faithful**: `tdi_cycle_123.py`'s core geometry and TDI
   thresholds match the source precisely. No changes recommended — it stays
   untouched per prior direction, and its own audit history (backtested,
   found breakeven-to-negative on FX, positive on indices) remains the
   relevant evidence for it.
2. **Already incorporated into `btmm_123.py`** (this session, pre-compaction):
   the Reset/200EMA-false-breakout setup, H4 bias alignment, and a weekly
   pivot location gate. This is a reasonable first cut at sections 3 and 4
   above, though not a mechanical match to "Critical Areas" specifically.
3. **Not yet built, and deliberately not recommended to build yet**: the
   4H-extreme-must-precede-1H-trigger hard gate (section 2), true Critical
   Areas (section 4), the A-B-C-D/Fibonacci target system (section 5), and
   DXY/VIX correlation filtering (section 6). These are the highest-value
   remaining items *if* the pattern turns out to have a real edge — but
   TDI123's own history is the direct lesson here: five rounds of "add an
   edge" (divergence fix, ATR stop, location filter, ketchup entry, session
   filter) were tried on that strategy, and only the two mechanical-
   correctness fixes plus the session filter actually helped. Adding all of
   this now, before a single backtest of `btmm_123.py` has run, repeats the
   exact mistake this project already paid for once.

**Recommended next step**: run the walk-forward backtest that was already
planned as the non-negotiable gate before `BTMM123_ALERTS_ENABLED` goes live
— on the *current* `btmm_123.py` as-is. Let the data say which of section
6's ideas (DXY/VIX filtering, in particular, given the indices-only edge
already found in TDI123's data) are worth a second backtest pass, rather than
building all of it speculatively first.
