# BTMM & CRT Strategy Audit — Code vs. Reference Material

A comprehensive audit comparing this dashboard's BTMM and CRT strategy implementations
against the trader's own reference PDFs (course notes, setup checklists, and strategy
decks). Conducted via 10 parallel research passes, each independently reading a source
PDF in full and comparing it line-by-line against the actual running code — not the
code's own comments/docstrings, which in several places were found to misdescribe what
the code actually does.

**Purpose:** a decision document. It records what's confirmed correct, what's missing,
what's inverted, and what's fabricated — so fixes can be prioritized deliberately rather
than guessed at. It does not itself change any code.

**Sources audited:**
- BTMM: `BTMM setups .pdf` (12 pages — the 7 named setups), `ID50 checklist (3).pdf` (15
  pages), `Safety Trade checklist (2).pdf` (11 pages — bundles Safety Trade / 22 Trade /
  50-50 Bounce), `BTMM FX ME MINDSHIFT.pdf` = `BTMM - MARKET MAKER CYCLE (2).pdf` (232
  pages, byte-identical duplicates — the full theory course).
- CRT: `5AM CRT.pdf` (18 pages), `1AM CRT.pdf` (27 pages), `The Candle Range Theory -
  Sham CPI v2.pdf` (116 pages — foundational/shared concepts).

**Code audited:** `index.html` (JS, drives the live dashboard), `service/btmm_core.py`
(Python, backtester + Discord alerts only), `service/crt_strategy.py` /
`service/crt_utils.py` / `service/alerts.py` (CRT).

---

## Part 1 — Architecture (read this before anything else)

### BTMM: two non-parity implementations

**The live "BTMM" dashboard tab runs 100% client-side in `index.html`'s JavaScript.**
Confirmed via `service/app.py`'s full route inventory: there is no `/btmm` endpoint.
`/candles/<symbol>` (app.py:108) returns only raw OHLC — no server-side analysis. The
browser fetches raw candles and runs `analyzePair()` (index.html:3288) entirely
client-side to populate every dashboard card.

**`service/btmm_core.py` is used only by the backtester and the Discord-alert
scheduler** — `service/backtest.py:11` (`from btmm_core import analyze`, invoked via
`POST /backtest`) and `service/scheduler.py:177` (`_run_alerts`). It never touches the
live dashboard tab.

**These two implementations are not faithful ports of each other.** Confirmed
divergences on same-named functions:

| Concept | JS (`index.html`) | Python (`btmm_core.py`) |
|---|---|---|
| Half a Batman | `detectHalfBatman` (~1790): EMA-alignment + volatility-contraction heuristic | `detect_half_batman` (~527): Asian-range 0.5×/1.0× extension-and-retrace | 
| — | **Two unrelated algorithms sharing only a name.** |
| M/W pattern tolerance | `detectMWPattern` (~1271): 2nd high must be **≤** 1st (strict) | `detect_mw_pattern` (~364): 2nd high may be **up to 0.2% higher** and still count |
| Level count | `detectLevelCount` (~1336): EMA13/50/200 **cross recency** | `detect_level_count` (~342): **count** of how many of 5 EMAs price is on the correct side of |
| Shark Fin | `detectSharkFin` (~1380): checks an explicit fast-crosses-signal event | `detect_shark_fin` (~407): drops the cross check, delegates to the looser "back inside band" test |
| Candlestick recognition | `detectNameableCandle` exists (Doji/Hammer/Shooting Star/Engulfing/RRT/COW) | **None exists anywhere in the file** |
| 22 Trade SL fallback | Falls back to Asian-range extreme | Falls back to the current bar's own high/low |

**Practical consequence: a backtest run against `btmm_core.py` does not validate what
the live dashboard tab actually signals or would have signaled historically.** They are
two different rule sets that happen to share setup names.

### CRT: 5AM is live, 1AM is dead code

**5AM CRT fires live Discord alerts and gets journaled** (`alert_crt_5am_setup`,
alerts.py:454; `scheduler.py:220` calls `analyze_universe_5am()`).

**1AM CRT is not running anywhere in production.** No `alert_crt_1am_setup` function
exists. No scheduler job calls `crt_strategy.analyze_universe()` (the 1AM version). No
dashboard route references `crt_strategy` for 1AM. `crt_strategy.analyze_pair`/
`analyze_universe` (1AM) are exercised only by `service/test_crt_strategy.py`. A design
doc already in the repo — `docs/superpowers/specs/2026-09-14-vwap9ema-live-replaces-1am-crt-design.md`
— confirms 1AM CRT's dashboard tab, route, scheduler job, Discord alert, and config flag
were **explicitly retired on 2026-09-14**, replaced by `vwap9ema_strategy.py`. Everything
below about 1AM describes dead code, kept here for completeness since the code still
exists in the repo and could be revived.

---

## Part 2 — BTMM: the central finding

### "Level 1/2/3" is the wrong concept, and it gates the strategy's top grade

**What the PDF actually says** (pp.104-108, reconfirmed across ~15 chart examples on
pp.134-161): Levels are the Market Maker Cycle drawn out over **2.5 to 5 days** of
directional movement, giving structure/bias in a dynamic market. Identified four ways:
(1) 2.5-5 days of rise/drop, (2) each level ≈ **1× ADR**, (3) Asian-session-box stacking
(consecutive daily Asian ranges stepping in one direction), (4) **EMA crosses** —

- **Level 1 = 13/50 EMA cross**
- **Level 2 = 50/200 EMA cross**
- **Level 3 = 50/800 or 200/800 EMA cross, or EMAs fully "fanned out"**

Pages 122-126 add a **Reset**: Market Makers sometimes book profit and continue rather
than reverse, restarting the level count — sometimes producing 4 or 5 levels in one
trend. A reset is **Confirmed** if it breaks the prior swing peak/trough before
continuing, **Unconfirmed** if it doesn't. p.115: "Knowing the levels is NOT as
important as recognizing the pattern, the timing, and the pushes."

**What the code actually does** — `detect_level_count()` (btmm_core.py:342-359) / JS
`detectLevelCount` (index.html:1336):
```python
levels = [stack["e5"], stack["e13"], stack["e50"], stack["e200"], stack["e800"]]
above  = sum(1 for e in levels if price > e)
below  = sum(1 for e in levels if price < e)
count  = above if direction == "bullish" else below
level_ii = count >= 4      # hard gate for A+ signal
level_i  = 1 <= count <= 3
```
This counts **how many of the 5 EMAs current price is on the correct side of, on a
single bar, right now.** No crossover detection anywhere. No multi-day persistence. No
ADR-per-leg measurement. No Asian-box-stacking logic exists in the file. No Reset
concept exists anywhere in either language.

**This mislabeled metric is a hard gate for the A+/Level-II signal** —
`is_aplus` requires `level["level_ii"]` (btmm_core.py:957). The strategy's own top
signal-quality tier is built on a metric that doesn't measure what the doctrine it's
named after actually measures.

JS's `detect3DayCycle` (index.html:1616) makes this worse by **conflating two separate
PDF doctrines** under one "Level I/II/III" label: it borrows the Levels *name* but
computes a 50/200-EMA relative-position bar-count ratio over the last 10 H4 bars, and
separately borrows semantics from a *different* PDF concept — "Trend Day 1/2/3"
(pp.118-120: Day 1 = surprise reversal, Day 2 = retail validation/MA crosses, Day 3 =
MM acceleration-and-separation/trap) — labeling its own Level III output "reversal
imminent," which actually contradicts the real Levels doctrine (Level 3 is normally the
most *extended continuation* leg, not itself the reversal signal — a Reset is).

---

## Part 3 — BTMM: setup-by-setup findings

### Confirmed M and W

> PDF rule: first leg closes back beyond 13 EMA "creating an angle"; second leg closes
> back above/below 13 with a candlestick pattern (RR, morning/evening star, COW); TDI
> confirmation = RSI above/below the MBL, crossing the signal line.

`detectMWPattern`/`detect_mw_pattern` is a pure swing-high/low zigzag finder (±3-candle
pivot window in JS, ±2 in Python) gated only on `p2 <= p1` (M) / `p2 >= p1` (W) plus a
trivial 0.05% price-move threshold. **No EMA13 reference of any kind, in either
language.** Candlestick recognition exists in JS (`detectNameableCandle`) but is not
called from this function. The **TDI "MBL crossing signal line" confirmation is dead
code everywhere it should apply**: the mid-band-line value (`bbMid`/`bb_mid`) is
computed in `calcTDI`/`calc_tdi` but never read anywhere else in either file (confirmed
by grep). Every existing TDI-leg check instead tests the outer Bollinger Band — a
materially looser condition. Morning/evening star (3-candle) patterns are not
implemented anywhere in the codebase at all.

**Verdict: MISMATCH.** Shares only the "M"/"W" output label with the PDF's actual rule.

### Advanced M and W

> PDF rule: first leg consolidates 8+ candles below/above 13 without closing beyond it,
> then shifts and closes beyond 13; TDI: RSI outside the band on leg 1, returns and
> closes beyond MBL crossing signal line on leg 2. Found at: HOD/LOD, Level 3 reversal
> areas, YH/YL, after SHH/SHL beyond the Asian range, at EMAs ("water"/"Mayo"), after ADR
> met/exceeded.

No detector combining an 8+-candle pre-break consolidation with the TDI
"outside→return-crossing-MBL" trigger exists. The closest analog, `detectHalfBatman`,
is a different heuristic (volatility contraction) never combined with M/W output. None
of the 6 named high-probability locations are gated for M/W specifically — no "yesterday
high/low" concept exists anywhere (grepped, zero hits), no Level-3-relative gate, no
Asian-range-relative-to-M/W-swing check.

**Verdict: NOT IMPLEMENTED.**

### London Patterns (Type 1/2/3, including "50/50/50")

> PDF rule: same entry/exit as M&W. Type 1 = M/W forms above/below the Asian range.
> Type 2 = M/W forms within the Asian range. Type 3 ("50/50/50") = price bounces off 50
> EMA, TDI RSI is above/below MBL AND the 50-static line crosses the signal line, all
> occupying 50% of the Asian range. (Reconfirmed p.212: Type 3's 2nd leg forms "at or
> near the 50% range of the Asia box.")

No Type 1/2/3 classification exists anywhere (grepped). `detectFiftyFiftyBounce`/
`detect_fifty_fifty_bounce` shares the "50/50" name but checks an unrelated condition
set: proximity to the 50 EMA (≤8 pips), a mislabeled "13/50 cross" gate (see below), a
generic TDI-leg-1 check, H1 trend alignment, and a "mid-day lull" time window
(11:00-14:00 or 16:00-22:00 GMT — the gap *between* London close and NY open, not the
London session itself). **The Asian range is never even passed into this function's
context** — there is structurally no way for it to check "above/within the Asian range"
or "occupies 50% of the Asian range." No reference to a "50-static TDI line" exists
anywhere.

**Verdict: NOT IMPLEMENTED.** The shared "50/50" numerology is coincidental — the PDF
means three distinct 50s (50 EMA / 50-static TDI line / 50% of Asian range); the code
means something else entirely.

### Half a Batman

> PDF rule (two types, both "incomplete M/W pattern, market-maker trap"):
> **Type 1** — outside structure first leg; price closes beyond 13, retests, but fails
> to reach the first leg by ≥10 pips; entry = a 10-12 pip shift candle closing beyond the
> "apex" (structure midpoint); confirmed by a close beyond 13 with TDI.
> **Type 2** — first leg closes beyond 13, traps off the lower (usually 50) EMA, does
> NOT go back through 13; same shift-candle/apex/13-close/TDI entry.

JS `detectHalfBatman`: gates on `level==='I'` (a coarse EMA13/50 trend proxy) plus a
volatility-contraction heuristic (last-5-bar avg range < 60% of last-20-bar avg range,
after an earlier >1.3× "move"). **No pip measurement of any kind. No apex/midpoint
concept. No TDI gate. No candle-close-vs-13 check.** Confidence is a flat hardcoded 70,
not derived from any measured quantity.

Python `detect_half_batman`: an entirely different pattern — price extends 0.5×/1.0×
beyond the Asian range boundary then retraces to it. Also has none of the PDF's specific
mechanics.

Neither implementation distinguishes Type 1 from Type 2. Neither is even a real,
independently-gated, backtestable setup — both only contribute a bonus-point badge to
other setups' scoring.

**Verdict: MISMATCH.** JS and Python model two different, unrelated things under the
same name, and neither matches the PDF.

### ID 50 (15-min "Intraday 50 bounce") / 50-50 1hr

> PDF rule (12-page setups doc + 15-page dedicated checklist, which is materially more
> detailed):
> 1. **Anchor present to the left** — a consolidation/range structure the pattern trends
>    away from. Explicitly OK if "ugly"/unclear (p6, p8 — "wait for the ID50 to form" as
>    confirmation if the anchor itself is ambiguous). Also fires after a **Reset Anchor**
>    forming mid-trend (pp.9-12), with the same sub-rules reapplied.
> 2. 13/50 EMA cross.
> 3. **First pullback to the 50 EMA is 25-50 pips** ("a quick move back to 50" —
>    corrected from an earlier paraphrase of "20-25"; the checklist's own p15 rules page
>    says 25-50).
> 4. Wait for price to **bounce or trap** off the 50 EMA (a distinct staging step, not
>    mere proximity).
> 5. Entry trigger: **RR, STAR, COW, or a Shift candle**, closing below/above 13.
> 6. TDI confirmation: **RSI/signal-line cross only** — explicitly does *not* require
>    RSI beyond the MBL, unlike other setups.
> 7. 50/200 EMA cross is an explicit bonus/"nice to have," not required.
> "Same rules apply on the 1hr 50/50 and 4hr 50/50" (p.14) — literally the same rule set,
> just run on a different timeframe.

Code has no "Anchor" detector at all — zero representation anywhere (grepped both
files). The gate labeled `"13/50 Cross Level I (fresh)"` in `detect_fifty_fifty_bounce`
is actually wired to `detect_level_count`'s alignment-count metric, not a cross of any
kind — and the file's own `detect_513_cross` function, despite its name, checks EMA(5)
vs EMA(13), not 13/50 at all, and isn't even used for this gate. Pullback size is never
measured — the only 50-EMA checks are static current-proximity tests (≤6-8 pips),
roughly 4-6× smaller than the PDF's 25-50 pip pullback-travel spec, and measuring a
different thing (proximity, not distance travelled). "Bounce or trap" isn't modeled as
a distinct step — it collapses into the same static proximity check. Entry-trigger
patterns: RR and COW exist (`detectNameableCandle`) but "STAR" and "Shift candle" don't
exist anywhere, the "close beyond 13" condition is never checked, and **even the
existing RR/COW detector is never called from `detectFiftyFiftyBounce`/
`detect_fifty_fifty_bounce` at all** — the setup standing in for ID50 fires with zero
candlestick awareness. **Worst mismatch: the code inverts the explicit "no MBL
requirement" rule** — its TDI gate (`tdi_first_leg`) requires RSI to be currently pinned
outside the Bollinger Band, the opposite of what the checklist calls for. No 50/200-EMA
bonus check exists. RESET-anchor repeatability is unmodeled (moot given anchor detection
doesn't exist regardless). **No dedicated backtest key exists for "id50"** — only
`bounce5050` is selectable, which itself doesn't implement these rules.

50/50-1hr doesn't exist as its own setup — the PDF's "run the same rules on 1hr instead"
is not what happens; instead, a *derived H1 trend direction* is checked as one gate
*inside* the 15-minute setup (a weaker, different thing than an independent H1-timeframe
instance of the same rule set).

**Verdict: PARTIAL MATCH degrading to MISMATCH.** Only a rough "near 50 EMA + some
EMA-alignment + HTF-trend-agreement" combination survives; every distinguishing PDF
rule (anchor, pullback-size, bounce/trap staging, full candlestick-trigger set,
close-beyond-13, and — critically — the *explicit non-requirement* of RSI-beyond-MBL) is
either absent or actively contradicted.

### 50/50 1hr

Not implemented as its own setup at all — see ID 50 above.

### M and W off Mayo

> PDF rule: reversal off the 200 EMA ("Mayo"). Can occur at any of 3 Levels but mostly
> at Level 2 — **explicitly not advised to trade back an anchor (Level 1)**. Same M&W
> entry rules: second leg closes beyond 13; TDI confirmation.

`detectEMA200Bounce` (index.html:1447) does check proximity to EMA200 (<25 pips) plus a
loose M/W-shape peak comparison — location is loosely checked. But: its function
signature doesn't even accept a level-count argument, so **it structurally cannot gate
on "Level 2, not Level 1"** — and this is moot anyway since the code's "Level" concept
doesn't correspond to the PDF's Level concept in the first place (see Part 2). No
EMA13-close check on either leg. No TDI reference at all inside this function —
confidence is a flat hardcoded value (90 or 70), not TDI-derived. Python has no
dedicated function at all; Mayo-proximity is one loose OR'd gate (price within 0.1% of
EMA200) inside the generic "safety" scoring proxy, also with no level-2 gating, no
13-EMA-close check, and only a generic (not Mayo-specific) TDI gate.

**Verdict: NOT IMPLEMENTED** for all three of the setup's distinguishing rules.

### Safety Trade

> Checklist (page 3 of its dedicated PDF — the file also bundles 22 Trade and 50/50
> Bounce, which are separate setups with their own checklists on later pages, not to be
> conflated):
> 1. Did the Anchor or Peak Formation "lock"?
> 2. Are you in **Level 1**? (page 2's diagram: "Level 1 = Expect to see Straight Aways,
>    V1, or Safe Trade" — i.e. Safety Trade specifically wants the *early*, freshest
>    level.)
> 3. Did the 13/50 or 50/200 EMA cross?
> 4. TDI Shark Fin: 1st leg outside the band, 2nd leg inside the band, forming M or W
>    (divergence).
> 5/6. For shorts/longs: nameable 2nd leg = **RRT, hammer, or doji** (not Star — Star
>    belongs to the sibling 22 Trade / 50-50 Bounce checklists specifically, confirmed
>    by reading all their pages).

JS `detectSafetyTrade`'s level gate requires `level === 'II' || 'III'` — the **opposite**
of what the checklist wants (Level 1). "Anchor/Peak Formation lock" has no detector at
all. The TDI outer-band Shark Fin logic is actually implemented correctly here (both
languages) — this is the one place in the whole audit where the "outside band → inside
band" mechanic is used exactly as intended, since Safety Trade's own rule genuinely
wants outer-band behavior, not an MBL cross. But the M/W structural half of rule 4 is
computed elsewhere (`detectMWPattern`, even passed into this setup's context object) and
then **silently never read** by `detectSafetyTrade`. Candlestick set in JS is a
superset (also allows Shooting Star, Engulfing, COW — those belong to the sibling
setups, not Safety Trade) and isn't tied to a confirmed M/W structure — it can pass on
any bare 2-candle shape in the last 2 candles. Python's "safety" proxy has **no
candlestick detection of any kind, no Level check, and derives its BUY/SELL direction
from EMA800 institutional bias — fully decoupled from the TDI/M-W logic that actually
defines the setup.** Extra gates the checklist never asks for are present in both
(kill-zone session restriction, Asian-range validity, H1 alignment in JS; a stop-hunt
requirement in Python).

**Verdict: MISMATCH.** Not the most faithfully-implemented setup in the codebase, despite
handling TDI band mechanics correctly — the Level gate is inverted and the M/W check is
silently skipped (JS), and Python is worse across the board.

---

## Part 4 — BTMM: risk management and session doctrine

### Trade management

> PDF (multiple pages, consistent across the theory course and the 12-page setups doc):
> SL = 3-5 pips (or 5-7 pips per a second page) beyond the shift candle / perceived
> HOD-LOD. TP = 20 pips (ADR<100) / 30 pips (ADR>100), or TP1=50 EMA/TP2=200 EMA, or
> generally 30-50 pips "for consistency." Move to break-even at 1:1. R:R should always be
> 1:1 or better. Exit triggers: price closes back over/under 13 EMA; Fib 38.2/61.8
> breached; RSI re-crosses the TDI signal line; a strong reversal candle. Avoid trading
> night hours to stay clear of traps.

- **SL buffer is a flat 2 pips** in `analyze()` — below the PDF's 3-5/5-7 pip spec, and
  not anchored to a "shift candle" (no shift-candle concept exists anywhere in either
  file).
- **TP is Asian-range-extension or R:R-multiple based** — not the PDF's fixed-pip or
  50/200-EMA-target mechanism. Not necessarily wrong, but a different formula than
  documented, and can land well outside the stated 30-50 pip "consistency" range.
- **R:R-floor (≥1:1) is present** via a sanity-clamp fallback — one of the few risk rules
  that does show up in code.
- **No break-even-at-1:1 logic exists anywhere.**
- **No "price closes back over 13 EMA → exit" trade-management logic exists anywhere** —
  the app computes entry/SL/TP once at signal time and never revisits a running position.
- **Zero Fibonacci logic exists in the codebase** (confirmed via search — no fib
  references at all in `btmm_core.py`).
- **No explicit night-hours exclusion.** More pointedly: there's a dedicated **"22
  Trade"** setup that fires specifically in the 22:00-02:00 GMT window — squarely
  "night hours" by most reasonable readings — in direct tension with this rule.

### Session timing / kill zones

> PDF (p.6, EST): Asian 8:30pm-3:00am (≈01:30-08:00 UTC), London 3:30am-9:00am
> (≈08:30-14:00 UTC), NY 9:30am-5:00pm (≈14:30-22:00 UTC) — together covering nearly the
> whole day, excluding only a ~3h "Dead Gap." Daily cycle vocabulary is explicitly
> **Accumulation → Stop Hunt → Trend Move → End of Day Reversal** — "Manipulation" and
> "Distribution" never appear anywhere in 232 pages.

Code's `KILL_ZONES` (London 07:00-10:00, Overlap 12:00-13:00, NY 13:00-16:00, Asian
19:00-22:00 GMT — identical between JS and Python, so at least internally consistent)
only covers 10 of 24 hours and barely overlaps the PDF's much wider session windows —
**and the code's "Asian" kill zone (19:00-22:00 GMT) actually falls inside the PDF's New
York session**, a straightforwardly wrong label. `detect_amd()` uses "Manipulation"/
"Distribution" terminology that doesn't appear in this doctrine at all, and its
Accumulation fallback bucket (17:00-07:00 UTC, 14 hours) swallows both the PDF's NY
session and the Dead Gap.

### Other gaps found in the full 232-page pass

- **Reset/Peak detection is absent** (see Part 2) — no confirmed/unconfirmed reset
  logic, no level-count restart after a reset, anywhere.
- **The top-down "confirm a super-clear H1 MM-cycle bias before dropping to M15 for
  entry" methodology** — explicitly called mandatory for avoiding fake-outs (pp.126-132,
  149-150) — is applied to only 1 of 4 named setups (`detect_fifty_fifty_bounce`'s
  `h1_aligned` gate). `detect_22_trade`, Safety Trade, and Three-Drive never check it.
- **2nd-leg entries generally should require RSI crossing both the signal line AND the
  TDI market baseline** (two separate, distinct crosses) — the market-baseline cross is
  never checked anywhere; only the signal-line relationship is tracked.
- **The Safety Trade proxy never checks proximity to the Asian range's 50% level**
  (`asian["mid"]`, which `detect_asian_range` already computes) even though the PDF
  pairs "pullback to 50 EMA" with "and the 50% level of the Asian range" as a joint
  confluence (pp.227-231).
- **`detectWeeklyCycle`** (index.html:1582) has no supporting doctrine anywhere in the
  232-page course — no "weekly cycle" concept was found in either half of the audit.
  Flag for further investigation if a source for this naming exists elsewhere.
- **ADR-consumption gate is backwards from doctrine.** The checklist scoring rewards
  *low* ADR consumption (`adr_c < 75`) as the positive signal, while the PDF (p.58) lists
  "after the ADR has been met or exceeded" as a place to specifically look for M/W
  reversal setups — arguably the opposite condition. May be intentional (avoiding
  exhausted continuation entries is a different, reasonable goal) but doesn't reflect
  the stated doctrine.

### Confirmed matches (BTMM)

- EMA periods (5, 13, 50, 200, 800) match the PDF's indicator list exactly (p.24 vs.
  `ema_stack`).
- "Mayo" = 200 EMA, confirmed explicitly in two separate places in the PDF.
- RRT and COW candlestick patterns are correctly named and detected in JS
  (`detectNameableCandle`) — just wired to the wrong setup(s), not absent.
- The R:R-floor (≥1:1) concept survives via a sanity-clamp fallback.
- Asian-range session hours in `detect_asian_range` (00:00-07:00 GMT) are within ~1.5h
  of the PDF's Asian window — the closest-matching session timing in the whole audit.

---

## Part 5 — CRT: findings

### The single biggest finding: fabricated PDF citations

`_build_crt_trade_plan` (alerts.py:397) and `alert_crt_5am_setup` (alerts.py:454) cite
"MADO PDF pages 22 & 27" and separately "page 18" as the source for the SL/TP1@1:2/
TP2@1:3 risk:reward system. **The 5AM CRT PDF this code path serves is only 18 pages
long — pages 22 and 27 do not exist in this document.** Page 18 (the real entry-mechanics
page) contains only timeframe/entry-model/order-block-definition text — no SL, no TP, no
R:R content anywhere in the 18-page document. Confirmed independently by the
116-page foundational CRT PDF too: no universal R:R doctrine exists there either.

**The entire SL/TP system is invented and misattributed.** (The 1:2/1:3 ratios *do*
genuinely appear in the worked examples of the *separate* 1AM CRT PDF — pages 22 and 27
of *that* document — so this looks like a citation mix-up between two different source
PDFs rather than pure fabrication. But the citation as written, attached to the live
5AM code path, is simply wrong.)

### 5AM CRT (the live strategy)

- **CRT high/low is defined incorrectly.** Code computes it as the combined max/min of
  the 5PM, 9PM, and 1AM H4 candles. The PDF's own "CRH"/"CRL" diagrams (pp.6, 10, 12)
  unambiguously mark the **5AM candle's own** high/low — a different, specific thing the
  code conflates with a separate PDF concept (the three "reference candles" used for
  narrative-building, p.4).
- **"Market Profile" typing (`TYPE_1_CONT`/`5AM_EXPANSION`) — worth the single largest
  confluence weight (3/12) — has no basis in the 5AM PDF at all.** It's machinery
  transplanted wholesale from the *separate* 1AM CRT strategy, per the code's own module
  docstring.
- **DOL doesn't gate anything, despite the PDF stating it should be "first priority."**
  `_crt_setup_from_sweep` never references `dol_bias` — a BUY/SELL can fire with neutral
  or opposing DOL. It's just 2 of 12 scoring points, same weight as SMT.
- **Order block definition drops the PDF's sweep precondition.** PDF: "the candle that
  digs below a low or above a high, when engulfed, is called an order block." Code:
  generic "opposite-colored candle before a >1.5×-average-displacement move" — never
  checks that the candle actually swept a prior high/low first. The 1.5× threshold has
  no PDF source.
- **Intraday profile detection is a coarser, time-shifted approximation** of the PDF's
  diagrams — M15-bucket-half-split instead of the PDF's fixed 5-7am clock window;
  next-H4-bucket direction instead of the PDF's drawn 7-10am window.
- Internal doc/code inconsistency: both `analyze_pair_5am`'s and `alert_crt_5am_setup`'s
  docstrings say the key window is "09:00-11:00 NY," but the actual constant used
  (`KEY_TIME_WINDOWS_5AM`, matching the PDF's real p.17 spec of 06:00-08:30 NY) is what
  actually runs. Runtime behavior is correct; the comments describing it aren't.

**Confirmed matches:** H4 alignment/bucket timing; the "buy below open / sell above
open" directional rule; key-time windows (exact match to PDF p.17); the three SMT
session-pair comparisons (London-Lunch↔London, Lunch↔NY, London↔NY, matching pp.14-16
exactly).

### 1AM CRT (dead code — see Part 1)

- **DOL timeframe is wrong even by the code's own logic** — PDF mandates the **daily**
  timeframe for 1AM's DOL; the code's `detect_dol()` operates exclusively on H4 buckets,
  daily candles are never touched anywhere in the pipeline.
- **The PDF's stated entry-direction trigger is computed but explicitly not used.** PDF:
  OHLC vs. OLHC candle-print order (which extreme forms first) relative to the 1AM open
  determines direction. Code computes this correctly (`ohlc_pattern()`) but the actual
  `setup` (BUY/SELL) comes from a different, self-invented "sweep the 5PM/9PM range then
  close back inside it" rule — `ohlc_pattern` is demoted to display-only, per the code's
  own comment explaining this was a deliberate override (the OHLC rule "degenerated into
  which way has price drifted so far" on a still-forming candle).
- **"Key level" (PDF's H4 narrative-building step, 6 structure types: OB/BB/RB/FVG/
  Lows/Highs) has no code counterpart** — only 1 of 6 structure types (OB) is
  implemented, on the wrong timeframe (M15, not H4) and for a different purpose (entry
  timing, not narrative-building).
- **CRT high/low definition matches exactly here** (combined 5PM+9PM H4 candles = the
  PDF's "two CRT candles" rule for the 1AM variant specifically) — the one clean
  definitional match in the whole 1AM audit.
- Market Profile Type 2 condition (5PM consolidation / 9PM manipulation / 1AM expansion)
  matches exactly; Type 1 in code doesn't require 5AM to also expand, though the PDF
  couples 1AM and 5AM together for Type 1.
- Key-time windows for London Open (2-3am) and Silver Bullet (3-4am) match exactly, but
  the PDF's explicitly-discouraged 1-2am hour still earns +1 confluence credit in code
  rather than being excluded, and the PDF's optional 4-5am window doesn't exist in code
  at all.
- Confluence scoring system, weights, and A/B/C grade thresholds are entirely invented —
  no PDF anywhere in this audit specifies a scoring system.

### Foundational CRT concepts (shared by both variants)

- **DOL is missing 2 of the PDF's 5 listed forms**: no discount/premium-of-range
  calculation, no equilibrium/50%-of-range calculation, anywhere in `crt_utils.py`. No
  short-term-vs-long-term DOL distinction exists (code produces one undifferentiated
  result).
- **SMT's self-led-vs-partner-led asymmetric scoring is a pure code invention** — the
  PDF always treats correlated-pair divergence symmetrically, with no "which pair led"
  concept.
- **`classify_candle()` implements only 3 of the PDF's 5 canonical market-profile types**
  (consolidation/manipulation/expansion) — "retracement" and "reversal" don't exist as
  classifier outputs anywhere.
- **Order block body-vs-wick selection rule is not implemented.** PDF: use the candle's
  body only if price barely closes past the open; use the full candle (wick included)
  only if price closes beyond the extreme. Code always uses the full high/low
  unconditionally. The "fills liquidity" OB purpose (FVG-fill/retest) isn't implemented
  either — only "purges liquidity via displacement."
- **No universal SL/TP/R:R doctrine exists in this 116-page PDF at all**, and — confirmed
  via grep — **no SL/TP/R:R math exists anywhere in `crt_utils.py`/`crt_strategy.py`
  either.** Consistent absence, not a mismatch, but means the dashboard's CRT "setups"
  ship with no attached risk parameters of their own; that logic lives entirely in
  `alerts.py`'s (partly fabricated, see above) trade-plan builder.

**Confirmed matches:** OHLC/OLHC classification logic (near-verbatim to the PDF's
Open-Range/Low(or High)-Manipulation/High(or Low)-Expansion/Close-Range framing); SMT
pair selection and divergence-direction logic; key-time windows generally (all code
windows are subsets of, or exact matches to, the PDF's session table and named "refined
key times"); Normal/Delayed Protraction and NY Continuation/Reversal intraday-profile
naming traces directly to PDF terminology.

---

## Part 6 — Suggested prioritization (for discussion, nothing here has been implemented)

Roughly ordered by (a) how central the concept is to the strategy's own doctrine and
(b) how cheap/contained a fix would be:

**High-value, likely contained fixes:**
1. Remove/correct the fabricated "MADO PDF pages 22 & 27" citation in `alerts.py` — pure
   documentation honesty fix, zero behavior change, already flagged as misleading.
2. Fix ID50's inverted TDI rule (stop requiring RSI-beyond-MBL) — a one-line-ish gate
   change with a clear, unambiguous PDF citation to justify it (same pattern as the
   TDI123 fix made earlier this session).
3. Wire `detectNameableCandle` into the setups that actually require it (M&W, London,
   ID50) instead of only Safety Trade — the detector already exists, it's a wiring gap.
4. Fix Safety Trade's inverted Level gate (wants Level 1, code requires Level II/III).

**Larger, more structural work:**
5. Decide what "Level 1/2/3" should actually mean (real EMA-crossover-sequence detection
   over a multi-day window) and reconcile `detect_level_count`, `detectLevelCount`, and
   `detect3DayCycle` around one correct implementation — this affects the A+ gate and
   several setups' scoring simultaneously, so it's higher-risk/higher-payoff.
6. Build a real "Anchor" detector for ID50/50-50/Half-Batman — currently the #1
   precondition across multiple setups and entirely unimplemented.
7. Reconcile JS vs Python `btmm_core.py` divergences (Half Batman, M/W tolerance, Level
   count, Shark Fin) if backtest fidelity to the live scanner matters going forward.

**Lower priority / judgment calls:**
- Session-timing/kill-zone realignment to the PDF's EST map — a bigger change with
  wide blast radius (touches every setup's gating), worth a deliberate look rather than
  a quick patch.
- Trade-management additions (break-even, 13-EMA exit, Fib exit) — genuinely new
  functionality (live position tracking), not just a detection-logic fix; the app is
  currently a signal scanner, not a position manager, so this is a bigger scope
  question, not a bug fix.
- 1AM CRT — since it's dead code, decide first whether it's worth reviving before
  investing in fixing it.

---

*Compiled from 10 independent research passes (7 covering BTMM, 3 covering CRT), each
reading its source PDF in full and cross-referencing exact line numbers against the
live repository. No finding here was paraphrased from another agent's summary — each
was independently re-verified against the source material and, where straightforward,
against live app behavior (e.g. the TDI123 precedent this session, and direct queries
against the running dashboard for the Level/ID50 findings).*
