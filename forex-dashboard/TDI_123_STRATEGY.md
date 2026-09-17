# TDI Cycle 123 — Complete Strategy Specification

Extracted verbatim from `service/tdi_cycle_123.py` (1020 lines), `service/btmm_core.py`
(RSI/EMA/TDI helpers, lines 1-160), `service/alerts.py` (alert-quality gating),
`service/config.py` (constants), `service/app.py` (`/tdi123`, `/tdi123/detail`
endpoints), and `service/trade_tracker.py` (auto-tracked journal resolution).

All numeric constants below are quoted **literally** from source — none rounded
or approximated. Source locations (`file.py:line`) are given so any of this can
be re-verified against the original repo if needed.

This document describes **actual runtime behavior**, including two places
where the code's own comments/docstrings describe intent that the code does
not actually enforce — those are flagged explicitly in §13 (Known
Discrepancies). If porting this strategy, decide deliberately whether to
match the code's real behavior or the documented intent.

---

## 0. Concept

TDI Cycle 123 is a classic 1-2-3 (peak-formation) price-reversal pattern,
confirmed by TDI (Traders Dynamic Index) indicator behavior instead of pure
price-structure doctrine — an "improvement on BTMM" per the source app's own
description. It runs on two entry timeframes independently:

- **H1**, biased by **H4** (does the higher timeframe agree on direction?)
- **M15**, biased by **H1**

Both timeframes are scored/graded by the exact same function
(`_analyze_timeframe`) — there is a single source of truth, not two parallel
implementations.

---

## 1. Data requirements

| Timeframe | Purpose | Bars fetched (scanner) | Bars fetched (chart detail) |
|---|---|---|---|
| H1 | Primary entry (H1 leg) | `DEFAULT_BACKFILL` = 3200 | `DEFAULT_BACKFILL` = 3200 |
| H4 | Bias for H1 entries | 200 | 200 |
| M15 | Primary entry (M15 leg) | `DEFAULT_BACKFILL` = 3200 | 3200 (only if `timeframe=M15` requested) |
| D1 | Weekly pivot HLC + ADR context | 60 | 60 |

`DEFAULT_BACKFILL = 3200` (`config.py:47`). `DEFAULT_INTERVAL = "15min"`
(`config.py:46`).

`INTERVAL_SECS` (`config.py:36-44`):
```python
{"1min": 60, "5min": 300, "15min": 900, "30min": 1800, "1h": 3600, "4h": 14400, "1day": 86400}
```

**Universe** — `PRIORITY_PAIRS` (`config.py:26-34`), 26 symbols
(`tdi_cycle_123.TDI123_UNIVERSE = list(PRIORITY_PAIRS)`):
```
EUR/USD, GBP/USD, USD/JPY, USD/CHF, AUD/USD, USD/CAD, NZD/USD,
EUR/GBP, EUR/JPY, EUR/CHF, EUR/AUD, GBP/JPY, GBP/CHF, AUD/JPY, CAD/JPY,
GBP/AUD, AUD/CAD, AUD/CHF, AUD/NZD,      # AUD crosses
XAU/USD, XAG/USD,                          # Gold, Silver
DE30, US30, USTEC,                         # DAX 40, Dow 30, Nasdaq 100
DXY                                        # US Dollar Index
```
(Broker-symbol mapping note preserved from source comments: MT5/Exness maps
these to `XAUUSDm`/`XAGUSDm`/`DE30m`/`US30m`/`USTECm`/`DXYm` — a suffix
convention, not part of the strategy logic itself.)

---

## 2. Core indicator: FSO_TDI

**RSI** — `_rsi(closes, period=13)` (`btmm_core.py:79-95`). Period = **13**,
Wilder-style smoothed (not plain-average) RSI:
- Guard: `if len(closes) < period + 1: return [50.0] * len(closes)`.
- Seed: `avg_g = sum(gains[:period]) / period`, `avg_l = sum(losses[:period]) / period`.
- Recursive update for `i in range(period, len(gains))`:
  ```python
  avg_g = (avg_g * (period - 1) + gains[i]) / period
  avg_l = (avg_l * (period - 1) + losses[i]) / period
  rs = avg_g / avg_l if avg_l else 100
  rsi_vals.append(100 - 100 / (1 + rs))
  ```
- Output left-padded with `50.0` so length matches `closes`.

**Fast/slow signal lines** — SMA of the RSI series:
- `fast_arr = _sma(rsi, 2)` — SMA(2), "green line."
- `slow_arr = _sma(rsi, 7)` — SMA(7), "red line."
- `_sma(values, period)` (`btmm_core.py:68-74`): for index `i`,
  `start = max(0, i - period + 1)`, average `values[start:i+1]` — a
  **partial-window average near the start**, not NaN/None padding.

**Baseline ("yellow line")** — computed separately in `tdi_cycle_123.py`, NOT
inside `calc_tdi()`:
```python
def _baseline_series(rsi_series, period=34):
    out = []
    for i in range(len(rsi_series)):
        start = max(0, i - period + 1)
        window = rsi_series[start:i+1]
        out.append(sum(window) / len(window))
    return out
```
(`tdi_cycle_123.py:343-350`, called with period **34** at line 705). Same
partial-window warmup as `_sma`.

**Bollinger Bands on RSI** — `calc_tdi()` (`btmm_core.py:98-125`), comment:
*"Bollinger Bands on RSI: 34-period SMA ± 1.6185σ (golden ratio — exact BTMM
spec)"*:
- Before 34 bars: `bb_upper, bb_lower, bb_mid = 68.0, 32.0, 50.0` (defaults).
- At ≥34 bars: `window = rsi[-34:]`; `mu = mean(window)`;
  `std = population_stdev(window)` (divide by N, not N-1);
  `bb_mid = mu`; `bb_upper = mu + 1.6185*std`; `bb_lower = mu - 1.6185*std`.
- Multiplier is the literal constant **1.6185**.

`calc_tdi()` return dict:
```python
{"rsi": rsi[-1], "fast": fast_arr[-1], "slow": slow_arr[-1], "fast_arr": fast_arr,
 "bb_upper": ..., "bb_lower": ..., "bb_mid": ...,
 "bullish": fast_arr[-1] > slow_arr[-1], "bearish": fast_arr[-1] < slow_arr[-1],
 "overbought": rsi[-1] > bb_upper, "oversold": rsi[-1] < bb_lower}
```
Only returns last-value scalars (+ `fast_arr`) — `_analyze_timeframe` rebuilds
the full `rsi_series`/`fast_arr`/`slow_arr`/`baseline_series` itself so it can
query values at arbitrary swing indices (`tdi_cycle_123.py:701-705`).

---

## 3. Swing detection

`_find_swings(bars, left=SWING_LEFT, right=SWING_RIGHT)` —
`tdi_cycle_123.py:133-158`. `SWING_LEFT = 2`, `SWING_RIGHT = 2` (lines 60-61).

```python
if len(bars) < left + right + 1: return []
for i in range(left, len(bars) - right):
    h_center, l_center = bars[i]["high"], bars[i]["low"]
    is_high = all(bars[j]["high"] <= h_center for j in range(i-left, i)) and \
              all(bars[j]["high"] <  h_center for j in range(i+1, i+right+1))
    is_low  = all(bars[j]["low"]  >= l_center for j in range(i-left, i)) and \
              all(bars[j]["low"]  >  l_center for j in range(i+1, i+right+1))
```

A bar is a **swing high** if its high is ≥ every high in the 2 bars to its
left (equality allowed on the left) AND strictly greater than every high in
the 2 bars to its right (no equality allowed on the right). Swing low is the
mirror. A bar can register as both a swing high and swing low simultaneously
(two separate entries). Each swing: `{"type": "high"|"low", "idx", "price", "ts_utc"}`.

**No minimum-size filter here** — every qualifying fractal is returned. The
size filter (`MIN_LEG1_PIPS`) is applied later, only to the p1→p2 leg. Because
the right-window needs 2 future bars, the final 2 bars of any series can never
register a swing.

---

## 4. 123 pattern geometry

`_find_123_pattern(swings, bars, symbol=None)` — `tdi_cycle_123.py:165-247`.

**Constants:**
```python
POINT3_OVERSHOOT_PCT  = 0.65   # p3 beyond p1 (stop-hunt) — generous bound
POINT3_SHORTFALL_PCT  = 0.25   # p3 short of p1 (shallow-miss retest) — tight bound
MIN_LEG1_PIPS          = 8.0   # minimum size of the p1→p2 leg, in pips
```
**Important:** this tolerance is **asymmetric**, not "±25% both ways."
Overshoot (p3 pushing past p1) tolerates up to 65% of leg1; shortfall (p3
falling short of p1) tolerates only up to 25% of leg1.

Algorithm:
1. Require `len(swings) >= 3`, else `None`.
2. `price = bars[-1]["close"]`; `pip = _pip_size(price, symbol)`.
3. `recent = swings[-6:]` — **only the last 6 swings** are considered.
4. Triple-nested loop over every combinatorial triple `a < b < c` within
   `recent` (not just adjacent triples) → `p1, p2, p3 = recent[a], recent[b], recent[c]`.
5. **Alternation requirement:** skip unless `p1.type == p2.type` is False,
   `p2.type == p3.type` is False, and `p1.type == p3.type` is True — i.e. only
   low-high-low or high-low-high triples survive.
6. `is_bullish = (p1.type == "low")`.
7. `leg1_range = abs(p2.price - p1.price)`; skip if `== 0`.
8. Skip if `leg1_range < MIN_LEG1_PIPS * pip` (8 pips minimum).
9. Overshoot/shortfall geometry (per direction):
   - Bullish: `overshoot = p1.price - p3.price` (positive = p3 pushed lower than p1).
   - Bearish: `overshoot = p3.price - p1.price` (positive = p3 pushed higher than p1).
   - If `overshoot >= 0`: valid only if `overshoot <= 0.65 * leg1_range`, tag `"overshoot"`.
   - If `overshoot < 0`: valid only if `-overshoot <= 0.25 * leg1_range`, tag `"shortfall"`.
10. Surviving triples become candidates:
    `{"direction", "p1", "p2", "p3", "leg1_range", "leg1_range_pips", "p3_kind", "p3_overshoot_pct"}`.
11. If no candidates, `None`.
12. **Selection:** sort by `(p3.idx, leg1_range)` descending, take the first —
    i.e. the candidate whose **p3 is most recent** wins; ties broken by larger
    `leg1_range`.

**Disqualifiers (exhaustive):** non-alternating swing types; `p1.type != p3.type`;
`leg1_range == 0`; `leg1_range < 8 pips`; overshoot > 65% of leg1; shortfall >
25% of leg1.

**Direction mapping:** Bullish = p1 low → p2 high → p3 low retesting p1's low
zone → setup = BUY. Bearish = p1 high → p2 low → p3 high retesting p1's high
zone → setup = SELL.

---

## 5. TDI extreme check (at point 3)

`_check_tdi_extreme(pattern, rsi_series, baseline_series)` —
`tdi_cycle_123.py:353-377`.

**Constants** (`tdi_cycle_123.py:54-57`, comment: *"TDI RSI: standard 63/68
overbought, 32/37 oversold (StrictlyCorrect spec)"*):
```python
TDI_OB_ZONE   = 63.0   # yellow-baseline extreme (bearish setup)
TDI_OB_STRICT = 68.0   # RSI overbought
TDI_OS_ZONE   = 37.0   # yellow-baseline extreme (bullish setup)
TDI_OS_STRICT = 32.0   # RSI oversold
```

```python
rsi_at_p3 = rsi_series[p3_idx]
baseline_at_p3 = baseline_series[p3_idx]
if is_bullish:
    baseline_extreme = baseline_at_p3 <= 37.0
    rsi_extreme      = rsi_at_p3 <= 32.0
else:
    baseline_extreme = baseline_at_p3 >= 63.0
    rsi_extreme      = rsi_at_p3 >= 68.0
present = baseline_extreme or rsi_extreme    # EITHER threshold
strong  = baseline_extreme and rsi_extreme   # BOTH thresholds
```
If `p3_idx` is out of range for either series: `{"present": False, "reason": "index out of TDI series"}`.

---

## 6. Divergence classification

`_check_divergence(pattern, rsi_series)` (`tdi_cycle_123.py:270-336`), using
`_rsi_extreme_at(rsi_series, idx, want_low, window=DIVERGENCE_RSI_WINDOW)`
(lines 254-267).

**Constants** (lines 81-89):
```python
DIVERGENCE_EQUAL_TOLERANCE_PCT = 0.05   # 5% of leg1 still counts as "equal"
DIVERGENCE_MIN_RSI_DELTA       = 2.0    # minimum RSI travel p1→p3
DIVERGENCE_RSI_WINDOW          = 2      # ±2-bar window around swing index
```

**`_rsi_extreme_at`** reads the RSI extreme from a **5-bar window** (idx-2 to
idx+2 inclusive) centered on the swing index, not the exact bar — comment:
*"the oscillator's own peak rarely lands on the exact bar of the price swing,
so read the RSI extreme from a small window centred on the swing index — this
mirrors how the divergence line is drawn across TDI peaks/troughs."*
```python
lo, hi = max(0, idx-2), min(len(rsi_series), idx+3)
seg = rsi_series[lo:hi]
return min(seg) if want_low else max(seg)
```

**Core logic:**
```python
rsi_p1 = _rsi_extreme_at(rsi_series, p1.idx, want_low=is_bullish)
rsi_p3 = _rsi_extreme_at(rsi_series, p3.idx, want_low=is_bullish)
price_move_ratio = (p3.price - p1.price) / (leg1_range or 1e-12)

if is_bullish:
    price_ok = price_move_ratio <= 0.05
    rsi_ok   = (rsi_p3 - rsi_p1) >= 2.0
    strong   = price_move_ratio < 0 and rsi_ok
else:
    price_ok = price_move_ratio >= -0.05
    rsi_ok   = (rsi_p3 - rsi_p1) <= -2.0
    strong   = price_move_ratio > 0 and rsi_ok

divergence = price_ok and rsi_ok
```

**Classification** (`reason` string):
| Condition | Label |
|---|---|
| `divergence and strong` | **"regular divergence"** |
| `divergence and not strong` | **"equal-level divergence"** (price within the 5%-of-leg1 tolerance band around p1, not truly beyond it, but RSI still diverged ≥2.0 pts) |
| `not divergence and rsi_ok` | "price failed to reach p1 extreme — confirmation, not divergence" |
| `not divergence and not rsi_ok` | "RSI did not diverge" |

Plain English: bullish divergence requires price at p3 to be equal-to-or-lower
than p1's low (within 5%-of-leg1 tolerance for "equal"), while RSI at p3 is
≥2.0 points higher than RSI at p1. "Strong"/regular additionally requires
price to have made a genuinely new (strictly lower) low. Bearish mirrors this.

### Ambiguous / vs-trend downgrade

Lives in `_analyze_timeframe` (lines 768-771, 890-894), not inside
`_check_divergence` itself:
```python
trend = _trend_regime(closes)
div_ambiguous = bool(div["present"] and trend["stacked"] and trend["direction"] != direction)
```
Scoring effect: if ambiguous, divergence scores only **+1** (instead of +2/+3)
and `strong` is forced `False` in the output row, with `reason` swapped to:

> *"counter-trend divergence against a stacked {direction} EMA cascade —
> unreliable per Davit p.9"*

**Verbatim source citation** (lines 763-767):
> `# Davit, "Pivot Trading with TDI" p.9: "When the market is in a strong trend`
> `# in either direction, oscillators do not function well ... Any signs of`
> `# divergence during a strong trend would be ambiguous at best." A 123 fired`
> `# against a fully-stacked EMA cascade is exactly that case, so the`
> `# divergence still counts but no longer carries a setup on its own.`

**`_trend_regime(closes)`** (lines 480-512) — "stacked" means: requires
`len(closes) >= 300`; computes e50, e200, e800 (e800 gated at `>=2400`, else
`None`); `span = abs(emas[0] - emas[-1])` (EMA-to-EMA spread, excluding spot
price); `TREND_STACK_MIN_SPAN_PCT = 0.0005` (0.05%, comment: "~8 pips on
EUR/AUD at 1.64") — if `span/price < 0.0005`, not stacked (EMAs converged,
range regime). Otherwise checks strict monotonic order `price > e50 > e200 >
e800` (bull) or reverse (bear).

---

## 7. Signal cross confirmation

`_check_signal_cross(pattern, fast_arr, slow_arr)` — `tdi_cycle_123.py:384-421`.
`SIGNAL_CROSS_MAX_AGE = 20` (line 92, bars).

```python
p3_idx = pattern.p3.idx
if p3_idx >= len(fast_arr) - 1: return {"present": False, "reason": "no post-p3 bars"}

is_bullish = (pattern.direction == "bullish")
held = (fast_arr[-1] > slow_arr[-1]) if is_bullish else (fast_arr[-1] < slow_arr[-1])
if not held: return {"present": False, "reason": "signal crossed back — momentum lost"}

last_bar = len(fast_arr) - 1
for i in range(last_bar, p3_idx, -1):     # walk backward from latest bar to p3
    prev_f, prev_s = fast_arr[i-1], slow_arr[i-1]
    cur_f, cur_s   = fast_arr[i],   slow_arr[i]
    crossed = (prev_f <= prev_s and cur_f > cur_s) if is_bullish \
        else (prev_f >= prev_s and cur_f < cur_s)
    if crossed:
        age = last_bar - i
        if age > 20: return {"present": False, "bars_since_cross": age, "reason": f"cross is {age} bars stale"}
        return {"present": True, "bars_since_cross": age, "cross_offset": i - p3_idx}
return {"present": False, "reason": "no cross after p3"}
```

Plain English: bullish wants fast SMA(2 of RSI) crossing **above** slow SMA(7
of RSI) ("green over red") at some bar strictly after p3; bearish wants fast
crossing **below** slow. Two conditions: (a) the cross must still be in effect
on the latest bar (no re-cross back since), and (b) the most recent such
cross (walking backward from latest bar to p3) must be ≤20 bars old. Cross
detection is a single-bar transition test (`prev <= prev` → `cur > cur`), not
multi-bar confirmation.

---

## 8. HTF bias

`_htf_bias(bias_candles)` — `tdi_cycle_123.py:650-662`. Called with H4
candles for an H1 entry, or H1 candles for an M15 entry — generic
"next-timeframe-up" function.

```python
if not bias_candles or len(bias_candles) < 60: return None
closes = [b.close for b in bias_candles]
e50 = ema_last(closes, 50)
e50_prev = calc_ema(closes[:-5], 50)[-1] if len(closes) > 55 else e50
price = closes[-1]
if price > e50 and e50 > e50_prev: return "bullish"
if price < e50 and e50 < e50_prev: return "bearish"
return "neutral"
```

Only EMA-50 is used. `e50_prev` is **not** a lookup into a precomputed series
at index -6 — it's a **fresh EMA-50 recomputed from `closes[:-5]`** (last 5
closes dropped, EMA-50 recalculated on the shorter series, final value taken).
This approximates "where EMA50 was 5 bars ago" but is not mathematically
identical to true index-based lookback, since dropping bars changes the whole
recursion. If `len(closes) <= 55`, `e50_prev = e50` — which forces
`"neutral"` (both slope comparisons become False when equal).

Result: `"bullish"` = price above EMA50 AND EMA50 rising. `"bearish"` = price
below EMA50 AND EMA50 falling. Else `"neutral"`.

---

## 9. Weekly pivot / location

**Week boundary** — `_MONDAY_EPOCH = 4*86400` (1970-01-05 00:00 UTC was a
Monday), `_WEEK_SECONDS = 7*86400` (`tdi_cycle_123.py:525-526`).

`_prev_week_hlc(bars, as_of_ts)` (lines 529-543):
```python
cur_week_start = _MONDAY_EPOCH + ((as_of_ts - _MONDAY_EPOCH) // _WEEK_SECONDS) * _WEEK_SECONDS
prev_start = cur_week_start - _WEEK_SECONDS
seg = [b for b in bars if prev_start <= b.ts_utc < cur_week_start]
if not seg: return None
return (max(b.high for b in seg), min(b.low for b in seg), seg[-1].close)
```
Week boundary = Monday 00:00:00 UTC. Uses only bars strictly inside the
**prior fully-completed week** — no lookahead into the current forming week.

**Fibonacci pivots** — `_weekly_fib_pivots(hlc)` (lines 546-559):
```python
FIB_PIVOT_LEVELS = ((0.382, 38), (0.618, 61), (0.786, 78), (1.0, 100), (1.382, 138))
H, L, C = hlc
rng = H - L
P = (H + L + C) / 3.0        # classic pivot formula
for r, tag in FIB_PIVOT_LEVELS:
    lv[f"R{tag}"] = P + r * rng
    lv[f"S{tag}"] = P - r * rng
```
Produces R38/S38, R61/S61, R78/S78, R100/S100, R138/S138 (tag names are
Fib-percentage labels, not literal ratio digits).

**`_location(price, direction, pivots)`** (lines 562-591):
```python
if direction == "bearish": loc, side = (price - P) / rng, "R"
else:                      loc, side = (P - price) / rng, "S"

if   loc >= 1.30:  quality, zone = "extended", f"{side}138+"
elif loc >= 0.90:  quality, zone = "prime",    f"{side}100"
elif loc >= 0.55:  quality, zone = "good",     f"{side}61-78"
elif loc >= 0.30:  quality, zone = "weak",     f"{side}38"
elif loc >= -0.30: quality, zone = "poor",     "mid-pivot"
else:              quality, zone = "wrongside","wrong-side"

ok = quality in ("good", "prime")
```
`loc` is signed distance from pivot P, normalized by the prior week's range,
oriented so positive = the setup's "reversal zone" (resistance-side for
sells, support-side for buys).

**Consumer rule** (`_analyze_timeframe`, lines 822-827) — location is **NOT
added to score**. Comment (lines 804-808):
> *"Note the location for information, but do NOT add it to the score. A
> 30-day walk-forward found the 61–100 zone did not separate winners from
> losers on this data (only the extreme 100-zone hinted positive, n too
> small), so it is not treated as an edge — just displayed, plus the
> wrong-side gate below."*

But it acts as a **hard grade cap**:
```python
# Davit's location gate (kept as a filter, not a score): a clean TDI setup on
# the WRONG side of the weekly pivot — selling into support or buying into
# resistance — is a sucker move. Cap those to C so they can't present as A/B.
if location["quality"] in ("poor", "wrongside") and grade in ("A", "B"):
    grade = "C"
    notes.append("grade capped: poor pivot location")
```

---

## 10. Session labeling & active window

`_session_label(ts_utc)` (`tdi_cycle_123.py:610-618`) — UTC hour boundaries:

| Session | UTC range |
|---|---|
| `Asian-pm` | [03, 07) |
| `London-open` | [07, 10) |
| `London` | [10, 13) |
| `NY-open` | [13, 16) |
| `NY-afternoon` | [16, 18) |
| `NY-late` | [18, 22) |
| `Asian` | [22, 24) ∪ [00, 03) (fallthrough) |

`_in_active_session(ts_utc)` (lines 621-623):
```python
SESSION_ACTIVE_START = 3    # UTC, inclusive
SESSION_ACTIVE_END   = 18   # UTC, exclusive
return SESSION_ACTIVE_START <= h < SESSION_ACTIVE_END
```
Active window = **UTC [03:00, 18:00) = SAST [05:00, 20:00)**. Derived from a
stated 60-day backtest finding (comment lines 597-604): the Asian dead-zone
(22:00-07:00 UTC) lost -0.83R over 33 signals (6% win rate), while
London+NY (07:00-16:00 UTC) carried the positive buckets. Note the coded
window (03:00-18:00 UTC) is **wider** than the raw backtest finding
(07:00-16:00 UTC) — a deliberate buffer, not an exact replay of the
backtest window.

*(Note: `btmm_core.py` has a separate, unrelated AMD/kill-zone scheme used by
other strategies in this codebase — not used by TDI Cycle 123. Don't conflate
them when porting.)*

---

## 11. ADR context

`_adr_context(h1_candles, d1_candles)` — `tdi_cycle_123.py:626-647`.
`ADR_PERIOD_DAYS = 14` (line 607).

```python
if not d1_candles or len(d1_candles) < 15: return None
today = date_of(h1_candles[-1].ts_utc)                      # UTC date of latest entry-TF bar
prior = [b for b in d1_candles if date_of(b.ts_utc) < today]  # strictly-before-today
if len(prior) < 14: return None
adr = mean(b.high - b.low for b in prior[-14:])
todays = [b for b in h1_candles if date_of(b.ts_utc) == today]
trng = (max(todays highs) - min(todays lows)) if todays else 0.0
return {"adr": adr, "consumed_pct": min(100, trng/adr*100), "remaining": max(adr - trng, 0)}
```
"Today" = UTC date of the **entry-timeframe's** most recent bar (not D1's own
last bar). Lookback = most recent 14 fully-completed prior days.
`btmm_core.calc_adr` is explicitly NOT reused here (comment: it assumes
96×15-min bars/day and can't operate on H1 data) — this is an independent
ADR calc specific to this module.

**`tp1_reachable`** (lines 861-869):
```python
tp1_reachable = abs(tp1 - entry) <= adr_ctx["remaining"]  if (adr_ctx and entry and tp1) else None
```
`None` (never `False`) when data is unavailable — "unknown," never a hard
fail on its own.

---

## 12. Ketchup / 13-EMA reclaim (⚠ informational only — see §13)

```python
ema13 = ema_last(closes, 13)
ketchup_reclaimed = (price > ema13) if direction == "bullish" else (price < ema13)
```
(`tdi_cycle_123.py:753-754`). For a BUY setup, "ketchup reclaimed" means
current price closes above EMA-13; for SELL, below it. Standard SMA-seeded
EMA-13, no special seeding.

**This is display-only — see §13, item 1, for why it is NOT a gate despite
the function's own docstring implying it should be.**

---

## 13. Known discrepancies (read before porting)

1. **Ketchup reclaim is NOT an entry gate**, despite `_analyze_timeframe`'s
   docstring saying *"Require price to have reclaimed it in the setup
   direction before the setup is treated as a live entry."* In actual code,
   `ketchup_reclaimed` only feeds the `notes` list and two informational
   output fields (`ema13`, `ketchup_reclaimed`) — it never touches `score`,
   `grade`, or `setup`. `alerts.py`'s own comment confirms this is
   deliberate: *"A clean 30-day A/B test showed that waiting for the reclaim
   raised win rate (27→37%) but wrecked R:R — entry chased price a median of
   6 bars into the move while the stop stayed at p3 — so expectancy got
   worse (−0.09→−0.40R). It is surfaced as an informational badge... not as
   an automated suppressor."* **If porting: implement as display-only unless
   you deliberately want to test gating on it again.**

2. **`TDI123_GRADE_A_ONLY` config flag exists but is dead weight.** It's
   defined in `config.py:67` (default `"false"`) and imported into
   `alerts.py`, but never referenced inside `_should_alert_tdi123` or
   `alert_tdi123_setup`. The actual grade gate is hardcoded: "A always
   passes, B passes conditionally" — not driven by this flag at all.

3. **Overshoot/shortfall tolerance is asymmetric, not "±25%."** Overshoot
   (p3 beyond p1) tolerates up to 65% of leg1; shortfall (p3 short of p1)
   tolerates only up to 25% of leg1. These are two different constants
   (`POINT3_OVERSHOOT_PCT = 0.65`, `POINT3_SHORTFALL_PCT = 0.25`).

4. **`ema_stack()` from `btmm_core.py` is NOT used by TDI Cycle 123.** It's
   only used by the separate BTMM-doctrine scanner. Don't port it as if it
   were part of this pipeline.

5. **EMA warmup thresholds are not a consistent multiplier of period.**
   EMA50 needs ≥50 bars (1×), EMA200 needs ≥300 bars (1.5×), EMA800 needs
   ≥2400 bars (3×). Quote the literals; don't infer a formula.

6. **The `/tdi123` scanner and `/tdi123/detail` endpoint fetch candles
   differently.** The scanner reads directly from the local SQLite cache
   (`cache.read_candles`) for performance (a prior version called
   `fetcher.get_candles` per-symbol and caused 85-120s timeouts from ~100
   concurrent SQLite connections). The detail endpoint uses `fetcher.get_candles`
   (cache-first, live-fetch-on-stale). Both use the same bar limits.

---

## 14. EMA cascade targets

`_ema_targets(closes, direction, current_price, leg1_range=0.0)` —
`tdi_cycle_123.py:428-473`. Uses `calc_ema`/`ema_last` from `btmm_core.py`.

### `calc_ema(closes, period)` seeding (`btmm_core.py:14-32`)
```python
k = 2.0 / (period + 1)
if len(closes) < period:
    # SHORT-DATA PATH — no SMA seed, seeds at closes[0]
    out = [closes[0]]
    for v in closes[1:]: out.append(v*k + out[-1]*(1-k))
    return out
# NORMAL PATH — SMA seed
out = [sum(closes[:period]) / period]
for v in closes[period:]: out.append(v*k + out[-1]*(1-k))
return [out[0]]*period + out[1:]   # front-padded to align with closes' indices
```
Normal path: **seeded with a plain SMA of the first `period` closes**, then
recursed with standard `EMA = v*k + prev*(1-k)`. Output front-padded by
repeating the seed value so length matches `closes`. Short-data path
(`len(closes) < period`) does NOT use an SMA seed — seeds at `closes[0]` and
recurses through every close (explicit code-comment warning: must never
flat-line at `closes[-1]`, which previously made `price > e800` structurally
always False).

`ema_last(closes, period) = calc_ema(closes, period)[-1]`.

### Warmup gating (enforced by the caller, not inside `calc_ema`)
```python
if len(closes) < 50: return {tp1: None, tp2: None, tp3: None, ema50: None, ema200: None, ema800: None, projected: False}
e50  = ema_last(closes, 50)
e200 = ema_last(closes, 200) if len(closes) >= 300  else None
e800 = ema_last(closes, 800) if len(closes) >= 2400 else None
```
**EMA-200 requires ≥300 closes. EMA-800 requires ≥2400 closes.** Rationale
(comment): "Convergence thresholds (seed influence < 1%) — same standard
used for BTMM's own EMA-200/800 'warm' flags." Below threshold, `calc_ema`
would either flat-line at current price or be too seed-biased to trust.

### Directional filter
```python
candidates = [v for v in (e50, e200, e800) if v is not None]
if bullish: targets = sorted(v for v in candidates if v > current_price)          # ascending
else:       targets = sorted((v for v in candidates if v < current_price), reverse=True)  # descending
```
Only EMAs on the "target" side of price qualify. Nearest one becomes TP1.

### Measured-move fallback
```python
if not targets and leg1_range > 0:
    sign = 1 if bullish else -1
    targets = [current_price + sign*leg1_range*m for m in (1.0, 2.0, 3.0)]
    projected = True
```
Fires **only** when zero EMA candidates passed the directional filter.
Produces TP1/TP2/TP3 at exactly `price ± 1×/2×/3× leg1_range`. Callers must
check `projected` before labeling targets "EMA-based."

`tp1, tp2, tp3 = targets[0], targets[1], targets[2]` where present, else `None`.

---

## 15. ATR & stop-loss sizing

`_atr(bars, period=14)` — `tdi_cycle_123.py:114-126`. `ATR_PERIOD = 14`.

```python
if len(bars) < 15: return None
trs = []
for i in range(len(bars)-14, len(bars)):
    tr = max(high-low, abs(high-prev_close), abs(low-prev_close))
    trs.append(tr)
return mean(trs)
```
Plain **SMA of True Range over the last 14 bars** (simple mean, NOT Wilder's
exponential ATR smoothing). Requires ≥15 bars.

**Stop-loss sizing** (`_analyze_timeframe`, lines 834-848):
```python
SL_STRUCT_ATR_MULT = 0.5
SL_MIN_ATR_MULT    = 1.0

atr = _atr(entry_candles) or (pattern.leg1_range * 0.5)   # fallback if <15 bars
p3p = pattern.p3.price
if setup == "BUY":
    sl = min(p3p - 0.5*atr, entry - 1.0*atr)   # farther of the two below entry
elif setup == "SELL":
    sl = max(p3p + 0.5*atr, entry + 1.0*atr)   # farther of the two above entry
```
Stop is placed either 0.5×ATR beyond p3's extreme wick, or 1.0×ATR from
entry — whichever produces the **larger** distance (more conservative). This
guarantees a minimum 1×ATR stop even when entry sits right on p3.

---

## 16. Scoring formula (15-pt scale)

`_analyze_timeframe`, `tdi_cycle_123.py:756-827`.

```python
score = 3                                   # base: pattern found
notes = [f"{timeframe} 123 geometry ok"]

if div_ambiguous:        score += 1; notes.append("divergence vs stacked ... — ambiguous")
elif div["strong"]:      score += 3; notes.append("regular divergence (price beyond p1)")
elif div["present"]:     score += 2; notes.append("equal-level divergence")

if extreme["strong"]:    score += 3; notes.append("TDI baseline + RSI both extreme")
elif extreme["present"]: score += 2; notes.append("TDI extreme (partial)")

if cross["present"]:     score += 2; notes.append("TDI signal cross confirmed")

if htf_aligned:           score += 2; notes.append(f"{bias_timeframe} bias aligned")

freshness = abs(price - p3_price) < 0.30 * pattern.leg1_range
if freshness:             score += 2; notes.append("fresh from p3")
```

**Component max points:** 3 (base) + 3 (divergence, 3-tier: ambiguous=1,
equal-level=2, regular=3) + 3 (TDI extreme, present=2/strong=3) + 2 (signal
cross) + 2 (HTF bias alignment) + 2 (freshness) = **15 max**. (The module
docstring only documents 2 divergence tiers; the code actually has 3 —
quote the code, not the docstring.)

**Freshness** = `abs(latest_close - p3_price) < 0.30 * leg1_range` — current
price must be within 30% of leg1's range (a fraction of the 1→2 leg, not
pips or a fixed value) of point 3's price.

### Grade thresholds
```python
if   score >= 11: grade = "A"
elif score >= 8:  grade = "B"
elif score >= 5:  grade = "C"
else:              grade = "NO-TRADE"
```
**A ≥ 11, B ≥ 8, C ≥ 5, else NO-TRADE** (out of 15). Identical for H1 and
M15 (shared function). Location-cap rule (§9) may then force A/B down to C.

`setup` = `"NO-TRADE"` if `grade == "NO-TRADE"`, else `"BUY"` if
`direction == "bullish"` else `"SELL"`.

### Early exits (before scoring runs at all)
- `len(entry_candles) < 100` → `{"grade": "NO-DATA", "reason": "need ≥100 {tf} candles", "score": 0}`.
- `len(swings) < 3` → `{"grade": "NO-TRADE", "reason": "not enough swings", "score": 0}`.
- `_find_123_pattern` returns `None` → `{"grade": "NO-TRADE", "reason": "no 123 pattern", "score": 0}`.

---

## 17. Full output row structure

`_analyze_timeframe()` return dict (successful path):

| Key | Meaning |
|---|---|
| `symbol` | Pair symbol |
| `setup` | `"BUY"` / `"SELL"` / `"NO-TRADE"` |
| `grade` | `"A"`/`"B"`/`"C"`/`"NO-TRADE"` (or `"NO-DATA"` on early exit) |
| `score` | 0-15 |
| `notes` | Semicolon-joined trace of scoring components that fired |
| `current_price` | Latest close on entry timeframe |
| `direction` | `"bullish"` / `"bearish"` |
| `pattern` | `{p1,p2,p3: {idx,price,ts_utc}, leg1_range_pips}` |
| `divergence` | `{present, strong, ambiguous, reason, rsi_at_p1, rsi_at_p3}` |
| `location` | `{quality, ok, zone, loc_ratio}` |
| `location_ok` | Shortcut = `location.ok` |
| `ema13` | Current EMA-13 |
| `ketchup_reclaimed` | bool — see §13.1, display-only |
| `session` | Session label string |
| `in_active_session` | bool |
| `adr_consumed_pct` | 0-100 or `None` |
| `adr_remaining_pips` | float or `None` |
| `tp1_reachable` | bool or `None` |
| `trend_regime` | `{stacked, direction, span_pct}` |
| `tdi_extreme` | `{present, strong, baseline_at_p3, rsi_at_p3}` |
| `signal_cross` | `{present, bars_since_cross/cross_offset/reason}` |
| `htf_bias` | `"bullish"`/`"bearish"`/`"neutral"`/`None` |
| `htf_bias_timeframe` | `"H4"` or `"H1"` |
| `htf_aligned` | bool |
| `tdi_now` | `{rsi, baseline, bb_upper, bb_lower}` (current-bar snapshot) |
| `targets` | `{L1,L2,L3, L1_pips,L2_pips,L3_pips, ema50,ema200,ema800, projected}` |
| `trade_plan` | `{entry, sl, sl_pips, tp1,tp2,tp3, rr1}` |
| `timeframe` | `"H1"` or `"M15"` |

`analyze_pair`'s H1 result may additionally carry an `"m15"` key holding a
full nested copy of this same structure (see §18).

Note: `calc_tdi`'s `fast`/`slow`/`overbought`/`oversold`/`bullish`/`bearish`
fields are NOT surfaced in the output row — only `rsi`, `bb_upper`,
`bb_lower`, and the separately-computed `baseline` appear in `tdi_now`.

---

## 18. `analyze_pair` / `analyze_universe` wiring

```python
def analyze_pair(symbol, h1_candles, h4_candles=None, d1_candles=None, m15_candles=None):
    h1_result = _analyze_timeframe(symbol, h1_candles, h4_candles, d1_candles,
                                    timeframe="H1", bias_timeframe="H4")
    if m15_candles:
        m15_result = _analyze_timeframe(symbol, m15_candles, h1_candles, d1_candles,
                                         timeframe="M15", bias_timeframe="H1")
        if m15_result["setup"] in ("BUY", "SELL"):
            h1_result["m15"] = m15_result
    return h1_result
```
- H1 leg: entry=H1 candles, bias=H4 candles.
- M15 leg (if provided): entry=M15 candles, bias=**H1 candles** (H1 candles
  serve double duty — H1's own entry series AND M15's bias series).
- Both legs share the same `d1_candles` (weekly pivot HLC).
- **Attach condition**: M15 result is attached at `h1_result["m15"]` only
  when `m15_result["setup"] in ("BUY","SELL")` — i.e. any tradeable grade
  (A/B/C all map to BUY/SELL). NO-TRADE/NO-DATA M15 results are silently
  dropped (no `"m15"` key at all).
- Return value is always `h1_result` — H1 is root, M15 is optional nested.

`analyze_universe(candles_by_pair)`:
- Iterates `TDI123_UNIVERSE` (= `PRIORITY_PAIRS`).
- Accepts either lowercase-numeral or lettered timeframe keys
  (`bundles.get("1h") or bundles.get("h1")`, etc.).
- Calls `analyze_pair(sym, h1, h4_candles=h4, d1_candles=d1, m15_candles=m15 or None)`.
- Wraps each call in try/except — one pair's failure never aborts the scan;
  substitutes `{"setup": "NO-TRADE", "grade": "NO-DATA", "reason": f"error: {e}", "score": 0}`.
- Aggregates `buys`/`sells`/`grade_a`/`grade_b` counts from the **H1-level
  results only** — M15-nested setups are not counted in these totals.

---

## 19. Alert quality gate — `_should_alert_tdi123(row)`

`alerts.py:681-719`.

```python
def _should_alert_tdi123(row):
    grade = row.get("grade")
    if grade not in ("A", "B"): return False

    if TDI123_SESSION_FILTER and row.get("in_active_session") is False: return False
    if TDI123_ADR_FILTER and row.get("tp1_reachable") is False: return False

    if grade == "A": return True

    div_present = row.get("divergence", {}).get("present", False)
    htf_aligned = row.get("htf_aligned", False)
    rr = (row.get("trade_plan") or {}).get("rr1") or 0
    return div_present and htf_aligned and rr >= 1.0
```
**Note the `is False` strict-identity checks** — a `None` flag (unknown data)
passes through, never a hard fail.

- Grade C/NO-TRADE/None → always reject.
- Session filter (default ON): reject if outside UTC [03:00,18:00) and the
  flag is explicitly `False` (not `None`).
- ADR filter (default OFF): reject if TP1 unreachable and flag explicitly `False`.
- **Grade A → always passes**, no further checks.
- **Grade B → requires ALL THREE**: divergence present + HTF bias aligned +
  R:R to TP1 ≥ 1.0.

Ketchup reclaim is explicitly NOT part of this gate (see §13.1).

---

## 20. Full alert gate chain — `alert_tdi123_setup(pair, row)`

`alerts.py:722-913`. Gates run **in this exact order**; any failure returns
immediately (no alert sent):

**Gate 0 — global enable:** `if not TDI123_ALERTS_ENABLED: return`.

**Gate 1 — M15 recursion (not a gate, but ordering-critical):**
```python
if row.get("m15") and row.get("timeframe") != "M15":
    alert_tdi123_setup(pair, row["m15"])
```
Runs **before** any of the gates below. Verbatim bug-fix comment (dated
2026-09-10):
> *"M15 leg evaluated FIRST and unconditionally — independent of whatever
> the H1 leg's gates decide below. Bug found 2026-09-10: this recursion
> used to sit after the H1 early-return gates, so a Grade A/B M15 setup was
> silently never evaluated whenever H1 didn't ALSO independently qualify
> (e.g. H1 grade C/NO-TRADE) — exactly the common case, since M15 and H1
> grade independently."*
Recursion guard: `row.get("timeframe") != "M15"` prevents infinite recursion.

**Gate 2 — setup direction:** `if setup not in ("BUY","SELL"): return`.

**Gate 3 — quality filter:** `if not _should_alert_tdi123(row): return` (§19).

**Gate 4 — signal cross confirmation:**
`if not (row.get("signal_cross") or {}).get("present"): return`.

**Gate 5 — trade-plan completeness:** requires `entry`, `sl`, AND `tp1` all
truthy.

**Gate 6 — multi-tier R:R check:**
```python
sl_dist = abs(entry - sl)
if sl_dist < 1e-9: return   # zero-distance guard

direction_ok = lambda tp: (setup=="BUY" and tp>entry) or (setup=="SELL" and tp<entry)
valid_tiers = [plan[k] for k in ("tp1","tp2","tp3") if plan.get(k) and direction_ok(plan[k])]
if not valid_tiers: return   # no tier on correct side of entry

best_rr = max(abs(tp - entry) / sl_dist for tp in valid_tiers)
if best_rr < 0.8: return    # minimum R:R = 0.8, best of L1/L2/L3
```
A plan passes if **at least one** of TP1/TP2/TP3 (whichever exist and sit on
the correct side of entry) reaches R:R ≥ **0.8** — not just TP1, since TP1
(the nearest EMA) is intentionally the tightest target.

**Explicit design-decision comment** (quoted in full — important for
porting fidelity):
> *"NOTE: intentionally NOT calling `_passes_quality_filters` here. That gate
> was calibrated for SNR's audit (86% of SNR losses were with-trend), and
> its H1-trend check silently blocks Grade A TDI 123 setups whenever the
> 3-push happens WITH the H1 trend — even though TDI 123's own gating
> (grade thresholds + htf_aligned requirement for Grade B) already handles
> trend context per user rule 'any A or B setup 05:00-20:00 SAST alerts'.
> Its distance gate is also a no-op here: TDI 123 uses current close as
> entry, so distance to entry_price ≈ 0."*

**Gate 7 — high-impact news filter** (default ON, `TDI123_NEWS_FILTER`,
window `TDI123_NEWS_WINDOW_MIN = 60` minutes):
```python
if TDI123_NEWS_FILTER:
    try:
        blocked = forexfactory.currencies_in_window(60, high_only=True)
    except Exception:
        blocked = set()   # FAILS OPEN — a feed error never blocks a trade
    if pair's currencies intersect blocked: return
```
Pair→currency mapping handles plain FX pairs (`"EUR/USD"` → `{EUR,USD}`) and
index/composite symbols via a lookup (`DE30→EUR`, `US30/USTEC/DXY→USD`, etc.).

**Gate 8 — alert throttle:**
```python
rule = f"tdi123_{timeframe.lower()}_{setup.lower()}_{grade}"   # e.g. "tdi123_h1_buy_A"
                                                                  # (grade NOT lower-cased)
if _is_throttled(pair, rule): return
```
Throttle key = `f"{pair}:{rule}"`. `_is_throttled`:
`time.time() - last_sent.get(key, 0) < RATE_LIMIT_SECS` where
`RATE_LIMIT_SECS = 3600` (one alert per pair+timeframe+direction+grade per
hour). State persists to `alerts_state.json` (flat `{key: last_sent_epoch}`
JSON), loaded at import and saved after every send — survives restarts.

After all gates pass: builds and sends the Discord embed (§21); only on
successful send does it mark the throttle and (if enabled) write the trade
journal entry (§22).

---

## 21. Discord alert embed (what to notify on)

Not exact Discord formatting, but the informational content a rebuild should
send:
- Direction (📈 BUY / 📉 SELL), grade badge (⭐ for A only), pair, timeframe.
- Colour: gold for Grade A; green/red (direction) otherwise.
- Grade + `score/15`.
- Setup label: "123 Peak Formation".
- Entry price; Stop Loss price + pip distance; Risk:Reward as `1:{rr1:.1f}`.
- L1 (50 EMA) target + pips — always shown. L2/L3 shown only if present.
- 123 pattern: p1 → p2 → p3 prices + leg-1 range in pips.
- TDI at P3: baseline value, RSI value, extreme flag.
- Divergence: RSI@p1 → RSI@p3, classified regular / equal-level / none.
- `{bias_timeframe} Bias`: direction + aligned/unaligned flag.
- Description: `row["notes"]`.
- Footer: `TDI Cycle 123 {timeframe} · {UTC ts} ({SAST ts} SAST)`.

---

## 22. Trade journal — open → resolve flow

**Schema/writer note:** the journal machinery (`cache.open_trade`,
`cache.close_trade`, `trade_tracker.resolve_open_trades`) is written
generically (parameterized by `setup: str`), but in actual wiring **TDI123 is
the only strategy using it today** — `trade_tracker.resolve_open_trades`'s
default parameter is literally `setup="TDI123"`, and no other strategy
module calls `cache.open_trade`.

**Open** — inside `alert_tdi123_setup`, only after a successful Discord post:
```python
cache.open_trade(pair=pair, direction=setup, entry=entry, sl=sl, tp1=tp1, tp2=tp2,
                  setup="TDI123", signal=rule, signal_score=row["score"],
                  gates_json=json.dumps({grade, score, session, in_active_session,
                                         divergence, location zone, adr_consumed_pct,
                                         tp1_reachable, htf_aligned}),
                  notes=row["notes"][:300])
```
Inserts a row with `ts_open = now (UTC epoch)`, `result` defaulting to
`'open'`. Exceptions are caught/logged, never propagate.

**Poll/resolve** — each scheduler cycle (gated by `TDI123_JOURNAL_ENABLED`),
calls `trade_tracker.resolve_open_trades(setup="TDI123")`:
1. Fetch all rows where `setup == "TDI123"` and `result in (None, "", "open")`.
2. For each, read up to 800 H1 candles for that pair, filtered to
   `ts_utc > ts_open`.
3. Resolve via:
   ```python
   def _resolve_one(direction, entry, sl, tp1, candles):
       buy = direction.upper() == "BUY"
       for c in candles:
           if buy:
               if c.low  <= sl:  return ("loss", sl)   # SL checked FIRST
               if c.high >= tp1: return ("win", tp1)
           else:
               if c.high >= sl:  return ("loss", sl)
               if c.low  <= tp1: return ("win", tp1)
       return None
   ```
   **SL is checked before TP1 on every bar** — if a single candle's range
   spans both levels, it resolves conservatively as a **loss**.
4. If unresolved and `now - ts_open > EXPIRE_HOURS*3600` (`EXPIRE_HOURS = 48`)
   and candles exist → force-close as **"expired"** at the last available
   close price.
5. If still unresolved and not expired → left open (`still_open`).
6. On win/loss/expired: `pl_pips = ((exit - entry) if BUY else (entry - exit)) / pip_size`,
   rounded 1dp, then `cache.close_trade(...)` sets `ts_close`, `exit_price`,
   `result`, `pl_pips`.

Result values: `"win"` (TP1 before SL), `"loss"` (SL first or same-bar tie),
`"expired"` (48h timeout, closed flat), or unresolved/`"open"`.

---

## 23. API endpoints

### `/tdi123` (scanner)
- In-memory cache, **TTL = 180s** (3 min).
- Reads candles directly from local SQLite cache (`cache.read_candles`) — NOT
  `fetcher.get_candles` — for performance (a prior per-symbol `fetcher`
  version spawned ~100 concurrent SQLite connections and caused 85-120s
  timeouts; direct cache reads are single-connection and ~10x faster).
- Fetch limits: H1=3200, H4=200, D1=60, M15=3200.
- Staleness flag: `h1_last is None or now > h1_last + 2*3600 + 60` (>2 H1
  intervals + 60s old).
- Calls `analyze_universe`, attaches `stale_pairs` + `cached_at`, caches, returns.

### `/tdi123/detail?symbol=X&timeframe=H1|M15`
- Uses `fetcher.get_candles` (cache-first, live-fetch-on-stale) — same
  limits as scanner (H1=3200, H4=200, D1=60, M15=3200-if-requested).
- Rebuilds full RSI/fast/slow/baseline series over the **full** entry-bar
  history (not pre-sliced).
- **`TDI123_CHART_DISPLAY_BARS = 400`** (module-level constant in `app.py`,
  not `config.py`) — bars actually drawn on the chart.
- **EMA convergence thresholds — confirmed exact:**
  `ema50` needs `>=50` closes, `ema200` needs `>=300`, `ema800` needs
  `>=2400` (fallback `[None]*len(closes)` below threshold). These are NOT a
  consistent multiplier of period (50=1×, 300=1.5×200, 2400=3×800) — quote
  the literals, don't infer a formula.
- **Display-window widening** (avoids clipping the pattern's own swing
  markers off-chart):
  ```python
  display_n = 400
  if pattern swing indices exist:
      display_n = max(400, len(entry_bars) - min_swing_idx + 5)
  display_n = min(display_n, len(entry_bars))
  offset = len(entry_bars) - display_n
  ```
  If `offset > 0`, pattern/p1/p2/p3 indices are rebased by subtracting
  `offset` so they stay valid against the sliced arrays. All series
  (candles, ema50/200/800, rsi/fast/slow/baseline) are tail-sliced by the
  same `offset`.
- Response: `{symbol, timeframe, stale, row, candles, ema50, ema200, ema800, tdi:{rsi,fast,slow,baseline}}`.

---

## 24. Configuration reference (complete)

All booleans read via `os.environ.get(NAME, "default").lower() in ("1","true","yes")`.

| Constant | Default | Meaning |
|---|---|---|
| `TDI123_ALERTS_ENABLED` | `true` | Master on/off for Discord alerts |
| `TDI123_GRADE_A_ONLY` | `false` | **Defined but unused — dead flag, see §13.2** |
| `TDI123_SESSION_FILTER` | `true` | Gate alerts to UTC [03:00,18:00) |
| `TDI123_ADR_FILTER` | `false` | Gate alerts on TP1-reachable-given-remaining-ADR (off by default — hard-gating it starves the strategy) |
| `TDI123_NEWS_FILTER` | `true` | Suppress alerts within `TDI123_NEWS_WINDOW_MIN` of high-impact news |
| `TDI123_NEWS_WINDOW_MIN` | `60` | Minutes, news suppression window |
| `TDI123_JOURNAL_ENABLED` | `true` | Auto-track fired alerts into the trade journal |
| `TDI123_WATCH_ALERTS_ENABLED` | `false` | (separate watch-alert feature, out of core scope) |

Shared infrastructure:
```python
PRIORITY_PAIRS = [ ... 26 symbols, see §1 ... ]
INTERVAL_SECS  = {"1min":60, "5min":300, "15min":900, "30min":1800, "1h":3600, "4h":14400, "1day":86400}
DEFAULT_INTERVAL = "15min"
DEFAULT_BACKFILL = 3200
```

---

## 25. Complete constants reference table

| Constant | Value | Source |
|---|---|---|
| RSI period | 13 (Wilder) | `btmm_core.py:79`, called w/ 13 at `tdi_cycle_123.py:702` |
| TDI fast SMA | 2 | `tdi_cycle_123.py:703` |
| TDI slow SMA | 7 | `tdi_cycle_123.py:704` |
| Baseline SMA | 34 | `tdi_cycle_123.py:343` |
| Bollinger multiplier | 1.6185 | `btmm_core.py:98-125` |
| `SWING_LEFT` / `SWING_RIGHT` | 2 / 2 | `tdi_cycle_123.py:60-61` |
| `POINT3_OVERSHOOT_PCT` | 0.65 | `tdi_cycle_123.py:70` |
| `POINT3_SHORTFALL_PCT` | 0.25 | `tdi_cycle_123.py:71` |
| `MIN_LEG1_PIPS` | 8.0 | `tdi_cycle_123.py:74` |
| `TDI_OB_ZONE` | 63.0 | `tdi_cycle_123.py:54` |
| `TDI_OB_STRICT` | 68.0 | `tdi_cycle_123.py:55` |
| `TDI_OS_ZONE` | 37.0 | `tdi_cycle_123.py:56` |
| `TDI_OS_STRICT` | 32.0 | `tdi_cycle_123.py:57` |
| `DIVERGENCE_EQUAL_TOLERANCE_PCT` | 0.05 | `tdi_cycle_123.py:81` |
| `DIVERGENCE_MIN_RSI_DELTA` | 2.0 | `tdi_cycle_123.py:84` |
| `DIVERGENCE_RSI_WINDOW` | 2 | `tdi_cycle_123.py:89` |
| `SIGNAL_CROSS_MAX_AGE` | 20 bars | `tdi_cycle_123.py:92` |
| `ATR_PERIOD` | 14 | `tdi_cycle_123.py:100` |
| `SL_STRUCT_ATR_MULT` | 0.5 | `tdi_cycle_123.py:101` |
| `SL_MIN_ATR_MULT` | 1.0 | `tdi_cycle_123.py:102` |
| `TREND_STACK_MIN_SPAN_PCT` | 0.0005 | `tdi_cycle_123.py:107` |
| EMA50 warmup floor | 50 closes | `tdi_cycle_123.py:448` / `app.py:321` |
| EMA200 warmup floor | 300 closes | `tdi_cycle_123.py:448,486` / `app.py:322` |
| EMA800 warmup floor | 2400 closes | `tdi_cycle_123.py:449,491` / `app.py:323` |
| HTF bias EMA period | 50 | `tdi_cycle_123.py:655` |
| Ketchup EMA period | 13 | `tdi_cycle_123.py:753` |
| Freshness threshold | 30% of leg1_range | `tdi_cycle_123.py:799` |
| `FIB_PIVOT_LEVELS` | (0.382,38)(0.618,61)(0.786,78)(1.0,100)(1.382,138) | `tdi_cycle_123.py:524` |
| Location thresholds | 1.30 / 0.90 / 0.55 / 0.30 / -0.30 | `tdi_cycle_123.py:578-589` |
| `SESSION_ACTIVE_START/END` | 3 / 18 (UTC hour) | `tdi_cycle_123.py:605-606` |
| `ADR_PERIOD_DAYS` | 14 | `tdi_cycle_123.py:607` |
| Grade thresholds | A≥11, B≥8, C≥5 (of 15) | `tdi_cycle_123.py:813-820` |
| Alert R:R minimum | 0.8 (best of L1/L2/L3) | `alerts.py:786` |
| Grade-B alert R:R minimum | 1.0 (TP1 only) | `alerts.py:719` (inside `_should_alert_tdi123`) |
| Alert throttle window | 3600s (1hr) | `alerts.py:31` |
| News suppression window | 60 min | `config.py:85` |
| Journal resolution lookback | 800 H1 candles | `trade_tracker.py` |
| Journal expiry | 48 hours | `trade_tracker.py:22` |
| Scanner cache TTL | 180s | `app.py:208` |
| Chart display bars | 400 | `app.py:259` |
| `DEFAULT_BACKFILL` | 3200 | `config.py:47` |

---

*End of specification. All content above was independently extracted by three
parallel research passes over the live source at
`C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\`, then
synthesized and cross-checked for consistency. No detail from those passes
was omitted.*
