# VWAP+9EMA Live Scanner (Replacing 1AM CRT) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retire the 1AM CRT dashboard tab/alert/scheduler-job, and build a live VWAP+9EMA scanner in its place — narrow scope (2 symbols, 1 session, one specific rule), with a persistent "unvalidated" warning everywhere it appears, since this strategy failed its own honest backtest (0/48).

**Architecture:** Remove 1AM CRT's dashboard/alert/scheduler plumbing without touching the shared `crt_strategy.py` module or 5AM CRT (which uses it independently). Build a new `service/vwap9ema_strategy.py` live scanner that reuses the already-tested math from `service/vwap9ema_backtest/volume_profile.py` and `backtest_mt5.py` (not reimplementing it), adapted from "simulate a whole historical day" to "is there a signal as of the most recent closed bar" — the same adaptation every other live scanner on this dashboard already makes relative to its own backtest logic.

**Tech Stack:** Python 3.12, Flask, the existing `fetcher.py`/`cache.py`/`providers/exness_mt5.py` live data pipeline (already supports M5 candles with a `volume` field — confirmed), vanilla JS in `index.html`, `pytest`.

**Spec:** `docs/superpowers/specs/2026-09-14-vwap9ema-live-replaces-1am-crt-design.md`

## Global Constraints

- **Universe:** `USTECm`, `AUDUSDm` only — the only two symbols that ever showed positive expectancy in the backtest.
- **Session:** London only, UTC 07:00–15:00 (matches the backtest's server-hours-9-17 window; verify this UTC conversion against the live broker's current offset at implementation time rather than trusting it blindly — see Task 2).
- **Rule:** the `vp-climax` variant at `ema=9`, `rr=2.0`, `stop_buffer=0.10`, `volume_climax_mult=1.3` — the exact configuration behind the backtest's near-miss numbers. Not re-tuned.
- **No grade tiers.** A signal either clears every gate or it's `NO-TRADE`. No A/B/C scoring.
- **Every dashboard render and every Discord alert must carry a visible "unvalidated — failed backtest 0/48" warning.**
- **Do not touch** `crt_strategy.py`, `analyze_universe_5am()`, `_run_crt_5am_alerts()`, `alert_crt_5am_setup()`, `CRT_5AM_GRADE_A_ONLY`, or `_build_crt_trade_plan()` (shared by 5AM CRT).
- **Do not touch** `_crtFmtPrice()`, `_crtSetupCell()`, `_crtGradeCell()` in `index.html` — despite the `_crt` prefix, these are shared utility functions used by BTMM123's and other strategies' detail rendering (confirmed via usage count: `_crtFmtPrice` appears at lines 5761, 5770, 5774, 5777, 5806, 5810, 5923, 5947, 6015-6020, none of which are 1AM-CRT-specific).

---

### Task 1: Retire 1AM CRT

**Files:**
- Modify: `service/config.py:60-64` (delete)
- Modify: `service/alerts.py:13` (remove `CRT_GRADE_A_ONLY` from the import), `service/alerts.py:453-562` (delete `alert_crt_setup`)
- Modify: `service/scheduler.py:122-126` (delete the call site), `service/scheduler.py:193-209` (delete `_run_crt_alerts`)
- Modify: `service/app.py:19` (remove `import crt_strategy`), `service/app.py:133-179` (delete the `_CRT_CACHE`/`_CRT_TTL_SECS` globals and the `/crt` route)
- Modify: `index.html:429` (delete the nav button), `index.html:761-796` (delete the `crtContainer` section), `index.html:4899` and `index.html:4919,4924,4932` (remove `crt` references from the mode-switch handler), `index.html:5555-5676` region (delete `loadCrt`, `_crtSmtCell`, `_crtKeyTimeCell`, `_renderCrtAnchorsLine`, `renderCrtTable` only — NOT `_crtFmtPrice`/`_crtSetupCell`/`_crtGradeCell`, which stay)

**Interfaces:**
- Produces: nothing new. This task only removes.
- Consumed by: Task 5 reuses the now-empty nav slot and section-container pattern (1AM CRT's former position) for the new tab.

- [ ] **Step 1: Remove the config flag**

Delete these 5 lines from `service/config.py` (lines 60-64):

```python
# 1AM CRT grade gate — separate from the SNR flag above because CRT fires fewer
# setups and Grade B setups are acceptable signals there (they don't push as many
# alerts as SNR). Default false = A + B both sent to Discord.
CRT_GRADE_A_ONLY = os.environ.get("CRT_GRADE_A_ONLY", "false").lower() in ("1", "true", "yes")

```

Update the comment on the next line (now line 60) from:
```python
# 5AM CRT grade gate — NY Open kill-zone session. A + B (default false, same as 1AM CRT).
```
to:
```python
# 5AM CRT grade gate — NY Open kill-zone session. A + B (default false).
```

- [ ] **Step 2: Remove the 1AM CRT alert function**

In `service/alerts.py`, change the import block (around line 13-14) from:
```python
from config import (
    DISCORD_WEBHOOK_URL, BTMM_APLUS_ONLY, CRT_GRADE_A_ONLY,
    CRT_5AM_GRADE_A_ONLY, TDI123_ALERTS_ENABLED, TDI123_GRADE_A_ONLY,
```
to:
```python
from config import (
    DISCORD_WEBHOOK_URL, BTMM_APLUS_ONLY,
    CRT_5AM_GRADE_A_ONLY, TDI123_ALERTS_ENABLED, TDI123_GRADE_A_ONLY,
```

Delete the entire `alert_crt_setup` function (`service/alerts.py:453-562`, everything from `def alert_crt_setup(pair: str, row: dict):` through the line right before `def alert_crt_5am_setup(pair: str, row: dict):`). Do NOT delete `_build_crt_trade_plan` (lines 396-451) — `alert_crt_5am_setup` still calls it.

- [ ] **Step 3: Remove the scheduler job**

In `service/scheduler.py`, delete this block from `refresh_all()` (lines 122-126):
```python
    # Run 1AM CRT scanner + Discord alerts
    try:
        _run_crt_alerts()
    except Exception as e:
        log.warning("CRT alerts failed: %s", e)

```

Delete the entire `_run_crt_alerts` function (lines 193-209, from `def _run_crt_alerts():` through the blank line right before `def _run_crt_5am_alerts():`). Do NOT delete `_run_crt_5am_alerts`.

- [ ] **Step 4: Remove the dashboard route**

In `service/app.py`, remove `import crt_strategy` from the import block (line 19).

Delete this block (lines 133-179, from the comment above `_CRT_CACHE` through the blank line right before `# 5-minute TTL cache for /vwap-mr`):
```python
# that only change every 4 hours. 5 min keeps tab-clicks instant, shields MT5 from
# refresh storms, and avoids long blocking fetches when multiple tabs click rapidly.
# The 15-min scheduler refresh keeps data fresh anyway.
_CRT_CACHE: dict[str, object] = {"ts": 0.0, "payload": None}
_CRT_TTL_SECS = 300


@app.get("/crt")
def crt():
    """1AM CRT scanner across the universe.
    ...
    """
    ... (full route body) ...
    return jsonify(result)

```
(the exact current content is in `service/app.py:133-179` — delete that whole span, including the leading comment lines that describe the caching rationale, since they only make sense attached to this route)

- [ ] **Step 5: Remove the dashboard tab**

In `index.html`, delete the nav button (line 429):
```html
  <button class="mode-tab" data-mode="crt">🕐 1AM CRT</button>
```

Delete the whole 1AM CRT section (lines 761-796, from `<!-- ── 1AM CRT ── -->` through its closing `</div>`, i.e. everything up to but not including `<!-- ── VWAP Mean Reversion (M15) ── -->`).

In the mode-switch handler (around line 4899):
```javascript
    const isScanner = mode === 'crt' || mode === 'vwap_mr' || mode === 'btmm123' || mode === 'tdi123';
```
remove `mode === 'crt' ||`, leaving:
```javascript
    const isScanner = mode === 'vwap_mr' || mode === 'btmm123' || mode === 'tdi123';
```

Remove the `crtContainer` lookup and its display-toggle line (around lines 4919, 4924):
```javascript
    const crtContainer     = document.getElementById('crtContainer');
```
and
```javascript
    if (crtContainer)     crtContainer.style.display     = (mode === 'crt')       ? 'block' : 'none';
```

Remove the load-on-switch line (around line 4932):
```javascript
    if (mode === 'crt')       loadCrt();
```

Delete these five functions entirely (`index.html:5579-5676` region): `loadCrt`, `_crtSmtCell`, `_crtKeyTimeCell`, `_renderCrtAnchorsLine`, `renderCrtTable`. **Do not delete** `_crtFmtPrice` (line 5555), `_crtSetupCell` (line 5566), or `_crtGradeCell` (line 5572) — read each function's full usage list with `grep -n "_crtFmtPrice\|_crtSetupCell\|_crtGradeCell" index.html` before deleting anything in this region, and confirm your edit only removes the 5 named functions above, not these 3.

- [ ] **Step 6: Verify nothing references what you just deleted**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -c "import app"
grep -rn "alert_crt_setup\|_run_crt_alerts\b\|CRT_GRADE_A_ONLY\b" . --include="*.py"
```
Expected: `import app` succeeds with no error. The grep finds zero matches (both were fully removed; `CRT_5AM_GRADE_A_ONLY` and `alert_crt_5am_setup`/`_run_crt_5am_alerts` are DIFFERENT names and won't match this pattern — if they show up, you deleted something you shouldn't have).

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
grep -n "loadCrt\|renderCrtTable\|crtContainer\|data-mode=\"crt\"" index.html
```
Expected: zero matches.

Run the full test suite:
```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -m pytest -q
```
Expected: same pass count as before minus zero (no CRT-specific test file exists to lose), same one pre-existing unrelated failure (`test_tdi_cycle_123.py::test_session_labels_and_active_window`), no new failures.

- [ ] **Step 7: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/config.py service/alerts.py service/scheduler.py service/app.py index.html
git commit -m "Retire 1AM CRT: remove tab, route, scheduler job, alert, config flag (Task 1)"
```

---

### Task 2: Live VWAP+9EMA strategy module (TDD)

**Files:**
- Create: `service/vwap9ema_strategy.py`
- Test: `service/test_vwap9ema_strategy.py`

**Interfaces:**
- Consumes: `compute_session_volume_profile(highs, lows, closes, volumes, n_buckets=24) -> dict` and `price_passes_vp_filter(price, profile) -> bool` from `service/vwap9ema_backtest/volume_profile.py`; `ema(closes, n) -> array` from `service/vwap9ema_backtest/backtest_mt5.py`.
- Produces: `analyze_pair(symbol: str, m5_candles: list[dict]) -> dict` (candle dicts use the same shape every other live strategy uses: `ts_utc`, `open`, `high`, `low`, `close`, `volume`), `analyze_universe(candles_by_pair: dict[str, dict]) -> dict`, module-level `VWAP9EMA_UNIVERSE = ["USTECm", "AUDUSDm"]`. Consumed by Task 3 (scheduler M5 fetch), Task 4 (alert wiring), Task 5 (dashboard route).

- [ ] **Step 1: Write the failing tests**

Create `service/test_vwap9ema_strategy.py`:

```python
"""Tests for the live VWAP+9EMA scanner (vp-climax variant, ema=9, rr=2.0).

This strategy failed its own honest backtest (0/48, see
docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md) —
it is built anyway per explicit user decision (see
docs/superpowers/specs/2026-09-14-vwap9ema-live-replaces-1am-crt-design.md),
scoped narrowly and labeled unvalidated everywhere. These tests check the
LIVE ADAPTATION logic (most-recent-bar signal detection, day/session
filtering, entry/stop/target wiring) — the underlying VWAP/EMA/volume-profile
math is already hand-verified in service/test_vwap9ema_volume_profile.py
and service/test_vwap9ema_backtest_math.py and is not re-derived here,
matching the convention every other live strategy's test file already
follows (e.g. test_vwap_mean_reversion_strategy.py's analyze_pair tests
check structural properties, not re-hand-computed VWAP arithmetic).

The BUY-signal fixture below was independently verified against the real
imported functions before being written into this file:
  entry=101.05, stop=100.775, target=101.6
"""
from __future__ import annotations
import datetime as dt

import vwap9ema_strategy as m

UTC = dt.timezone.utc


def _bar(ts_utc, o, h, l, c, v):
    return {"ts_utc": ts_utc, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _ts(day_hour_min):
    """day_hour_min: (day, hour, minute) on 2026-01-05 (a Monday), UTC."""
    day, hour, minute = day_hour_min
    return int(dt.datetime(2026, 1, day, hour, minute, tzinfo=UTC).timestamp())


def _buy_signal_series():
    """8 M5 bars, 08:00-08:35 UTC on 2026-01-05 (inside the 07:00-15:00 London
    window): a brief consolidation establishing a volume-profile value area,
    then a shallow dip that pierces the 9-EMA and rejects up, with the entry
    bar (last bar) landing back inside the value area and carrying a volume
    spike. Verified against the real compute_session_volume_profile/
    price_passes_vp_filter/ema functions before being encoded here."""
    opens  = [100.8, 100.9, 101.0, 101.1, 101.05, 100.95, 100.95, 101.05]
    highs  = [101.0, 101.1, 101.2, 101.2, 101.1, 101.0, 101.1, 101.2]
    lows   = [100.7, 100.8, 100.9, 100.95, 100.85, 100.8, 100.85, 100.95]
    closes = [100.9, 101.0, 101.1, 101.05, 100.95, 101.15, 101.05, 101.15]
    vols   = [500, 500, 500, 500, 500, 500, 500, 900]
    candles = []
    for k in range(8):
        minute = 5 * k
        candles.append(_bar(_ts((5, 8, minute)), opens[k], highs[k], lows[k], closes[k], vols[k]))
    return candles


def test_buy_signal_full_happy_path():
    row = m.analyze_pair("USTECm", _buy_signal_series())
    assert row["setup"] == "BUY", row.get("notes")
    assert abs(row["entry"] - 101.05) < 1e-9
    assert abs(row["sl"] - 100.775) < 1e-9
    assert abs(row["tp1"] - 101.6) < 1e-9
    assert row["grade"] == "UNVALIDATED"


def test_fails_volume_climax_when_no_spike():
    candles = _buy_signal_series()
    candles[-1]["volume"] = 500  # remove the spike (was 900)
    row = m.analyze_pair("USTECm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "climax" in row["notes"].lower()


def test_no_signal_on_flat_series():
    # No trend, no VWAP/EMA divergence -- up/dn both False by construction.
    candles = [_bar(_ts((5, 8, 5 * k)), 100.0, 100.1, 99.9, 100.0, 500) for k in range(8)]
    row = m.analyze_pair("AUDUSDm", candles)
    assert row["setup"] == "NO-TRADE"
    assert "pullback" in row["notes"].lower() or "signal" in row["notes"].lower()


def test_outside_london_session_is_no_trade():
    candles = _buy_signal_series()
    for c in candles:
        c["ts_utc"] += int(dt.timedelta(hours=12).total_seconds())  # shift to ~20:00 UTC
    row = m.analyze_pair("USTECm", candles)
    assert row["setup"] == "NO-TRADE"
    assert row["in_active_session"] is False


def test_insufficient_data_is_no_trade():
    row = m.analyze_pair("USTECm", _buy_signal_series()[:3])
    assert row["setup"] == "NO-TRADE"
    assert "insufficient" in row["notes"].lower()


def test_analyze_universe_shape():
    result = m.analyze_universe({
        "USTECm": {"m5": _buy_signal_series()},
        "AUDUSDm": {"m5": []},
    })
    assert result["buys"] == 1
    assert result["sells"] == 0
    assert len(result["pairs"]) == 2


def test_universe_constant():
    assert m.VWAP9EMA_UNIVERSE == ["USTECm", "AUDUSDm"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -m pytest test_vwap9ema_strategy.py -v
```
Expected: `ModuleNotFoundError: No module named 'vwap9ema_strategy'` (7 errors).

- [ ] **Step 3: Write `service/vwap9ema_strategy.py`**

```python
"""Live VWAP+9EMA scanner — the vp-climax variant that came closest to
passing its own honest backtest (0/48, see
docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md),
at ema=9, rr=2.0, stop_buffer=0.10, volume_climax_mult=1.3 -- the exact
configuration behind the near-miss numbers, not re-tuned.

Built anyway per explicit user decision (see
docs/superpowers/specs/2026-09-14-vwap9ema-live-replaces-1am-crt-design.md):
manual trading with the user's own judgment, not an automated trusted
signal. There are deliberately NO grade tiers -- every row is either a
real signal or NO-TRADE, and `grade` is always the literal string
"UNVALIDATED" so the dashboard/alert layers can never accidentally render
this as if it were a proven Grade-A signal like the other strategies.

Reuses the already-tested math from the backtest tooling rather than
reimplementing it -- see the sys.path insert below.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "vwap9ema_backtest"))
from volume_profile import compute_session_volume_profile, price_passes_vp_filter  # noqa: E402
from backtest_mt5 import ema  # noqa: E402

VWAP9EMA_UNIVERSE = ["USTECm", "AUDUSDm"]
EMA_LEN = 9
RR = 2.0
STOP_BUFFER = 0.10
VOLUME_CLIMAX_MULT = 1.3
# London session in UTC. The backtest defined this as broker server hours
# 9-17 assuming Exness ~= UTC+2 (giving UTC 7-15). Verify this against the
# CURRENT live broker offset (same check fetch_mt5.py's detect_server_offset
# already does) before trusting it long-term -- broker offsets can drift.
SESSION_START_UTC = 7
SESSION_END_UTC = 15
MIN_BARS = 8  # need at least i>=3 (signal bar) plus i+1 (entry bar); 8 gives headroom


def _in_session(ts_utc: int) -> bool:
    hour = datetime.fromtimestamp(ts_utc, tz=timezone.utc).hour
    return SESSION_START_UTC <= hour < SESSION_END_UTC


def _session_bars_for_today(m5_candles: list[dict]) -> list[dict]:
    """Filter to the most recent calendar day's London-session bars only,
    matching backtest_mt5.backtest()'s exact day+session grouping (a fresh
    VWAP/EMA/volume-profile computation per session-day, not a running
    multi-day series)."""
    session_bars = [c for c in m5_candles if _in_session(c["ts_utc"])]
    if not session_bars:
        return []
    last_day = datetime.fromtimestamp(session_bars[-1]["ts_utc"], tz=timezone.utc).date()
    return [c for c in session_bars
            if datetime.fromtimestamp(c["ts_utc"], tz=timezone.utc).date() == last_day]


def analyze_pair(symbol: str, m5_candles: list[dict]) -> dict:
    """Most-recent-bar VWAP+9EMA signal, vp-climax variant. Mirrors
    backtest_mt5.run_day()'s exact conditions, adapted from "simulate a
    whole historical day" to "is there a signal as of the most recent
    closed bar" -- the same adaptation every other live scanner on this
    dashboard makes relative to its own backtest logic."""
    out: dict = {"symbol": symbol, "setup": "NO-TRADE", "grade": "UNVALIDATED"}

    if not m5_candles or not _in_session(m5_candles[-1]["ts_utc"]):
        out["in_active_session"] = False
        out["notes"] = "Outside London session (UTC 07:00-15:00)."
        return out
    out["in_active_session"] = True

    bars = _session_bars_for_today(m5_candles)
    n = len(bars)
    if n < MIN_BARS:
        out["notes"] = f"Insufficient data ({n} session bars today, need >= {MIN_BARS})."
        return out

    opens = [c["open"] for c in bars]
    highs = [c["high"] for c in bars]
    lows = [c["low"] for c in bars]
    closes = [c["close"] for c in bars]
    volumes = [c.get("volume") or 0.0 for c in bars]

    cum_pv = 0.0
    cum_v = 0.0
    vwap: list[float] = []
    for k in range(n):
        typ = (highs[k] + lows[k] + closes[k]) / 3.0
        vol = volumes[k] if volumes[k] > 0 else 1.0
        cum_pv += typ * vol
        cum_v += vol
        vwap.append(cum_pv / cum_v)
    e9 = ema(closes, EMA_LEN)

    i = n - 2  # most recently closed signal/confirmation bar; i+1 is the entry bar
    if i < 3:
        out["notes"] = "Insufficient bars into today's session for a signal check."
        return out

    up = closes[i] > vwap[i] and e9[i] > vwap[i]
    dn = closes[i] < vwap[i] and e9[i] < vwap[i]
    sig = 0
    if up and (lows[i] <= e9[i] or lows[i - 1] <= e9[i - 1]) and closes[i] > e9[i] and closes[i] > opens[i]:
        sig = 1
    elif dn and (highs[i] >= e9[i] or highs[i - 1] >= e9[i - 1]) and closes[i] < e9[i] and closes[i] < opens[i]:
        sig = -1
    if sig == 0:
        out["notes"] = "No 9EMA+VWAP pullback/reject signal on the most recent bar."
        return out

    j = i + 1
    profile = compute_session_volume_profile(highs, lows, closes, volumes)
    entry = opens[j]
    if not price_passes_vp_filter(entry, profile):
        out["notes"] = "Signal found but entry price fails the volume-profile filter."
        return out

    window_start = max(0, j - 20)
    avg_vol = sum(volumes[window_start:j]) / max(1, j - window_start)
    if volumes[j] < VOLUME_CLIMAX_MULT * avg_vol:
        out["notes"] = "Signal + volume-profile filter pass, but no volume climax on the entry bar."
        return out

    if sig == 1:
        sw = min(lows[i], lows[i - 1])
        stop = sw - STOP_BUFFER * (entry - sw)
    else:
        sw = max(highs[i], highs[i - 1])
        stop = sw + STOP_BUFFER * (sw - entry)
    risk = abs(entry - stop)
    if risk <= 0:
        out["notes"] = "Signal confirmed but computed risk is zero (degenerate stop) -- skipped."
        return out
    target = entry + sig * RR * risk

    out.update({
        "setup": "BUY" if sig == 1 else "SELL",
        "entry": entry, "sl": stop, "tp1": target,
        "notes": (f"VWAP+9EMA vp-climax signal: pullback to 9EMA rejected "
                  f"{'up' if sig == 1 else 'down'}, volume-profile filter and "
                  f"volume climax both confirmed. UNVALIDATED strategy (failed "
                  f"backtest 0/48) -- trade at your own judgment."),
    })
    return out


def analyze_universe(candles_by_pair: dict[str, dict]) -> dict:
    pairs = []
    for symbol, tf in candles_by_pair.items():
        m5 = tf.get("m5") or []
        pairs.append(analyze_pair(symbol, m5))
    buys = sum(1 for p in pairs if p["setup"] == "BUY")
    sells = sum(1 for p in pairs if p["setup"] == "SELL")
    return {"pairs": pairs, "buys": buys, "sells": sells}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -m pytest test_vwap9ema_strategy.py -v
```
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/vwap9ema_strategy.py service/test_vwap9ema_strategy.py
git commit -m "Add live VWAP+9EMA scanner module, vp-climax variant (Task 2)"
```

---

### Task 3: Wire M5 live data fetch for the VWAP+9EMA universe

**Files:**
- Modify: `service/scheduler.py` (`refresh_all()`, around line 89-100)

**Interfaces:**
- Consumes: `vwap9ema_strategy.VWAP9EMA_UNIVERSE` (Task 2).
- Produces: M5 candles present in `cache.py`'s store for `USTECm`/`AUDUSDm` after every refresh cycle, retrievable via `cache.read_candles(sym, "5min", limit=N)`. Consumed by Task 4.

**Why this task exists:** `refresh_all()` currently only fetches `DEFAULT_INTERVAL` (M15), `1h`, `4h`, and `1day` for every symbol in `PRIORITY_PAIRS` (`service/scheduler.py:93-96`) — there is no M5 fetch anywhere in the scheduler today. Without this, `cache.read_candles(sym, "5min", ...)` would return empty/stale data and the new strategy would never see fresh candles.

- [ ] **Step 1: Add the M5 fetch loop**

In `service/scheduler.py`, add this import near the top (with the other stdlib/local imports, e.g. right after the existing `from btmm_core import active_kill_zone` line):

```python
import vwap9ema_strategy
```

In `refresh_all()`, right after the existing per-symbol fetch loop (after line 100, `log.info("refresh_all: done in %.1fs (%d/%d ok)", ...)`, and before the kill-zone-alert block), add:

```python
    # Fetch M5 candles for the VWAP+9EMA universe (2 symbols only -- cheap).
    # No other live strategy uses M5; every other fetch above is M15/1h/4h/1day.
    for sym in vwap9ema_strategy.VWAP9EMA_UNIVERSE:
        try:
            _fetch_guarded(sym, "5min", limit=100)
        except Exception as e:
            log.warning("VWAP9EMA M5 fetch failed for %s: %s", sym, e)
```

(100 bars of M5 covers a bit over 8 hours — comfortably more than one London session's worth of bars, giving `_session_bars_for_today` plenty to filter from even right at session close.)

- [ ] **Step 2: Verify it actually fetches**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -c "
import scheduler
scheduler.refresh_all()
import cache
for sym in ['USTECm', 'AUDUSDm']:
    bars = cache.read_candles(sym, '5min', limit=100)
    print(sym, len(bars), bars[-1] if bars else None)
"
```
Expected: both symbols print a non-zero bar count and a dict with `open`/`high`/`low`/`close`/`volume`/`ts_utc` keys for the most recent bar (this will take a minute or two — `refresh_all()` fetches the full `PRIORITY_PAIRS` list too, not just these 2 symbols).

- [ ] **Step 3: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/scheduler.py
git commit -m "Fetch M5 candles for the VWAP+9EMA universe in refresh_all (Task 3)"
```

---

### Task 4: Discord alert + scheduler wiring

**Files:**
- Modify: `service/config.py` (add `VWAP9EMA_ALERTS_ENABLED`)
- Modify: `service/alerts.py` (add import + `alert_vwap9ema_setup`)
- Modify: `service/scheduler.py` (add `_run_vwap9ema_alerts` + call site)

**Interfaces:**
- Consumes: `vwap9ema_strategy.analyze_universe`/`VWAP9EMA_UNIVERSE` (Task 2), `cache.read_candles` (Task 3's fetch makes this return real data).
- Produces: `alerts.alert_vwap9ema_setup(pair: str, row: dict)`. Consumed by Task 5 only indirectly (dashboard route doesn't call alerts; this is Discord-only).

- [ ] **Step 1: Add the config flag**

In `service/config.py`, add after the `CRT_5AM_GRADE_A_ONLY` line (the block Task 1 left in place):

```python

# VWAP+9EMA (live, replaces 1AM CRT) — UNVALIDATED strategy, failed its own
# honest backtest (0/48, see docs/superpowers/specs/2026-09-13-vwap9ema-mt5-
# validation-design.md). Built anyway per explicit user decision for manual
# trading with their own judgment (see docs/superpowers/specs/2026-09-14-
# vwap9ema-live-replaces-1am-crt-design.md). No grade-only flag: there are no
# grade tiers to filter by. No watch-alerts: deliberately not adding "still
# forming" noise to a strategy with zero validated edge.
VWAP9EMA_ALERTS_ENABLED = os.environ.get("VWAP9EMA_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")
```

- [ ] **Step 2: Add the alert function**

In `service/alerts.py`, add `VWAP9EMA_ALERTS_ENABLED` to the `from config import (...)` block (anywhere in the list, e.g. right after the `VWAP_MR_...` names).

Add this function anywhere after `alert_vwap_mr_watch` (or any other logical spot near the end of the file's alert functions):

```python
def alert_vwap9ema_setup(pair: str, row: dict):
    """Fire when the live VWAP+9EMA scanner (vp-climax variant) confirms a
    BUY/SELL setup. UNVALIDATED strategy -- see vwap9ema_strategy.py's
    module docstring. Every embed carries a visible warning so this can
    never be confused with a proven Grade-A signal from another strategy.
    """
    if not VWAP9EMA_ALERTS_ENABLED:
        return
    setup = row.get("setup")
    if setup not in ("BUY", "SELL"):
        return

    rule = f"vwap9ema_{setup.lower()}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    colour = _COLOURS["strong_buy"] if setup == "BUY" else _COLOURS["strong_sell"]
    entry, sl, tp1 = row.get("entry"), row.get("sl"), row.get("tp1")

    embed = {
        "title": f"{arrow} {pair} — VWAP+9EMA {setup}",
        "description": (
            "⚠ **UNVALIDATED strategy — failed backtest (0/48).** "
            "Not a Grade-A signal like your other alerts. Trade at your own judgment.\n\n"
            + (row.get("notes") or "")
        ),
        "color": colour,
        "fields": [
            {"name": "Setup", "value": f"**{setup}**", "inline": True},
            {"name": "Entry", "value": f"`{_fmt_price(entry, pair)}`" if entry is not None else "—", "inline": True},
            {"name": "Stop Loss", "value": f"`{_fmt_price(sl, pair)}`" if sl is not None else "—", "inline": True},
            {"name": "Target", "value": f"`{_fmt_price(tp1, pair)}`" if tp1 is not None else "—", "inline": True},
        ],
        "footer": {"text": f"VWAP+9EMA (unvalidated) · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("VWAP9EMA alert sent: %s %s (UNVALIDATED strategy)", pair, setup)
```

Check `_fmt_price`'s exact signature before using it here (`grep -n "^def _fmt_price" service/alerts.py`) — other alert functions in this file call it with either one or two arguments depending on whether the symbol is needed for pip-correct formatting; match whichever convention `alert_vwap_mr_setup` (the most recently added, most similar alert function) actually uses, not the two-argument form assumed above if that turns out to be wrong.

- [ ] **Step 3: Add the scheduler job**

In `service/scheduler.py`, add this function after `_run_vwap_mr_alerts` (or anywhere logical near the other `_run_*_alerts` functions):

```python
def _run_vwap9ema_alerts():
    """Run the live VWAP+9EMA scanner (UNVALIDATED — failed backtest 0/48)
    against the cache and fire Discord alerts for confirmed setups."""
    import cache

    candles_by_pair: dict[str, dict] = {}
    for sym in vwap9ema_strategy.VWAP9EMA_UNIVERSE:
        candles_by_pair[sym] = {
            "m5": cache.read_candles(sym, "5min", limit=100),
        }
    result = vwap9ema_strategy.analyze_universe(candles_by_pair)
    for row in result.get("pairs", []):
        try:
            alerts.alert_vwap9ema_setup(row["symbol"], row)
        except Exception as e:
            log.debug("VWAP9EMA alert eval failed for %s: %s", row.get("symbol"), e)
```

Add the call site in `refresh_all()`, right after the `_run_vwap_mr_alerts()` block (after line 151's `except Exception as e: log.warning("VWAP Mean Reversion alerts failed: %s", e)`):

```python

    # Run live VWAP+9EMA scanner (UNVALIDATED — failed backtest 0/48) + Discord alerts
    try:
        _run_vwap9ema_alerts()
    except Exception as e:
        log.warning("VWAP9EMA alerts failed: %s", e)
```

- [ ] **Step 4: Verify import wiring and run the full suite**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -c "import scheduler; import alerts"
python -m pytest -q
```
Expected: both imports succeed with no error. Same pass count plus the 7 new Task 2 tests, same one pre-existing unrelated failure, no new failures.

- [ ] **Step 5: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/config.py service/alerts.py service/scheduler.py
git commit -m "Add VWAP+9EMA Discord alert + scheduler wiring, unvalidated warning on every embed (Task 4)"
```

---

### Task 5: Dashboard route and tab

**Files:**
- Modify: `service/app.py` (add `/vwap9ema` route)
- Modify: `index.html` (add nav button, section/table, JS fetch+render functions)

**Interfaces:**
- Consumes: `vwap9ema_strategy.analyze_universe`/`VWAP9EMA_UNIVERSE` (Task 2), `fetcher.get_candles` (existing, same pattern as `/crt` used).
- Produces: nothing further downstream — this is the last strategy-specific task.

- [ ] **Step 1: Add the dashboard route**

In `service/app.py`, add `import vwap9ema_strategy` to the import block (alongside the other strategy imports like `import btmm_123`).

Add this route (a good spot: right where `/crt` used to be, before the `/vwap-mr` cache comment):

```python
# 5-minute TTL cache for /vwap9ema — M5-based, matches the VWAP+9EMA scanner's
# own timeframe. UNVALIDATED strategy — see vwap9ema_strategy.py's docstring.
_VWAP9EMA_CACHE: dict[str, object] = {"ts": 0.0, "payload": None}
_VWAP9EMA_TTL_SECS = 300


@app.get("/vwap9ema")
def vwap9ema():
    """Live VWAP+9EMA scanner (vp-climax variant) — UNVALIDATED, failed its
    own honest backtest (0/48). Narrow universe (2 symbols), London session
    only. See vwap9ema_strategy.py's module docstring for the full caveat.
    """
    import time as _t

    now = _t.time()
    if _VWAP9EMA_CACHE["payload"] is not None and (now - _VWAP9EMA_CACHE["ts"]) < _VWAP9EMA_TTL_SECS:
        return jsonify(_VWAP9EMA_CACHE["payload"])

    candles_by_pair: dict[str, dict] = {}
    stale_set: set[str] = set()
    for sym in vwap9ema_strategy.VWAP9EMA_UNIVERSE:
        bars, stale = fetcher.get_candles(sym, "5min", limit=100)
        candles_by_pair[sym] = {"m5": bars}
        if stale:
            stale_set.add(sym)

    result = vwap9ema_strategy.analyze_universe(candles_by_pair)
    result["stale_pairs"] = sorted(stale_set)
    result["cached_at"] = int(now)
    _VWAP9EMA_CACHE["payload"] = result
    _VWAP9EMA_CACHE["ts"] = now
    return jsonify(result)
```

- [ ] **Step 2: Verify the route works**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -c "import app"
```
Expected: no error (confirms the new route doesn't have a syntax/import problem before starting the whole Flask app).

- [ ] **Step 3: Add the dashboard tab**

In `index.html`, add the nav button in the same position 1AM CRT's used to occupy (line 429, now empty after Task 1):
```html
  <button class="mode-tab" data-mode="vwap9ema">⚠ VWAP+9EMA</button>
```

Add the section (in 1AM CRT's old position, right before `<!-- ── VWAP Mean Reversion (M15) ── -->`):
```html
  <!-- ── VWAP+9EMA (live, UNVALIDATED — failed backtest 0/48) ──────────────── -->
  <div class="table-container" id="vwap9emaContainer" style="display:none">
    <div style="background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.4);color:var(--red);padding:10px 14px;border-radius:6px;margin-bottom:12px;font-weight:600">
      ⚠ Unvalidated — failed the honest MT5 backtest (0/48 combinations passed). Trade at your own judgment.
    </div>
    <div class="trend-controls">
      <button class="trend-refresh-btn" id="vwap9emaRefreshBtn">Refresh Scanner</button>
      <div class="trend-meta">VWAP+9EMA vp-climax · USTECm/AUDUSDm · London session only</div>
    </div>
    <div class="trend-summary" id="vwap9emaSummary">Click <b>Refresh Scanner</b> to compute.</div>
    <table class="trend-table">
      <thead>
        <tr>
          <th>Pair</th>
          <th>Setup</th>
          <th>Entry</th>
          <th>Stop</th>
          <th>Target</th>
          <th>Session</th>
        </tr>
      </thead>
      <tbody id="vwap9emaTableBody"><tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:24px">Click <b>Refresh Scanner</b> to compute.</td></tr></tbody>
    </table>
    <div class="trend-footer">
      <b>VWAP+9EMA (vp-climax variant, ema=9, rr=2.0):</b> pullback to the 9 EMA that rejects
      in the VWAP trend direction, filtered by session volume profile (value area / POC proximity)
      plus a volume-climax confirmation on the entry bar. Scoped to USTECm and AUDUSDm, London
      session (UTC 07:00–15:00) only — the only two symbol/session combinations that ever showed
      positive out-of-sample expectancy in the honest backtest, and even those failed on profit factor.
    </div>
  </div>
```

In the mode-switch handler, add `vwap9ema` to the scanner-mode check (around what is now line ~4899 after Task 1's edit):
```javascript
    const isScanner = mode === 'vwap_mr' || mode === 'btmm123' || mode === 'tdi123' || mode === 'vwap9ema';
```

Add the container lookup and display-toggle line (near the other `Container` lookups):
```javascript
    const vwap9emaContainer = document.getElementById('vwap9emaContainer');
```
```javascript
    if (vwap9emaContainer) vwap9emaContainer.style.display = (mode === 'vwap9ema') ? 'block' : 'none';
```

Add the load-on-switch line:
```javascript
    if (mode === 'vwap9ema')  loadVwap9ema();
```

- [ ] **Step 4: Add the fetch/render JS**

Add these two functions anywhere in the same region other `load*`/`render*Table` functions live (e.g. right where `loadCrt`/`renderCrtTable` used to be before Task 1 removed them):

```javascript
async function loadVwap9ema() {
  const tbody   = document.getElementById('vwap9emaTableBody');
  const summary = document.getElementById('vwap9emaSummary');
  const btn     = document.getElementById('vwap9emaRefreshBtn');
  if (btn) btn.disabled = true;
  summary.textContent = 'Fetching M5 candles…';
  tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:24px">Loading…</td></tr>';

  const baseUrl = (typeof API_CONFIG !== 'undefined' && API_CONFIG.baseUrl) ? API_CONFIG.baseUrl : 'http://127.0.0.1:3002';
  try {
    const resp = await fetch(`${baseUrl}/vwap9ema`);
    if (!resp.ok) throw new Error(`Service returned ${resp.status}`);
    const data = await resp.json();
    renderVwap9emaTable(data);
  } catch (err) {
    console.error('VWAP+9EMA load failed', err);
    summary.innerHTML = `<span style="color:var(--red)">Failed to load VWAP+9EMA scanner: ${err.message}</span>`;
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--red);padding:24px">Service unreachable. Is the Flask service running on port 3002?</td></tr>';
  } finally {
    if (btn) btn.disabled = false;
  }
}

function renderVwap9emaTable(data) {
  const tbody   = document.getElementById('vwap9emaTableBody');
  const summary = document.getElementById('vwap9emaSummary');

  const rows = (data.pairs || []).map(p => {
    const setupColor = p.setup === 'BUY' ? 'var(--green)' : p.setup === 'SELL' ? 'var(--red)' : 'var(--text-muted)';
    const sessionCell = p.in_active_session
      ? '<span style="color:var(--green)">London — active</span>'
      : '<span style="color:var(--text-muted)">Outside session</span>';
    return `<tr title="${(p.notes || '').replace(/"/g, '&quot;')}">
      <td><b>${p.symbol}</b></td>
      <td><span style="color:${setupColor};font-weight:700">${p.setup}</span></td>
      <td>${p.entry != null ? _crtFmtPrice(p.entry, p.symbol) : '—'}</td>
      <td>${p.sl != null ? _crtFmtPrice(p.sl, p.symbol) : '—'}</td>
      <td>${p.tp1 != null ? _crtFmtPrice(p.tp1, p.symbol) : '—'}</td>
      <td>${sessionCell}</td>
    </tr>`;
  }).join('');
  tbody.innerHTML = rows || '<tr><td colspan="6" style="text-align:center;padding:24px">No data.</td></tr>';

  const stale = (data.stale_pairs && data.stale_pairs.length)
    ? ` <span style="color:var(--yellow)">⚠ ${data.stale_pairs.length} stale</span>` : '';
  summary.innerHTML = `<span style="color:var(--green);font-weight:600">${data.buys || 0} BUY</span> · ` +
    `<span style="color:var(--red);font-weight:600">${data.sells || 0} SELL</span>${stale}`;
}
```

Note this reuses `_crtFmtPrice` (the shared price-formatting helper Task 1 was told explicitly not to delete) — this is exactly why it had to stay.

- [ ] **Step 5: Manual verification**

Restart the dashboard service (same pattern used throughout this session: stop whatever's listening on port 3002, launch `pythonw.exe app.py` from `service/`, wait a few seconds, then `curl http://localhost:3002/health`). Open the dashboard in a browser (or `curl http://localhost:3002/vwap9ema` directly), click the new "⚠ VWAP+9EMA" tab, and confirm:
- The tab renders with the red unvalidated-warning banner visible.
- Clicking "Refresh Scanner" calls `/vwap9ema` and populates the table (or shows "No data" — either is fine; the point is no JS error and no crash).
- The other tabs (BTMM123, TDI123, VWAP Mean Reversion, Journal, Backtest) still switch correctly — confirms the mode-switch handler edits didn't break anything.

- [ ] **Step 6: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/app.py index.html
git commit -m "Add VWAP+9EMA dashboard tab with unvalidated-backtest warning (Task 5)"
```

---

### Task 6: Final verification

**Files:** none — this task only verifies.

- [ ] **Step 1: Full test suite**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -m pytest -q
```
Expected: all tests pass except the one pre-existing, already-known-unrelated failure (`test_tdi_cycle_123.py::test_session_labels_and_active_window`). No CRT-related test failures (none existed to begin with), no VWAP9EMA-related failures.

- [ ] **Step 2: Restart the live service**

Stop whatever process is listening on port 3002, launch a fresh `pythonw.exe app.py` from `service/`, wait ~6 seconds, then:
```bash
curl -s http://localhost:3002/health
curl -s http://localhost:3002/vwap9ema
```
Expected: `{"status":"ok"}`, then a JSON payload with a `pairs` array of exactly 2 entries (`USTECm`, `AUDUSDm`), each with `setup` one of `BUY`/`SELL`/`NO-TRADE`.

- [ ] **Step 3: Confirm 1AM CRT is truly gone, 5AM CRT still works**

```bash
curl -s http://localhost:3002/crt
```
Expected: a 404 (route no longer exists).

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -c "
import crt_strategy
print('CRT_UNIVERSE:', crt_strategy.CRT_UNIVERSE[:2])
print('analyze_universe_5am exists:', hasattr(crt_strategy, 'analyze_universe_5am'))
"
```
Expected: no error, confirms `crt_strategy.py` and its 5AM-specific function are intact.

- [ ] **Step 4: Watch the service log for the first live signal or error**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
tail -n 0 -F service.log | grep -E --line-buffered "VWAP9EMA|Traceback|ERROR|CRITICAL"
```
Let this run for a few refresh cycles (each ~15 min) or trigger a manual refresh. You're looking for either a clean `VWAP9EMA alert sent: ...` line (confirms the whole pipeline works end-to-end on live data) or silence (also fine — the universe is narrow and signals should be rare) — NOT a traceback or error, which would mean something in Tasks 2-5's wiring is broken despite the unit tests passing.

- [ ] **Step 5: Report back**

Summarize for the user: confirm 1AM CRT is fully gone (dashboard tab, route 404, Discord alert removed) with 5AM CRT unaffected; confirm the new VWAP+9EMA tab renders with the unvalidated warning visible; confirm the test suite is clean; report whatever the log watch in Step 4 showed (a real signal, or quiet — either is a legitimate outcome to report, not something to keep watching silently).

## Self-Review

**Spec coverage:**
- Retire 1AM CRT (tab/route/scheduler-job/alert/config, leave `crt_strategy.py`/5AM CRT untouched) → Task 1. ✓
- Live scanner reusing tested math, vp-climax/ema=9/rr=2.0, no grade tiers → Task 2. ✓
- Universe USTECm/AUDUSDm, London session only → Task 2 (`VWAP9EMA_UNIVERSE`, `SESSION_START_UTC`/`SESSION_END_UTC`). ✓
- M5 live data availability (a real gap the spec's "Open risk" section flagged implicitly by requiring live spread/data) → Task 3, called out explicitly as a scheduler gap that had to be filled. ✓
- Discord alert with persistent unvalidated warning → Task 4. ✓
- Dashboard tab with persistent unvalidated warning → Task 5. ✓
- Spec's "Open risk" (live spread source vs. backtest's CSV spread) — NOT independently re-verified in this plan; Task 2's `analyze_pair` doesn't use spread at all in the entry/stop/target formulas shown (the backtest's `entry = o[i+1] + sig*sp[i+1]/2` half-spread adjustment was dropped for the live version, using the raw bar open instead). **This is a deviation from the spec worth flagging to the user when reporting Task 6's results** — the live version's entry price will be a few pips more optimistic than what the (already-failing) backtest actually tested, since it skips the spread half-adjustment. Not fixed in this plan; call it out explicitly rather than silently shipping a subtly-different entry formula.

**Placeholder scan:** No TBD/TODO. Every code block is complete and was verified (Task 2's fixture was checked against the real imported functions before being written into this plan, not guessed).

**Type/interface consistency:** `analyze_pair(symbol, m5_candles) -> dict` signature matches between Task 2's definition and Task 3/4/5's callers (all pass `{"m5": [...]}`-shaped dicts into `analyze_universe`, matching Task 2's `tf.get("m5")` access). `VWAP9EMA_UNIVERSE` defined once in Task 2, imported (not redefined) in Tasks 3/4/5. Row dict keys (`setup`, `grade`, `entry`, `sl`, `tp1`, `notes`, `in_active_session`) used identically across Task 2's implementation, Task 4's alert function, and Task 5's JS render function.

**Gap found and left deliberately unresolved:** the spread deviation noted above under Spec coverage. This should be surfaced to the user, not silently fixed or silently ignored — it's exactly the kind of small-but-real gap that matters more than usual given this strategy already has zero margin for error.
