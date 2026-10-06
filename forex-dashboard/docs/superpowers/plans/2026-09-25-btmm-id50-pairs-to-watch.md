# BTMM ID50 "Pairs to Watch" Panel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **NOTE — git commits:** this workspace's rules (AGENTS.md) forbid committing unless the user explicitly asks. The "Commit" steps this skill template normally prescribes are therefore **omitted**; tasks end with a verification step instead.

**Goal:** Surface pairs at the earliest ID50 formation stage — a fresh 13/50 EMA cross (≤ 20 M15 bars) with a confirmed pre-impulse anchor — in a "Pairs to Watch" panel on the ID50 dashboard tab, without disturbing the audited signal pipeline.

**Architecture:** Add a pure `_watch_row(symbol, m15_bars, live_row)` helper to `btmm_id50.py` that replays the existing tested H2/H3 detectors (`_cross_state`, `_anchor_fractal`) and is wired into `analyze_universe()` so the existing `/id50` response gains `watches` + `watch_count`. No backend endpoint change. The frontend renders a new panel from the same cached payload.

**Tech Stack:** Python 3 (Flask backend), pytest, vanilla HTML/CSS/JS single-page dashboard (Flask serves `index.html`).

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `service/btmm_id50.py` | Watch-row helper + universe wiring | Modify |
| `service/test_btmm_id50.py` | TDD tests for `_watch_row` + universe | Modify |
| `index.html` | ID50 "Pairs to Watch" panel + `renderId50Watch()` | Modify |
| `service/app.py` | No change (pass-through of new result keys) | — |

---

### Task 1: `_watch_row` helper with tests (TDD)

**Files:**
- Modify: `service/test_btmm_id50.py` (append watch tests after the existing detectors section)
- Modify: `service/btmm_id50.py` (add `_watch_row` after `_anchor_fractal`, near line 126)

- [ ] **Step 1: Write the failing tests**

Append to `service/test_btmm_id50.py`:

```python
# ──────────────────────────────────────────────────────────────────────
# _watch_row — precursor "Pairs to Watch" (fresh 13/50 cross + anchor)
# ──────────────────────────────────────────────────────────────────────

def _watch_bars():
    """M15 bars: 60 flat then a steady rally -> fresh bullish e13/e50 cross
    near the end (bars_ago <= 20), with a pre-cross swing low at idx 5 that
    anchors the setup (per test_anchor_fractal_picks_recent_swing_low_before_cross)."""
    closes = [1.1000] * 60 + [1.1000 + 0.0006 * i for i in range(1, 13)]
    bars = [{"ts_utc": i * 900, "open": c, "high": c + 0.0001,
             "low": c - 0.0001, "close": c} for i, c in enumerate(closes)]
    bars[5] = {"ts_utc": 5 * 900, "open": 1.1000, "high": 1.1001,
               "low": 1.0998, "close": 1.0999}
    return bars


def test_watch_row_present_for_fresh_cross_plus_anchor():
    row = m._watch_row("EUR/USD", _watch_bars(), {"setup": "NO-TRADE"})
    assert row is not None
    assert row["symbol"] == "EUR/USD"
    assert row["direction"] == "bullish"
    assert row["cross_age"] <= m.CROSS_AGE_ACCEPT_MAX
    assert row["anchor"] == 1.0998
    assert row["price"] > 1.10


def test_watch_row_excludes_stale_cross():
    bars = _watch_bars()
    last = bars[-1]["close"]
    for i in range(1, 41):  # 40 slow continuation bars -> cross ages well past 20 bars
        c = last + 0.0001 * i
        bars.append({"ts_utc": len(bars) * 900, "open": c - 0.0001,
                     "high": c + 0.0001, "low": c - 0.0001, "close": c})
    closes = [b["close"] for b in bars]
    cs = m._cross_state(closes)
    assert cs["direction"] == "bullish"               # same (last) cross, just older
    assert cs["bars_ago"] > m.CROSS_AGE_ACCEPT_MAX    # fixture is genuinely stale
    assert m._watch_row("EUR/USD", bars, {"setup": "NO-TRADE"}) is None


def test_watch_row_excludes_missing_anchor():
    closes = [1.1000] * 60 + [1.1000 + 0.0006 * i for i in range(1, 13)]
    bars = [{"ts_utc": i * 900, "open": c, "high": c + 0.0001,
             "low": c - 0.0001, "close": c} for i, c in enumerate(closes)]
    assert m._cross_state(closes)["idx"] is not None    # cross present, no dip bar
    assert m._watch_row("EUR/USD", bars, {"setup": "NO-TRADE"}) is None


def test_watch_row_dedupes_same_direction_live_signal():
    bars = _watch_bars()
    assert m._watch_row("EUR/USD", bars, {"setup": "BUY"}) is None      # bullish cross + BUY
    row = m._watch_row("EUR/USD", bars, {"setup": "SELL"})              # per-direction dedupe
    assert row is not None
    assert row["direction"] == "bullish"


def test_watch_row_none_on_short_data():
    assert m._watch_row("EUR/USD", _watch_bars()[:40], {"setup": "NO-TRADE"}) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run (from `forex-dashboard/service`): `python -m pytest -q test_btmm_id50.py::test_watch_row_present_for_fresh_cross_plus_anchor`
Expected: `FAILED ... AttributeError: module 'btmm_id50' has no attribute '_watch_row'`

- [ ] **Step 3: Implement `_watch_row`**

Add to `service/btmm_id50.py`, immediately after `_anchor_fractal` (line 126):

```python
def _watch_row(symbol: str, m15_bars: list[dict],
               live_row: Optional[dict] = None) -> Optional[dict]:
    """Precursor ID50 watch candidate — W1-W4 (see the 2026-09-25 design):
    W1 fresh 13/50 cross exists, W2 confirmed pre-impulse anchor, W3 cross
    not older than CROSS_AGE_ACCEPT_MAX bars, W4 no same-direction live
    signal already fired. Replays only the tested H2/H3 detectors; never
    alters the audited signal pipeline. Returns None to skip."""
    if not m15_bars or len(m15_bars) < 60:
        return None
    closes = [b["close"] for b in m15_bars]
    cross = _cross_state(closes)
    if cross["idx"] is None or cross["direction"] is None:
        return None
    if cross["bars_ago"] > CROSS_AGE_ACCEPT_MAX:
        return None
    anchor = _anchor_fractal(m15_bars, cross["idx"], cross["direction"])
    if not anchor:
        return None
    setup = (live_row or {}).get("setup")
    if (cross["direction"] == "bullish" and setup == "BUY") or \
       (cross["direction"] == "bearish" and setup == "SELL"):
        return None
    return {
        "symbol": symbol,
        "direction": cross["direction"],
        "cross_age": cross["bars_ago"],
        "anchor": anchor["anchor"],
        "price": closes[-1],
    }
```

- [ ] **Step 4: Run the watch tests to verify they pass**

Run: `python -m pytest -q test_btmm_id50.py -k watch_row`
Expected: `5 passed`

- [ ] **Step 5: Run the full ID50 test file to confirm no regression**

Run: `python -m pytest -q test_btmm_id50.py`
Expected: all pass (existing 38 + 5 new = 43)

---

### Task 2: Wire `watches` into `analyze_universe`

**Files:**
- Modify: `service/btmm_id50.py` (`analyze_universe`, lines 593-623)

- [ ] **Step 1: Write the failing test** (append to `service/test_btmm_id50.py`)

```python
def test_analyze_universe_watches():
    a_bars = _watch_bars()                      # fresh bullish cross + anchor
    b_bars = _watch_bars()                      # same series, cross made 8 bars older
    last = b_bars[-1]["close"]
    for i in range(1, 9):
        c = last + 0.0001 * i
        b_bars.append({"ts_utc": len(b_bars) * 900, "open": c - 0.0001,
                       "high": c + 0.0001, "low": c - 0.0001, "close": c})
    result = m.analyze_universe({"GBP/USD": {"m15": b_bars},
                                 "EUR/USD": {"m15": a_bars}})
    syms = [w["symbol"] for w in result["watches"]]
    assert result["watch_count"] == 2
    assert set(syms) == {"EUR/USD", "GBP/USD"}
    ages = [w["cross_age"] for w in result["watches"]]
    assert ages == sorted(ages)                 # freshest cross first
    assert result["watches"][0]["symbol"] == "EUR/USD"
```

Note: `analyze_universe` iterates the configured `BTMM_ID50_UNIVERSE` (27 pairs), not the bundle keys; pairs absent from the bundle get empty candles, NO-TRADE, and no watch row. The fixture above only needs the two bundled symbols to appear in `watches`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest -q test_btmm_id50.py::test_analyze_universe_watches`
Expected: `FAILED ... KeyError: 'watches'`

- [ ] **Step 3: Implement the wiring**

In `service/btmm_id50.py` `analyze_universe`:

Insert `watch_rows: list[dict] = []` just before the `for sym in BTMM_ID50_UNIVERSE:` loop, capture the row per pair, and append the watch row inside the existing `try` block after `pairs_out.append(row)`:

```python
    watch_rows: list[dict] = []
    for sym in BTMM_ID50_UNIVERSE:
        bundles = candles_by_pair.get(sym, {}) or {}
        m15 = bundles.get("m15") or bundles.get("M15") or []
        h1 = bundles.get("1h") or bundles.get("h1") or []
        try:
            row = analyze_pair(sym, m15, h1 or None)
        except Exception as e:  # noqa: BLE001
            log.exception("btmm_id50 failed for %s: %s", sym, e)
            row = {"symbol": sym, "setup": "NO-TRADE", "grade": "NO-DATA",
                   "reason": f"error: {e}", "score": 0}
        pairs_out.append(row)
        watch = _watch_row(sym, m15, row)
        if watch:
            watch_rows.append(watch)
```

Add the two result keys (freshest-cross-first) to the returned dict:

```python
        "pairs": pairs_out,
        "watches": sorted(watch_rows, key=lambda w: w["cross_age"]),
        "watch_count": len(watch_rows),
```

- [ ] **Step 4: Run the new test to verify it passes**

Run: `python -m pytest -q test_btmm_id50.py::test_analyze_universe_watches`
Expected: `1 passed`

- [ ] **Step 5: Run the full ID50 test file + compile check**

Run: `python -m pytest -q test_btmm_id50.py; python -m py_compile btmm_id50.py`
Expected: all 44 pass; `py_compile` silent.

---

### Task 3: "Pairs to Watch" panel in `index.html`

**Files:**
- Modify: `index.html` (panel markup after line 905; `renderId50Watch` after `renderId50Table`)

- [ ] **Step 1: Add the panel markup**

Insert between the `id50Summary` div (line 905) and the signals `<table>` (line 906):

```html
    <div style="margin:14px 0 6px;padding:10px 12px;background:var(--bg-elevated);border:1px solid var(--border);border-radius:6px">
      <div style="display:flex;justify-content:space-between;align-items:baseline;gap:8px;flex-wrap:wrap;margin-bottom:4px">
        <div style="font-weight:600;font-size:.9rem">👀 Pairs to Watch <span id="id50WatchCount" class="mono" style="color:var(--green)">0</span></div>
        <div style="font-size:.72rem;color:var(--text-secondary)">fresh 13/50 cross (≤ 5h) + confirmed anchor — setup still forming</div>
      </div>
      <table class="trend-table snr-table">
        <thead>
          <tr><th>Pair</th><th>Direction</th><th>Cross age</th><th>Anchor</th><th>Price</th></tr>
        </thead>
        <tbody id="id50WatchBody"><tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:14px">—</td></tr></tbody>
      </table>
    </div>
```

- [ ] **Step 2: Call the renderer from `loadId50`**

At line 6201, change:
```js
    renderId50Table(data);
```
to:
```js
    renderId50Table(data);
    renderId50Watch(data);
```

- [ ] **Step 3: Add the `renderId50Watch` function**

Insert immediately after `renderId50Table`'s closing brace (after line 6314):

```js
function renderId50Watch(data) {
  const body  = document.getElementById('id50WatchBody');
  const count = document.getElementById('id50WatchCount');
  const watches = (data.watches || []).slice().sort((a, b) => a.cross_age - b.cross_age);
  if (count) count.textContent = watches.length;
  if (!watches.length) {
    body.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:14px">No pairs currently at the watch stage.</td></tr>';
    return;
  }
  const pill = (d) => d === 'bullish'
    ? '<span style="color:#fff;background:var(--green);border-radius:3px;padding:1px 7px;font-size:.72rem;font-weight:600">BULLISH</span>'
    : '<span style="color:#fff;background:var(--red);border-radius:3px;padding:1px 7px;font-size:.72rem;font-weight:600">BEARISH</span>';
  body.innerHTML = watches.map(w => `
    <tr title="13/50 ${w.direction} cross ${w.cross_age} bars ago with confirmed pre-impulse anchor">
      <td><b>${w.symbol}</b></td>
      <td>${pill(w.direction)}</td>
      <td class="mono">${w.cross_age} bars</td>
      <td class="mono">${_crtFmtPrice(w.anchor, w.symbol)}</td>
      <td class="mono">${_crtFmtPrice(w.price, w.symbol)}</td>
    </tr>`).join('');
}
```

- [ ] **Step 4: Static sanity check (no JS syntax errors)**

Run (from repo root `C:\Users\jzpit\OneDrive\Documents\OpenCode`):
`node --check` is not applicable (HTML). Instead verify with the browser-console probe script already used before:
`python "C:\Users\jzpit\AppData\Local\Temp\opencode\check_id50.js"` is a Node probe; if unavailable, rely on the live validation in Task 4 (the dashboard only parses JS when the tab is opened — errors surface in `loadId50()`; Task 4 confirms).

---

### Task 4: Live verification and restart

**Files:**
- none (verification only)

- [ ] **Step 1: Full test suite + compile**

Run (from `forex-dashboard/service`):
`python -m pytest -q; python -m py_compile btmm_id50.py app.py`
Expected: all tests pass (222 existing + 6 new = 228); `py_compile` silent.

- [ ] **Step 2: Restart the live dashboard** (established, user-approved flow)

Run:
```powershell
$c = Get-NetTCPConnection -LocalPort 3002 -State Listen -ErrorAction SilentlyContinue
if ($c) { Stop-Process -Id $c[0].OwningProcess -Force; "stopped PID $($c[0].OwningProcess)" }
Start-Sleep -Seconds 2
& wscript.exe "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\StartBTMM.vbs"
Start-Sleep -Seconds 25
$n = Get-NetTCPConnection -LocalPort 3002 -State Listen -ErrorAction SilentlyContinue
if ($n) { "listening, new PID $($n[0].OwningProcess)" }
```
Expected: a **new** PID (fresh process) listening on 3002.

- [ ] **Step 3: Verify `/id50` returns the new keys**

Run:
```powershell
$r = Invoke-RestMethod -Uri "http://127.0.0.1:3002/id50"
"watch_count={0}" -f $r.watch_count
"watch_sample={0}" -f ($r.watches | ConvertTo-Json -Compress)
```
Expected:
- `watch_count` is an int (≥ 0).
- `watches` is an array; each element has `symbol`, `direction` ∈ {`bullish`,`bearish`}, `cross_age` int, `anchor` float, `price` float. (Crosses only exist intraday; if 0 now, the shape is still validated — the panel shows the empty state.)

- [ ] **Step 4: Browser check**

Open `http://127.0.0.1:3002/`, switch to the **ID50 M15** tab, click **Refresh Scanner**. Expected:
- "Pairs to Watch" panel above the signals table.
- Count tile matches `watches` length; rows show Pill / Cross age / Anchor / Price.
- Empty state text when `watch_count` is 0.
- No console errors from `loadId50`/`renderId50Watch`.

**Definition of done:** steps above green — 228 tests pass, dashboard restarted with the new PID, `/id50` returns `watches`/`watch_count`, and the panel renders.

---

## Self-Review (run before finishing)

**Spec coverage:** W1/W2/W3/W4 → Task 1 `_watch_row` (each criterion has a dedicated test); watch-row fields → Task 1 return dict; universe wiring + ordering + counts → Task 2; panel markup/columns/empty state/refresh wiring → Task 3; DoD + error handling (empty data → None; empty universe → `[]`) → Tasks 1 & 2; rollback → module + panel are reversible deletions; no commits → noted in header.

**Placeholder scan:** no TBD/TODO; every code step contains real code; commands have expected output.

**Type consistency:** field names (`symbol`, `direction`, `cross_age`, `anchor`, `price`) and constants (`CROSS_AGE_ACCEPT_MAX`, `_watch_row` signature) are identical across the module, tests, and frontend. Frontend reads `data.watches`/`data.watch_count`, matching the `analyze_universe` return keys.