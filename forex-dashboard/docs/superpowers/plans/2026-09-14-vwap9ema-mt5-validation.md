# VWAP+9EMA MT5 Honest Backtest Validation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the offline data-fetch and backtest-validation tooling for the VWAP+9EMA strategy (a previously-retired "doesn't work" strategy being re-validated with real broker data this time), and run the full 48-combination validation sweep to produce a go/no-go report.

**Architecture:** Port two provided standalone scripts (`fetch_mt5.py`, `backtest_mt5.py`) into `service/vwap9ema_backtest/`, add a new session Volume Profile module with its own hand-verified unit tests, wire a `--variant` flag into the backtest engine for three tested variants (baseline / VP-filtered / VP-filtered+volume-climax), then build and run an orchestration script that sweeps all symbol × session × variant combinations and writes a markdown results report. No changes to the live dashboard, scheduler, or Discord alerts in this plan — that's Phase 4, deferred until real numbers exist.

**Tech Stack:** Python 3.12, `MetaTrader5` package (already installed, connects to the running Exness MT5 terminal), `pandas`/`numpy`/`matplotlib` (already installed), `pytest`.

**Spec:** `docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md`

## Global Constraints

- **Symbols (8):** `USTECm`, `EURUSDm`, `GBPUSDm`, `USDJPYm`, `USDCHFm`, `AUDUSDm`, `USDCADm`, `NZDUSDm`.
- **History:** 2021-01-01 → present (~5 years of M5 bars).
- **Sessions (2, tested separately):** London (server hours 9–17), NY (server hours 15–23).
- **Variants (3, tested independently per symbol/session):** `baseline` (plain VWAP+9EMA), `vp-filtered` (+ session Volume Profile value-area/POC gate), `vp-climax` (+ entry-bar volume-climax requirement on top of `vp-filtered`).
- **OOS acceptance criteria (per symbol/session/variant):** expectancy (R) > 0 **and** profit factor ≥ 1.2 **and** ≥ 150 trades **and** positive in the majority of tested years (full-sample per-year breakdown).
- **No changes** to `app.py`, `scheduler.py`, `alerts.py`, `config.py`, or any live dashboard/Discord code in this plan.
- `service/vwap9ema_backtest/data/` and `service/vwap9ema_backtest/results/` are gitignored (multi-year CSVs, equity-curve PNGs — local artifacts, not committed). `VALIDATION_RESULTS.md` in that same folder **is** committed — it's the deliverable.
- Only the new Volume Profile module gets dedicated `pytest` coverage (hand-computed reconciliation), matching the spec's own testing policy — the backtest engine itself is validated by its own output, same as the existing `vwap_mean_reversion_backtest.py` tool.

---

### Task 1: Scaffolding + port `fetch_mt5.py`

**Files:**
- Create: `service/vwap9ema_backtest/fetch_mt5.py`
- Create: `service/vwap9ema_backtest/data/.gitkeep`
- Create: `service/vwap9ema_backtest/results/.gitkeep`
- Modify: `.gitignore` (repo root: `C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\.gitignore`)

**Interfaces:**
- Produces: a runnable `fetch_mt5.py` CLI (`--list`, `--symbols`, `--from`, `--to`, `--tf`, `--outdir`) that writes `<outdir>/<SYMBOL>_M5.csv` with columns `time,open,high,low,close,tick_volume,spread_points,point`.

- [ ] **Step 1: Create the directory structure**

```bash
mkdir -p "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\vwap9ema_backtest\data"
mkdir -p "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\vwap9ema_backtest\results"
touch "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\vwap9ema_backtest\data\.gitkeep"
touch "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\vwap9ema_backtest\results\.gitkeep"
```

- [ ] **Step 2: Add gitignore entries**

Append to `.gitignore`:

```
# VWAP+9EMA MT5 backtest tooling — local data/results only, not the report
service/vwap9ema_backtest/data/*.csv
service/vwap9ema_backtest/results/*.png
service/vwap9ema_backtest/__pycache__/
```

- [ ] **Step 3: Create `service/vwap9ema_backtest/fetch_mt5.py`**

```python
"""
fetch_mt5.py  —  pull M5 bars (with REAL per-bar spread + tick volume) from MT5.

Ported near-verbatim from the source MT5_Execution_Plan.md's provided script
(C:\\Users\\jzpit\\Downloads\\fetch_mt5.py) into the repo for reproducibility.
Runs standalone — does not touch app.py/scheduler.py/alerts.py.

Requires a running, logged-in MetaTrader 5 terminal (Exness) — the Python
`MetaTrader5` package attaches to it. Confirmed working on this machine.

Examples:
    python fetch_mt5.py --list
    python fetch_mt5.py --symbols USTECm EURUSDm GBPUSDm --from 2021-01-01
"""
import argparse, sys, datetime as dt
import pandas as pd, numpy as np

def connect():
    import MetaTrader5 as mt5
    if not mt5.initialize():
        print("initialize() failed:", mt5.last_error()); sys.exit(1)
    return mt5

def list_symbols(mt5):
    want = ("US100","USTEC","NAS","NDX","EUR","GBP","USD","JPY","CHF","CAD","AUD","NZD")
    for s in mt5.symbols_get():
        if any(w in s.name.upper() for w in want):
            print(f"{s.name:16s} digits={s.digits} point={s.point}")

def detect_server_offset(df):
    """Guess broker server UTC offset from the tick-volume profile.
    The daily volume trough sits ~21:00-22:00 UTC (FX rollover). We map the
    observed trough hour to 22:00 UTC and report the implied offset."""
    vh = df.groupby(df['time'].dt.hour)['tick_volume'].mean()
    trough = int(vh.idxmin())
    offset = (trough - 22) % 24
    if offset > 12: offset -= 24
    return offset, trough

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--symbols", nargs="*", default=[])
    ap.add_argument("--from", dest="dfrom", default="2021-01-01")
    ap.add_argument("--to", dest="dto", default=None)
    ap.add_argument("--tf", default="M5")
    ap.add_argument("--outdir", default="data")
    a = ap.parse_args()

    mt5 = connect()
    print("MT5:", mt5.version(), "| terminal:", mt5.terminal_info().name if mt5.terminal_info() else "?")
    if a.list:
        list_symbols(mt5); mt5.shutdown(); return

    TF = {"M1":mt5.TIMEFRAME_M1,"M5":mt5.TIMEFRAME_M5,"M15":mt5.TIMEFRAME_M15}[a.tf]
    d0 = dt.datetime.fromisoformat(a.dfrom)
    d1 = dt.datetime.fromisoformat(a.dto) if a.dto else dt.datetime.now()
    import os; os.makedirs(a.outdir, exist_ok=True)

    for sym in a.symbols:
        if not mt5.symbol_select(sym, True):
            print(f"!! {sym}: cannot select (check exact name via --list)"); continue
        info = mt5.symbol_info(sym)
        rates = mt5.copy_rates_range(sym, TF, d0, d1)
        if rates is None or len(rates)==0:
            print(f"!! {sym}: no data ({mt5.last_error()})"); continue
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')   # broker SERVER time
        df = df.rename(columns={'spread':'spread_points'})
        df['point'] = info.point
        keep = ['time','open','high','low','close','tick_volume','spread_points','point']
        df = df[keep]
        off, trough = detect_server_offset(df)
        path = f"{a.outdir}/{sym}_{a.tf}.csv"
        df.to_csv(path, index=False)
        print(f"OK {sym}: {len(df):>7} bars {df['time'].min()}..{df['time'].max()} "
              f"| median spread={df['spread_points'].median():.0f}pts point={info.point} "
              f"| server offset≈UTC{off:+d} (vol trough {trough:02d}h)  -> {path}")
    mt5.shutdown()

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Verify it runs against the live MT5 terminal**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\vwap9ema_backtest"
python fetch_mt5.py --list
```

Expected: prints `MT5: (...)` version line, then a list including `USTECm`, `EURUSDm`, `GBPUSDm`, `USDJPYm`, `USDCHFm`, `AUDUSDm`, `USDCADm`, `NZDUSDm`. (Already confirmed working during brainstorming — this step re-confirms after the file move.)

- [ ] **Step 5: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/vwap9ema_backtest/fetch_mt5.py service/vwap9ema_backtest/data/.gitkeep service/vwap9ema_backtest/results/.gitkeep .gitignore
git commit -m "VWAP+9EMA: port fetch_mt5.py data-fetch tool into the repo (Task 1)"
```

---

### Task 2: Session Volume Profile module (TDD, hand-verified)

**Files:**
- Create: `service/vwap9ema_backtest/volume_profile.py`
- Test: `service/test_vwap9ema_volume_profile.py`

**Interfaces:**
- Produces: `compute_session_volume_profile(highs, lows, closes, volumes, n_buckets=24) -> dict` with keys `bucket_edges`, `bucket_volumes`, `bucket_width`, `poc_bucket`, `poc_price`, `vah`, `val`, `lvn_buckets`. `price_passes_vp_filter(price, profile) -> bool`. `price_in_lvn(price, profile) -> bool`.
- Consumed by: Task 3 (`backtest_mt5.py`).

- [ ] **Step 1: Write the failing test**

Create `service/test_vwap9ema_volume_profile.py`:

```python
"""Hand-computed reconciliation test for the session Volume Profile calc.

4 synthetic bars, one per bucket (n_buckets=4), volumes chosen so POC,
value area, and the one LVN bucket are all traceable by hand — see the
worked arithmetic in docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md.

Bars (h, l, c chosen so typical price (h+l+c)/3 lands exactly on 101/103/105/107):
  bucket0 [100,102): typ=101, vol=10
  bucket1 [102,104): typ=103, vol=60   <- POC
  bucket2 [104,106): typ=105, vol=20
  bucket3 [106,108]: typ=107, vol=5

total=95, value-area target=0.70*95=66.5
Expand from POC(60): compare bucket0(10) vs bucket2(20) -> take bucket2 (20).
captured=80 >= 66.5 -> stop. lo_idx=1, hi_idx=2 -> val=102, vah=106.
LVN threshold = 0.10*60=6 -> bucket3(5) qualifies, bucket0(10) does not.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "vwap9ema_backtest"))
from volume_profile import compute_session_volume_profile, price_passes_vp_filter, price_in_lvn  # noqa: E402

HIGHS = [102, 104, 106, 108]
LOWS = [100, 102, 104, 106]
CLOSES = [101, 103, 105, 107]
VOLUMES = [10, 60, 20, 5]


def _profile():
    return compute_session_volume_profile(HIGHS, LOWS, CLOSES, VOLUMES, n_buckets=4)


def test_bucket_edges_and_width():
    p = _profile()
    assert p["bucket_width"] == 2.0
    assert p["bucket_edges"] == [100.0, 102.0, 104.0, 106.0, 108.0]


def test_bucket_volumes_assigned_correctly():
    p = _profile()
    assert p["bucket_volumes"] == [10.0, 60.0, 20.0, 5.0]


def test_poc_is_highest_volume_bucket():
    p = _profile()
    assert p["poc_bucket"] == 1
    assert p["poc_price"] == 103.0


def test_value_area_expands_toward_larger_neighbor():
    p = _profile()
    assert p["val"] == 102.0
    assert p["vah"] == 106.0


def test_lvn_bucket_below_ten_percent_of_poc():
    p = _profile()
    assert p["lvn_buckets"] == [3]


def test_price_passes_vp_filter_inside_value_area():
    p = _profile()
    assert price_passes_vp_filter(104.0, p) is True


def test_price_passes_vp_filter_at_poc():
    p = _profile()
    assert price_passes_vp_filter(103.0, p) is True


def test_price_fails_vp_filter_outside_value_area_and_far_from_poc():
    p = _profile()
    assert price_passes_vp_filter(107.0, p) is False


def test_price_in_lvn_true_for_lvn_bucket():
    p = _profile()
    assert price_in_lvn(107.0, p) is True


def test_price_in_lvn_false_for_non_lvn_bucket():
    p = _profile()
    assert price_in_lvn(101.0, p) is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -m pytest test_vwap9ema_volume_profile.py -v
```

Expected: `ModuleNotFoundError: No module named 'volume_profile'` (10 errors).

- [ ] **Step 3: Implement `service/vwap9ema_backtest/volume_profile.py`**

```python
"""Session-anchored Volume Profile — POC / Value Area / Low-Volume Nodes.

Computed per session-day group of M5 bars (the same bars already filtered
to one session window and one calendar day by backtest_mt5.backtest()),
using each bar's typical price (H+L+C)/3 — the same typical-price
convention the existing session VWAP calc uses — bucketed into N
equal-width price bands across that session's observed high/low range.

See docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md
("Volume Profile confluence layer") for the calculation spec and the
tick-volume-as-proxy caveat this is built under.
"""
from __future__ import annotations

N_BUCKETS = 24
VALUE_AREA_PCT = 0.70
LVN_PCT_OF_POC = 0.10


def compute_session_volume_profile(highs, lows, closes, volumes, n_buckets: int = N_BUCKETS) -> dict:
    """Build a volume profile from one session-day's OHLCV arrays (equal length)."""
    n = len(highs)
    if n == 0:
        raise ValueError("compute_session_volume_profile: empty input")

    range_lo = min(lows)
    range_hi = max(highs)
    if range_hi <= range_lo:
        range_hi = range_lo + 1e-9  # degenerate single-price session guard

    bucket_width = (range_hi - range_lo) / n_buckets
    bucket_edges = [range_lo + i * bucket_width for i in range(n_buckets + 1)]
    bucket_volumes = [0.0] * n_buckets

    for i in range(n):
        typ = (highs[i] + lows[i] + closes[i]) / 3.0
        idx = int((typ - range_lo) / bucket_width)
        idx = max(0, min(n_buckets - 1, idx))
        bucket_volumes[idx] += volumes[i]

    total_volume = sum(bucket_volumes)
    poc_bucket = max(range(n_buckets), key=lambda i: bucket_volumes[i])
    poc_price = (bucket_edges[poc_bucket] + bucket_edges[poc_bucket + 1]) / 2.0

    lo_idx = hi_idx = poc_bucket
    captured = bucket_volumes[poc_bucket]
    target = VALUE_AREA_PCT * total_volume
    while captured < target and (lo_idx > 0 or hi_idx < n_buckets - 1):
        vol_below = bucket_volumes[lo_idx - 1] if lo_idx > 0 else -1.0
        vol_above = bucket_volumes[hi_idx + 1] if hi_idx < n_buckets - 1 else -1.0
        if vol_below >= vol_above:
            lo_idx -= 1
            captured += bucket_volumes[lo_idx]
        else:
            hi_idx += 1
            captured += bucket_volumes[hi_idx]

    val = bucket_edges[lo_idx]
    vah = bucket_edges[hi_idx + 1]

    lvn_threshold = LVN_PCT_OF_POC * bucket_volumes[poc_bucket]
    lvn_buckets = [i for i in range(n_buckets) if bucket_volumes[i] < lvn_threshold]

    return {
        "bucket_edges": bucket_edges,
        "bucket_volumes": bucket_volumes,
        "bucket_width": bucket_width,
        "poc_bucket": poc_bucket,
        "poc_price": poc_price,
        "vah": vah,
        "val": val,
        "lvn_buckets": lvn_buckets,
    }


def _bucket_of(price: float, profile: dict) -> int:
    edges = profile["bucket_edges"]
    n_buckets = len(profile["bucket_volumes"])
    if price <= edges[0]:
        return 0
    if price >= edges[-1]:
        return n_buckets - 1
    return int((price - edges[0]) / profile["bucket_width"])


def price_passes_vp_filter(price: float, profile: dict) -> bool:
    """True if price sits inside the value area, or within one
    bucket-width of POC — the spec's soft-accept zone for a pullback entry."""
    if profile["val"] <= price <= profile["vah"]:
        return True
    return abs(price - profile["poc_price"]) <= profile["bucket_width"]


def price_in_lvn(price: float, profile: dict) -> bool:
    """True if price's bucket is a low-volume node (< 10% of POC's volume)."""
    return _bucket_of(price, profile) in profile["lvn_buckets"]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -m pytest test_vwap9ema_volume_profile.py -v
```

Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/vwap9ema_backtest/volume_profile.py service/test_vwap9ema_volume_profile.py
git commit -m "VWAP+9EMA: add session Volume Profile calc, hand-verified (Task 2)"
```

---

### Task 3: Port `backtest_mt5.py` and wire in the 3 variants

**Files:**
- Create: `service/vwap9ema_backtest/backtest_mt5.py`
- Create: `service/vwap9ema_backtest/_generate_synthetic_csv.py` (smoke-test helper, not production code)

**Interfaces:**
- Consumes: `volume_profile.compute_session_volume_profile`, `volume_profile.price_passes_vp_filter` (Task 2).
- Produces: `backtest(df, session, RR=2.0, buf=0.10, emaLen=9, variant="baseline") -> list[dict]` (each trade dict has keys `t, dir, entry, exit, r, ret, outcome, year`), `stats(trades: list[dict]) -> dict` (keys `n, win, exp_r, pf, net, mdd` or `{n: 0}` if empty), module-level `SESSIONS` dict and `VARIANTS` tuple.
- Consumed by: Task 5 (`run_validation_sweep.py`).

- [ ] **Step 1: Create `service/vwap9ema_backtest/backtest_mt5.py`**

```python
"""
backtest_mt5.py  —  backtest the 9 EMA + VWAP rejection setup on MT5 M5 data,
using the REAL per-bar spread saved by fetch_mt5.py.

Ported from the source MT5_Execution_Plan.md's provided script
(C:\\Users\\jzpit\\Downloads\\backtest_mt5.py), extended with a --variant
flag (baseline / vp-filtered / vp-climax) per
docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md
("Volume Profile confluence layer").

    python backtest_mt5.py --symbol USTECm --session NY --validate
    python backtest_mt5.py --symbol USTECm --session NY --variant vp-climax --validate

CSV schema (from fetch_mt5.py): time,open,high,low,close,tick_volume,spread_points,point
Times are BROKER SERVER time (~UTC+2). Sessions below are in server hours; adjust
--sess-start/--sess-end if your broker offset differs (fetch_mt5 prints it).
"""
import argparse
import sys
from pathlib import Path
import pandas as pd, numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from volume_profile import compute_session_volume_profile, price_passes_vp_filter  # noqa: E402

# session windows in SERVER time (defaults assume Exness ≈ UTC+2)
SESSIONS = {"London": (9, 17), "NY": (15, 23), "Both": (9, 23)}
VARIANTS = ("baseline", "vp-filtered", "vp-climax")

def ema(a, n):
    k = 2/(n+1); o = np.empty_like(a); o[0] = a[0]
    for i in range(1, len(a)): o[i] = a[i]*k + o[i-1]*(1-k)
    return o

def run_day(g, RR, buf, emaLen, variant="baseline"):
    o=g['open'].values;h=g['high'].values;l=g['low'].values;c=g['close'].values
    v=g['tick_volume'].values.astype(float);ts=g['time'].values
    sp=(g['spread_points'].values*g['point'].values)   # per-bar spread in PRICE
    n=len(g)
    if n<12: return []
    typ=(h+l+c)/3; vwap=np.cumsum(typ*v)/np.cumsum(v); e9=ema(c,emaLen)

    profile = None
    if variant in ("vp-filtered", "vp-climax"):
        profile = compute_session_volume_profile(list(h), list(l), list(c), list(v))

    trades=[]; i=3
    while i<n-1:
        up = c[i]>vwap[i] and e9[i]>vwap[i]
        dn = c[i]<vwap[i] and e9[i]<vwap[i]
        sig=0
        if up and (l[i]<=e9[i] or l[i-1]<=e9[i-1]) and c[i]>e9[i] and c[i]>o[i]: sig=1
        elif dn and (h[i]>=e9[i] or h[i-1]>=e9[i-1]) and c[i]<e9[i] and c[i]<o[i]: sig=-1
        if sig==0: i+=1; continue

        entry=o[i+1] + sig*sp[i+1]/2          # pay half the ENTRY bar's spread

        if profile is not None and not price_passes_vp_filter(entry, profile):
            i+=1; continue

        if variant == "vp-climax":
            window_start = max(0, i+1-20)
            avg_vol = v[window_start:i+1].mean()
            if v[i+1] < 1.3 * avg_vol:
                i+=1; continue

        if sig==1:
            sw=min(l[i],l[i-1]); stop=sw-buf*(entry-sw)
        else:
            sw=max(h[i],h[i-1]); stop=sw+buf*(sw-entry)
        risk=abs(entry-stop)
        if risk<=0: i+=1; continue
        tgt=entry+sig*RR*risk
        outcome=None; exitpx=None; j=i+1
        while j<n:
            if sig==1:
                if l[j]<=stop: exitpx=stop-sp[j]/2; outcome='stop'; break
                if h[j]>=tgt:  exitpx=tgt -sp[j]/2; outcome='target'; break
            else:
                if h[j]>=stop: exitpx=stop+sp[j]/2; outcome='stop'; break
                if l[j]<=tgt:  exitpx=tgt +sp[j]/2; outcome='target'; break
            j+=1
        if outcome is None: exitpx=c[n-1]-sig*sp[n-1]/2; outcome='eod'; j=n-1
        r=sig*(exitpx-entry)/risk; ret=sig*(exitpx-entry)/entry
        trades.append(dict(t=ts[i+1],dir=sig,entry=entry,exit=exitpx,r=r,ret=ret,outcome=outcome,
                           year=pd.Timestamp(ts[i+1]).year))
        i=j+1
    return trades

def backtest(df, session, RR=2.0, buf=0.10, emaLen=9, variant="baseline"):
    s,e=SESSIONS[session]
    d=df[(df['time'].dt.hour>=s)&(df['time'].dt.hour<e)].copy()
    d['day']=d['time'].dt.date
    tr=[]
    for _,g in d.groupby('day'): tr+=run_day(g.sort_values('time'),RR,buf,emaLen,variant)
    return tr

def stats(tr):
    if not tr: return dict(n=0)
    r=np.array([t['r'] for t in tr]); ret=np.array([t['ret'] for t in tr])
    gains=r[r>0].sum(); losses=-r[r<0].sum()
    pf=gains/losses if losses>0 else float('inf')
    eq=np.cumprod(1+ret); dd=((eq-np.maximum.accumulate(eq))/np.maximum.accumulate(eq)).min()
    return dict(n=len(r),win=(r>0).mean(),exp_r=r.mean(),pf=pf,net=eq[-1]-1,mdd=dd)

def pr(tag,s):
    if s.get('n',0)==0: print(f"{tag}: no trades"); return
    print(f"{tag}: n={s['n']:4d}  win={s['win']*100:4.1f}%  expR={s['exp_r']:+.3f}  "
          f"PF={s['pf']:.2f}  net={s['net']*100:+6.1f}%  maxDD={s['mdd']*100:5.1f}%")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--symbol",required=True)
    ap.add_argument("--session",default="NY",choices=list(SESSIONS))
    ap.add_argument("--variant",default="baseline",choices=list(VARIANTS))
    ap.add_argument("--rr",type=float,default=2.0)
    ap.add_argument("--ema",type=int,default=9)
    ap.add_argument("--buf",type=float,default=0.10)
    ap.add_argument("--datadir",default="data")
    ap.add_argument("--validate",action="store_true")
    ap.add_argument("--sess-start",type=int,default=None)
    ap.add_argument("--sess-end",type=int,default=None)
    a=ap.parse_args()
    if a.sess_start is not None: SESSIONS[a.session]=(a.sess_start,a.sess_end)

    df=pd.read_csv(f"{a.datadir}/{a.symbol}_M5.csv",parse_dates=['time'])
    print(f"=== {a.symbol}  {df['time'].min()}..{df['time'].max()}  ({len(df)} bars)  variant={a.variant} ===")

    tr=backtest(df,a.session,a.rr,a.buf,a.ema,a.variant)
    pr(f"[{a.session} rr{a.rr} ema{a.ema} {a.variant}] FULL", stats(tr))

    if a.validate:
        cut=df['time'].quantile(0.60)
        ins=df[df['time']<=cut]; oos=df[df['time']>cut]
        pr("  in-sample (first 60%) ", stats(backtest(ins,a.session,a.rr,a.buf,a.ema,a.variant)))
        pr("  OUT-OF-SAMPLE (last 40%)", stats(backtest(oos,a.session,a.rr,a.buf,a.ema,a.variant)))
        print("  -- parameter sensitivity (expR / PF, full sample) --")
        for el in (7,9,13,21):
            row=[]
            for rr in (1.5,2.0,3.0):
                s=stats(backtest(df,a.session,rr,a.buf,el,a.variant))
                row.append(f"rr{rr}: {s.get('exp_r',0):+.3f}/{s.get('pf',0):.2f}")
            print(f"    ema{el:2d}  "+"   ".join(row))
        print("  -- per year (expR / net%) --")
        yrs={}
        for t in tr: yrs.setdefault(t['year'],[]).append(t)
        for y in sorted(yrs):
            s=stats(yrs[y]); print(f"    {y}: n={s['n']:4d} expR={s['exp_r']:+.3f} net={s['net']*100:+.1f}%")
        rets=np.array([t['ret'] for t in tr])
        if len(rets)>20:
            dds=[]
            rng=np.random.default_rng(0)
            for _ in range(2000):
                p=rng.permutation(rets); eq=np.cumprod(1+p)
                dds.append(((eq-np.maximum.accumulate(eq))/np.maximum.accumulate(eq)).min())
            print(f"  -- Monte Carlo maxDD: median={np.median(dds)*100:.1f}%  5th pct(worst)={np.percentile(dds,5)*100:.1f}%")

    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        ret=np.array([t['ret'] for t in tr]); eq=np.cumprod(1+ret)
        plt.figure(figsize=(10,5)); plt.plot([pd.Timestamp(t['t']) for t in tr],eq)
        plt.axhline(1,ls='--',color='gray'); plt.title(f"{a.symbol} {a.session} {a.variant} rr{a.rr} — equity (net of real spread)")
        plt.ylabel("equity (x)"); plt.grid(alpha=.3); plt.tight_layout()
        import os; os.makedirs("results",exist_ok=True)
        plt.savefig(f"results/{a.symbol}_{a.session}_{a.variant}_rr{a.rr}.png",dpi=120)
        print(f"  saved results/{a.symbol}_{a.session}_{a.variant}_rr{a.rr}.png")
    except Exception as ex:
        print("  (chart skipped:",ex,")")

if __name__=="__main__":
    main()
```

- [ ] **Step 2: Create the synthetic-data smoke-test helper `service/vwap9ema_backtest/_generate_synthetic_csv.py`**

```python
"""One-off helper: generate a synthetic M5 CSV to smoke-test backtest_mt5.py's
--variant wiring without needing real MT5 data. Not part of the production
fetch pipeline. Run once, use to sanity-check Task 3, then ignore."""
import numpy as np
import pandas as pd

def build_synthetic_day(date: str, base_price: float = 100.0) -> pd.DataFrame:
    """One NY-session day (15:00-22:55 server time, 5-min bars): a steady
    uptrend, a mid-session pullback to the 9 EMA that rejects (bullish,
    with a volume climax on the rejection bar), and a late low-volume
    chop patch — enough to exercise all three variants differently."""
    times = pd.date_range(f"{date} 15:00", f"{date} 22:55", freq="5min")
    n = len(times)
    trend = np.linspace(0, 8, n)
    price = base_price + trend
    mid = n // 2
    price[mid:mid+3] -= [0.3, 1.2, 0.5]
    close = price
    open_ = np.roll(close, 1); open_[0] = base_price
    high = np.maximum(open_, close) + 0.15
    low = np.minimum(open_, close) - 0.15
    volume = np.full(n, 500.0)
    volume[mid+2] = 900.0
    volume[n-10:n-5] = 50.0
    spread_points = np.full(n, 8.0)
    point = np.full(n, 0.1)
    return pd.DataFrame(dict(time=times, open=open_, high=high, low=low,
                              close=close, tick_volume=volume,
                              spread_points=spread_points, point=point))

if __name__ == "__main__":
    days = [build_synthetic_day(d) for d in ["2026-01-05", "2026-01-06", "2026-01-07"]]
    df = pd.concat(days, ignore_index=True)
    df.to_csv("data/SYNTH_M5.csv", index=False)
    print(f"Wrote data/SYNTH_M5.csv: {len(df)} bars")
```

- [ ] **Step 3: Run the smoke test**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\vwap9ema_backtest"
python _generate_synthetic_csv.py
python backtest_mt5.py --symbol SYNTH --session NY --variant baseline
python backtest_mt5.py --symbol SYNTH --session NY --variant vp-filtered
python backtest_mt5.py --symbol SYNTH --session NY --variant vp-climax
```

Expected: all three commands run without error; the printed `FULL` trade count (`n=`) for `baseline` is **≥** the count for `vp-filtered`, which is **≥** the count for `vp-climax` (each variant only adds restrictions on top of the last, so it can never produce more trades than the previous one). It's fine if all three show `n=0` or `no trades` on this tiny 3-day synthetic sample — the ordering (never increasing) is what this step actually checks, and no traceback occurred.

- [ ] **Step 4: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/vwap9ema_backtest/backtest_mt5.py service/vwap9ema_backtest/_generate_synthetic_csv.py
git commit -m "VWAP+9EMA: port backtest_mt5.py, wire in 3 volume-profile variants (Task 3)"
```

---

### Task 4: Fetch real historical data for the full universe

**Files:**
- No new files — this task runs Task 1's `fetch_mt5.py` for real, populating `service/vwap9ema_backtest/data/` (gitignored).

**Interfaces:**
- Produces: `data/USTECm_M5.csv`, `data/EURUSDm_M5.csv`, `data/GBPUSDm_M5.csv`, `data/USDJPYm_M5.csv`, `data/USDCHFm_M5.csv`, `data/AUDUSDm_M5.csv`, `data/USDCADm_M5.csv`, `data/NZDUSDm_M5.csv` — consumed by Task 6.

- [ ] **Step 1: Run the fetch for all 8 symbols**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\vwap9ema_backtest"
python fetch_mt5.py --symbols USTECm EURUSDm GBPUSDm USDJPYm USDCHFm AUDUSDm USDCADm NZDUSDm --from 2021-01-01
```

This can take several minutes (5 years of M5 bars × 8 symbols).

- [ ] **Step 2: Verify the Phase 1 gate from the spec**

For each of the 8 printed `OK <symbol>: ...` lines, confirm:
- Bar count is in the tens-of-thousands range (5 years of M5 ≈ 300k+ bars if 24h, fewer for FX with weekend gaps — sanity-check it's not near-zero).
- Date range (`min..max`) actually spans close to 2021-01-01 → today.
- `median spread=...pts` is a small positive number (not 0, not absurdly large).
- No `!!` failure lines for any of the 8 symbols. If any symbol fails (can't be selected), stop and report the exact error — do not proceed with a partial universe silently.

- [ ] **Step 3: Spot-check one CSV**

```bash
python -c "import pandas as pd; df = pd.read_csv('data/USTECm_M5.csv'); print(df.shape); print(df.head()); print(df['tick_volume'].describe())"
```

Expected: non-empty dataframe, `tick_volume` values are positive integers, no NaN in `open/high/low/close`.

- [ ] **Step 4: No commit needed**

`data/*.csv` is gitignored per Task 1 — this step produces local artifacts only, nothing to commit.

---

### Task 5: Validation sweep orchestrator (TDD for the pass/fail predicate)

**Files:**
- Create: `service/vwap9ema_backtest/run_validation_sweep.py`
- Test: `service/test_vwap9ema_validation_sweep.py`

**Interfaces:**
- Consumes: `backtest_mt5.backtest`, `backtest_mt5.stats`, `backtest_mt5.VARIANTS`, `backtest_mt5.SESSIONS` (Task 3).
- Produces: `passes_acceptance(oos_stats: dict, per_year_stats: dict) -> bool`, `per_year_breakdown(trades: list[dict]) -> dict[int, dict]`, `run_one(symbol, session, variant) -> dict`, `run_sweep() -> list[dict]`, `render_report(results: list[dict]) -> str`.
- Consumed by: Task 6 (running the real sweep).

- [ ] **Step 1: Write the failing test for the acceptance predicate**

Create `service/test_vwap9ema_validation_sweep.py`:

```python
"""Unit tests for the VWAP+9EMA validation sweep's pass/fail predicate.

Uses synthetic stats dicts — no real market data or MT5 access needed,
since passes_acceptance() is a pure function over already-computed stats.
Acceptance rule per docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md
("Validation gate"): OOS expectancy > 0 AND OOS profit factor >= 1.2 AND
OOS trade count >= 150 AND positive in the majority of tested years.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "vwap9ema_backtest"))
from run_validation_sweep import passes_acceptance  # noqa: E402


def test_passes_when_all_criteria_met():
    oos = dict(n=200, exp_r=0.15, pf=1.5)
    years = {2023: dict(exp_r=0.1), 2024: dict(exp_r=0.2), 2025: dict(exp_r=-0.05)}
    assert passes_acceptance(oos, years) is True


def test_fails_when_too_few_trades():
    oos = dict(n=100, exp_r=0.15, pf=1.5)
    years = {2023: dict(exp_r=0.1), 2024: dict(exp_r=0.2)}
    assert passes_acceptance(oos, years) is False


def test_fails_when_expectancy_not_positive():
    oos = dict(n=200, exp_r=0.0, pf=1.5)
    years = {2023: dict(exp_r=0.1)}
    assert passes_acceptance(oos, years) is False


def test_fails_when_profit_factor_below_threshold():
    oos = dict(n=200, exp_r=0.15, pf=1.1)
    years = {2023: dict(exp_r=0.1)}
    assert passes_acceptance(oos, years) is False


def test_fails_when_majority_of_years_negative():
    oos = dict(n=200, exp_r=0.15, pf=1.5)
    years = {2023: dict(exp_r=-0.1), 2024: dict(exp_r=-0.2), 2025: dict(exp_r=0.3)}
    assert passes_acceptance(oos, years) is False


def test_fails_when_no_year_data():
    oos = dict(n=200, exp_r=0.15, pf=1.5)
    assert passes_acceptance(oos, {}) is False


def test_fails_when_oos_has_zero_trades():
    oos = dict(n=0)
    years = {2023: dict(exp_r=0.1)}
    assert passes_acceptance(oos, years) is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -m pytest test_vwap9ema_validation_sweep.py -v
```

Expected: `ModuleNotFoundError: No module named 'run_validation_sweep'` (7 errors).

- [ ] **Step 3: Implement `service/vwap9ema_backtest/run_validation_sweep.py`**

```python
"""Runs the full VWAP+9EMA validation sweep: every symbol x session x variant
combination in the approved universe, applying the spec's own pass/fail
acceptance criteria, and writes VALIDATION_RESULTS.md.

See docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md
("Validation gate") for the exact criteria this implements.
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from backtest_mt5 import backtest, stats, VARIANTS  # noqa: E402

SYMBOLS = ["USTECm", "EURUSDm", "GBPUSDm", "USDJPYm", "USDCHFm", "AUDUSDm", "USDCADm", "NZDUSDm"]
SESSIONS_TO_TEST = ("London", "NY")
DATADIR = Path(__file__).parent / "data"
MIN_TRADES = 150
MIN_PF = 1.2


def passes_acceptance(oos_stats: dict, per_year_stats: dict) -> bool:
    """The spec's exact go/no-go rule for one symbol/session/variant
    combination's out-of-sample stats and full-sample per-year breakdown."""
    if oos_stats.get("n", 0) < MIN_TRADES:
        return False
    if oos_stats.get("exp_r", 0) <= 0:
        return False
    if oos_stats.get("pf", 0) < MIN_PF:
        return False
    if not per_year_stats:
        return False
    positive_years = sum(1 for s in per_year_stats.values() if s.get("exp_r", 0) > 0)
    return positive_years > len(per_year_stats) / 2


def per_year_breakdown(trades: list[dict]) -> dict:
    years: dict = {}
    for t in trades:
        years.setdefault(t["year"], []).append(t)
    return {y: stats(trs) for y, trs in years.items()}


def run_one(symbol: str, session: str, variant: str) -> dict:
    df = pd.read_csv(DATADIR / f"{symbol}_M5.csv", parse_dates=["time"])
    full_trades = backtest(df, session, variant=variant)
    cut = df["time"].quantile(0.60)
    oos_df = df[df["time"] > cut]
    oos_trades = backtest(oos_df, session, variant=variant)
    oos_stats = stats(oos_trades)
    year_stats = per_year_breakdown(full_trades)
    passed = passes_acceptance(oos_stats, year_stats)
    return dict(symbol=symbol, session=session, variant=variant,
                oos_stats=oos_stats, year_stats=year_stats, passed=passed)


def run_sweep() -> list:
    results = []
    for symbol in SYMBOLS:
        for session in SESSIONS_TO_TEST:
            for variant in VARIANTS:
                results.append(run_one(symbol, session, variant))
    return results


def render_report(results: list) -> str:
    lines = ["# VWAP+9EMA Validation Results", ""]
    passed = [r for r in results if r["passed"]]
    lines.append(f"**{len(passed)} / {len(results)} combinations passed.**")
    lines.append("")
    lines.append("| Symbol | Session | Variant | OOS n | OOS expR | OOS PF | Result |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in results:
        s = r["oos_stats"]
        verdict = "PASS" if r["passed"] else "FAIL"
        lines.append(f"| {r['symbol']} | {r['session']} | {r['variant']} | "
                      f"{s.get('n', 0)} | {s.get('exp_r', 0):+.3f} | {s.get('pf', 0):.2f} | {verdict} |")
    return "\n".join(lines) + "\n"


def main():
    results = run_sweep()
    report = render_report(results)
    out_path = Path(__file__).parent / "VALIDATION_RESULTS.md"
    out_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"Written to {out_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
python -m pytest test_vwap9ema_validation_sweep.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/vwap9ema_backtest/run_validation_sweep.py service/test_vwap9ema_validation_sweep.py
git commit -m "VWAP+9EMA: add validation sweep orchestrator + acceptance predicate tests (Task 5)"
```

---

### Task 6: Run the real 48-combination sweep and report results

**Files:**
- Create (gitignored except the report): `service/vwap9ema_backtest/results/*.png`
- Create (committed): `service/vwap9ema_backtest/VALIDATION_RESULTS.md`

**Interfaces:**
- Consumes: Task 4's fetched CSVs, Task 5's `run_sweep()`/`render_report()`.
- Produces: the final go/no-go report — the deliverable this whole plan exists to produce.

- [ ] **Step 1: Run the full sweep**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service\vwap9ema_backtest"
python run_validation_sweep.py
```

This runs all 48 combinations (8 symbols × 2 sessions × 3 variants) — each does a full-sample + OOS backtest. Expect this to take a few minutes given 5 years of M5 data per symbol.

- [ ] **Step 2: Verify the report was written correctly**

```bash
cat VALIDATION_RESULTS.md
```

Expected: a markdown table with exactly 48 data rows, a `PASS`/`FAIL` verdict in every row, and the summary line (`N / 48 combinations passed`) matching the actual count of `PASS` rows in the table.

- [ ] **Step 3: Commit the report**

```bash
cd "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
git add service/vwap9ema_backtest/VALIDATION_RESULTS.md
git commit -m "VWAP+9EMA: run full 48-combination validation sweep, record results (Task 6)"
```

- [ ] **Step 4: Report the go/no-go result directly to the user**

This is the decision point the spec calls out explicitly — do not silently proceed to any dashboard work. Summarize for the user: how many combinations passed, which symbol/session/variant combinations (if any), and the actual expectancy/PF/trade-count numbers for each pass. If zero combinations passed, say so plainly — per the spec, that's a legitimate, useful result (it would corroborate the 2026-09-11 "doesn't work" verdict with real evidence this time), and Phase 4 does not proceed. If one or more combinations passed, ask the user whether they want a Phase 4 follow-up plan now.

---

## Self-Review

**Spec coverage:**
- Data-fetch tooling (spec "Architecture — Phase 1-3", `fetch_mt5.py`) → Task 1. ✓
- Session Volume Profile calc + hand-computed test (spec "Testing" exception clause) → Task 2. ✓
- Backtest engine port + 3 variants (spec "Volume Profile confluence layer") → Task 3. ✓
- Real historical data pull for the approved 8-symbol universe (spec "Universe & test parameters") → Task 4. ✓
- 48-run sweep with the exact OOS acceptance criteria (spec "Validation gate") → Task 5 (predicate + orchestrator) and Task 6 (actually running it). ✓
- `VALIDATION_RESULTS.md` report artifact (spec "Validation gate", explicit file path) → Task 6. ✓
- Gitignore for `data/`/`results/`, report itself committed (spec "Architecture — Phase 1-3") → Task 1 (gitignore) and Task 6 (commit the report only). ✓
- Phase 4 (dashboard/Discord) — explicitly excluded from this plan per the spec's own "Implementation planning note"; Task 6 Step 4 hands the decision back to the user rather than silently building it. ✓
- Spec's "Open risk" (verify provided scripts' math independently) — addressed by Task 2's hand-computed test for the *new* Volume Profile math (the genuinely new, unverified logic) and Task 3's synthetic-data smoke test for the *ported* VWAP/EMA/entry logic (confirms it runs and the three variants filter monotonically, without claiming full formula reconciliation for code that was a near-verbatim, low-risk port).

**Placeholder scan:** No TBD/TODO. All code blocks are complete, runnable files, not sketches. No task says "similar to Task N" without inlining the actual code.

**Type/interface consistency:** `backtest(df, session, RR, buf, emaLen, variant)` signature is identical between Task 3's definition and Task 5's `run_one()` call site. `stats()` returns `dict(n=...)` alone for the empty case in both the original script (Task 3) and what Task 5's `passes_acceptance()` expects (`oos_stats.get("n", 0)`, `oos_stats.get("exp_r", 0)`, `.get("pf", 0)` — all use `.get` with defaults, so the `n`-only empty dict from `stats([])` doesn't crash it, and correctly fails the trade-count check). `VARIANTS` tuple defined once in `backtest_mt5.py`, imported (not redefined) in Task 5. Volume profile's `profile` dict keys (`val`, `vah`, `poc_price`, `bucket_width`, `lvn_buckets`) are used identically in Task 2's implementation and Task 3's `run_day()` consumption.

**Gap check:** none found — proceeding to save.
