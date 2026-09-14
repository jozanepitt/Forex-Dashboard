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
