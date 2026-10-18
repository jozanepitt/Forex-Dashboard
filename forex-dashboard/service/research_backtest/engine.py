"""Bar-by-bar simulator.

A signal is computed on bars that have already closed. The fill is the next
tradable price after ``known_at_ts`` (the signal bar's close), never that
bar's own close. When one M1 bar touches both the stop and the target, the
stop is filled. That is pessimistic and removes a common source of
look-ahead inside a candle.

Prices are Dukascopy BID. Buys pay the full Exness spread on entry; sells
pay it when they buy back. Slippage is adverse on market entries and on
stops, and is not credited on targets.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

import numpy as np

import instruments
from bot.costs import cost_model
from bot.signals import LOOKBACK, Signal, TF_SECONDS, v9_candidate_prefilter

MAX_HOLD_SECS = 10 * 86400
SPLIT_TS = int(datetime(2025, 4, 1, tzinfo=timezone.utc).timestamp())


@dataclass
class Spec:
    name: str
    strategy: str
    timeframe: str
    symbols: str          # "all" or "core"
    extract: Callable[..., Optional[Signal]]
    prefilter: Optional[Callable[[list[dict]], bool]] = None
    entry_mode: str = "close"
    needs_d1: bool = False


def _pnl(side: str, entry_fill: float, exit_fill: float, risk: float, pip: float) -> tuple[float, float]:
    pnl = (exit_fill - entry_fill) if side == "BUY" else (entry_fill - exit_fill)
    return pnl / risk, pnl / pip


def simulate(
    side: str,
    m1: dict[str, np.ndarray],
    known_at: int,
    entry: float,
    sl: float,
    tp: float,
    spread: float,
    slip: float,
    pip: float,
    entry_kind: str,
    valid_until: Optional[int],
    time_stop_ts: Optional[int],
) -> Optional[dict]:
    """Simulate one trade. Returns None when the order never fills or the
    geometry is unusable. ``r_wide`` repeats the same path at twice the spread.
    """
    ts = m1["ts"]
    if len(ts) == 0 or pip <= 0:
        return None
    risk = abs(entry - sl)
    if risk < 2 * pip:
        return None
    i0 = int(np.searchsorted(ts, known_at, side="left"))
    if i0 >= len(ts):
        return None
    if int(ts[i0]) - known_at > 3 * 86400:
        return None

    op, hi, lo, cl = m1["open"], m1["high"], m1["low"], m1["close"]

    def _exit_from(fill_i: int, entry_fill: float, spread_now: float) -> Optional[tuple]:
        hard_dead = int(ts[fill_i]) + MAX_HOLD_SECS
        candidates = [hard_dead, int(ts[-1]) + 60]
        if time_stop_ts is not None:
            candidates.append(int(time_stop_ts))
        deadline = min(candidates)
        i_dead = int(np.searchsorted(ts, deadline, side="left"))
        i_dead = max(i_dead, fill_i + 1)
        i_dead = min(i_dead, len(ts))
        seg_h = hi[fill_i:i_dead]
        seg_l = lo[fill_i:i_dead]
        if side == "BUY":
            sl_ix = np.flatnonzero(seg_l <= sl)
            tp_ix = np.flatnonzero(seg_h >= tp)
        else:
            sl_ix = np.flatnonzero(seg_h >= sl)
            tp_ix = np.flatnonzero(seg_l <= tp)
        i_sl = int(sl_ix[0]) if len(sl_ix) else None
        i_tp = int(tp_ix[0]) if len(tp_ix) else None
        if i_sl is None and i_tp is None:
            if i_dead <= fill_i:
                return None
            j = i_dead - 1
            if time_stop_ts is not None and deadline == int(time_stop_ts):
                outcome = "time"
            elif deadline == hard_dead:
                outcome = "maxhold"
            else:
                outcome = "eod"
            raw_exit = float(cl[j])
            barrier = "market"
        elif i_tp is None or (i_sl is not None and i_sl <= i_tp):
            j = fill_i + i_sl
            outcome = "sl"
            raw_exit = float(sl)
            gap = (float(op[j]) <= sl) if side == "BUY" else (float(op[j]) >= sl)
            if gap:
                raw_exit = float(op[j])
            barrier = "sl"
        else:
            j = fill_i + i_tp
            outcome = "tp"
            raw_exit = float(tp)
            gap = False
            barrier = "tp"
        if barrier == "market":
            exit_fill = (raw_exit - slip) if side == "BUY" else (raw_exit + spread_now + slip)
        elif barrier == "sl":
            if side == "BUY":
                exit_fill = raw_exit - slip
            else:
                exit_fill = raw_exit + spread_now + slip
        else:
            exit_fill = raw_exit if side == "BUY" else raw_exit - spread_now
        return outcome, int(ts[j]), exit_fill, entry_fill

    if entry_kind == "limit":
        limit_end = valid_until if valid_until is not None else known_at + 6 * 3600
        i1 = int(np.searchsorted(ts, limit_end, side="left"))
        if i1 <= i0:
            return None
        if side == "BUY":
            hits = np.flatnonzero(lo[i0:i1] <= entry)
            entry_fill = entry + spread
            entry_fill_wide = entry + spread * 2
        else:
            hits = np.flatnonzero(hi[i0:i1] >= entry)
            entry_fill = entry
            entry_fill_wide = entry
        if len(hits) == 0:
            return None
        fill_i = i0 + int(hits[0])
        if side == "BUY" and (entry_fill <= sl or entry_fill >= tp):
            return None
        if side == "SELL" and (entry_fill >= sl or entry_fill <= tp):
            return None
    else:
        fill_i = i0
        if side == "BUY":
            entry_fill = float(op[fill_i]) + spread + slip
            entry_fill_wide = float(op[fill_i]) + spread * 2 + slip
        else:
            entry_fill = float(op[fill_i]) - slip
            entry_fill_wide = float(op[fill_i]) - slip
        if side == "BUY" and (entry_fill <= sl or entry_fill >= tp):
            return None
        if side == "SELL" and (entry_fill >= sl or entry_fill <= tp):
            return None

    base = _exit_from(fill_i, entry_fill, spread)
    wide = _exit_from(fill_i, entry_fill_wide, spread * 2)
    if base is None or wide is None:
        return None
    outcome, exit_ts, exit_fill, entry_fill = base
    _, _, exit_wide, entry_wide = wide
    r, pips = _pnl(side, entry_fill, exit_fill, risk, pip)
    r_wide, pips_wide = _pnl(side, entry_wide, exit_wide, risk, pip)
    return {
        "outcome": outcome,
        "entry_ts": int(ts[fill_i]),
        "exit_ts": exit_ts,
        "entry_fill": entry_fill,
        "exit_fill": exit_fill,
        "r": r,
        "r_wide": r_wide,
        "pips": pips,
        "pips_wide": pips_wide,
        "risk_pips": risk / pip,
    }


def run_spec(
    symbol: str,
    bars_by_tf: dict[str, list[dict]],
    arrays_by_tf: dict[str, dict[str, np.ndarray]],
    m1: dict[str, np.ndarray],
    spec: Spec,
    audit_prefilter: bool = True,
) -> list[dict]:
    tf = spec.timeframe
    entry_bars = bars_by_tf[tf]
    entry_ts = arrays_by_tf[tf]["ts"]
    n = len(entry_bars)
    lookback, htf_name, htf_lb = LOOKBACK[(spec.strategy, tf)]
    tf_secs = TF_SECONDS[tf]
    if n <= lookback + 2:
        return []

    htf_bars = bars_by_tf.get(htf_name) if htf_name else None
    htf_close = None
    if htf_bars is not None:
        htf_secs = TF_SECONDS[htf_name]
        htf_close = arrays_by_tf[htf_name]["ts"] + htf_secs
    d1_bars = bars_by_tf.get("D1") if spec.needs_d1 else None
    d1_close = None
    if d1_bars is not None:
        d1_close = arrays_by_tf["D1"]["ts"] + TF_SECONDS["D1"]

    model = cost_model(symbol)
    if model is None:
        return []
    spread, slip = model
    pip = instruments.pip_size(symbol, float(entry_bars[lookback]["close"]))

    prefilter = spec.prefilter
    if spec.strategy == "vwap9ema":
        arr = arrays_by_tf[tf]
        prefilter = v9_candidate_prefilter(arr["ts"], arr["high"], arr["low"], arr["close"], arr["volume"])
    if prefilter is not None and audit_prefilter:
        misses = 0
        rng = np.random.default_rng(7)
        # Wide enough that a rare cross is likely to be in the sample. The
        # sample only calls the scanner on bars the filter wants to skip.
        sample_n = 200 if spec.strategy == "vwap9ema" else 40
        sample = rng.choice(np.arange(lookback, n - 1), size=min(sample_n, n - lookback - 1), replace=False)
        for i in sample:
            window = entry_bars[max(0, i - lookback + 1): i + 1]
            if prefilter(window):
                continue
            sig = _call_extract(spec, symbol, window, htf_bars, htf_close, d1_bars, d1_close,
                                tf, int(entry_ts[i]) + tf_secs)
            if sig is not None:
                misses += 1
        if misses:
            print(f"  prefilter dropped {misses} real signals on {symbol} {spec.name}; disabled", flush=True)
            prefilter = None

    trades = []
    latch = None
    block_until = -1
    i = lookback
    while i < n - 1:
        bar_open = int(entry_ts[i])
        if bar_open < block_until:
            i += 1
            continue
        known = bar_open + tf_secs
        window = entry_bars[max(0, i - lookback + 1): i + 1]
        if prefilter is not None and not prefilter(window):
            i += 1
            continue
        sig = _call_extract(spec, symbol, window, htf_bars, htf_close, d1_bars, d1_close, tf, known)
        if sig is None:
            latch = None
            i += 1
            continue
        if sig.dedup_key == latch:
            i += 1
            continue
        latch = sig.dedup_key
        sim = simulate(
            sig.side, m1, sig.known_at_ts, sig.entry, sig.sl, sig.tp,
            spread, slip, pip, sig.entry_kind, sig.valid_until_ts, sig.time_stop_ts,
        )
        if sim is None:
            i += 1
            continue
        sim.update({
            "strategy": spec.name,
            "symbol": symbol,
            "timeframe": tf,
            "grade": sig.grade,
            "side": sig.side,
            "signal_ts": sig.known_at_ts,
            "alert": sig.alert,
            "split": "oos" if sig.known_at_ts >= SPLIT_TS else "is",
        })
        trades.append(sim)
        block_until = sim["exit_ts"]
        i += 1
    return trades


def _call_extract(spec, symbol, window, htf_bars, htf_close, d1_bars, d1_close, tf, known):
    htf = []
    if htf_bars is not None and htf_close is not None:
        _, _, htf_lb = LOOKBACK[(spec.strategy, tf)]
        end = int(np.searchsorted(htf_close, known, side="right"))
        htf = htf_bars[max(0, end - htf_lb): end]
    if spec.strategy == "engulfing":
        return spec.extract(symbol, window, htf, tf, spec.entry_mode)
    if spec.strategy in ("vwap_bounce", "vwap_mr", "vwap9ema", "btmm"):
        return spec.extract(symbol, window, tf)
    if spec.strategy == "id50":
        return spec.extract(symbol, window, htf, tf)
    if spec.strategy == "tdi123":
        d1 = []
        if d1_bars is not None and d1_close is not None:
            end = int(np.searchsorted(d1_close, known, side="right"))
            d1 = d1_bars[max(0, end - 40): end]
        return spec.extract(symbol, window, htf, d1, tf)
    raise KeyError(spec.strategy)


def run_crt(symbol: str, m15_bars: list[dict], m15_ts: np.ndarray,
            partner_bars: Optional[list[dict]], partner_ts: Optional[np.ndarray],
            m1: dict[str, np.ndarray], which: str) -> list[dict]:
    """One decision per New York day, on the information available at the
    end of that strategy's key window. The setup candle is still forming;
    later minutes are used only as the fill path."""
    from bot.signals import NY, extract_crt

    model = cost_model(symbol)
    if model is None or len(m15_ts) < 64:
        return []
    spread, slip = model
    pip = instruments.pip_size(symbol, float(m15_bars[100]["close"]))
    # Unique NY dates covered by the M15 series. Step of 4 bars (~1 hour)
    # cannot skip a calendar day that actually has data.
    days = sorted({datetime.fromtimestamp(int(t), tz=timezone.utc).astimezone(NY).date()
                   for t in m15_ts[::4]})
    hour, minute = (8, 30) if which == "5am" else (4, 0)
    name = "crt_5am" if which == "5am" else "crt_1am"
    trades = []
    latch = None
    block_until = -1
    for day in days:
        now = datetime(day.year, day.month, day.day, hour, minute, tzinfo=NY)
        cutoff = int(now.timestamp())
        if cutoff < block_until:
            continue
        # A bar is closed only when its open is at least one bar before cutoff.
        end = int(np.searchsorted(m15_ts, cutoff - 900 + 1, side="left"))
        if end < 64:
            continue
        window = m15_bars[max(0, end - 800): end]
        partner_window = None
        if partner_bars is not None and partner_ts is not None:
            pend = int(np.searchsorted(partner_ts, cutoff - 900 + 1, side="left"))
            partner_window = partner_bars[max(0, pend - 800): pend]
        sig = extract_crt(symbol, window, partner_window, now, which=which)
        if sig is None:
            latch = None
            continue
        if sig.dedup_key == latch:
            continue
        latch = sig.dedup_key
        sim = simulate(
            sig.side, m1, sig.known_at_ts, sig.entry, sig.sl, sig.tp,
            spread, slip, pip, sig.entry_kind, sig.valid_until_ts, None,
        )
        if sim is None:
            continue
        sim.update({
            "strategy": name,
            "symbol": symbol,
            "timeframe": "M15",
            "grade": sig.grade,
            "side": sig.side,
            "signal_ts": sig.known_at_ts,
            "alert": sig.alert,
            "split": "oos" if sig.known_at_ts >= SPLIT_TS else "is",
        })
        trades.append(sim)
        block_until = sim["exit_ts"]
    return trades
