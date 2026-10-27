"""Turn each live scanner's row into one normalized signal.

The backtest and the execution bot both call these functions, so a fill
model cannot drift away from the rules on the dashboard. Scanners are
imported inside the functions to keep ``import bot.risk`` light.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

# (strategy, timeframe) -> (entry lookback bars, higher-tf name or "", htf lookback)
LOOKBACK: dict[tuple[str, str], tuple[int, str, int]] = {
    ("engulfing", "M1"): (80, "M5", 80),
    ("engulfing", "M5"): (80, "M15", 80),
    ("engulfing", "M15"): (80, "H1", 80),
    ("engulfing", "H1"): (120, "H4", 80),
    ("engulfing", "H4"): (80, "D1", 80),
    ("vwap_bounce", "M1"): (1600, "", 0),
    ("vwap_bounce", "M5"): (400, "", 0),
    ("vwap_bounce", "M15"): (400, "", 0),
    ("vwap_mr", "M5"): (400, "", 0),
    ("vwap_mr", "M15"): (220, "", 0),
    ("vwap9ema", "M1"): (1700, "", 0),
    ("vwap9ema", "M5"): (576, "", 0),
    ("id50", "M5"): (320, "M15", 120),
    ("id50", "M15"): (320, "H1", 120),
    ("tdi123", "M5"): (500, "M15", 120),
    ("tdi123", "M15"): (500, "H1", 160),
    ("tdi123", "H1"): (2400, "H4", 200),
    ("btmm", "M15"): (2500, "", 0),
}

TF_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600, "H4": 14400, "D1": 86400}
TF_LABEL = {
    "M1": "M1", "M5": "M5", "M15": "M15", "H1": "H1", "H4": "H4", "D1": "D1",
}
HTF_LABEL = {
    "M1": "M5", "M5": "M15", "M15": "H1", "H1": "H4", "H4": "D1",
}


@dataclass
class Signal:
    strategy: str
    side: str                 # BUY or SELL
    grade: str
    entry: float
    sl: float
    tp: float
    known_at_ts: int          # bar close time; nothing before this is a fill
    dedup_key: str
    alert: bool               # matches the live Discord grade/session gate
    entry_kind: str = "market"
    valid_until_ts: Optional[int] = None
    time_stop_ts: Optional[int] = None
    score: Optional[float] = None


def _ok_geometry(side: str, entry: float, sl: float, tp: float) -> bool:
    if side == "BUY":
        return sl < entry and tp > entry
    if side == "SELL":
        return sl > entry and tp < entry
    return False


def _base(strategy: str, side: str, grade: str, entry, sl, tp, known: int,
          dedup: str, alert: bool, **kwargs) -> Optional[Signal]:
    try:
        entry_f, sl_f, tp_f = float(entry), float(sl), float(tp)
    except (TypeError, ValueError):
        return None
    if not _ok_geometry(side, entry_f, sl_f, tp_f):
        return None
    return Signal(
        strategy=strategy, side=side, grade=str(grade),
        entry=entry_f, sl=sl_f, tp=tp_f, known_at_ts=int(known),
        dedup_key=dedup, alert=bool(alert), **kwargs,
    )


def extract_engulfing(symbol: str, window: list[dict], htf: list[dict],
                      tf: str, entry_mode: str = "close") -> Optional[Signal]:
    from engulfing_strategy import analyze_timeframe

    row = analyze_timeframe(symbol, window, htf, TF_LABEL[tf], htf_label=HTF_LABEL[tf])
    if row.get("setup") not in ("BUY", "SELL"):
        return None
    if entry_mode == "retrace50":
        price = row.get("entry_50")
        kind = "limit"
        valid = window[-1]["ts_utc"] + TF_SECONDS[tf] + 3 * TF_SECONDS[tf]
    else:
        price = row.get("entry")
        kind = "market"
        valid = None
    known = window[-1]["ts_utc"] + TF_SECONDS[tf]
    grade = row.get("grade") or ""
    return _base(
        "engulfing", row["setup"], grade, price, row.get("sl"), row.get("tp1"),
        known, f"eng:{tf}:{entry_mode}:{window[-1]['ts_utc']}",
        alert=grade in ("A", "B"),
        entry_kind=kind, valid_until_ts=valid, score=row.get("score"),
    )


def extract_bounce(symbol: str, window: list[dict], tf: str) -> Optional[Signal]:
    from vwap_bounce_strategy import analyze_pair

    row = analyze_pair(symbol, window)
    if row.get("setup") not in ("BUY", "SELL"):
        return None
    known = window[-1]["ts_utc"] + TF_SECONDS[tf]
    grade = row.get("grade") or ""
    return _base(
        "vwap_bounce", row["setup"], grade, row.get("entry"), row.get("sl"), row.get("tp1"),
        known, f"bounce:{window[-1]['ts_utc']}", alert=grade == "A",
        score=row.get("score"),
    )


def extract_mr(symbol: str, window: list[dict], tf: str) -> Optional[Signal]:
    from vwap_mean_reversion_strategy import TIME_STOP_BARS, analyze_pair

    row = analyze_pair(symbol, window)
    if row.get("setup") not in ("BUY", "SELL"):
        return None
    known = window[-1]["ts_utc"] + TF_SECONDS[tf]
    grade = row.get("grade") or ""
    score = row.get("score") or 0
    conf = row.get("confirmation") or {}
    cidx = conf.get("idx")
    time_stop = None
    if cidx is not None:
        left = (int(cidx) + TIME_STOP_BARS) - (len(window) - 1)
        time_stop = known + max(left, 0) * TF_SECONDS[tf]
    ext = row.get("extension") or {}
    eidx = ext.get("idx")
    if eidx is not None and 0 <= int(eidx) < len(window):
        dedup = f"mr:{window[int(eidx)]['ts_utc']}:{row['setup']}"
    else:
        dedup = f"mr:{window[-1]['ts_utc']}"
    return _base(
        "vwap_mr", row["setup"], grade, row.get("entry"), row.get("sl"), row.get("tp1"),
        known, dedup, alert=(grade == "A" and score >= 9),
        time_stop_ts=time_stop, score=score,
    )


def extract_vwap9(symbol: str, window: list[dict], tf: str) -> Optional[Signal]:
    from vwap9ema_strategy import analyze_pair

    row = analyze_pair(symbol, window)
    if row.get("setup") not in ("BUY", "SELL"):
        return None
    known = window[-1]["ts_utc"] + TF_SECONDS[tf]
    cross_ts = row.get("cross_ts") or window[-1]["ts_utc"]
    return _base(
        "vwap9ema", row["setup"], row.get("grade") or "UNVALIDATED",
        row.get("entry"), row.get("sl"), row.get("tp1"),
        known, f"v9:{cross_ts}", alert=True,
    )


def extract_id50(symbol: str, window: list[dict], htf: list[dict], tf: str) -> Optional[Signal]:
    from btmm_id50 import analyze_pair

    row = analyze_pair(symbol, window, htf or None)
    if row.get("setup") not in ("BUY", "SELL"):
        return None
    plan = row.get("trade_plan") or {}
    known = window[-1]["ts_utc"] + TF_SECONDS[tf]
    grade = row.get("grade") or ""
    in_sess = row.get("in_active_session")
    cross = row.get("cross") or {}
    cidx = cross.get("idx")
    if cidx is not None and 0 <= int(cidx) < len(window):
        dedup = f"id50:{window[int(cidx)]['ts_utc']}:{row['setup']}"
    else:
        dedup = f"id50:{window[-1]['ts_utc']}:{row['setup']}"
    return _base(
        "id50", row["setup"], grade, plan.get("entry"), plan.get("sl"), plan.get("tp1"),
        known, dedup,
        alert=grade in ("A+", "A") and in_sess is not False,
        score=row.get("score"),
    )


def extract_tdi(symbol: str, window: list[dict], htf: list[dict], d1: list[dict],
                tf: str) -> Optional[Signal]:
    from tdi_cycle_123 import _analyze_timeframe

    row = _analyze_timeframe(
        symbol, window, htf or None, d1 or None, TF_LABEL[tf], HTF_LABEL[tf],
    )
    if row.get("setup") not in ("BUY", "SELL"):
        return None
    plan = row.get("trade_plan") or {}
    known = window[-1]["ts_utc"] + TF_SECONDS[tf]
    grade = row.get("grade") or ""
    p3 = (row.get("pattern") or {}).get("p3") or {}
    dedup = f"tdi:{p3.get('ts_utc')}:{row['setup']}"
    return _base(
        "tdi123", row["setup"], grade, plan.get("entry"), plan.get("sl"), plan.get("tp1"),
        known, dedup,
        alert=grade in ("A", "B") and row.get("in_active_session") is not False,
        score=row.get("score"),
    )


def extract_btmm(symbol: str, window: list[dict], tf: str) -> Optional[Signal]:
    from btmm_core import analyze

    row = analyze(window, symbol)
    setup = row.get("active_setup") or None
    if not setup:
        return None
    direction = setup.get("direction")
    side = "BUY" if direction == "bullish" else "SELL" if direction == "bearish" else ""
    if not side:
        return None
    entry, sl, tp = setup.get("entry"), setup.get("sl"), setup.get("tp1")
    known = window[-1]["ts_utc"] + TF_SECONDS[tf]
    key = str(setup.get("key") or "setup")
    try:
        entry_f = float(entry)
        close = float(window[-1]["close"])
        away = abs(entry_f - close)
        risk = abs(entry_f - float(sl))
    except (TypeError, ValueError):
        return None
    # A limit rests on the far side of the close. A buy printed above the
    # close is a stop, and the old abs() check sent it as a buy limit, which
    # then filled because the bid was already under that price.
    resting = (side == "BUY" and entry_f <= close) or (side == "SELL" and entry_f >= close)
    kind = "limit" if resting and risk > 0 and away > 0.25 * risk else "market"
    valid = known + 8 * 3600 if kind == "limit" else None
    return _base(
        "btmm", side, "A+" if key == "aplus" else key,
        entry, sl, tp, known,
        f"btmm:{key}:{side}:{round(float(entry), 6)}:{round(float(sl), 6)}",
        alert=(key == "aplus"),
        entry_kind=kind, valid_until_ts=valid, score=setup.get("gatesPassed"),
    )


def _partner_buckets(partner_m15: Optional[list[dict]], now_ny: datetime, which: str) -> Optional[dict]:
    if not partner_m15:
        return None
    from crt_strategy import latest_populated_session
    from crt_utils import build_h4_buckets

    buckets = build_h4_buckets(partner_m15)
    sess = latest_populated_session(buckets, now_ny)
    if which == "5am":
        anchor_5 = sess["crt_1am"] + timedelta(hours=4)
        return {
            "5am": buckets.get(anchor_5),
            "1am": buckets.get(sess["crt_1am"]),
            "ny": buckets.get(anchor_5 + timedelta(hours=4)),
        }
    return {
        "1am": buckets.get(sess["crt_1am"]),
        "asia": buckets.get(sess["asia"]),
    }


def extract_crt(symbol: str, m15: list[dict], partner_m15: Optional[list[dict]],
                now_ny: datetime, which: str = "5am") -> Optional[Signal]:
    """CRT at a specific New York clock time. ``m15`` must already be cut
    so it contains no bar that was still open at ``now_ny``."""
    from alerts import _build_crt_trade_plan
    from crt_strategy import analyze_pair, analyze_pair_5am

    partner_bk = _partner_buckets(partner_m15, now_ny, which)
    if which == "5am":
        row = analyze_pair_5am(symbol, m15, partner_bk, now_ny=now_ny)
        candle_key = "candle_5am"
        end_hour = 12
        strategy = "crt_5am"
    else:
        row = analyze_pair(symbol, m15, partner_bk, now_ny=now_ny)
        candle_key = "candle_1am"
        end_hour = 8
        strategy = "crt_1am"
    if row.get("setup") not in ("BUY", "SELL"):
        return None
    plan = _build_crt_trade_plan(symbol, row, row["setup"], candle_key)
    if not plan:
        return None
    known = int(now_ny.timestamp())
    end = now_ny.replace(hour=end_hour, minute=0, second=0, microsecond=0)
    if end <= now_ny:
        end = now_ny + timedelta(hours=3)
    grade = row.get("grade") or ""
    kt = row.get("key_time_status")
    session = row.get("session_5am_sast") or row.get("session_1am_sast") or now_ny.strftime("%Y-%m-%d")
    return _base(
        strategy, row["setup"], grade, plan["entry"], plan["sl"], plan["tp1"],
        known, f"{strategy}:{session}:{row['setup']}",
        alert=grade in ("A", "B") and kt in ("ACTIVE", "WAITING"),
        entry_kind="limit", valid_until_ts=int(end.timestamp()), score=row.get("score"),
    )


def current_signal(strategy: str, symbol: str, timeframe: str,
                   candles: dict[str, list[dict]],
                   partner_m15: Optional[list[dict]] = None,
                   entry_mode: str = "close") -> Optional[Signal]:
    """Latest signal from closed candles. Used by the execution bot.

    ``candles`` keys are timeframe names (M1, M5, M15, H1, H4, D1). The
    forming bar must already have been dropped by the caller.
    """
    if strategy in ("crt_5am", "crt_1am"):
        which = "5am" if strategy == "crt_5am" else "1am"
        m15 = candles.get("M15") or []
        return extract_crt(symbol, m15[-800:], partner_m15[-800:] if partner_m15 else None,
                           datetime.now(NY), which)

    lookback, htf_name, htf_lb = LOOKBACK[(strategy, timeframe)]
    window = (candles.get(timeframe) or [])[-lookback:]
    if len(window) < min(lookback, 40):
        return None
    htf = (candles.get(htf_name) or [])[-htf_lb:] if htf_name else []
    if strategy == "engulfing":
        return extract_engulfing(symbol, window, htf, timeframe, entry_mode)
    if strategy == "vwap_bounce":
        return extract_bounce(symbol, window, timeframe)
    if strategy == "vwap_mr":
        return extract_mr(symbol, window, timeframe)
    if strategy == "vwap9ema":
        return extract_vwap9(symbol, window, timeframe)
    if strategy == "id50":
        return extract_id50(symbol, window, htf, timeframe)
    if strategy == "tdi123":
        d1 = (candles.get("D1") or [])[-40:]
        return extract_tdi(symbol, window, htf, d1, timeframe)
    if strategy == "btmm":
        return extract_btmm(symbol, window, timeframe)
    raise KeyError(strategy)


def mr_prefilter(window: list[dict]) -> bool:
    """Loose superset of the VWAP-MR extension gate.

    Returns True when the full scanner must run. Uses the same session
    volume-weighted sigma as the strategy, but only on the last few bars,
    and a threshold below the strategy's |z| >= 2 so a real signal is not
    dropped. The backtest audits this against the real scanner and disables
    it if it ever disagrees.
    """
    n = len(window)
    if n < 40:
        return True
    day = window[-1]["ts_utc"] - window[-1]["ts_utc"] % 86400
    start = 0
    for i, bar in enumerate(window):
        if bar["ts_utc"] >= day:
            start = i
            break
    if n - start < 15:
        return False
    tps = []
    vols = []
    for bar in window[start:]:
        tp = (bar["high"] + bar["low"] + bar["close"]) / 3.0
        vol = bar.get("volume") or 0.0
        tps.append(tp)
        vols.append(vol if vol > 0 else 1.0)
    # Running VWAP at each session bar, then sigma only for the last 8.
    cum_pv = 0.0
    cum_v = 0.0
    vwaps = []
    for tp, vol in zip(tps, vols):
        cum_pv += tp * vol
        cum_v += vol
        vwaps.append(cum_pv / cum_v)
    begin = max(0, len(tps) - 8)
    for i in range(begin, len(tps)):
        vwap_i = vwaps[i]
        num = 0.0
        den = 0.0
        for k in range(i + 1):
            num += vols[k] * (tps[k] - vwap_i) ** 2
            den += vols[k]
        sigma = (num / den) ** 0.5 if den else 0.0
        if sigma <= 1e-12:
            return True
        z = (window[start + i]["close"] - vwap_i) / sigma
        if abs(z) >= 1.5:
            return True
    return False


def v9_candidate_prefilter(ts, high, low, close, volume):
    """Superset of the VWAP+9EMA cross, computed once for the whole series.

    ``analyze_pair`` rebuilds the 9 EMA and the session VWAP on every call.
    On a million M1 bars that dominates the run. The EMA here is the same
    recurrence over the whole file. On the lookback the harness actually
    passes (576 M5 bars, 1700 M1 bars) the seed is gone in float64, so the
    value at the right edge matches the windowed EMA. Bars where the two
    series merely graze are included too, and the harness still audits the
    filter against the real scanner.
    """
    import numpy as np
    from vwap9ema_strategy import (
        CROSS_LOOKBACK_BARS, EMA_LEN, MIN_BARS, SESSION_END_UTC, SESSION_START_UTC,
    )

    n = len(ts)
    if n == 0:
        return lambda window: True
    k = 2.0 / (EMA_LEN + 1)
    e9 = np.empty(n, dtype=np.float64)
    e9[0] = float(close[0])
    one_m_k = 1.0 - k
    for i in range(1, n):
        e9[i] = float(close[i]) * k + e9[i - 1] * one_m_k

    day = (ts // 86400) * 86400
    vol = np.where(volume > 0, volume, 1.0).astype(np.float64)
    typ = (high.astype(np.float64) + low.astype(np.float64) + close.astype(np.float64)) / 3.0
    vwap = np.empty(n, dtype=np.float64)
    cum_pv = 0.0
    cum_v = 0.0
    prev_day = None
    for i in range(n):
        d = int(day[i])
        if d != prev_day:
            cum_pv = 0.0
            cum_v = 0.0
            prev_day = d
        v = float(vol[i])
        cum_pv += float(typ[i]) * v
        cum_v += v
        vwap[i] = cum_pv / cum_v

    crossed = np.zeros(n, dtype=bool)
    if n > 1:
        up = (e9[:-1] <= vwap[:-1]) & (e9[1:] > vwap[1:])
        dn = (e9[:-1] >= vwap[:-1]) & (e9[1:] < vwap[1:])
        crossed[1:] = up | dn
    near = np.abs(e9 - vwap) <= np.maximum(np.abs(close.astype(np.float64)) * 1e-8, 1e-8)
    interesting = crossed | near
    cand = interesting.copy()
    for lag in range(1, CROSS_LOOKBACK_BARS):
        shifted = np.zeros(n, dtype=bool)
        shifted[lag:] = interesting[:-lag]
        same = np.zeros(n, dtype=bool)
        same[lag:] = day[lag:] == day[:-lag]
        cand |= shifted & same

    hours = (ts % 86400) // 3600
    in_sess = (hours >= SESSION_START_UTC) & (hours < SESSION_END_UTC)
    change = np.empty(n, dtype=bool)
    change[0] = True
    if n > 1:
        change[1:] = day[1:] != day[:-1]
    starts = np.flatnonzero(change)
    group = np.cumsum(change) - 1
    idx_in_day = np.arange(n) - starts[group]
    cand &= in_sess & (idx_in_day >= (MIN_BARS - 1))
    allowed = set(int(t) for t in ts[cand])

    def prefilter(window: list[dict]) -> bool:
        if len(window) < MIN_BARS:
            return True
        return int(window[-1]["ts_utc"]) in allowed

    return prefilter
