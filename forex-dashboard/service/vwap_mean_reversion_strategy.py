"""VWAP Mean Reversion scanner (M15).

Adapted from an external research specification (VWAP_Mean_Reversion_
Strategy.md/.pdf) written for US equity index futures/ETFs (ES/NQ/SPY/QQQ)
on US market hours, explicitly self-described as "a hypothesis
specification, not a validated edge" requiring multi-year walk-forward
validation before trust. This module adapts its MECHANICAL FRAMEWORK
(VWAP + volume-weighted sigma bands, efficiency-ratio regime filter,
stall/rejection confirmation, ATR-floored stops, T1/T2 targets) onto forex
M15 candles as a live heuristic scanner. No walk-forward validation, kill
criteria, or backtest infrastructure is implemented — see
docs/superpowers/specs/2026-09-10-vwap-mean-reversion-strategy-design.md
for the full rationale and every deviation from the source document's
literal (US-market) wording.

Pipeline per pair:
    1. VWAP: cumulative typical-price VWAP, reset at 00:00 UTC calendar day
       (deviation: doc anchors to 09:30 ET; forex has no single open).
    2. Sigma: cumulative volume-weighted stdev of (TP - VWAP_now), definition
       (a) from the doc, using forex tick-volume as the weight (deviation:
       no consolidated forex volume exists).
    3. Regime filter: Efficiency Ratio ER(20) < 0.35, HARD gate — no signal
       at all in trending conditions.
    4. Extension: |z| >= 2.0 at bar 15+ of the session.
    5. Exhaustion + fading volume: scored, not gated (deviation: tick-volume
       is too noisy to hard-gate on).
    6. Stall confirmation (any of 3 types) within 6 bars of the extension,
       or the setup is void.
    7. Stop = max(structural, 0.75x ATR14). Targets: TP1 = VWAP, TP2 =
       overshoot at z = +/-0.75 (informational only — no position tracking).
    8. Score -> A/B/C grade (NO-TRADE if regime fails, no extension, or
       confirmation times out).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import instruments
from tdi_cycle_123 import _atr

VWAP_MR_UNIVERSE = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD",
]

MIN_BARS_BEFORE_SIGNAL = 15
Z_EXTENSION_THRESHOLD = 2.0
EXHAUSTION_VOL_MULT = 1.5
CONFIRMATION_TIMEOUT_BARS = 6
ER_LOOKBACK = 20
ER_REGIME_THRESHOLD = 0.35
STOP_ATR_MULT = 0.75
ATR_PERIOD = 14
TIME_STOP_BARS = 20
T2_Z_OVERSHOOT = 0.75
MIN_CANDLES_REQUIRED = 40


def _vwap_series(candles: list[dict]) -> list[float]:
    """Cumulative typical-price VWAP, reset at each UTC calendar-day
    boundary. Deviation from doc §2.1 (09:30 ET anchor): forex has no
    single market open, so 00:00 UTC is used, matching the retired
    VWAP+9EMA scanner's convention."""
    out: list[float] = []
    cum_pv = 0.0
    cum_vol = 0.0
    cur_day = None
    for c in candles:
        day = datetime.fromtimestamp(c["ts_utc"], tz=timezone.utc).date()
        if day != cur_day:
            cur_day = day
            cum_pv = 0.0
            cum_vol = 0.0
        tp = (c["high"] + c["low"] + c["close"]) / 3.0
        vol = c.get("volume") or 0
        if vol <= 0:
            vol = 1.0  # equal-weight fallback when a feed reports no tick volume
        cum_pv += tp * vol
        cum_vol += vol
        out.append(cum_pv / cum_vol if cum_vol else tp)
    return out


def _sigma_series(candles: list[dict], vwap: list[float]) -> list[float]:
    """True cumulative volume-weighted stdev (doc §2.2, definition (a)):
    sigma_t = sqrt( sum_i[ vol_i * (tp_i - vwap_t)^2 ] / sum_i[vol_i] )
    for i = session-start..t, recomputed against the CURRENT bar's vwap_t
    for every historical bar — not a running estimate using vwap_i at each
    historical i. Deviation: vol_i is a forex tick-volume proxy, not true
    consolidated volume."""
    out: list[float] = []
    day_start_idx = 0
    cur_day = None
    tps: list[float] = []
    vols: list[float] = []
    for i, c in enumerate(candles):
        day = datetime.fromtimestamp(c["ts_utc"], tz=timezone.utc).date()
        if day != cur_day:
            cur_day = day
            day_start_idx = i
        tp = (c["high"] + c["low"] + c["close"]) / 3.0
        vol = c.get("volume") or 0
        if vol <= 0:
            vol = 1.0
        tps.append(tp)
        vols.append(vol)

        vwap_t = vwap[i]
        num = 0.0
        den = 0.0
        for k in range(day_start_idx, i + 1):
            num += vols[k] * (tps[k] - vwap_t) ** 2
            den += vols[k]
        out.append((num / den) ** 0.5 if den else 0.0)
    return out


def _z_scores(candles: list[dict], vwap: list[float], sigma: list[float]) -> list[Optional[float]]:
    """z_t = (Close_t - VWAP_t) / sigma_t (doc §2.3)."""
    out: list[Optional[float]] = []
    for i, c in enumerate(candles):
        if sigma[i] and sigma[i] > 1e-12:
            out.append((c["close"] - vwap[i]) / sigma[i])
        else:
            out.append(None)
    return out


def _atr_series(candles: list[dict], period: int = ATR_PERIOD) -> list[Optional[float]]:
    """Simple (SMA) ATR, one value per bar — None until `period` bars of
    true range are available. A per-bar series (unlike the shared
    tdi_cycle_123._atr, which returns only the current single value) is
    needed for the d_t cross-check series."""
    trs: list[float] = []
    out: list[Optional[float]] = []
    for i, c in enumerate(candles):
        if i == 0:
            tr = c["high"] - c["low"]
        else:
            prev_close = candles[i - 1]["close"]
            tr = max(
                c["high"] - c["low"],
                abs(c["high"] - prev_close),
                abs(c["low"] - prev_close),
            )
        trs.append(tr)
        if i + 1 >= period:
            out.append(sum(trs[i + 1 - period:i + 1]) / period)
        else:
            out.append(None)
    return out


def _d_scores(candles: list[dict], vwap: list[float],
              atr_series: list[Optional[float]]) -> list[Optional[float]]:
    """d_t = (Close_t - VWAP_t) / ATR_14 (doc §2.3) — an ATR-normalised
    cross-check the doc recommends specifically because volume-based sigma
    is unreliable, which is exactly forex's situation here."""
    out: list[Optional[float]] = []
    for i, c in enumerate(candles):
        atr = atr_series[i]
        if atr and atr > 1e-12:
            out.append((c["close"] - vwap[i]) / atr)
        else:
            out.append(None)
    return out


def _efficiency_ratio(closes: list[float], i: int, n: int = ER_LOOKBACK) -> Optional[float]:
    """Kaufman Efficiency Ratio (doc §3.2a):
    ER_n = |Close_t - Close_{t-n}| / sum(|Close_k - Close_{k-1}|) over the last n bars.
    Near 1.0 = clean directional movement (trend). Near 0 = chop.
    None if fewer than n prior bars exist."""
    if i < n:
        return None
    net_change = abs(closes[i] - closes[i - n])
    path_sum = sum(abs(closes[k] - closes[k - 1]) for k in range(i - n + 1, i + 1))
    if path_sum <= 1e-12:
        return 0.0
    return net_change / path_sum


def _bars_into_session(candles: list[dict], i: int) -> int:
    """1-based count of how many bars (including bar i) fall on the same
    UTC calendar day as bar i, counting backwards. Used to enforce
    "no signal before bar 15 of the session" (doc §2.4) against the actual
    daily VWAP-reset boundary, not just the raw array index."""
    day = datetime.fromtimestamp(candles[i]["ts_utc"], tz=timezone.utc).date()
    count = 0
    for k in range(i, -1, -1):
        if datetime.fromtimestamp(candles[k]["ts_utc"], tz=timezone.utc).date() != day:
            break
        count += 1
    return count


def _find_extension(candles: list[dict], z: list[Optional[float]]) -> Optional[dict]:
    """Most recent bar within the live-relevant window where |z| crosses
    the extension threshold and the bar is at least MIN_BARS_BEFORE_SIGNAL
    into its session (doc §2.4 hard rule, §3.3.1 extension gate).
    direction "short" means price extended ABOVE the band (fade with a
    SELL); "long" means extended BELOW (fade with a BUY)."""
    n = len(candles)
    earliest = max(0, n - 1 - CONFIRMATION_TIMEOUT_BARS)
    for i in range(n - 1, earliest - 1, -1):
        zi = z[i]
        if zi is None:
            continue
        if _bars_into_session(candles, i) < MIN_BARS_BEFORE_SIGNAL:
            continue
        if zi >= Z_EXTENSION_THRESHOLD:
            return {"idx": i, "direction": "short", "z": zi}
        if zi <= -Z_EXTENSION_THRESHOLD:
            return {"idx": i, "direction": "long", "z": zi}
    return None


def _check_confirmation(candles: list[dict], z: list[Optional[float]],
                        extension: dict) -> Optional[dict]:
    """Stall/rejection confirmation (doc §3.3.3), any of:
      (a) a bar closes back inside the band (|z| < threshold)
      (b) a rejection wick >= 50% of that bar's total range, on the side
          that fades the extension
      (c) two consecutive bars with lower highs (short) / higher lows (long)
          following the extreme
    Searched within CONFIRMATION_TIMEOUT_BARS of the extension bar; returns
    None (setup void) if nothing fires in that window (doc §3.3 timeout)."""
    ext_idx = extension["idx"]
    direction = extension["direction"]
    n = len(candles)
    last_idx = n - 1
    end = min(last_idx, ext_idx + CONFIRMATION_TIMEOUT_BARS)

    streak = 0
    prev_high = candles[ext_idx]["high"]
    prev_low = candles[ext_idx]["low"]

    for k in range(ext_idx + 1, end + 1):
        c = candles[k]
        zk = z[k]

        if zk is not None and abs(zk) < Z_EXTENSION_THRESHOLD:
            return {"idx": k, "type": "close_inside_band", "bars_since_extension": k - ext_idx}

        rng = c["high"] - c["low"]
        if rng > 1e-9:
            if direction == "short":
                wick = c["high"] - max(c["open"], c["close"])
            else:
                wick = min(c["open"], c["close"]) - c["low"]
            if wick / rng >= 0.5:
                return {"idx": k, "type": "rejection_wick", "bars_since_extension": k - ext_idx}

        if direction == "short":
            if c["high"] < prev_high:
                streak += 1
                if streak >= 2:
                    return {"idx": k, "type": "two_bar_pattern", "bars_since_extension": k - ext_idx}
            else:
                streak = 0
            prev_high = c["high"]
        else:
            if c["low"] > prev_low:
                streak += 1
                if streak >= 2:
                    return {"idx": k, "type": "two_bar_pattern", "bars_since_extension": k - ext_idx}
            else:
                streak = 0
            prev_low = c["low"]

    return None


def _exhaustion_volume(candles: list[dict], ext_idx: int) -> bool:
    """Extension-bar volume > 1.5x the 20-bar average (doc §3.3.2). Scored,
    not a hard gate — deviation: forex tick-volume is too noisy to trust
    as a binary gate (see spec deviation #8)."""
    lookback = candles[max(0, ext_idx - 20):ext_idx]
    if not lookback:
        return False
    avg_vol = sum((c.get("volume") or 0) for c in lookback) / len(lookback)
    if avg_vol <= 0:
        return False
    ext_vol = candles[ext_idx].get("volume") or 0
    return ext_vol > EXHAUSTION_VOL_MULT * avg_vol


def _fading_volume(candles: list[dict], ext_idx: int, confirm_idx: int) -> bool:
    """Confirmation-bar volume < extension-bar volume (doc §3.3.4). Scored,
    same tick-volume-reliability reasoning as _exhaustion_volume."""
    ext_vol = candles[ext_idx].get("volume") or 0
    confirm_vol = candles[confirm_idx].get("volume") or 0
    return bool(ext_vol) and confirm_vol < ext_vol


def _session_status(hour_utc: int) -> str:
    """Informational session bucket, reused unchanged from the retired
    VWAP+9EMA scanner. Deviation from doc §3.5 (US-market session close):
    forex trades continuously, so time-of-day is scored, never a hard
    exclusion (doc §4 itself: 'treat time-of-day as something you measure,
    not something you assume')."""
    if 13 <= hour_utc < 17:
        return "ACTIVE"       # London-NY overlap
    if 8 <= hour_utc < 13:
        return "LONDON"
    if 17 <= hour_utc < 21:
        return "NY-LATE"
    return "ASIAN"


def _score_and_grade(exhaustion: bool, fading: bool, confirmation_type: str,
                     session_status: str) -> tuple[int, str]:
    """Score out of 10 (matches the retired VWAP+9EMA's convention).
    Base 3 = a real confirmed setup exists at all."""
    score = 3
    if exhaustion:
        score += 2
    if fading:
        score += 1
    conf_points = {"rejection_wick": 2, "close_inside_band": 1, "two_bar_pattern": 1}
    score += conf_points.get(confirmation_type, 0)
    session_points = {"ACTIVE": 2, "LONDON": 1, "NY-LATE": 1, "ASIAN": 0}
    score += session_points.get(session_status, 0)
    grade = "A" if score >= 8 else "B" if score >= 6 else "C"
    return score, grade
