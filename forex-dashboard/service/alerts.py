"""Alert rule engine + Discord webhook dispatcher."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
import instruments
from config import (
    DISCORD_WEBHOOK_URL, BTMM_APLUS_ONLY, CRT_GRADE_A_ONLY,
    CRT_5AM_GRADE_A_ONLY, TDI123_ALERTS_ENABLED, TDI123_GRADE_A_ONLY,
    TDI123_SESSION_FILTER, TDI123_ADR_FILTER,
    TDI123_NEWS_FILTER, TDI123_NEWS_WINDOW_MIN, TDI123_JOURNAL_ENABLED,
    TDI123_WATCH_ALERTS_ENABLED,
    BTMM123_ALERTS_ENABLED, BTMM123_SESSION_FILTER, BTMM123_NEWS_FILTER,
    BTMM123_GRADE_A_ONLY, BTMM123_WATCH_ALERTS_ENABLED,
    VWAP_MR_ALERTS_ENABLED, VWAP_MR_GRADE_A_ONLY, VWAP_MR_MIN_SCORE,
    VWAP_MR_WATCH_ALERTS_ENABLED, VWAP_MR_NEWS_FILTER,
)
from providers import forexfactory

log = logging.getLogger("alerts")

WEBHOOK_URL: Optional[str] = DISCORD_WEBHOOK_URL or None

RATE_LIMIT_SECS = 3600      # one alert per pair+rule per hour
MIN_CHECKLIST   = 7         # minimum gates that must pass before firing signal alert (out of 13)

# Persist throttle state to disk so service restarts don't re-fire suppressed alerts
_STATE_FILE = Path(__file__).parent / "alerts_state.json"
_last_sent: dict[str, float] = {}


def _load_state() -> None:
    global _last_sent
    try:
        if _STATE_FILE.exists():
            _last_sent = json.loads(_STATE_FILE.read_text())
    except Exception:
        _last_sent = {}


def _save_state() -> None:
    try:
        _STATE_FILE.write_text(json.dumps(_last_sent))
    except Exception:
        pass


_load_state()

# Colour codes for Discord embeds
_COLOURS = {
    "strong_buy":  0x00E676,   # green
    "strong_sell": 0xFF1744,   # red
    "setup":       0xA855F7,   # purple
    "kill_zone":   0xFFD93D,   # yellow
    "info":        0x4FC3F7,   # blue
    "watch":       0x8B949E,   # grey — "still forming", not a trade signal
}

_SETUP_NAMES = {
    "aplus":      "A+ Setup",
    "safety":     "Safety Trade",
    "trade22":    "22 Trade",
    "bounce5050": "50/50 Bounce",
    "threeDrive": "Three-Drive",
}


def _throttle_key(pair: str, rule: str) -> str:
    return f"{pair}:{rule}"


def _is_throttled(pair: str, rule: str) -> bool:
    key = _throttle_key(pair, rule)
    last = _last_sent.get(key, 0)
    return time.time() - last < RATE_LIMIT_SECS


def _mark_sent(pair: str, rule: str):
    _last_sent[_throttle_key(pair, rule)] = time.time()
    _save_state()


def _post_discord(embed: dict) -> bool:
    """POST one embed to Discord. Returns True on success."""
    if not WEBHOOK_URL:
        log.debug("DISCORD_WEBHOOK_URL not set — alert suppressed")
        return False
    try:
        resp = requests.post(
            WEBHOOK_URL,
            json={"embeds": [embed]},
            timeout=8,
        )
        if resp.status_code == 204:
            return True
        log.warning("Discord returned %d: %s", resp.status_code, resp.text[:200])
        return False
    except Exception as e:
        log.warning("Discord post failed: %s", e)
        return False


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _now_sast_str() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%H:%M")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pip_size(price: float, symbol: Optional[str] = None) -> float:
    """Pip/point size — resolves by symbol via the instrument spec table
    (authoritative), falling back to price magnitude only for unknown symbols."""
    return instruments.pip_size(symbol, price)


def _fmt_price(price: float, symbol: Optional[str] = None) -> str:
    """Format price using the instrument's display precision."""
    return instruments.fmt_price(symbol, price)


def _level_fields(entry: float, sl: float, tp1: float,
                  tp2: Optional[float], direction: str,
                  symbol: Optional[str] = None) -> list[dict]:
    """Build Discord field blocks for entry/SL/TP1/TP2/RR."""
    pip  = _pip_size(entry, symbol)
    mult = -1 if direction in ("sell", "bearish") else 1

    sl_pips  = round(abs(entry - sl)  / pip)
    tp1_pips = round(abs(tp1 - entry) / pip)
    rr       = tp1_pips / sl_pips if sl_pips else 0

    fields = [
        {"name": "Direction",  "value": "**BUY**  ↑" if mult == 1 else "**SELL** ↓", "inline": True},
        {"name": "Entry",      "value": f"`{_fmt_price(entry, symbol)}`",               "inline": True},
        {"name": "Stop Loss",  "value": f"`{_fmt_price(sl, symbol)}`  (−{sl_pips} pips)", "inline": True},
        {"name": "TP1",        "value": f"`{_fmt_price(tp1, symbol)}`  (+{tp1_pips} pips)", "inline": True},
        {"name": "Risk:Reward","value": f"**1 : {rr:.2f}**",                           "inline": True},
    ]
    if tp2 is not None:
        tp2_pips = round(abs(tp2 - entry) / pip)
        fields.insert(4, {"name": "TP2", "value": f"`{_fmt_price(tp2, symbol)}`  (+{tp2_pips} pips)", "inline": True})

    return fields


# ── Public alert functions ────────────────────────────────────────────────────

def alert_strong_signal(pair: str, direction: str, score: float,
                        checklist: int, signal: str, price: Optional[float] = None,
                        asian_range: Optional[dict] = None):
    """Fire when EMA stack + checklist confirm a Strong Buy or Strong Sell."""
    rule = f"strong_{direction}"
    if _is_throttled(pair, rule):
        return
    colour = _COLOURS["strong_buy"] if direction == "buy" else _COLOURS["strong_sell"]
    arrow  = "📈" if direction == "buy" else "📉"

    fields: list[dict] = [
        {"name": "Score",     "value": f"**{score:+.0f}**",     "inline": True},
        {"name": "Checklist", "value": f"**{checklist}/13**",   "inline": True},
    ]

    if price:
        pip = _pip_size(price)
        if asian_range and asian_range.get("valid"):
            rng = asian_range["high"] - asian_range["low"]
            if direction == "buy":
                sl  = asian_range["low"]  - 2 * pip
                tp1 = asian_range["high"] + 0.5 * rng
                tp2 = asian_range["high"] + rng
            else:
                sl  = asian_range["high"] + 2 * pip
                tp1 = asian_range["low"]  - 0.5 * rng
                tp2 = asian_range["low"]  - rng
        else:
            sl  = price - 20 * pip if direction == "buy" else price + 20 * pip
            tp1 = price + 30 * pip if direction == "buy" else price - 30 * pip
            tp2 = price + 60 * pip if direction == "buy" else price - 60 * pip
        # Safety clamp: SL must always be on the correct side of the reference price
        if direction == "buy":
            sl = min(sl, price - 2 * pip)
        else:
            sl = max(sl, price + 2 * pip)
        # TP sanity: recalculate if price moved past Asian-range TPs
        sl_dist = abs(price - sl)
        if direction == "buy" and tp1 <= price + 2 * pip:
            tp1 = price + sl_dist
            tp2 = price + sl_dist * 2
        elif direction == "sell" and tp1 >= price - 2 * pip:
            tp1 = price - sl_dist
            tp2 = price - sl_dist * 2
        if not _check_rr(price, sl, tp1, direction, symbol=pair):
            return  # suppress garbage trade plans
        fields += _level_fields(price, sl, tp1, tp2, direction, symbol=pair)

    embed = {
        "title":  f"{arrow} {pair} — {signal}",
        "color":  colour,
        "fields": fields,
        "footer": {"text": f"BTMM Dashboard · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("alert sent: %s %s score=%+.0f cl=%d/13", pair, rule, score, checklist)


def _check_rr(entry: float, sl: float, tp1: float, direction: str,
              min_rr: float = 0.8, symbol: Optional[str] = None) -> bool:
    """Sanity-check the trade plan. Returns True if R:R is acceptable.

    Rejects garbage plans where TP1 is on the wrong side of entry or R:R < min_rr.
    """
    pip = _pip_size(entry, symbol)
    sl_dist  = abs(entry - sl)
    tp1_dist = abs(tp1 - entry)
    if sl_dist < pip:
        return False  # SL too tight / on wrong side
    rr = tp1_dist / sl_dist
    if rr < min_rr:
        log.warning("R:R check FAILED: entry=%s sl=%s tp1=%s rr=1:%.2f (min %.1f)",
                    _fmt_price(entry), _fmt_price(sl), _fmt_price(tp1), rr, min_rr)
        return False
    # TP must be on the correct side of entry
    if direction in ("buy", "bullish") and tp1 <= entry:
        log.warning("R:R check FAILED: BUY but tp1 %s <= entry %s", _fmt_price(tp1), _fmt_price(entry))
        return False
    if direction in ("sell", "bearish") and tp1 >= entry:
        log.warning("R:R check FAILED: SELL but tp1 %s >= entry %s", _fmt_price(tp1), _fmt_price(entry))
        return False
    return True


def alert_aplus_setup(pair: str, score: float, checklist: int,
                      direction: str, entry: float, sl: float,
                      tp1: float, tp2: Optional[float], kz: Optional[str]):
    """
    Fire on A+ setups only — score ≥ 70, checklist ≥ 10/13, Level II confirmed.
    Uses a gold embed with a ⭐ A+ badge to distinguish from ordinary setups.
    """
    rule = "aplus_setup"
    if _is_throttled(pair, rule):
        return
    if not _check_rr(entry, sl, tp1, direction, symbol=pair):
        log.warning("A+ alert BLOCKED for %s: bad R:R — trade plan invalid", pair)
        return
    colour = 0xFFD700   # gold
    arrow  = "📈⭐" if direction in ("buy", "bullish") else "📉⭐"
    label  = "BUY" if direction in ("buy", "bullish") else "SELL"

    fields = _level_fields(entry, sl, tp1, tp2, direction, symbol=pair)
    fields += [
        {"name": "Tier",      "value": "🏆 **A+ Setup**",              "inline": True},
        {"name": "Score",     "value": f"**{score:+.0f} / 100**",      "inline": True},
        {"name": "Checklist", "value": f"**{checklist}/13 gates**",    "inline": True},
        {"name": "Kill Zone", "value": f"**{kz or 'Active'}**",        "inline": True},
    ]

    embed = {
        "title":       f"{arrow} {pair} — A+ {label}  ·  Score {score:+.0f}",
        "description": (
            "All high-confluence gates aligned. "
            "Level II EMA stack confirmed. "
            "This is a highest-tier BTMM setup — wait for the kill-zone candle."
        ),
        "color":       colour,
        "fields":      fields,
        "footer":      {"text": f"BTMM Dashboard · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("A+ alert sent: %s %s score=%+.0f cl=%d/13 kz=%s",
                 pair, label, score, checklist, kz)


def alert_active_setup(pair: str, setup_key: str, gates_passed: int,
                       gates_total: int, direction: str, entry: float,
                       sl: float, tp1: float, tp2: Optional[float],
                       confidence: str, checklist: int = 0):
    """Fire when a named BTMM setup is confirmed — includes full trade plan."""
    rule = f"setup_{setup_key}"
    if _is_throttled(pair, rule):
        return
    if not _check_rr(entry, sl, tp1, direction, symbol=pair):
        log.warning("Setup alert BLOCKED for %s (%s): bad R:R", pair, setup_key)
        return
    name   = _SETUP_NAMES.get(setup_key, setup_key)
    colour = _COLOURS["strong_buy"] if direction == "bullish" else _COLOURS["strong_sell"]

    fields = _level_fields(entry, sl, tp1, tp2, direction, symbol=pair)
    fields += [
        {"name": "Setup",      "value": f"**{name}**",                                  "inline": True},
        {"name": "Gates",      "value": f"**{gates_passed}/{gates_total}**",             "inline": True},
        {"name": "Confidence", "value": f"**{confidence.capitalize()}**",                "inline": True},
        {"name": "Checklist",  "value": f"**{checklist}/13**",                           "inline": True},
    ]

    embed = {
        "title":  f"🎯 {pair} — {name}",
        "color":  colour,
        "fields": fields,
        "footer": {"text": f"BTMM Dashboard · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("alert sent: %s setup=%s gates=%d/%d", pair, setup_key, gates_passed, gates_total)


def alert_kill_zone_open(zone_name: str):
    """Fire once per kill zone opening."""
    rule = f"kz_{zone_name.lower().replace(' ', '_')}"
    if _is_throttled("__kz__", rule):
        return
    embed = {
        "title":       f"⏱ Kill Zone Open — {zone_name}",
        "description": "BTMM kill zone is now active. Watch for setups.",
        "color":       _COLOURS["kill_zone"],
        "footer":      {"text": f"BTMM Dashboard · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent("__kz__", rule)
        log.info("alert sent: kill zone %s", zone_name)


def alert_scheduler_stall(stale_minutes: int):
    """Fire when the scheduler hasn't refreshed market data in a while.

    This is the safety net that turns a silent signal blackout into a loud,
    visible warning so you never again go a whole session with no signals."""
    rule = "scheduler_stall"
    if _is_throttled("__watchdog__", rule):
        return
    embed = {
        "title":       "🚨 Signal service STALLED — no fresh data",
        "description": (
            f"The scheduler has not refreshed market data for **~{stale_minutes} min**.\n"
            "Discord signals may be stale or stopped. Check the service / MT5 terminal "
            "and restart the dashboard if needed."
        ),
        "color":       _COLOURS["strong_sell"],
        "footer":      {"text": f"Watchdog · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent("__watchdog__", rule)
        log.warning("alert sent: scheduler stall (%d min)", stale_minutes)


def alert_adr_exhausted(pair: str, adr_pct: float):
    """Fire when ADR > 85% consumed — warns against new entries."""
    rule = "adr_exhausted"
    if _is_throttled(pair, rule):
        return
    embed = {
        "title":       f"⚠ {pair} — ADR {adr_pct:.0f}% Consumed",
        "description": "Daily range nearly exhausted. Avoid new entries — fade risk is high.",
        "color":       _COLOURS["info"],
        "footer":      {"text": f"BTMM Dashboard · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("alert sent: %s ADR exhausted %.0f%%", pair, adr_pct)


def alert_513_cross(pair: str, direction: str, price: float):
    """Fire when EMA(5) crosses EMA(13) inside a kill zone — the BTMM entry trigger."""
    if direction is None:
        return
    rule   = f"cross_513_{direction}"
    if _is_throttled(pair, rule):
        return
    arrow  = "📈" if direction == "bullish" else "📉"
    colour = _COLOURS["strong_buy"] if direction == "bullish" else _COLOURS["strong_sell"]
    label  = "Bullish" if direction == "bullish" else "Bearish"
    embed = {
        "title":       f"{arrow} {pair} — 5/13 EMA Cross ({label})",
        "description": (f"EMA 5 crossed {'above' if direction == 'bullish' else 'below'} EMA 13 "
                        f"inside a kill zone. Potential entry signal."),
        "color":       colour,
        "fields":      [{"name": "Price", "value": f"`{_fmt_price(price)}`", "inline": True}],
        "footer":      {"text": f"BTMM Dashboard · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("alert sent: %s 5/13 cross %s @ %s", pair, direction, _fmt_price(price))


def _build_crt_trade_plan(pair: str, row: dict, setup: str, candle_key: str) -> Optional[dict]:
    """Build entry/SL/TP1/TP2 for a CRT setup per MADO PDF spec.

    Strategy spec:
      Entry = M15 OB mid (if available) else 1AM/5AM candle open ("buy below open / sell above").
      SL    = beyond the OB extreme (or candle extreme as fallback), padded by 0.25× ATR-like buffer.
      TP1   = entry ± risk × 2.0   (1:2 RR — strategy minimum, PDF pages 22 & 27)
      TP2   = entry ± risk × 3.0   (1:3 RR — strategy preferred target)

    Returns None if we lack the data to build a sane plan.
    """
    candle = row.get(candle_key) or {}
    entry_zone = row.get("entry_zone") or {}
    entry_ob = row.get("entry_ob")

    candle_open  = candle.get("open")
    candle_high  = candle.get("high")
    candle_low   = candle.get("low")
    if candle_open is None or candle_high is None or candle_low is None:
        return None

    # Entry — prefer OB mid (PDF page 17: "Entry Model = Model#1 / OB"); else candle open
    if entry_ob and entry_ob.get("mid") is not None:
        entry = float(entry_ob["mid"])
        ob_high = entry_ob.get("high")
        ob_low  = entry_ob.get("low")
    else:
        entry = float(entry_zone.get("level") or candle_open)
        ob_high = ob_low = None

    # SL — beyond the OB extreme (or candle extreme as a fallback)
    try:
        min_sl = instruments.min_sl_distance(pair, entry)
    except Exception:
        min_sl = 0.0

    if setup == "BUY":
        sl_anchor = ob_low if ob_low is not None else candle_low
        sl = float(sl_anchor) - min_sl
        if sl >= entry:
            return None
        risk = entry - sl
        tp1 = entry + risk * 2.0
        tp2 = entry + risk * 3.0
    else:  # SELL
        sl_anchor = ob_high if ob_high is not None else candle_high
        sl = float(sl_anchor) + min_sl
        if sl <= entry:
            return None
        risk = sl - entry
        tp1 = entry - risk * 2.0
        tp2 = entry - risk * 3.0

    return {"entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "risk": risk,
            "rr1": 2.0, "rr2": 3.0, "ob_used": entry_ob is not None}


def alert_crt_setup(pair: str, row: dict):
    """Fire when a 1AM CRT scanner row reaches A/B grade in a tradeable key-time window.

    `row` is one element from crt_strategy.analyze_universe()['pairs'].
    Throttled to one alert per (pair, setup direction) per session — the rule key includes
    the session date so re-firing across sessions is allowed but not within a session.
    """
    setup    = row.get("setup")          # 'BUY' / 'SELL' / 'NO-TRADE'
    grade    = row.get("grade")          # 'A' / 'B' / 'C' / 'NO-TRADE' / 'NO-DATA'
    kt       = row.get("key_time_status")  # 'PRE-1AM' / 'WAITING' / 'ACTIVE' / 'LATE' / 'MISSED'
    is_live  = row.get("session_is_live", True)
    session  = row.get("session_1am_sast", "")
    if setup not in ("BUY", "SELL"):
        return
    allowed_grades = ("A",) if CRT_GRADE_A_ONLY else ("A", "B")
    if grade not in allowed_grades:
        if grade == "B":
            log.debug("CRT SUPPRESSED %s: Grade B (A-only mode)", pair)
        return
    if kt not in ("WAITING", "ACTIVE"):
        return
    if not is_live:
        return

    rule = f"crt_{setup.lower()}_{session.replace(' ', '_').replace(':', '')}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    grade_badge = "⭐ " if grade == "A" else ""
    colour = 0xFFD700 if grade == "A" else (_COLOURS["strong_buy"] if setup == "BUY" else _COLOURS["strong_sell"])
    entry_zone = row.get("entry_zone") or {}
    entry_side = entry_zone.get("side", "")
    entry_level = entry_zone.get("level")
    entry_str = (
        f"{'Sell ≥' if entry_side == 'above_open' else 'Buy ≤' if entry_side == 'below_open' else '—'} "
        f"`{_fmt_price(entry_level)}`" if entry_level is not None else "—"
    )

    c1 = row.get("candle_1am") or {}
    crt_hi, crt_lo = row.get("crt_high"), row.get("crt_low")
    crt_str = f"`{_fmt_price(crt_hi)}` / `{_fmt_price(crt_lo)}`" if (crt_hi and crt_lo) else "—"

    # Guard: _fmt_price crashes on None — candle_1am should always be populated when
    # setup is BUY/SELL, but be defensive in case of data gaps.
    c1_open  = c1.get("open")
    c1_close = c1.get("close")
    c1_str   = (
        f"O `{_fmt_price(c1_open)}` → C `{_fmt_price(c1_close)}` ({c1.get('type', '?')})"
        if c1_open is not None and c1_close is not None else "—"
    )

    # SMT label — simplify partner-led variants for readability
    smt_raw = row.get("smt", "NONE")
    smt_label = {
        "BULLISH-DIVERGENCE":         "🟢 Bullish divergence",
        "BEARISH-DIVERGENCE":         "🔴 Bearish divergence",
        "BULLISH-DIVERGENCE-PARTNER": "🟡 Partner bullish div",
        "BEARISH-DIVERGENCE-PARTNER": "🟡 Partner bearish div",
        "NONE":                       "—",
    }.get(smt_raw, smt_raw)

    # Build trade plan (entry/SL/TP1@1:2/TP2@1:3 per MADO 1AM CRT PDF page 22)
    plan = _build_crt_trade_plan(pair, row, setup, candle_key="candle_1am")
    if plan:
        plan_str = (
            f"Entry `{_fmt_price(plan['entry'])}` · SL `{_fmt_price(plan['sl'])}`\n"
            f"TP1 `{_fmt_price(plan['tp1'])}` (1:{plan['rr1']:.0f}) · "
            f"TP2 `{_fmt_price(plan['tp2'])}` (1:{plan['rr2']:.0f})"
        )
        plan_source = "M15 OB" if plan["ob_used"] else "1AM open"
    else:
        plan_str = "—"
        plan_source = "—"

    # Intraday profile from new 1AM detector (normal_protraction / delayed_protraction)
    intra = row.get("intraday_profile") or {}
    intra_str = intra.get("label", "—") if isinstance(intra, dict) else "—"

    fields = [
        {"name": "Setup",        "value": f"**{setup}**",                                            "inline": True},
        {"name": "Grade",        "value": f"{grade_badge}**{grade} ({row.get('score', 0)}/10)**",    "inline": True},
        {"name": "Key Time",     "value": f"**{kt}** · {row.get('key_time_window_sast', '')}",       "inline": True},
        {"name": "Trade Plan",   "value": plan_str,                                                   "inline": False},
        {"name": "Entry Source", "value": plan_source,                                                "inline": True},
        {"name": "Entry Zone",   "value": entry_str,                                                  "inline": True},
        {"name": "1AM Candle",   "value": c1_str,                                                     "inline": True},
        {"name": "CRT H/L",      "value": crt_str,                                                    "inline": True},
        {"name": "Market Profile","value": f"{row.get('profile_type', '?')} — {row.get('profile_label', '')}", "inline": False},
        {"name": "Intraday",     "value": intra_str,                                                  "inline": True},
        {"name": "DOL Bias",     "value": row.get("dol_bias", "?") or "—",                           "inline": True},
        {"name": "SMT",          "value": smt_label,                                                  "inline": True},
        {"name": "OHLC Pattern", "value": row.get("ohlc_pattern", "?"),                               "inline": True},
    ]

    provisional = row.get("provisional", False)
    m15_count   = row.get("m15_count", 0)
    prov_suffix = f"  ⚠ forming ({m15_count}/16 M15s)" if provisional else ""

    embed = {
        "title":       f"{arrow} {grade_badge}{pair} — 1AM CRT {setup}{prov_suffix}",
        "description": row.get("notes") or "1AM CRT scanner confluence reached A/B grade.",
        "color":       colour,
        "fields":      fields,
        "footer":      {"text": f"1AM CRT · session {session} SAST · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("CRT alert sent: %s %s grade=%s kt=%s session=%s",
                 pair, setup, grade, kt, session)


def alert_crt_5am_setup(pair: str, row: dict):
    """Fire when a 5AM CRT scanner row reaches Grade A in the NY Open kill zone.

    `row` is one element from crt_strategy.analyze_universe_5am()['pairs'].
    Grade A only by default (CRT_5AM_GRADE_A_ONLY=true). Throttled per session.
    Key window: 09:00–11:00 NY / 11:00–13:00 SAST.
    """
    setup   = row.get("setup")
    grade   = row.get("grade")
    kt      = row.get("key_time_status")
    is_live = row.get("session_is_live", True)
    session = row.get("session_5am_sast", "")
    if setup not in ("BUY", "SELL"):
        return
    allowed_grades = ("A",) if CRT_5AM_GRADE_A_ONLY else ("A", "B")
    if grade not in allowed_grades:
        if grade == "B":
            log.debug("CRT-5AM SUPPRESSED %s: Grade B (A-only mode)", pair)
        return
    if kt not in ("WAITING", "ACTIVE"):
        return
    if not is_live:
        return

    rule = f"crt5am_{setup.lower()}_{session.replace(' ', '_').replace(':', '')}"
    if _is_throttled(pair, rule):
        return

    arrow       = "📈" if setup == "BUY" else "📉"
    grade_badge = "⭐ " if grade == "A" else ""
    colour      = 0xFFD700 if grade == "A" else (_COLOURS["strong_buy"] if setup == "BUY" else _COLOURS["strong_sell"])
    entry_zone  = row.get("entry_zone") or {}
    entry_side  = entry_zone.get("side", "")
    entry_level = entry_zone.get("level")
    entry_str   = (
        f"{'Sell ≥' if entry_side == 'above_open' else 'Buy ≤' if entry_side == 'below_open' else '—'} "
        f"`{_fmt_price(entry_level)}`" if entry_level is not None else "—"
    )

    c5      = row.get("candle_5am") or {}
    crt_hi, crt_lo = row.get("crt_high"), row.get("crt_low")
    crt_str = f"`{_fmt_price(crt_hi)}` / `{_fmt_price(crt_lo)}`" if (crt_hi and crt_lo) else "—"
    c5_open, c5_close = c5.get("open"), c5.get("close")
    c5_str  = (
        f"O `{_fmt_price(c5_open)}` → C `{_fmt_price(c5_close)}` ({c5.get('type', '?')})"
        if c5_open is not None and c5_close is not None else "—"
    )

    smt_raw   = row.get("smt", "NONE")
    smt_label = {
        "BULLISH-DIVERGENCE":         "🟢 Bullish divergence",
        "BEARISH-DIVERGENCE":         "🔴 Bearish divergence",
        "BULLISH-DIVERGENCE-PARTNER": "🟡 Partner bullish div",
        "BEARISH-DIVERGENCE-PARTNER": "🟡 Partner bearish div",
        "NONE":                       "—",
    }.get(smt_raw, smt_raw)

    # Build trade plan (entry/SL/TP1@1:2/TP2@1:3 per MADO 5AM CRT PDF page 18)
    plan = _build_crt_trade_plan(pair, row, setup, candle_key="candle_5am")
    if plan:
        plan_str = (
            f"Entry `{_fmt_price(plan['entry'])}` · SL `{_fmt_price(plan['sl'])}`\n"
            f"TP1 `{_fmt_price(plan['tp1'])}` (1:{plan['rr1']:.0f}) · "
            f"TP2 `{_fmt_price(plan['tp2'])}` (1:{plan['rr2']:.0f})"
        )
        plan_source = "M15 OB" if plan["ob_used"] else "5AM open"
    else:
        plan_str = "—"
        plan_source = "—"

    # Intraday profile from new 5AM detector (london_lunch_low / ny_continuation / ny_reversal)
    intra = row.get("intraday_profile") or {}
    intra_str = intra.get("label", "—") if isinstance(intra, dict) else "—"
    smt_sess  = row.get("smt_session")
    smt_value = f"{smt_label} ({smt_sess})" if smt_sess and smt_label != "—" else smt_label

    fields = [
        {"name": "Setup",        "value": f"**{setup}**",                                              "inline": True},
        {"name": "Grade",        "value": f"{grade_badge}**{grade} ({row.get('score', 0)}/12)**",      "inline": True},
        {"name": "Key Time",     "value": f"**{kt}** · {row.get('key_time_window_sast', '')}",         "inline": True},
        {"name": "Trade Plan",   "value": plan_str,                                                     "inline": False},
        {"name": "Entry Source", "value": plan_source,                                                  "inline": True},
        {"name": "Entry Zone",   "value": entry_str,                                                    "inline": True},
        {"name": "5AM Candle",   "value": c5_str,                                                      "inline": True},
        {"name": "CRT H/L",      "value": crt_str,                                                     "inline": True},
        {"name": "Market Profile","value": f"{row.get('profile_type', '?')} — {row.get('profile_label', '')}", "inline": False},
        {"name": "Intraday",     "value": intra_str,                                                    "inline": True},
        {"name": "DOL Bias",     "value": row.get("dol_bias", "?") or "—",                             "inline": True},
        {"name": "SMT",          "value": smt_value,                                                    "inline": True},
        {"name": "OHLC Pattern", "value": row.get("ohlc_pattern", "?"),                                 "inline": True},
    ]

    provisional = row.get("provisional", False)
    m15_count   = row.get("m15_count", 0)
    prov_suffix = f"  ⚠ forming ({m15_count}/16 M15s)" if provisional else ""

    embed = {
        "title":       f"{arrow} {grade_badge}{pair} — 5AM CRT {setup}{prov_suffix}",
        "description": row.get("notes") or "5AM CRT scanner: NY Open kill zone confluence.",
        "color":       colour,
        "fields":      fields,
        "footer":      {"text": f"5AM CRT · session {session} SAST · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("CRT-5AM alert sent: %s %s grade=%s kt=%s session=%s",
                 pair, setup, grade, kt, session)


def evaluate_pair(pair_symbol: str, signal: str, score: float,
                  checklist_score: int, active_setup: Optional[dict],
                  adr_consumed: float, price: Optional[float] = None,
                  kz: Optional[str] = None, cross_513: Optional[dict] = None,
                  asian_range: Optional[dict] = None,
                  confidence: Optional[str] = None):
    """
    Called by scheduler after each refresh cycle for each priority pair.
    Evaluates all rules and fires Discord alerts as needed.

    **HIGH CONFIDENCE ONLY**: Only setups where 4+ BTMM factors align
    (confidence == 'high') are sent to Discord. Low/medium confidence
    trades are noise and are suppressed entirely.

    Alert priority (highest to lowest):
      1. A+ setup  — score ≥ 70, 10+/13 gates, Level II confirmed  → gold embed
      2. Named setup (Safety Trade etc.)  → purple embed
      3. Strong signal (checklist ≥ 7/13) → green/red embed
      4. 5/13 EMA cross inside kill zone  → directional embed
      5. ADR exhaustion warning           → info embed
    """
    # ── HIGH-CONFIDENCE GATE ─────────────────────────────────────────────────
    # STRICT: Only fire trade alerts when BOTH:
    #   1. Overall BTMM confidence is "high" (4+ factors aligned)
    #   2. Setup-specific confidence is "high" (if a named setup exists)
    # This eliminates ALL medium/low noise from Discord entirely.
    is_high_conf = confidence == "high"
    setup_conf = active_setup.get("confidence", "low") if active_setup else "low"
    setup_is_high = setup_conf == "high"

    if not is_high_conf:
        log.debug("SUPPRESSED %s: overall confidence=%s (not high)", pair_symbol, confidence)

    setup_key = active_setup.get("key") if active_setup else None

    # Rule 1 — A+ setup (highest tier — A+ is always high confidence by definition)
    if setup_key == "aplus" and is_high_conf:
        alert_aplus_setup(
            pair=pair_symbol,
            score=score,
            checklist=checklist_score,
            direction=active_setup.get("direction", "bullish"),
            entry=active_setup.get("entry", price or 0),
            sl=active_setup.get("sl", 0),
            tp1=active_setup.get("tp1", 0),
            tp2=active_setup.get("tp2"),
            kz=kz,
        )

    # ── A+-ONLY MODE ─────────────────────────────────────────────────────────
    # When enabled (default), the A+ setup above is the ONLY BTMM alert we send.
    # Everything below (named/Safety setups, Strong signals, 5/13 cross, ADR) is
    # suppressed as noise. Flip BTMM_APLUS_ONLY=false to restore the full set.
    if BTMM_APLUS_ONLY:
        return

    # Rule 2 — Named setup (Safety Trade etc.) — BOTH confidences must be high
    if setup_key == "aplus" and is_high_conf:
        pass  # already sent above
    elif setup_key and active_setup and is_high_conf and setup_is_high:
        alert_active_setup(
            pair=pair_symbol,
            setup_key=setup_key,
            gates_passed=active_setup["gatesPassed"],
            gates_total=active_setup.get("gatesTotal", 7),
            direction=active_setup.get("direction", "bullish"),
            entry=active_setup.get("entry", price or 0),
            sl=active_setup.get("sl", 0),
            tp1=active_setup.get("tp1", 0),
            tp2=active_setup.get("tp2"),
            confidence="high",
            checklist=checklist_score,
        )
    elif setup_key and active_setup and not (is_high_conf and setup_is_high):
        log.debug("SUPPRESSED %s setup=%s: overall_conf=%s setup_conf=%s",
                  pair_symbol, setup_key, confidence, setup_conf)

    # Rule 3 — Bare strong signal — high confidence + Strong signal only
    elif checklist_score >= MIN_CHECKLIST and is_high_conf:
        if signal in ("Strong Buy", "A+ Buy"):
            alert_strong_signal(pair_symbol, "buy",  score, checklist_score, signal, price, asian_range)
        elif signal in ("Strong Sell", "A+ Sell"):
            alert_strong_signal(pair_symbol, "sell", score, checklist_score, signal, price, asian_range)

    # Rule 4 — 5/13 EMA cross during a kill zone — high confidence only
    if cross_513 and cross_513.get("crossed") and kz and is_high_conf:
        alert_513_cross(pair_symbol, cross_513["direction"], price or 0)

    # Rule 5 — ADR > 85% consumed (warning only — always fires regardless of confidence)
    if adr_consumed > 85 and kz:
        alert_adr_exhausted(pair_symbol, adr_consumed)


# ── TDI Cycle 123 (Peak Formation reversal) ──────────────────────────────────

# Indices don't carry a slash-separated currency pair; map the ones we watch to
# the currency whose high-impact news actually moves them.
_INDEX_CCY = {"DE30": "EUR", "GER40": "EUR", "US30": "USD", "USTEC": "USD",
              "NAS100": "USD", "SPX500": "USD", "UK100": "GBP", "JP225": "JPY",
              "DXY": "USD"}  # US Dollar Index reacts to USD news


def _pair_currencies(pair: str) -> set[str]:
    """The currency codes exposed by a symbol: {EUR, USD} for EUR/USD, {XAU, USD}
    for XAU/USD, {USD} for USTEC. Used to match against the news blocklist."""
    if "/" in pair:
        a, b = pair.split("/", 1)
        return {a.strip().upper(), b.strip().upper()}
    p = pair.strip().upper()
    return {_INDEX_CCY.get(p, p)}


def _news_blocks_pair(pair: str, blocked_currencies: set[str]) -> bool:
    """True if either of the pair's currencies has a high-impact event pending.
    Pure (blocklist injected) so it's unit-testable without network/clock."""
    return bool(_pair_currencies(pair) & (blocked_currencies or set()))


def _should_alert_tdi123(row: dict) -> bool:
    """Determine if TDI123 setup meets quality threshold for alerting.

    Gold standard: Grade A always, or Grade B only if divergence + bias-timeframe
    aligned + R:R >= 1.0. Same rule for every timeframe (H1 biased by H4, M15
    biased by H1) — `row["htf_aligned"]` already reflects whichever bias
    timeframe applies.
    """
    grade = row.get("grade")
    if grade not in ("A", "B"):
        return False

    # Timing gate — user rule: allow alerts (both A and B) inside the
    # 05:00–20:00 SAST trading window (03:00–18:00 UTC, defined in
    # tdi_cycle_123.SESSION_ACTIVE_START/END). Outside that window, block.
    # ADR reachability stays off by default (as a hard gate it starves
    # the strategy). A None flag means data was unavailable — never a
    # hard fail.
    if TDI123_SESSION_FILTER and row.get("in_active_session") is False:
        return False
    if TDI123_ADR_FILTER and row.get("tp1_reachable") is False:
        return False

    # NOTE: the 13-EMA "ketchup reclaim" is deliberately NOT gated here. A clean
    # 30-day A/B test showed that waiting for the reclaim raised win rate
    # (27→37 %) but wrecked R:R — entry chased price a median of 6 bars into the
    # move while the stop stayed at p3 — so expectancy got worse (−0.09→−0.40R).
    # It is surfaced as an informational badge (row["ketchup_reclaimed"]) for the
    # trader to use at their discretion, not as an automated suppressor.

    if grade == "A":
        return True

    # Grade B: divergence + H4 aligned + R:R >= 1.0. rr1 is None when the trade
    # plan has no TP1 target — treat that as failing the gate, not a crash.
    div_present = row.get("divergence", {}).get("present", False)
    htf_aligned = row.get("htf_aligned", False)
    rr = (row.get("trade_plan") or {}).get("rr1") or 0
    return div_present and htf_aligned and rr >= 1.0


def alert_tdi123_setup(pair: str, row: dict):
    """Fire when the TDI Cycle 123 scanner signals a tradeable setup.

    Gates:
      - Alerts globally enabled (TDI123_ALERTS_ENABLED)
      - Grade A (or A+B when TDI123_GRADE_A_ONLY=false)
      - Signal cross confirmed (required — un-crossed patterns are still-forming)
      - R:R sanity on trade plan
      - Reuses SNR distance + trend quality filters — 123 is a reversal setup
        so trend-gate applies the same way.
    """
    if not TDI123_ALERTS_ENABLED:
        return

    # M15 leg evaluated FIRST and unconditionally — independent of whatever
    # the H1 leg's gates decide below. Bug found 2026-09-10: this recursion
    # used to sit after the H1 early-return gates, so a Grade A/B M15 setup
    # was silently never evaluated whenever H1 didn't ALSO independently
    # qualify (e.g. H1 grade C/NO-TRADE) — exactly the common case, since M15
    # and H1 grade independently. Guarded by timeframe so a future scanner
    # change that nests "m15" one level deeper can't recurse unboundedly
    # (review finding 2026-09-10).
    if row.get("m15") and row.get("timeframe") != "M15":
        alert_tdi123_setup(pair, row["m15"])

    setup = row.get("setup")
    grade = row.get("grade")
    if setup not in ("BUY", "SELL"):
        return

    # Apply quality filter: Grade A always, Grade B only with divergence + H4 aligned + R:R >= 1.0
    if not _should_alert_tdi123(row):
        log.debug("TDI123 FILTERED %s: grade %s does not meet quality threshold", pair, grade)
        return

    # Confirmation gate: signal cross must have fired
    if not (row.get("signal_cross") or {}).get("present"):
        log.debug("TDI123 SUPPRESSED %s: signal cross not confirmed", pair)
        return

    plan = row.get("trade_plan") or {}
    entry = plan.get("entry")
    sl = plan.get("sl")
    tp1 = plan.get("tp1")
    if not (entry and sl and tp1):
        log.debug("TDI123 SUPPRESSED %s: incomplete trade plan", pair)
        return

    # Multi-tier R:R check for TDI 123 — the strategy explicitly cascades
    # through L1 (50 EMA), L2 (200 EMA), L3 (800 EMA); trader scales out
    # at each. A plan is valid if AT LEAST ONE tier hits R:R >= 0.8, not
    # just TP1 (which is intentionally the tightest target — often <1R
    # away when a big HTF EMA sits closer than p3-based SL). Also validates
    # each target is on the correct side of entry.
    sl_dist = abs(entry - sl)
    if sl_dist < 1e-9:
        log.warning("TDI123 alert BLOCKED for %s: SL too tight (0 distance)", pair)
        return
    direction_ok = lambda tp: (setup == "BUY" and tp > entry) or (setup == "SELL" and tp < entry)
    valid_tiers = [plan.get(k) for k in ("tp1", "tp2", "tp3") if plan.get(k) and direction_ok(plan.get(k))]
    if not valid_tiers:
        log.warning("TDI123 alert BLOCKED for %s: no tier on correct side of entry", pair)
        return
    best_rr = max(abs(tp - entry) / sl_dist for tp in valid_tiers)
    if best_rr < 0.8:
        log.warning("TDI123 alert BLOCKED for %s: bad R:R (best of L1/L2/L3 = 1:%.2f, min 1:0.8)",
                    pair, best_rr)
        return

    # NOTE: intentionally NOT calling _passes_quality_filters here. That gate
    # was calibrated for SNR's audit (86% of SNR losses were with-trend), and
    # its H1-trend check silently blocks Grade A TDI 123 setups whenever the
    # 3-push happens WITH the H1 trend — even though TDI 123's own gating
    # (grade thresholds + htf_aligned requirement for Grade B) already handles
    # trend context per user rule "any A or B setup 05:00-20:00 SAST alerts".
    # Its distance gate is also a no-op here: TDI 123 uses current close as
    # entry, so distance to entry_price ≈ 0.

    # News gate: suppress if either of the pair's currencies has a high-impact
    # event within the window. Fails open — a feed error never blocks a trade.
    if TDI123_NEWS_FILTER:
        try:
            blocked = forexfactory.currencies_in_window(TDI123_NEWS_WINDOW_MIN, high_only=True)
        except Exception as e:  # noqa: BLE001
            blocked = set()
            log.debug("TDI123 news check failed for %s (fail-open): %s", pair, e)
        if _news_blocks_pair(pair, blocked):
            hit = _pair_currencies(pair) & blocked
            log.info("TDI123 SUPPRESSED %s: high-impact news within %dm (%s)",
                     pair, TDI123_NEWS_WINDOW_MIN, ",".join(sorted(hit)))
            return

    timeframe = row.get("timeframe") or "H1"
    bias_tf = row.get("htf_bias_timeframe") or "H4"

    # Timeframe is part of the throttle key — an H1 and an M15 alert on the
    # same pair/direction/grade are distinct setups and must not collide.
    rule = f"tdi123_{timeframe.lower()}_{setup.lower()}_{grade}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    grade_badge = "⭐ " if grade == "A" else ""
    colour = 0xFFD700 if grade == "A" else (_COLOURS["strong_buy"] if setup == "BUY" else _COLOURS["strong_sell"])

    pattern = row.get("pattern") or {}
    tdi_extreme = row.get("tdi_extreme") or {}
    div = row.get("divergence") or {}
    targets = row.get("targets") or {}

    tp2 = plan.get("tp2")
    tp3 = plan.get("tp3")
    sl_pips = plan.get("sl_pips") or 0
    tp1_pips = targets.get("L1_pips") or 0
    tp2_pips = targets.get("L2_pips") or 0
    tp3_pips = targets.get("L3_pips") or 0
    rr1 = plan.get("rr1") or 0

    fields = [
        {"name": "Direction",    "value": f"**{'📈 BUY' if setup == 'BUY' else '📉 SELL'}**",   "inline": True},
        {"name": "Grade",        "value": f"{grade_badge}**{grade} ({row.get('score', 0)}/15)**", "inline": True},
        {"name": "Setup",        "value": "**123 Peak Formation**",                              "inline": True},
        {"name": "🎯 Entry",       "value": f"`{_fmt_price(entry, pair)}`",                                  "inline": True},
        {"name": "🛑 Stop Loss",   "value": f"`{_fmt_price(sl, pair)}`  (−{sl_pips} pips)",                 "inline": True},
        {"name": "Risk:Reward",   "value": f"**1 : {rr1:.1f}**",                                            "inline": True},
        {"name": "✅ L1 (50 EMA)", "value": f"`{_fmt_price(tp1, pair)}`  (+{tp1_pips} pips)",              "inline": True},
    ]
    if tp2:
        fields.append({"name": "🎯 L2 (200 EMA)", "value": f"`{_fmt_price(tp2, pair)}`  (+{tp2_pips} pips)", "inline": True})
    if tp3:
        fields.append({"name": "🎯 L3 (800 EMA)", "value": f"`{_fmt_price(tp3, pair)}`  (+{tp3_pips} pips)", "inline": True})
    if not tp2 and not tp3:
        fields.append({"name": "​", "value": "​", "inline": True})

    p1 = pattern.get("p1", {})
    p2 = pattern.get("p2", {})
    p3 = pattern.get("p3", {})
    fields += [
        {"name": "123 Pattern",
         "value": f"1 `{_fmt_price(p1.get('price'), pair)}` → 2 `{_fmt_price(p2.get('price'), pair)}` → 3 `{_fmt_price(p3.get('price'), pair)}`  ({pattern.get('leg1_range_pips', 0)} pips leg-1)",
         "inline": False},
        {"name": "TDI at P3",
         "value": (f"baseline `{tdi_extreme.get('baseline_at_p3', 0)}` · RSI `{tdi_extreme.get('rsi_at_p3', 0)}`"
                   + (" ✅ extreme" if tdi_extreme.get('present') else " (not extreme)")),
         "inline": True},
        {"name": "Divergence",
         "value": (f"RSI {div.get('rsi_at_p1', 0)} → {div.get('rsi_at_p3', 0)} "
                   + ("✅ regular" if div.get("strong")
                      else "✅ equal-level" if div.get("present")
                      else "❌ none")),
         "inline": True},
        {"name": f"{bias_tf} Bias",
         "value": (str(row.get("htf_bias") or "n/a").capitalize()
                   + (" ✅" if row.get("htf_aligned") else " (unaligned)")),
         "inline": True},
    ]

    embed = {
        "title":       f"{arrow} {grade_badge}{pair} — TDI Cycle 123 [{timeframe}] {setup}",
        "description": row.get("notes") or "TDI Cycle 123 Peak Formation — improvements-on-BTMM setup.",
        "color":       colour,
        "fields":      fields,
        "footer":      {"text": f"TDI Cycle 123 {timeframe} · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("TDI123 alert sent: %s [%s] %s grade=%s score=%d",
                 pair, timeframe, setup, grade, row.get("score", 0))
        # Journal the fired signal so its real outcome can be tracked later.
        if TDI123_JOURNAL_ENABLED:
            try:
                import cache
                loc = row.get("location") or {}
                dv = row.get("divergence") or {}
                gates = {
                    "grade": grade, "score": row.get("score"),
                    "session": row.get("session"),
                    "in_active_session": row.get("in_active_session"),
                    "divergence": ("strong" if dv.get("strong")
                                   else "equal" if dv.get("present") else "none"),
                    "location": loc.get("zone"),
                    "adr_consumed_pct": row.get("adr_consumed_pct"),
                    "tp1_reachable": row.get("tp1_reachable"),
                    "htf_aligned": row.get("htf_aligned"),
                }
                cache.open_trade(
                    pair=pair, direction=setup, entry=entry, sl=sl, tp1=tp1, tp2=tp2,
                    setup="TDI123", signal=rule, signal_score=row.get("score"),
                    gates_json=json.dumps(gates), notes=(row.get("notes") or "")[:300],
                )
            except Exception as e:  # noqa: BLE001
                log.warning("TDI123 journal open failed for %s: %s", pair, e)


def _tdi123_watch_reasons(pair: str, row: dict) -> list[tuple[str, bool, str]]:
    """Checklist mirroring alert_tdi123_setup's gates, in the same order, for
    the watch embed. The news lookup is the only side effect (fails open like
    the real gate). Never used to suppress the real alert — only to describe
    what is still missing."""
    grade = row.get("grade")
    checks: list[tuple[str, bool, str]] = []

    if grade == "A":
        checks.append(("Grade", True, "A"))
    else:
        div = row.get("divergence") or {}
        htf_aligned = bool(row.get("htf_aligned"))
        rr_b = (row.get("trade_plan") or {}).get("rr1") or 0
        checks.append(("Divergence present", bool(div.get("present")),
                       "yes" if div.get("present") else "none"))
        checks.append(("HTF bias aligned", htf_aligned,
                       f"{row.get('htf_bias_timeframe', 'H4')}: {row.get('htf_bias', '?')}"))
        checks.append(("Grade-B R:R ≥ 1.0", rr_b >= 1.0, f"1:{rr_b:.2f}"))

    if TDI123_SESSION_FILTER:
        checks.append(("Active session", row.get("in_active_session") is not False,
                       row.get("session", "?")))
    if TDI123_ADR_FILTER:
        checks.append(("TP1 within remaining ADR", row.get("tp1_reachable") is not False,
                       "reachable" if row.get("tp1_reachable") else "unreachable"))

    cross = bool((row.get("signal_cross") or {}).get("present"))
    checks.append(("Signal cross confirmed", cross, "confirmed" if cross else "not yet"))

    plan = row.get("trade_plan") or {}
    entry, sl, tp1 = plan.get("entry"), plan.get("sl"), plan.get("tp1")
    plan_complete = bool(entry and sl and tp1)
    checks.append(("Trade plan complete", plan_complete, "ok" if plan_complete else "incomplete"))

    if plan_complete:
        setup = row.get("setup")
        sl_dist = abs(entry - sl)
        direction_ok = lambda tp: (setup == "BUY" and tp > entry) or (setup == "SELL" and tp < entry)
        valid_tiers = [plan.get(k) for k in ("tp1", "tp2", "tp3") if plan.get(k) and direction_ok(plan.get(k))]
        best_rr = (max(abs(tp - entry) / sl_dist for tp in valid_tiers)
                  if valid_tiers and sl_dist > 1e-9 else 0)
        checks.append(("R:R ≥ 0.8 (best tier)", best_rr >= 0.8, f"1:{best_rr:.2f}"))

    if TDI123_NEWS_FILTER:
        try:
            blocked = forexfactory.currencies_in_window(TDI123_NEWS_WINDOW_MIN, high_only=True)
        except Exception:  # noqa: BLE001
            blocked = set()
        news_clear = not _news_blocks_pair(pair, blocked)
        checks.append(("No high-impact news window", news_clear,
                       "clear" if news_clear else f"blocked within {TDI123_NEWS_WINDOW_MIN}m"))

    return checks


def alert_tdi123_watch(pair: str, row: dict):
    """Post a 'still forming' embed for Grade A/B TDI123 setups that don't yet
    clear every gate in alert_tdi123_setup, so a setup can be monitored on
    Discord as it develops instead of only on the dashboard. Skips silently
    once everything passes — the real alert already covers that case."""
    if not TDI123_WATCH_ALERTS_ENABLED:
        return

    # M15 leg evaluated first and unconditionally — see alert_tdi123_setup's
    # matching fix note (2026-09-10). Without this, a Grade B M15 setup was
    # never watched whenever H1 didn't also independently qualify. Guarded by
    # timeframe so a future scanner change that ever nests "m15" one level
    # deeper can't cause unbounded recursion (review finding 2026-09-10).
    if row.get("m15") and row.get("timeframe") != "M15":
        alert_tdi123_watch(pair, row["m15"])

    grade = row.get("grade")
    setup = row.get("setup")
    if grade not in ("A", "B") or setup not in ("BUY", "SELL"):
        return

    try:
        checks = _tdi123_watch_reasons(pair, row)
    except Exception as e:  # noqa: BLE001
        log.warning("TDI123 watch checklist crashed for %s: %s", pair, e)
        return

    # Skip only when the real alert is actually live to catch this — if
    # TDI123_ALERTS_ENABLED is off, nothing else will ever tell the user this
    # setup is fully qualified, so keep posting even at "all clear" (review
    # finding 2026-09-10: this used to skip unconditionally on all-pass,
    # silently producing zero notification whenever the real alert was off).
    if TDI123_ALERTS_ENABLED and all(passed for _, passed, _ in checks):
        return  # everything passes and the real alert is live — it already fired this

    timeframe = row.get("timeframe") or "H1"
    rule = f"tdi123watch_{timeframe.lower()}_{setup.lower()}_{grade}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    lines = [f"{'✅' if ok else '❌'} {label} — {detail}" for label, ok, detail in checks]

    embed = {
        "title":       f"👀 {arrow} {pair} — TDI Cycle 123 [{timeframe}] {setup} watching (Grade {grade})",
        "description": "Still forming — not a trade signal yet.\n" + "\n".join(lines),
        "color":       _COLOURS["watch"],
        "fields":      [
            {"name": "Grade", "value": f"**{grade} ({row.get('score', 0)}/15)**", "inline": True},
        ],
        "footer":      {"text": f"TDI Cycle 123 {timeframe} · Monitoring only · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("TDI123 watch alert sent: %s [%s] %s grade=%s", pair, timeframe, setup, grade)


# ── BTMM 123 (classic 1-2-3 price action, BTMM-doctrine confirmed) ───────────
# Replaces the removed Malaysian SNR Emperor slot. Reuses _pair_currencies /
# _news_blocks_pair above (already pair-agnostic, not TDI123-specific).

def _should_alert_btmm123(row: dict) -> bool:
    """Grade A always; Grade B unless BTMM123_GRADE_A_ONLY is set (same
    A+B convention as CRT/TDI123); Grade C never. Session gate mirrors
    TDI123's: block outside the active window, but a missing/unknown
    session flag (None) never hard-fails."""
    if row.get("grade") not in ("A", "B"):
        return False
    if BTMM123_GRADE_A_ONLY and row.get("grade") == "B":
        return False
    if BTMM123_SESSION_FILTER and row.get("in_active_session") is False:
        return False
    return True


def alert_btmm123_setup(pair: str, row: dict):
    """Fire when the BTMM 123 scanner signals a tradeable setup.

    Gates:
      - Alerts globally enabled (BTMM123_ALERTS_ENABLED)
      - Grade A always, Grade B unless BTMM123_GRADE_A_ONLY=true (same
        A+B convention as CRT/TDI123); Grade C never
      - R:R sanity on trade plan (min 1:0.8 on TP1)
    """
    if not BTMM123_ALERTS_ENABLED:
        return

    # M15 leg evaluated first and unconditionally — see alert_tdi123_setup's
    # matching fix note (2026-09-10): recursing after the H1 gates meant a
    # Grade A/B M15 setup was silently skipped whenever H1 didn't ALSO
    # independently qualify. Guarded by timeframe so a future scanner change
    # that nests "m15" one level deeper can't recurse unboundedly (review
    # finding 2026-09-10).
    if row.get("m15") and row.get("timeframe") != "M15":
        alert_btmm123_setup(pair, row["m15"])

    setup = row.get("setup")
    grade = row.get("grade")
    if setup not in ("BUY", "SELL"):
        return
    if not _should_alert_btmm123(row):
        log.debug("BTMM123 FILTERED %s: grade %s / session does not meet threshold", pair, grade)
        return

    plan = row.get("trade_plan") or {}
    entry = plan.get("entry")
    sl = plan.get("sl")
    tp1 = plan.get("tp1")
    if not (entry and sl and tp1):
        log.debug("BTMM123 SUPPRESSED %s: incomplete trade plan", pair)
        return

    sl_dist = abs(entry - sl)
    if sl_dist < 1e-9:
        log.warning("BTMM123 alert BLOCKED for %s: SL too tight (0 distance)", pair)
        return
    if not _check_rr(entry, sl, tp1, "buy" if setup == "BUY" else "sell", min_rr=0.8, symbol=pair):
        log.warning("BTMM123 alert BLOCKED for %s: bad R:R", pair)
        return

    if BTMM123_NEWS_FILTER:
        try:
            blocked = forexfactory.currencies_in_window(60, high_only=True)
        except Exception as e:  # noqa: BLE001
            blocked = set()
            log.debug("BTMM123 news check failed for %s (fail-open): %s", pair, e)
        if _news_blocks_pair(pair, blocked):
            hit = _pair_currencies(pair) & blocked
            log.info("BTMM123 SUPPRESSED %s: high-impact news within 60m (%s)",
                     pair, ",".join(sorted(hit)))
            return

    # Timeframe is part of the throttle key — an H1 and an M15 alert on the
    # same pair/direction/grade are distinct setups and must not collide
    # (see TDI123's identical convention).
    timeframe = row.get("timeframe") or "H1"
    rule = f"btmm123_{timeframe.lower()}_{row.get('setup_type', '123')}_{setup.lower()}_{grade}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    colour = 0xFFD700

    setup_type = row.get("setup_type", "123")
    setup_label = "BTMM Re-set" if setup_type == "reset" else "BTMM 123"
    pattern = row.get("pattern") or {}
    level = row.get("level") or {}
    hunt = row.get("stop_hunt") or {}
    p1, p2, p3 = pattern.get("p1", {}), pattern.get("p2", {}), pattern.get("p3", {})
    tp2, tp3 = plan.get("tp2"), plan.get("tp3")
    sl_pips = plan.get("sl_pips") or 0
    rr1 = plan.get("rr1") or 0

    if setup_type == "reset":
        reset_info = row.get("reset") or {}
        pattern_field = {
            "name": "Re-set",
            "value": f"200EMA false-breakout at `{_fmt_price(reset_info.get('extreme'), pair)}`",
            "inline": False,
        }
    else:
        pattern_field = {
            "name": "123 Pattern",
            "value": f"1 `{_fmt_price(p1.get('price'), pair)}` → 2 `{_fmt_price(p2.get('price'), pair)}` → 3 `{_fmt_price(p3.get('price'), pair)}`  ({pattern.get('leg1_range_pips', 0)} pips leg-1)",
            "inline": False,
        }

    fields = [
        {"name": "Direction", "value": f"**{'📈 BUY' if setup == 'BUY' else '📉 SELL'}**", "inline": True},
        {"name": "Grade",     "value": f"⭐ **{grade} ({row.get('score', 0)}/17)**",        "inline": True},
        {"name": "Setup",     "value": f"**{setup_label}**",                               "inline": True},
        {"name": "🎯 Entry",     "value": f"`{_fmt_price(entry, pair)}`",                  "inline": True},
        {"name": "🛑 Stop Loss", "value": f"`{_fmt_price(sl, pair)}`  (−{sl_pips} pips)",  "inline": True},
        {"name": "Risk:Reward",  "value": f"**1 : {rr1:.1f}**",                            "inline": True},
        pattern_field,
        {"name": "EMA Level",
         "value": f"{'Level II' if level.get('level_ii') else 'Level I' if level.get('level_i') else 'none'} ({level.get('count', 0)}/5 aligned)",
         "inline": True},
        {"name": "Stop Hunt",
         "value": "✅ confirmed" if hunt.get("active") else "—",
         "inline": True},
    ]

    embed = {
        "title":       f"{arrow} ⭐ {pair} — {setup_label} {setup}",
        "description": row.get("notes") or "BTMM 123 — classic 1-2-3 reversal, BTMM-doctrine confirmed.",
        "color":       colour,
        "fields":      fields,
        "footer":      {"text": f"BTMM 123 · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("BTMM123 alert sent: %s %s grade=%s score=%d", pair, setup, grade, row.get("score", 0))


def _btmm123_watch_reasons(pair: str, row: dict) -> list[tuple[str, bool, str]]:
    """Checklist mirroring alert_btmm123_setup's gates, in the same order."""
    grade = row.get("grade")
    checks: list[tuple[str, bool, str]] = []

    if grade == "A":
        checks.append(("Grade", True, "A"))
    else:
        grade_ok = not BTMM123_GRADE_A_ONLY
        checks.append(("Grade B allowed", grade_ok,
                       "allowed" if grade_ok else "BTMM123_GRADE_A_ONLY is on"))

    if BTMM123_SESSION_FILTER:
        checks.append(("Active session", row.get("in_active_session") is not False,
                       row.get("session", "?")))

    plan = row.get("trade_plan") or {}
    entry, sl, tp1 = plan.get("entry"), plan.get("sl"), plan.get("tp1")
    plan_complete = bool(entry and sl and tp1)
    checks.append(("Trade plan complete", plan_complete, "ok" if plan_complete else "incomplete"))

    if plan_complete:
        setup = row.get("setup")
        rr_ok = _check_rr(entry, sl, tp1, "buy" if setup == "BUY" else "sell", min_rr=0.8, symbol=pair)
        checks.append(("R:R ≥ 0.8", rr_ok, "ok" if rr_ok else "below minimum"))

    if BTMM123_NEWS_FILTER:
        try:
            blocked = forexfactory.currencies_in_window(60, high_only=True)
        except Exception:  # noqa: BLE001
            blocked = set()
        news_clear = not _news_blocks_pair(pair, blocked)
        checks.append(("No high-impact news window", news_clear,
                       "clear" if news_clear else "blocked within 60m"))

    return checks


def alert_btmm123_watch(pair: str, row: dict):
    """Post a 'still forming' embed for Grade A/B BTMM123 setups that don't
    yet clear every gate in alert_btmm123_setup. Independent of BTMM123_
    ALERTS_ENABLED by design — lets you watch setups develop while real
    BTMM123 alerts stay off. Skips silently once everything passes."""
    if not BTMM123_WATCH_ALERTS_ENABLED:
        return

    # M15 leg evaluated first and unconditionally — see alert_tdi123_setup's
    # matching fix note (2026-09-10). Guarded by timeframe so a future scanner
    # change that nests "m15" one level deeper can't recurse unboundedly
    # (review finding 2026-09-10).
    if row.get("m15") and row.get("timeframe") != "M15":
        alert_btmm123_watch(pair, row["m15"])

    grade = row.get("grade")
    setup = row.get("setup")
    if grade not in ("A", "B") or setup not in ("BUY", "SELL"):
        return

    try:
        checks = _btmm123_watch_reasons(pair, row)
    except Exception as e:  # noqa: BLE001
        log.warning("BTMM123 watch checklist crashed for %s: %s", pair, e)
        return

    # Skip only when the real alert is actually live — see alert_tdi123_watch's
    # matching fix note (2026-09-10). Under BTMM123's own shipped defaults
    # (BTMM123_ALERTS_ENABLED=false, BTMM123_WATCH_ALERTS_ENABLED=true) the
    # unconditional version silently produced ZERO notification for a fully-
    # qualified setup — exactly the moment the user most needs to hear about it.
    if BTMM123_ALERTS_ENABLED and all(passed for _, passed, _ in checks):
        return

    timeframe = row.get("timeframe") or "H1"
    setup_type = row.get("setup_type", "123")
    rule = f"btmm123watch_{timeframe.lower()}_{setup_type}_{setup.lower()}_{grade}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    setup_label = "BTMM Re-set" if setup_type == "reset" else "BTMM 123"
    lines = [f"{'✅' if ok else '❌'} {label} — {detail}" for label, ok, detail in checks]

    embed = {
        "title":       f"👀 {arrow} {pair} — {setup_label} {setup} watching (Grade {grade})",
        "description": "Still forming — not a trade signal yet.\n" + "\n".join(lines),
        "color":       _COLOURS["watch"],
        "fields":      [
            {"name": "Grade", "value": f"**{grade} ({row.get('score', 0)}/17)**", "inline": True},
        ],
        "footer":      {"text": f"{setup_label} · Monitoring only · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("BTMM123 watch alert sent: %s %s grade=%s", pair, setup, grade)


# ── VWAP Mean Reversion (M15) ────────────────────────────────────────────────

def _should_alert_vwap_mr(row: dict) -> bool:
    """Hard floor: only fire on score >= VWAP_MR_MIN_SCORE (default 9/10),
    regardless of grade label. Grade C and NO-TRADE never alert."""
    if row.get("grade") not in ("A", "B"):
        return False
    if VWAP_MR_GRADE_A_ONLY and row.get("grade") == "B":
        return False
    return (row.get("score") or 0) >= VWAP_MR_MIN_SCORE


def alert_vwap_mr_setup(pair: str, row: dict, is_test: bool = False):
    """Fire when the VWAP Mean Reversion scanner confirms a Grade A/B setup
    at or above VWAP_MR_MIN_SCORE.

    `row` is one element from vwap_mean_reversion_strategy.analyze_universe()['pairs'].
    Pass is_test=True to send a clearly-labelled one-off test embed that
    bypasses the grade/score gate and the hourly throttle (does not mark the
    rule as sent, so it never suppresses a real alert that follows) —
    matches the VWAP+9EMA convention this replaces.
    """
    if not is_test and not VWAP_MR_ALERTS_ENABLED:
        return

    setup = row.get("setup")
    grade = row.get("grade")
    if setup not in ("BUY", "SELL"):
        return
    if not is_test and not _should_alert_vwap_mr(row):
        log.debug("VWAP_MR FILTERED %s: grade %s / score %s below threshold", pair, grade, row.get("score"))
        return

    entry, sl, tp1 = row.get("entry"), row.get("sl"), row.get("tp1")
    if not (entry and sl and tp1):
        log.debug("VWAP_MR SUPPRESSED %s: incomplete trade plan", pair)
        return

    # R:R / direction sanity (every trade-plan alert in this file gates on
    # this before posting) -- see 2026-09-10 review finding on the retired
    # VWAP+9EMA alert, which had no such check.
    if not _check_rr(entry, sl, tp1, "buy" if setup == "BUY" else "sell", min_rr=0.8, symbol=pair):
        log.warning("VWAP_MR alert BLOCKED for %s: bad R:R", pair)
        return

    # News gate: suppress if either of the pair's currencies has a high-
    # impact event within 60 min. Fails open -- a feed error never blocks
    # a trade (same convention as TDI123/BTMM123).
    if not is_test and VWAP_MR_NEWS_FILTER:
        try:
            blocked = forexfactory.currencies_in_window(60, high_only=True)
        except Exception as e:  # noqa: BLE001
            blocked = set()
            log.debug("VWAP_MR news check failed for %s (fail-open): %s", pair, e)
        if _news_blocks_pair(pair, blocked):
            hit = _pair_currencies(pair) & blocked
            log.info("VWAP_MR SUPPRESSED %s: high-impact news within 60m (%s)",
                     pair, ",".join(sorted(hit)))
            return

    rule = f"vwap_mr_{setup.lower()}_{grade}"
    if not is_test and _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    colour = 0x60A5FA if not is_test else 0x9CA3AF

    pip = _pip_size(entry, pair)
    sl_pips = row.get("sl_pips") or round(abs(entry - sl) / pip, 1)
    tp1_pips = row.get("tp1_pips") or round(abs(tp1 - entry) / pip, 1)
    rr = tp1_pips / sl_pips if sl_pips else 0

    ext = row.get("extension") or {}
    conf = row.get("confirmation") or {}
    conf_label = {
        "rejection_wick": "Rejection wick", "close_inside_band": "Close back inside band",
        "two_bar_pattern": "Two-bar reversal pattern",
    }.get(conf.get("type"), conf.get("type") or "—")

    fields = [
        {"name": "Direction",   "value": f"**{'📈 BUY' if setup == 'BUY' else '📉 SELL'}**", "inline": True},
        {"name": "Grade",       "value": f"⭐ **{grade} ({row.get('score', 0)}/10)**",        "inline": True},
        {"name": "Regime (ER)", "value": f"{row.get('er', 0):.2f} (< 0.35 required)",         "inline": True},
        {"name": "🎯 Entry",    "value": f"`{_fmt_price(entry, pair)}`",                      "inline": True},
        {"name": "🛑 Stop Loss","value": f"`{_fmt_price(sl, pair)}`  (−{sl_pips} pips)",       "inline": True},
        {"name": "🎯 TP1 (VWAP)","value": f"`{_fmt_price(tp1, pair)}`  (+{tp1_pips} pips)",   "inline": True},
        {"name": "Risk:Reward", "value": f"**1 : {rr:.1f}**",                                 "inline": True},
        {"name": "Extension",   "value": f"z = {ext.get('z', 0):.2f}",                        "inline": True},
        {"name": "Confirmation","value": conf_label,                                          "inline": True},
        {"name": "Exhaustion vol", "value": "✅" if row.get("exhaustion_volume") else "—",    "inline": True},
        {"name": "Fading vol",  "value": "✅" if row.get("fading_volume") else "—",           "inline": True},
        {"name": "Session",     "value": row.get("session_status") or "—",                    "inline": True},
    ]
    if row.get("tp2") is not None:
        tp2_pips = row.get("tp2_pips") or round(abs(row["tp2"] - entry) / pip, 1)
        fields.append({"name": "🎯 TP2 (overshoot)", "value": f"`{_fmt_price(row['tp2'], pair)}`  (+{tp2_pips} pips)", "inline": True})

    title = f"{arrow} {pair} — VWAP Mean Reversion {setup}"
    if is_test:
        title = f"🧪 TEST — {title}"

    embed = {
        "title":       title,
        "description": row.get("notes") or "VWAP Mean Reversion — extension + stall confirmation.",
        "color":       colour,
        "fields":      fields,
        "footer":      {"text": f"VWAP Mean Reversion (M15) · {_now_utc_str()} ({_now_sast_str()} SAST)"
                                  + (" · TEST SEND, not a live signal" if is_test else "")},
    }
    if _post_discord(embed):
        if not is_test:
            _mark_sent(pair, rule)
        log.info("VWAP_MR alert sent%s: %s %s grade=%s score=%d",
                 " (TEST)" if is_test else "", pair, setup, grade, row.get("score", 0))


def _vwap_mr_watch_reasons(pair: str, row: dict) -> list[tuple[str, bool, str]]:
    """Checklist mirroring alert_vwap_mr_setup's gates, in the same order."""
    checks: list[tuple[str, bool, str]] = []

    regime_ok = bool(row.get("regime_ok"))
    er = row.get("er")
    checks.append(("Regime filter (ER < 0.35)", regime_ok,
                   f"{er:.2f}" if er is not None else "unavailable"))

    ext = row.get("extension")
    checks.append(("Extension (|z| >= 2.0)", bool(ext),
                   f"z={ext['z']:.2f}" if ext else "none yet"))

    conf = row.get("confirmation")
    checks.append(("Stall confirmation", bool(conf),
                   conf["type"] if conf else "not yet / timed out"))

    score = row.get("score") or 0
    checks.append((f"Score >= {VWAP_MR_MIN_SCORE}/10", score >= VWAP_MR_MIN_SCORE, f"{score}/10"))

    entry, sl, tp1 = row.get("entry"), row.get("sl"), row.get("tp1")
    plan_complete = bool(entry and sl and tp1)
    checks.append(("Trade plan complete", plan_complete, "ok" if plan_complete else "incomplete"))

    if plan_complete:
        setup = row.get("setup")
        rr_ok = _check_rr(entry, sl, tp1, "buy" if setup == "BUY" else "sell", min_rr=0.8, symbol=pair)
        checks.append(("R:R >= 0.8", rr_ok, "ok" if rr_ok else "below minimum"))

    if VWAP_MR_NEWS_FILTER:
        try:
            blocked = forexfactory.currencies_in_window(60, high_only=True)
        except Exception:  # noqa: BLE001
            blocked = set()
        news_clear = not _news_blocks_pair(pair, blocked)
        checks.append(("No high-impact news window", news_clear,
                       "clear" if news_clear else "blocked within 60m"))

    return checks


def alert_vwap_mr_watch(pair: str, row: dict):
    """Post a 'still forming' embed for VWAP Mean Reversion setups that
    don't yet clear every gate in alert_vwap_mr_setup. Independent of
    VWAP_MR_ALERTS_ENABLED by design, matching the TDI123/BTMM123 watch
    pattern (2026-09-10) -- including its Critical fix: skip on an all-pass
    result only when the real alert's own switch is actually enabled."""
    if not VWAP_MR_WATCH_ALERTS_ENABLED:
        return

    setup = row.get("setup")
    if setup not in ("BUY", "SELL"):
        return

    try:
        checks = _vwap_mr_watch_reasons(pair, row)
    except Exception as e:  # noqa: BLE001
        log.warning("VWAP_MR watch checklist crashed for %s: %s", pair, e)
        return

    if VWAP_MR_ALERTS_ENABLED and all(passed for _, passed, _ in checks):
        return  # everything passes and the real alert is live -- it already fired this

    rule = f"vwap_mr_watch_{setup.lower()}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    lines = [f"{'✅' if ok else '❌'} {label} — {detail}" for label, ok, detail in checks]

    embed = {
        "title":       f"👀 {arrow} {pair} — VWAP Mean Reversion {setup} watching",
        "description": "Still forming — not a trade signal yet.\n" + "\n".join(lines),
        "color":       _COLOURS["watch"],
        "fields":      [{"name": "Grade", "value": f"**{row.get('grade', 'NO-TRADE')} ({row.get('score', 0)}/10)**", "inline": True}],
        "footer":      {"text": f"VWAP Mean Reversion (M15) · Monitoring only · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("VWAP_MR watch alert sent: %s %s", pair, setup)
