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
from config import DISCORD_WEBHOOK_URL

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
    if grade not in ("A", "B"):
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

    fields = [
        {"name": "Setup",        "value": f"**{setup}**",                                            "inline": True},
        {"name": "Grade",        "value": f"{grade_badge}**{grade} ({row.get('score', 0)}/10)**",    "inline": True},
        {"name": "Key Time",     "value": f"**{kt}** · {row.get('key_time_window_sast', '')}",       "inline": True},
        {"name": "Entry",        "value": entry_str,                                                  "inline": True},
        {"name": "1AM Candle",   "value": c1_str,                                                     "inline": True},
        {"name": "CRT H/L",      "value": crt_str,                                                    "inline": True},
        {"name": "Profile",      "value": f"{row.get('profile_type', '?')} — {row.get('profile_label', '')}", "inline": False},
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




def alert_snr_setup(pair: str, row: dict):
    """Fire when Malaysian SNR Emperor signals a tradeable setup.

    QUALITY GATE — only sends alerts that are actually tradeable:
    - Grade A or B (score ≥ 7/15)
    - Active storyline with confirmed H4 breakout
    - Entry tier = Low or Medium risk (NOT high risk / 50-50)
    - Setup confidence = high or medium

    This ensures only setups that passed the Emperor's 5-action SOP
    (Zone → Rejection → Breakout → Confluence → Entry) reach Discord.
    """
    setup = row.get("setup")
    grade = row.get("grade")
    if setup not in ("BUY", "SELL"):
        return
    if grade not in ("A", "B"):
        return

    storyline = row.get("storyline", {})
    entry_tier = row.get("entry_tier", {})
    tier = entry_tier.get("tier", "no_setup")
    confidence = entry_tier.get("confidence", "low")
    setup_num = entry_tier.get("setup_num", 0)

    # QUALITY GATE: reject high-risk (50/50) and no-setup tiers
    if tier in ("no_setup", "high"):
        log.debug("SNR SUPPRESSED %s: tier=%s (only low/medium risk sent)", pair, tier)
        return

    # QUALITY GATE: storyline must be confirmed (H4 BO)
    if not storyline.get("confirmed"):
        log.debug("SNR SUPPRESSED %s: storyline not confirmed", pair)
        return

    # QUALITY GATE: confidence must be at least medium
    if confidence == "low" or confidence == "none":
        log.debug("SNR SUPPRESSED %s: confidence=%s", pair, confidence)
        return

    # R:R sanity check — reject garbage trade plans before sending to Discord
    plan = row.get("trade_plan", {})
    plan_entry_chk = plan.get("entry")
    plan_sl_chk = plan.get("sl")
    plan_tp1_chk = plan.get("tp1")
    if plan_entry_chk and plan_sl_chk and plan_tp1_chk:
        direction_rr = "buy" if setup == "BUY" else "sell"
        if not _check_rr(plan_entry_chk, plan_sl_chk, plan_tp1_chk, direction_rr, min_rr=0.8, symbol=pair):
            log.warning("SNR alert BLOCKED for %s: bad R:R — trade plan invalid", pair)
            return

    from_level = storyline.get("from_level")
    rule = f"msnr_{setup.lower()}_{tier}_{setup_num}_{_fmt_price(from_level) if from_level else 'x'}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    grade_badge = "⭐ " if grade == "A" else ""
    colour = 0xFFD700 if grade == "A" else (_COLOURS["strong_buy"] if setup == "BUY" else _COLOURS["strong_sell"])

    # Emperor setup names
    setup_names = {1: "High Risk (50/50)", 2: "Medium Risk", 3: "Low Risk (BO+QM)",
                   4: "Continuation"}
    setup_label = setup_names.get(setup_num, f"Setup {setup_num}")

    engulfing = row.get("engulfing_patterns", [])
    eng_parts = []
    for e in engulfing[-3:]:
        zone = e.get("zone_status", "")
        zone_badge = f" [{zone}]" if zone in ("PEZ", "FEZ") else ""
        eng_parts.append(f"{e['type']} ({e['direction']}){zone_badge}")
    eng_str = ", ".join(eng_parts) if eng_parts else "—"

    roadblocks = row.get("roadblocks", [])
    rb_str = ", ".join(f"`{_fmt_price(r['price'])}` ({r.get('snr_type', '')})"
                       for r in roadblocks[:3]) if roadblocks else "Clear ✅"

    tl_conf = row.get("tl_confluence", {})
    tl_str = (f"{tl_conf['tl_type']} TL + {tl_conf['snr_type']} SNR"
              if tl_conf.get("active") else "—")

    # SNR type breakdown
    snr_types = row.get("snr_types", {})
    type_parts = [f"{v} {k}" for k, v in snr_types.items() if v > 0]
    types_str = ", ".join(type_parts[:4]) if type_parts else "—"

    stl_from_type = storyline.get("from_snr_type", "classic")
    stl_str = "—"
    if from_level:
        to_str = _fmt_price(storyline.get("to_level")) if storyline.get("to_level") else "?"
        confirmed_badge = " ✅" if storyline.get("confirmed") else " ⏳"
        stl_str = (f"{storyline.get('direction', '?').capitalize()} from "
                   f"`{_fmt_price(from_level)}` → `{to_str}`"
                   f" ({stl_from_type}){confirmed_badge}")

    # ── Trade Plan (Entry / SL / TP1 / TP2 / R:R) ──────────────────
    plan = row.get("trade_plan", {})
    plan_entry = plan.get("entry")
    plan_sl    = plan.get("sl")
    plan_tp1   = plan.get("tp1")
    plan_tp2   = plan.get("tp2")
    plan_rr1   = plan.get("rr1", 0)
    plan_rr2   = plan.get("rr2", 0)
    sl_pips    = plan.get("sl_pips", 0)
    tp1_pips   = plan.get("tp1_pips", 0)
    tp2_pips   = plan.get("tp2_pips", 0)

    direction = "buy" if setup == "BUY" else "sell"

    fields = [
        {"name": "Direction",    "value": f"**{'📈 BUY' if setup == 'BUY' else '📉 SELL'}**",   "inline": True},
        {"name": "Grade",        "value": f"{grade_badge}**{grade} ({row.get('score', 0)}/15)**", "inline": True},
        {"name": "Entry Tier",   "value": f"**{setup_label}** ({confidence})",                   "inline": True},
    ]

    # Trade plan block — only if we have calculated levels
    if plan_entry and plan_sl and plan_tp1:
        fields += [
            {"name": "🎯 Entry",  "value": f"`{_fmt_price(plan_entry)}`",                                  "inline": True},
            {"name": "🛑 Stop Loss", "value": f"`{_fmt_price(plan_sl)}`  (−{sl_pips} pips)",               "inline": True},
            {"name": "Risk:Reward", "value": f"**1 : {plan_rr1:.1f}**" + (f" / 1 : {plan_rr2:.1f}" if plan_rr2 else ""), "inline": True},
            {"name": "✅ TP1",     "value": f"`{_fmt_price(plan_tp1)}`  (+{tp1_pips} pips)",               "inline": True},
        ]
        if plan_tp2:
            fields.append(
                {"name": "🎯 TP2",     "value": f"`{_fmt_price(plan_tp2)}`  (+{tp2_pips} pips)",           "inline": True},
            )
        # Blank inline for alignment
        fields.append({"name": "​", "value": "​", "inline": True})
    else:
        fields.append(
            {"name": "Entry", "value": f"`{_fmt_price(entry_tier.get('entry_price'))}`" if entry_tier.get("entry_price") else "—", "inline": True},
        )

    fields += [
        {"name": "Storyline",    "value": stl_str,                                               "inline": False},
        {"name": "Engulfing",    "value": eng_str,                                               "inline": True},
        {"name": "X-Factor",     "value": tl_str,                                                "inline": True},
        {"name": "Roadblocks",   "value": rb_str,                                                "inline": True},
    ]

    embed = {
        "title":       f"{arrow} {grade_badge}{pair} — SNR {setup} ({setup_label})",
        "description": row.get("notes") or "Malaysian SNR Emperor — storyline confirmed with entry trigger.",
        "color":       colour,
        "fields":      fields,
        "footer":      {"text": f"Malaysian SNR Emperor · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("SNR alert sent: %s %s grade=%s tier=%s setup=%d conf=%s",
                 pair, setup, grade, tier, setup_num, confidence)


def alert_snr_m15_setup(pair: str, row: dict):
    """Fire when the M15 SNR Fast Scanner signals a tradeable setup.

    QUALITY GATE — same standards as H4 scanner but tagged as M15:
    - Grade A or B (score >= 7/15)
    - Active storyline with confirmed M15 breakout
    - Entry tier = Low or Medium risk
    - Confidence = high or medium

    Throttle key includes "m15_" prefix so M15 and H4 alerts
    are tracked independently (you can get both for the same pair).
    """
    setup = row.get("setup")
    grade = row.get("grade")
    if setup not in ("BUY", "SELL"):
        return
    if grade not in ("A", "B"):
        return

    storyline = row.get("storyline", {})
    entry_tier = row.get("entry_tier", {})
    tier = entry_tier.get("tier", "no_setup")
    confidence = entry_tier.get("confidence", "low")
    setup_num = entry_tier.get("setup_num", 0)

    if tier in ("no_setup", "high"):
        log.debug("SNR-M15 SUPPRESSED %s: tier=%s", pair, tier)
        return
    if not storyline.get("confirmed"):
        log.debug("SNR-M15 SUPPRESSED %s: storyline not confirmed", pair)
        return
    if confidence in ("low", "none"):
        log.debug("SNR-M15 SUPPRESSED %s: confidence=%s", pair, confidence)
        return

    # R:R sanity check — M15 has tighter stops, more susceptible to noise
    plan = row.get("trade_plan", {})
    plan_entry = plan.get("entry")
    plan_sl = plan.get("sl")
    plan_tp1 = plan.get("tp1")
    if plan_entry and plan_sl and plan_tp1:
        direction_rr = "buy" if setup == "BUY" else "sell"
        if not _check_rr(plan_entry, plan_sl, plan_tp1, direction_rr, min_rr=0.8, symbol=pair):
            log.warning("SNR-M15 alert BLOCKED for %s: bad R:R — trade plan invalid", pair)
            return

    from_level = storyline.get("from_level")
    rule = f"m15_msnr_{setup.lower()}_{tier}_{setup_num}_{_fmt_price(from_level) if from_level else 'x'}"
    if _is_throttled(pair, rule):
        return

    arrow = "📈" if setup == "BUY" else "📉"
    grade_badge = "⭐ " if grade == "A" else ""
    colour = 0xFFD700 if grade == "A" else (_COLOURS["strong_buy"] if setup == "BUY" else _COLOURS["strong_sell"])

    setup_names = {1: "High Risk (50/50)", 2: "Medium Risk", 3: "Low Risk (BO+QM)",
                   4: "Continuation"}
    setup_label = setup_names.get(setup_num, f"Setup {setup_num}")

    engulfing = row.get("engulfing_patterns", [])
    eng_parts = []
    for e in engulfing[-3:]:
        zone = e.get("zone_status", "")
        zone_badge = f" [{zone}]" if zone in ("PEZ", "FEZ") else ""
        eng_parts.append(f"{e['type']} ({e['direction']}){zone_badge}")
    eng_str = ", ".join(eng_parts) if eng_parts else "—"

    roadblocks = row.get("roadblocks", [])
    rb_str = ", ".join(f"`{_fmt_price(r['price'])}` ({r.get('snr_type', '')})"
                       for r in roadblocks[:3]) if roadblocks else "Clear ✅"

    tl_conf = row.get("tl_confluence", {})
    tl_str = (f"{tl_conf['tl_type']} TL + {tl_conf['snr_type']} SNR"
              if tl_conf.get("active") else "—")

    stl_from_type = storyline.get("from_snr_type", "classic")
    stl_str = "—"
    if from_level:
        to_str = _fmt_price(storyline.get("to_level")) if storyline.get("to_level") else "?"
        confirmed_badge = " ✅" if storyline.get("confirmed") else " ⏳"
        stl_str = (f"{storyline.get('direction', '?').capitalize()} from "
                   f"`{_fmt_price(from_level)}` → `{to_str}`"
                   f" ({stl_from_type}){confirmed_badge}")

    plan = row.get("trade_plan", {})
    plan_entry = plan.get("entry")
    plan_sl    = plan.get("sl")
    plan_tp1   = plan.get("tp1")
    plan_tp2   = plan.get("tp2")
    plan_rr1   = plan.get("rr1", 0)
    plan_rr2   = plan.get("rr2", 0)
    sl_pips    = plan.get("sl_pips", 0)
    tp1_pips   = plan.get("tp1_pips", 0)
    tp2_pips   = plan.get("tp2_pips", 0)

    fields = [
        {"name": "Direction",    "value": f"**{'📈 BUY' if setup == 'BUY' else '📉 SELL'}**",   "inline": True},
        {"name": "Grade",        "value": f"{grade_badge}**{grade} ({row.get('score', 0)}/15)**", "inline": True},
        {"name": "Entry Tier",   "value": f"**{setup_label}** ({confidence})",                   "inline": True},
    ]

    if plan_entry and plan_sl and plan_tp1:
        fields += [
            {"name": "🎯 Entry",  "value": f"`{_fmt_price(plan_entry)}`",                                  "inline": True},
            {"name": "🛑 Stop Loss", "value": f"`{_fmt_price(plan_sl)}`  (−{sl_pips} pips)",               "inline": True},
            {"name": "Risk:Reward", "value": f"**1 : {plan_rr1:.1f}**" + (f" / 1 : {plan_rr2:.1f}" if plan_rr2 else ""), "inline": True},
            {"name": "✅ TP1",     "value": f"`{_fmt_price(plan_tp1)}`  (+{tp1_pips} pips)",               "inline": True},
        ]
        if plan_tp2:
            fields.append(
                {"name": "🎯 TP2",     "value": f"`{_fmt_price(plan_tp2)}`  (+{tp2_pips} pips)",           "inline": True},
            )
        fields.append({"name": "​", "value": "​", "inline": True})
    else:
        fields.append(
            {"name": "Entry", "value": f"`{_fmt_price(entry_tier.get('entry_price'))}`" if entry_tier.get("entry_price") else "—", "inline": True},
        )

    fields += [
        {"name": "Storyline",    "value": stl_str,                                               "inline": False},
        {"name": "Engulfing",    "value": eng_str,                                               "inline": True},
        {"name": "X-Factor",     "value": tl_str,                                                "inline": True},
        {"name": "Roadblocks",   "value": rb_str,                                                "inline": True},
    ]

    embed = {
        "title":       f"{arrow} {grade_badge}{pair} — ⚡ M15 SNR {setup} ({setup_label})",
        "description": row.get("notes") or "Malaysian SNR Emperor M15 Fast Scanner — early entry signal.",
        "color":       colour,
        "fields":      fields,
        "footer":      {"text": f"Malaysian SNR Emperor ⚡M15 · {_now_utc_str()} ({_now_sast_str()} SAST)"},
    }
    if _post_discord(embed):
        _mark_sent(pair, rule)
        log.info("SNR-M15 alert sent: %s %s grade=%s tier=%s setup=%d conf=%s",
                 pair, setup, grade, tier, setup_num, confidence)


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

    # Rule 2 — Named setup (Safety Trade etc.) — BOTH confidences must be high
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
