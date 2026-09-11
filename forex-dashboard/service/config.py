"""Service configuration: priority pairs, Twelve Data keys, credit limits.

Keys are read from environment (or .env). Never commit .env.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

SERVICE_ROOT = Path(__file__).parent.resolve()
load_dotenv(SERVICE_ROOT / ".env")

DB_PATH = SERVICE_ROOT / "candles.db"

SERVICE_PORT = int(os.environ.get("SERVICE_PORT", "3002"))
SERVICE_HOST = os.environ.get("SERVICE_HOST", "127.0.0.1")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# MT5 provider — primary source when terminal is running and logged in.
MT5_ENABLED = os.environ.get("MT5_ENABLED", "true").lower() in ("1", "true", "yes")
MT5_SYMBOL_SUFFIX = os.environ.get("MT5_SYMBOL_SUFFIX", "m")

DAILY_CREDIT_LIMIT = int(os.environ.get("DAILY_CREDIT_LIMIT", "800"))
CREDIT_SAFETY_MARGIN = int(os.environ.get("CREDIT_SAFETY_MARGIN", "50"))

PRIORITY_PAIRS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD",
    "USD/CAD", "NZD/USD", "EUR/GBP", "EUR/JPY", "EUR/CHF",
    "EUR/AUD", "GBP/JPY", "GBP/CHF", "AUD/JPY", "CAD/JPY",
    "GBP/AUD", "AUD/CAD", "AUD/CHF", "AUD/NZD",  # AUD crosses — clean structure
    "XAU/USD", "XAG/USD",  # Gold, Silver — MT5 maps to XAUUSDm / XAGUSDm
    "DE30", "US30", "USTEC",  # DAX 40, Dow 30, Nasdaq 100 — MT5 maps to DE30m / US30m / USTECm
    "DXY",  # US Dollar Index — MT5 maps to DXYm on Exness
]

INTERVAL_SECS = {
    "1min":   60,
    "5min":   300,
    "15min":  900,
    "30min":  1800,
    "1h":     3600,
    "4h":     14400,
    "1day":   86400,
}

DEFAULT_INTERVAL = "15min"
DEFAULT_BACKFILL = 3200

# Per-strategy Discord alert toggles. Dashboard /btmm display is unaffected —
# these only gate the scheduler's Discord dispatch.
BTMM_ALERTS_ENABLED = os.environ.get("BTMM_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")

# BTMM A+-only mode: when True, the ONLY BTMM alert sent is the highest-tier
# "A+ Setup" (gold embed, "This is a highest-tier BTMM setup"). All other BTMM
# alerts — Strong Buy/Sell, named/Safety setups, 5/13 cross, ADR warnings, and
# kill-zone-open pings — are suppressed as noise. Set to "false" to get the
# full (noisier) BTMM alert set back.
BTMM_APLUS_ONLY = os.environ.get("BTMM_APLUS_ONLY", "true").lower() in ("1", "true", "yes")

# 1AM CRT grade gate — separate from the SNR flag above because CRT fires fewer
# setups and Grade B setups are acceptable signals there (they don't push as many
# alerts as SNR). Default false = A + B both sent to Discord.
CRT_GRADE_A_ONLY = os.environ.get("CRT_GRADE_A_ONLY", "false").lower() in ("1", "true", "yes")

# 5AM CRT grade gate — NY Open kill-zone session. A + B (default false, same as 1AM CRT).
CRT_5AM_GRADE_A_ONLY = os.environ.get("CRT_5AM_GRADE_A_ONLY", "false").lower() in ("1", "true", "yes")

# TDI Cycle 123 — improvements-on-BTMM scanner (FSO_TDI + 123 Peak + divergence).
# Grade gating same convention as CRT: default A + B both sent. Set "true" to
# restrict to Grade A only if signal volume gets noisy.
TDI123_ALERTS_ENABLED = os.environ.get("TDI123_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")
TDI123_GRADE_A_ONLY = os.environ.get("TDI123_GRADE_A_ONLY", "false").lower() in ("1", "true", "yes")
# Tier-1 timing filters (added 2026-07-23 after a 60-day winner/loser analysis).
# SESSION (default ON): only fire in the active London+NY block (07:00-16:00 UTC).
#   A clean 4-arm A/B flipped expectancy from -0.40R to +0.25R just by removing
#   the Asian dead-zone (which lost -0.83R over 33 signals).
# ADR (default OFF): skip when TP1 is further than the day's remaining ADR.
#   The diagnosis is real (88% of signals are "unreachable") but as a HARD gate
#   it's too aggressive — it cut the sample to 9 trades (2 when combined with
#   session), starving the strategy. Kept available (fields still exposed) for a
#   future adaptive-TARGET tweak rather than a filter. Enable via env if desired.
TDI123_SESSION_FILTER = os.environ.get("TDI123_SESSION_FILTER", "true").lower() in ("1", "true", "yes")
TDI123_ADR_FILTER = os.environ.get("TDI123_ADR_FILTER", "false").lower() in ("1", "true", "yes")
# NEWS (default ON): suppress a signal when either of the pair's currencies has a
# high-impact ForexFactory event within ±window minutes. Doctrine: "avoid trading
# during major news — price action is unpredictable" — a release drives a wick
# that blows through the ATR stop regardless of setup quality. Live-only (the FF
# feed is current-week), so it can't be backtested retrospectively.
TDI123_NEWS_FILTER = os.environ.get("TDI123_NEWS_FILTER", "true").lower() in ("1", "true", "yes")
TDI123_NEWS_WINDOW_MIN = int(os.environ.get("TDI123_NEWS_WINDOW_MIN", "60"))
# JOURNAL (default ON): log every fired alert to the trades table and auto-resolve
# its outcome (win at TP1 / loss at SL) from cached candles each refresh, so the
# dashboard builds a REAL track record instead of relying on backtests.
TDI123_JOURNAL_ENABLED = os.environ.get("TDI123_JOURNAL_ENABLED", "true").lower() in ("1", "true", "yes")
# WATCH (default ON): post a grey "still forming" embed for A/B setups that
# don't yet clear every gate above, listing exactly what's missing, so a
# setup can be monitored on Discord as it develops instead of only on the
# dashboard. Independent of TDI123_ALERTS_ENABLED — never duplicates a real
# alert (skips silently once every gate has actually passed).
TDI123_WATCH_ALERTS_ENABLED = os.environ.get("TDI123_WATCH_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")

# BTMM 123 — classic 1-2-3 price action confirmed by BTMM doctrine (EMA Level
# cascade + stop hunt + Asian range) instead of a TDI/oscillator dependency.
# Replaces the removed Malaysian SNR Emperor slot.
# ALERTS default FALSE: this is a brand-new strategy with no track record.
# TDI Cycle 123's own history (looked fine on paper, proved breakeven-to-
# negative only after a proper walk-forward backtest) is why — BTMM 123 stays
# dashboard-visible/Discord-silent until a backtest shows a real edge.
BTMM123_ALERTS_ENABLED = os.environ.get("BTMM123_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")
BTMM123_SESSION_FILTER = os.environ.get("BTMM123_SESSION_FILTER", "true").lower() in ("1", "true", "yes")
BTMM123_NEWS_FILTER = os.environ.get("BTMM123_NEWS_FILTER", "true").lower() in ("1", "true", "yes")
# Grade gate — same convention as CRT/TDI123: default false = Grade A + B
# both sent to Discord. Set "true" to restrict to Grade A only.
BTMM123_GRADE_A_ONLY = os.environ.get("BTMM123_GRADE_A_ONLY", "false").lower() in ("1", "true", "yes")
# WATCH (default ON): same "still forming" monitoring embed as TDI123_WATCH_
# ALERTS_ENABLED, deliberately independent of BTMM123_ALERTS_ENABLED — lets
# you watch A/B setups develop even while real BTMM123 alerts stay off.
BTMM123_WATCH_ALERTS_ENABLED = os.environ.get("BTMM123_WATCH_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")

# VWAP Mean Reversion (M15) — replaces VWAP+9EMA (2026-09-11). Adapted from
# an external US-equity research spec; see
# docs/superpowers/specs/2026-09-10-vwap-mean-reversion-strategy-design.md
# for the full rationale and every deviation from the source document.
# MIN_SCORE=9 carries forward the user's VWAP9EMA tightening (2026-09-10):
# only the best 9/10 and 10/10 setups reach Discord.
VWAP_MR_ALERTS_ENABLED = os.environ.get("VWAP_MR_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")
VWAP_MR_GRADE_A_ONLY = os.environ.get("VWAP_MR_GRADE_A_ONLY", "false").lower() in ("1", "true", "yes")
VWAP_MR_MIN_SCORE = int(os.environ.get("VWAP_MR_MIN_SCORE", "9"))
VWAP_MR_WATCH_ALERTS_ENABLED = os.environ.get("VWAP_MR_WATCH_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")
# News gate (doc §3.2/§3.3.5): suppress if either of the pair's currencies
# has a high-impact ForexFactory event within 60 min. Reuses the exact same
# infrastructure as TDI123/BTMM123 (forexfactory.currencies_in_window +
# alerts._news_blocks_pair), applied at the alert layer (see spec deviation #11).
VWAP_MR_NEWS_FILTER = os.environ.get("VWAP_MR_NEWS_FILTER", "true").lower() in ("1", "true", "yes")


def load_keys():
    """Return list of {name, value} for keys defined in env.

    Keys are numbered TWELVEDATA_KEY_1..N. Stops at first gap.
    """
    keys = []
    i = 1
    while True:
        value = os.environ.get(f"TWELVEDATA_KEY_{i}")
        if not value:
            break
        name = os.environ.get(f"TWELVEDATA_KEY_{i}_NAME", f"key{i}")
        keys.append({"name": name, "value": value})
        i += 1
    return keys
