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
# Bot token for the on-demand /analyse slash command (discord_bot.py). Separate
# credential from the webhook above -- a webhook URL can't authenticate a bot
# gateway connection.
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")

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
    "BTC/USD",  # Bitcoin — MT5 maps to BTCUSDm on Exness; spec in instruments.py
    "ETH/USD",  # Ethereum — MT5 maps to ETHUSDm on Exness; spec in instruments.py
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

# 5AM CRT grade gate — NY Open kill-zone session. A + B (default false).
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
# Asset classes that never get a TDI123 Discord alert (dashboard still shows
# them). Metals and crypto went 2 wins from 16 in the walk-forward replay and
# 0 from 12 in the live journal. Comma-separated; set empty to alert on all.
# Midline reset (source diagram: point 2 returns to the 50 line between the two
# extremes). Reported on every row as row["midline"]; only ENFORCED when this is
# true. Level is the sell-side threshold; buys use the mirror (100 - level).
TDI123_MIDLINE_RULE = os.environ.get("TDI123_MIDLINE_RULE", "false").lower() in ("1", "true", "yes")
TDI123_MIDLINE_LEVEL_SELL = float(os.environ.get("TDI123_MIDLINE_LEVEL_SELL", "50"))
# Which 1-2-3 the scanner looks for:
#   "peak"  — the multi-day peak formation read off the TDI (RSI beyond 68/32,
#             back to the 50 line, back into the 63-68 / 37-32 zone short of
#             point 1). This is what the source charts show.
#   "swing" — the older 5-bar-fractal price pattern (a ~14-hour double top).
# Default stays "swing": the 2026-10-13 replay of "peak" (27 pairs, gross of
# spread) gave H4 -0.04R over 78 formations, H1 -0.15R over 94 and M15 +0.09R
# over 151 — faithful to the source charts, but no edge shown. Set
# TDI123_PATTERN_MODE=peak to run it.
TDI123_PATTERN_MODE = os.environ.get("TDI123_PATTERN_MODE", "swing").strip().lower()
TDI123_EXCLUDED_CLASSES = tuple(
    c.strip().lower() for c in os.environ.get("TDI123_EXCLUDED_CLASSES", "metal,crypto").split(",") if c.strip())
# JOURNAL (default ON): log every fired alert to the trades table and auto-resolve
# its outcome (win at TP1 / loss at SL) from cached candles each refresh, so the
# dashboard builds a REAL track record instead of relying on backtests.
TDI123_JOURNAL_ENABLED = os.environ.get("TDI123_JOURNAL_ENABLED", "true").lower() in ("1", "true", "yes")
# WATCH (default OFF — 2026-09-13: disabled per user request, was firing
# "still forming" signals before they were ready and drowning out real
# alerts): post a grey "still forming" embed for A/B setups that don't yet
# clear every gate above, listing exactly what's missing, so a setup can be
# monitored on Discord as it develops instead of only on the dashboard.
# Independent of TDI123_ALERTS_ENABLED — never duplicates a real alert
# (skips silently once every gate has actually passed).
TDI123_WATCH_ALERTS_ENABLED = os.environ.get("TDI123_WATCH_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")

# Engulfing — trend-following engulfing-candle scanner (engulfing_strategy.py):
# H1 entries with the H4 trend and H4 entries with the daily trend, on forex,
# gold, silver and oil. Replaced BTMM 123 on 2026-10-13. ALERTS default FALSE: the walk-forward test over 27 pairs showed
# no edge (M15 with-trend, entry at the next open, 2R target: 13,550 signals,
# 33% wins, -0.01R before spread) and about 195 M15 signals a day.
ENGULF_ALERTS_ENABLED = os.environ.get("ENGULF_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")
ENGULF_GRADE_A_ONLY = os.environ.get("ENGULF_GRADE_A_ONLY", "true").lower() in ("1", "true", "yes")
ENGULF_NEWS_FILTER = os.environ.get("ENGULF_NEWS_FILTER", "true").lower() in ("1", "true", "yes")

# BTMM ID50 — M15 50-EMA-bounce retest scanner (design
# docs/superpowers/specs/2026-09-24-btmm-id50-scanner-design.md). Shares
# BTMM123's default-off discipline: dashboard-visible/Discord-silent until a
# walk-forward backtest shows an edge (same rationale as BTMM 123 above).
BTMM_ID50_ALERTS_ENABLED = os.environ.get("BTMM_ID50_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")
BTMM_ID50_SESSION_FILTER = os.environ.get("BTMM_ID50_SESSION_FILTER", "true").lower() in ("1", "true", "yes")
BTMM_ID50_NEWS_FILTER = os.environ.get("BTMM_ID50_NEWS_FILTER", "true").lower() in ("1", "true", "yes")
# Grade gate — same convention as BTMM123/TDI123: default true keeps ONLY the
# top-tier Grade A setups (A+ and A) sent; set "false" to allow Grade A + B.
BTMM_ID50_GRADE_A_ONLY = os.environ.get("BTMM_ID50_GRADE_A_ONLY", "true").lower() in ("1", "true", "yes")
# WATCH — same "still forming" monitoring embed pattern, deliberately
# independent of BTMM_ID50_ALERTS_ENABLED.
BTMM_ID50_WATCH_ALERTS_ENABLED = os.environ.get("BTMM_ID50_WATCH_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")

# VWAP Mean Reversion (M15) — replaces VWAP+9EMA (2026-09-11). Adapted from
# an external US-equity research spec; see
# docs/superpowers/specs/2026-09-10-vwap-mean-reversion-strategy-design.md
# for the full rationale and every deviation from the source document.
# MIN_SCORE=9 restricts alerts to best-confidence setups (9/10 and 10/10):
# only the highest-confidence trades trigger Discord. Side effect worth
# knowing (final review finding, 2026-09-11): the max score without
# exhaustion volume is 8, and the max score in the ASIAN session is 8 -- so
# at MIN_SCORE=9 this is a de-facto hard requirement for exhaustion volume
# AND a de-facto exclusion of the ASIAN session, even though both factors
# are deliberately "scored, not gated" in the strategy module itself (see
# spec deviations #8 and #19). This is an intentional alert-layer tightening
# on top of the scanner's own looser scoring, not a bug in either layer --
# lower this value if you want ASIAN-session or no-exhaustion setups to
# reach Discord again.
# ALERTS_ENABLED defaults true (unlike BTMM123, which defaults false as a
# from-scratch strategy with zero external grounding): this strategy is
# based on an external, if unvalidated-for-forex, published methodology,
# and shipping it alerts-on was an explicit user request when this strategy
# was built, mirroring the same default the VWAP+9EMA strategy it replaces
# also shipped with.
VWAP_MR_ALERTS_ENABLED = os.environ.get("VWAP_MR_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")
# GRADE_A_ONLY default true (2026-09-14: restored per user request, same
# signal-fatigue fix as BTMM123/TDI123 two days earlier — B/C setups no
# longer alert). Note grade A itself now also requires a deep (>=3.0 sigma)
# extension, not just score>=8 — see _score_and_grade's depth cap in
# vwap_mean_reversion_strategy.py.
VWAP_MR_GRADE_A_ONLY = os.environ.get("VWAP_MR_GRADE_A_ONLY", "true").lower() in ("1", "true", "yes")
VWAP_MR_MIN_SCORE = int(os.environ.get("VWAP_MR_MIN_SCORE", "9"))
# WATCH default false (2026-09-14: disabled per user request — was posting
# "still forming" embeds even for Grade NO-TRADE (0/10) setups, same
# premature-signal issue fixed for TDI123/BTMM123 on 2026-09-12).
VWAP_MR_WATCH_ALERTS_ENABLED = os.environ.get("VWAP_MR_WATCH_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")
# News gate (doc §3.2/§3.3.5): suppress if either of the pair's currencies
# has a high-impact ForexFactory event within 60 min. Reuses the exact same
# infrastructure as TDI123/BTMM123 (forexfactory.currencies_in_window +
# alerts._news_blocks_pair), applied at the alert layer (see spec deviation #11).
VWAP_MR_NEWS_FILTER = os.environ.get("VWAP_MR_NEWS_FILTER", "true").lower() in ("1", "true", "yes")

# VWAP+9EMA (live, replaces 1AM CRT) — UNVALIDATED strategy, failed its own
# honest backtest (0/48, see docs/superpowers/specs/2026-09-13-vwap9ema-mt5-
# validation-design.md). Built anyway per explicit user decision for manual
# trading with their own judgment (see docs/superpowers/specs/2026-09-14-
# vwap9ema-live-replaces-1am-crt-design.md). No grade-only flag: there are no
# grade tiers to filter by. No watch-alerts: deliberately not adding "still
# forming" noise to a strategy with zero validated edge.
VWAP9EMA_ALERTS_ENABLED = os.environ.get("VWAP9EMA_ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")

# VWAP Bounce (M15) — trend-continuation entries off the session VWAP with the
# 50-bar MVWAP as the trend filter. UNVALIDATED (rules come from chart review,
# not a backtest), so Discord is OFF by default: dashboard-visible only until
# a walk-forward test shows an edge. Same default-off discipline as BTMM123.
VWAP_BOUNCE_ALERTS_ENABLED = os.environ.get("VWAP_BOUNCE_ALERTS_ENABLED", "false").lower() in ("1", "true", "yes")
VWAP_BOUNCE_GRADE_A_ONLY = os.environ.get("VWAP_BOUNCE_GRADE_A_ONLY", "true").lower() in ("1", "true", "yes")
VWAP_BOUNCE_NEWS_FILTER = os.environ.get("VWAP_BOUNCE_NEWS_FILTER", "true").lower() in ("1", "true", "yes")


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
