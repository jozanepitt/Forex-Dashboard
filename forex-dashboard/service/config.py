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

# Grade-A-only mode for SNR Emperor H4 and M15 SNR scanners.
# When True, only Grade A setups are sent to Discord; Grade B (and below) are
# suppressed as noise. Set to "false" to allow Grade B alerts again.
ALERTS_GRADE_A_ONLY = os.environ.get("ALERTS_GRADE_A_ONLY", "true").lower() in ("1", "true", "yes")

# 1AM CRT grade gate — separate from the SNR flag above because CRT fires fewer
# setups and Grade B setups are acceptable signals there (they don't push as many
# alerts as SNR). Default false = A + B both sent to Discord.
CRT_GRADE_A_ONLY = os.environ.get("CRT_GRADE_A_ONLY", "false").lower() in ("1", "true", "yes")

# 5AM CRT grade gate — NY Open kill-zone session. A + B (default false, same as 1AM CRT).
CRT_5AM_GRADE_A_ONLY = os.environ.get("CRT_5AM_GRADE_A_ONLY", "false").lower() in ("1", "true", "yes")

# Quality filters for SNR (H4 + M15) — based on 2026-06-09 pattern audit.
# DISTANCE filter: skip signal if price is too far from the entry zone when fired
#   (stale setups never fill or fill into established momentum). 0 disables.
# TREND filter: skip signal if direction aligns with H1 trend (SNR is a reversal
#   strategy; with-trend signals had 0% win rate in audit). Disable to allow all.
ALERTS_DISTANCE_FILTER_PIPS = float(os.environ.get("ALERTS_DISTANCE_FILTER_PIPS", "50"))
ALERTS_TREND_FILTER_ENABLED = os.environ.get("ALERTS_TREND_FILTER_ENABLED", "true").lower() in ("1", "true", "yes")

# EMS gate for M15 SNR signals — based on "The Alchemist EMS Trinity" + MSNR
# ALCHEMIST notes. When True, an M15 SNR alert only fires if the higher
# timeframe (H4) storyline agrees in direction AND price shows a liquidity
# sweep AND a market structure shift. This turns M15 from a standalone (noisy)
# signal source into a precision refinement of the HTF bias. Set "false" to
# revert to the old standalone M15 behaviour.
SNR_M15_EMS_GATE_ENABLED = os.environ.get("SNR_M15_EMS_GATE_ENABLED", "true").lower() in ("1", "true", "yes")


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
