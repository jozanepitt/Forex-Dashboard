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
