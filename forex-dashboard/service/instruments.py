"""Instrument specifications — single source of truth for pip/point size,
display precision, and per-asset stop-loss sizing.

Values are anchored to the broker's own MT5 `symbol_info` (digits / point /
tick_size), cross-checked against standard trading conventions:

  - 5-digit FX majors:  pip = 0.0001  (= 10 x MT5 point)
  - 3-digit JPY pairs:  pip = 0.01    (= 10 x MT5 point)
  - Gold (XAU):         pip = 0.10    (conventional $0.10 pip; MT5 3-digit)
  - Silver (XAG):       pip = 0.01    (= 10 x MT5 point)
  - Indices (DE30/US30/USTEC): pip = 1.0  (one index point)
  - Crypto (BTC/ETH):   pip = 1.0 / 0.1  (one dollar point)

WHY a table instead of price thresholds: the previous code inferred pip size
from the price magnitude (e.g. `price > 5000 -> 1.0`). That silently breaks
when a price crosses a threshold — e.g. Gold at ~4540 is one rally away from
the >5000 index bucket, which would flip its pip from 0.10 to 1.0 (10x wrong).
Keying off the symbol removes that whole class of bug.

Stop sizing: stops are still structure- and ATR-driven (which auto-scales with
volatility). This table adds (a) a per-instrument minimum-SL floor so volatile
instruments aren't given noise-tight stops, and (b) per-asset-class ATR caps so
e.g. Nasdaq is allowed a realistic ~100-200pt stop instead of being capped at
~50. All values are tunable here in one place.
"""
from __future__ import annotations

from typing import Optional

# Per-asset-class stop-loss behaviour (ATR multiples + structure buffers).
#   atr_cap_*  : maximum SL distance as a multiple of ATR (H4 uses daily ATR,
#                M15 uses H1 ATR). Only binds when structure is far away.
#   buf_*      : buffer placed beyond a structure level, as a multiple of ATR.
#   atr_fallback_*: SL distance (ATR multiple) when no structure level exists.
_CLASS_SL = {
    #            atr_cap_h4 atr_cap_m15 buf_h4 buf_m15 fb_h4 fb_m15
    "fx":     dict(atr_cap_h4=0.8, atr_cap_m15=0.6, buf_h4=0.15, buf_m15=0.10, fb_h4=0.5, fb_m15=0.4),
    "jpy":    dict(atr_cap_h4=0.8, atr_cap_m15=0.6, buf_h4=0.15, buf_m15=0.10, fb_h4=0.5, fb_m15=0.4),
    "metal":  dict(atr_cap_h4=1.2, atr_cap_m15=0.9, buf_h4=0.20, buf_m15=0.15, fb_h4=0.6, fb_m15=0.5),
    "index":  dict(atr_cap_h4=1.5, atr_cap_m15=1.0, buf_h4=0.25, buf_m15=0.20, fb_h4=0.7, fb_m15=0.5),
    "crypto": dict(atr_cap_h4=1.5, atr_cap_m15=1.0, buf_h4=0.25, buf_m15=0.20, fb_h4=0.7, fb_m15=0.5),
}

# Per-symbol spec. `min_sl_pips` is the floor in this instrument's pip/point unit.
SYMBOL_SPECS: dict[str, dict] = {
    # ── FX majors / crosses (5-digit, pip = 0.0001) ──
    "EUR/USD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "GBP/USD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "AUD/USD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "NZD/USD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "USD/CHF": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "USD/CAD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "EUR/GBP": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "EUR/CHF": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "EUR/AUD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=12),
    "GBP/CHF": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=12),
    "GBP/AUD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=12),  # volatile GBP cross — wider floor
    "AUD/CAD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "AUD/CHF": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),
    "AUD/NZD": dict(pip=0.0001, digits=5, cls="fx",  min_sl_pips=10),  # tight range pair
    # ── JPY pairs (3-digit, pip = 0.01) ──
    "USD/JPY": dict(pip=0.01,   digits=3, cls="jpy", min_sl_pips=10),
    "EUR/JPY": dict(pip=0.01,   digits=3, cls="jpy", min_sl_pips=10),
    "GBP/JPY": dict(pip=0.01,   digits=3, cls="jpy", min_sl_pips=12),
    "AUD/JPY": dict(pip=0.01,   digits=3, cls="jpy", min_sl_pips=10),
    "CAD/JPY": dict(pip=0.01,   digits=3, cls="jpy", min_sl_pips=10),
    # ── Metals ──
    "XAU/USD": dict(pip=0.10,   digits=2, cls="metal", min_sl_pips=60),   # 60 pips = $6.00
    "XAG/USD": dict(pip=0.01,   digits=3, cls="metal", min_sl_pips=40),   # 40 pips = $0.40
    # ── Indices (pip = 1 index point). Floors ≈ 0.2–0.33% of price so stops
    #    clear intraday noise (Nasdaq is the most volatile → widest floor). ──
    "DE30":    dict(pip=1.0,    digits=1, cls="index", min_sl_pips=50),    # DAX  ~25k
    "US30":    dict(pip=1.0,    digits=1, cls="index", min_sl_pips=75),    # Dow  ~51k
    "USTEC":   dict(pip=1.0,    digits=1, cls="index", min_sl_pips=100),   # Nas  ~30k
    # ── Crypto (reference; not currently in the universe) ──
    "BTC/USD": dict(pip=1.0,    digits=1, cls="crypto", min_sl_pips=150),
    "ETH/USD": dict(pip=0.1,    digits=2, cls="crypto", min_sl_pips=50),
}


def _norm(symbol: Optional[str]) -> str:
    return symbol.strip().upper() if symbol else ""


def _pip_from_price(price: Optional[float]) -> float:
    """Legacy price-magnitude fallback — only used for symbols missing from the
    spec table. Kept conservative; real instruments resolve via SYMBOL_SPECS."""
    if price is None:
        return 0.0001
    if price > 5000:
        return 1.0
    if price > 500:
        return 0.10
    if price > 10:
        return 0.01
    return 0.0001


def spec(symbol: Optional[str]) -> Optional[dict]:
    return SYMBOL_SPECS.get(_norm(symbol))


def pip_size(symbol: Optional[str] = None, price: Optional[float] = None) -> float:
    """Pip/point size for an instrument. Prefers the symbol spec; falls back to
    price magnitude only for unknown symbols."""
    s = spec(symbol)
    if s:
        return s["pip"]
    return _pip_from_price(price)


def price_decimals(symbol: Optional[str] = None, price: Optional[float] = None) -> int:
    s = spec(symbol)
    if s:
        return s["digits"]
    if price is None:
        return 5
    if price > 5000:
        return 1
    if price > 10:
        return 3
    return 5


def fmt_price(symbol: Optional[str], price: Optional[float]) -> str:
    if price is None:
        return "—"
    return f"{price:.{price_decimals(symbol, price)}f}"


def asset_class(symbol: Optional[str]) -> str:
    s = spec(symbol)
    return s["cls"] if s else "fx"


def min_sl_distance(symbol: Optional[str], price: Optional[float] = None) -> float:
    """Minimum stop distance in price terms (min_sl_pips x pip)."""
    s = spec(symbol)
    pip = pip_size(symbol, price)
    floor_pips = s["min_sl_pips"] if s else 10
    return floor_pips * pip


def sl_params(symbol: Optional[str], timeframe: str = "h4") -> dict:
    """Return SL sizing parameters for the symbol's asset class.

    timeframe: 'h4' (daily-ATR scale) or 'm15' (H1-ATR scale).
    Keys: atr_cap, buffer, atr_fallback (all ATR multiples).
    """
    cls = asset_class(symbol)
    c = _CLASS_SL.get(cls, _CLASS_SL["fx"])
    if timeframe == "m15":
        return {"atr_cap": c["atr_cap_m15"], "buffer": c["buf_m15"], "atr_fallback": c["fb_m15"]}
    return {"atr_cap": c["atr_cap_h4"], "buffer": c["buf_h4"], "atr_fallback": c["fb_h4"]}
