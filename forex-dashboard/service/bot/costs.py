"""Exness Standard cost assumptions.

These are NOT live quotes and NOT Dukascopy's own spread. Dukascopy BID
candles are the price path. A constant Exness Standard spread (commission
$0 on Standard) plus a small adverse slippage is applied on top.

Figures are typical averages for an Exness Standard account, rounded a
little wider than the tightest numbers the broker advertises, because a
backtest that uses the minimum spread overstates fills. They will not match
the user's terminal at a given second. Raw Spread / Zero accounts are
tighter and charge commission; they are not what this table describes.

Pip sizes come from instruments.py (XAU pip = 0.10, so a $0.22 gold spread
is 2.2 pips in this repo's unit).
"""
from __future__ import annotations

from typing import Optional

# symbol -> (spread in price units, stop/entry slippage in price units)
_COSTS: dict[str, tuple[float, float]] = {
    "EUR/USD": (0.00011, 0.00002),   # 1.1 pip, 0.2 pip slip
    "GBP/USD": (0.00015, 0.00003),
    "AUD/USD": (0.00014, 0.00002),
    "NZD/USD": (0.00018, 0.00003),
    "USD/CHF": (0.00015, 0.00002),
    "USD/CAD": (0.00017, 0.00003),
    "EUR/GBP": (0.00016, 0.00003),
    "EUR/AUD": (0.00020, 0.00004),
    "EUR/CHF": (0.00018, 0.00003),
    "GBP/AUD": (0.00028, 0.00005),
    "GBP/CHF": (0.00024, 0.00004),
    "AUD/CAD": (0.00020, 0.00004),
    "AUD/CHF": (0.00020, 0.00004),
    "AUD/NZD": (0.00022, 0.00004),
    "USD/JPY": (0.012, 0.003),       # 1.2 pip, 0.3 pip slip
    "EUR/JPY": (0.018, 0.004),
    "GBP/JPY": (0.028, 0.006),
    "AUD/JPY": (0.018, 0.004),
    "CAD/JPY": (0.018, 0.004),
    "XAU/USD": (0.22, 0.07),         # $0.22 spread, $0.07 slip
    "XAG/USD": (0.025, 0.008),
    "USOIL": (0.04, 0.02),
    "UKOIL": (0.05, 0.02),
}


def cost_model(symbol: str) -> Optional[tuple[float, float]]:
    """Return (spread_price, slippage_price) or None if the symbol is unknown.

    Unknown symbols fail closed: the bot will not trade them and the
    backtest will not invent a tight spread.
    """
    return _COSTS.get(symbol.strip().upper())


def spread_price(symbol: str) -> Optional[float]:
    model = cost_model(symbol)
    return model[0] if model else None


def describe() -> list[dict]:
    """Rows for the report: symbol, spread, slippage."""
    import instruments

    rows = []
    for symbol, (spread, slip) in _COSTS.items():
        pip = instruments.pip_size(symbol)
        rows.append({
            "symbol": symbol,
            "spread_price": spread,
            "spread_pips": round(spread / pip, 2),
            "slippage_price": slip,
            "slippage_pips": round(slip / pip, 2),
            "commission": 0.0,
        })
    return rows
