"""One-off helper: generate a synthetic M5 CSV to smoke-test backtest_mt5.py's
--variant wiring without needing real MT5 data. Not part of the production
fetch pipeline. Run once, use to sanity-check Task 3, then ignore."""
import numpy as np
import pandas as pd

def build_synthetic_day(date: str, base_price: float = 100.0) -> pd.DataFrame:
    """One NY-session day (15:00-22:55 server time, 5-min bars): a steady
    uptrend, a mid-session pullback to the 9 EMA that rejects (bullish,
    with a volume climax on the rejection bar), and a late low-volume
    chop patch — enough to exercise all three variants differently."""
    times = pd.date_range(f"{date} 15:00", f"{date} 22:55", freq="5min")
    n = len(times)
    trend = np.linspace(0, 8, n)
    price = base_price + trend
    mid = n // 2
    price[mid:mid+3] -= [0.3, 1.2, 0.5]
    close = price
    open_ = np.roll(close, 1); open_[0] = base_price
    high = np.maximum(open_, close) + 0.15
    low = np.minimum(open_, close) - 0.15
    volume = np.full(n, 500.0)
    volume[mid+2] = 900.0
    volume[n-10:n-5] = 50.0
    spread_points = np.full(n, 8.0)
    point = np.full(n, 0.1)
    return pd.DataFrame(dict(time=times, open=open_, high=high, low=low,
                              close=close, tick_volume=volume,
                              spread_points=spread_points, point=point))

if __name__ == "__main__":
    days = [build_synthetic_day(d) for d in ["2026-01-05", "2026-01-06", "2026-01-07"]]
    df = pd.concat(days, ignore_index=True)
    df.to_csv("data/SYNTH_M5.csv", index=False)
    print(f"Wrote data/SYNTH_M5.csv: {len(df)} bars")
