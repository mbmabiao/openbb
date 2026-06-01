from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Make `src/` imports work when running from repo root.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data.market_data import clean_price_history_frame, fetch_price_history, to_dataframe


def to_new_york_time(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")

    # If timestamps are naive, assume they are already local-like timestamps.
    # If they are timezone-aware, convert to New York time.
    try:
        if getattr(dt.dt, "tz", None) is not None:
            return dt.dt.tz_convert("America/New_York")
    except Exception:
        pass

    return dt.dt.tz_localize("America/New_York", nonexistent="shift_forward", ambiguous="NaT")


def inspect_premarket(
    symbol: str = "MSFT",
    provider: str | None = None,
    start_date: str = "2026-05-01",
    end_date: str = "2026-05-31",
    interval: str = "5m",
) -> None:
    print("=" * 80)
    print(f"Testing {symbol} | interval={interval} | {start_date} -> {end_date}")
    print(f"provider={provider!r}")
    print("=" * 80)

    result = fetch_price_history(
        symbol_value=symbol,
        start_date_value=start_date,
        end_date_value=end_date,
        provider_value=provider,
        interval_value=interval,
        adjustment_value="splits_only",
        extended_hours_value=True,
    )

    raw = to_dataframe(result)
    df = clean_price_history_frame(raw)

    if df.empty:
        print("No data returned.")
        return

    df = df.copy()
    df["ny_datetime"] = to_new_york_time(df["date"])
    df["ny_date"] = df["ny_datetime"].dt.date
    df["ny_time"] = df["ny_datetime"].dt.time

    premarket = df[
        (df["ny_time"] >= pd.to_datetime("06:30").time())
        & (df["ny_time"] < pd.to_datetime("09:30").time())
    ].copy()

    regular = df[
        (df["ny_time"] >= pd.to_datetime("09:30").time())
        & (df["ny_time"] < pd.to_datetime("16:00").time())
    ].copy()

    after_hours = df[
        (df["ny_time"] >= pd.to_datetime("16:00").time())
        | (df["ny_time"] < pd.to_datetime("06:30").time())
    ].copy()

    print(f"Total rows:       {len(df)}")
    print(f"Premarket rows:   {len(premarket)}")
    print(f"Regular rows:     {len(regular)}")
    print(f"After-hours rows: {len(after_hours)}")
    print()
    print(f"Date range raw: {df['date'].min()} -> {df['date'].max()}")
    print(f"Date range NY:  {df['ny_datetime'].min()} -> {df['ny_datetime'].max()}")
    print()

    if not premarket.empty:
        by_day = premarket.groupby("ny_date").size().tail(10)
        print("Premarket row count by day, last 10 days:")
        print(by_day.to_string())
        print()
        print("Sample premarket rows:")
        print(
            premarket[
                ["date", "ny_datetime", "open", "high", "low", "close", "volume"]
            ]
            .tail(20)
            .to_string(index=False)
        )
    else:
        print("No 06:30-09:30 New York premarket rows found.")
        print("This may mean the provider does not return extended-hours data for this symbol/interval.")


if __name__ == "__main__":
    # Try provider=None first, then try common providers manually if needed.
    # Examples:
    # inspect_premarket(symbol="MSFT", provider="yfinance")
    # inspect_premarket(symbol="TSLA", provider="yfinance")
    inspect_premarket(symbol="MSFT", provider=None)