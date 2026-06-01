from __future__ import annotations

import pandas as pd

from data.market_data import clean_price_history_frame, fetch_price_history, to_dataframe


def inspect_premarket(symbol: str = "MSFT", provider: str | None = "yfinance") -> None:
    result = fetch_price_history(
        symbol_value=symbol,
        start_date_value="2026-05-01",
        end_date_value="2026-06-01",
        provider_value=provider,
        interval_value="5m",
        adjustment_value="splits_only",
        extended_hours_value=True,
    )
    df = clean_price_history_frame(to_dataframe(result))
    if df.empty:
        print("No rows returned.")
        return

    timestamps = pd.to_datetime(df["date"], errors="coerce")
    if getattr(timestamps.dt, "tz", None) is None:
        timestamps = timestamps.dt.tz_localize("UTC")
    df = df.assign(ny_time=timestamps.dt.tz_convert("America/New_York"))
    premarket = df.loc[
        (df["ny_time"].dt.time >= pd.Timestamp("06:30").time())
        & (df["ny_time"].dt.time < pd.Timestamp("09:30").time())
    ].copy()

    print(f"Symbol: {symbol}")
    print(f"Total 5m rows: {len(df)}")
    print(f"Premarket 06:30-09:30 rows: {len(premarket)}")
    print(premarket.loc[:, ["ny_time", "open", "high", "low", "close", "volume"]].head(20))


if __name__ == "__main__":
    inspect_premarket()
