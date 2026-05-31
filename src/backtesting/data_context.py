from __future__ import annotations

from datetime import timedelta

import pandas as pd

from data.market_data import clean_price_history_frame, fetch_price_history, to_dataframe
from strategies.base import StrategyContext


def build_strategy_context(
    symbol: str,
    primary_timeframe: str,
    required_timeframes: list[str],
    start_date: str | None,
    end_date: str | None,
    strategy_config: dict,
    provider: str | None = None,
) -> StrategyContext:
    """
    Load OHLCV frames for all required timeframes.

    MVP alignment assumption: each strategy receives raw frames keyed by timeframe.
    Helper functions below can forward-fill higher timeframe values to the primary
    frame. For daily-to-intraday alignment, daily values are shifted by one day so a
    completed daily candle is only visible to later intraday bars.
    """
    timeframes = _unique_timeframes([primary_timeframe, *required_timeframes])
    data = {
        timeframe: _load_timeframe_frame(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            provider=provider,
        )
        for timeframe in timeframes
    }
    return StrategyContext(
        symbol=symbol,
        primary_timeframe=primary_timeframe,
        data=data,
        config=strategy_config,
    )


def align_higher_timeframe_to_primary(
    primary: pd.DataFrame,
    higher: pd.DataFrame,
    columns: list[str],
    *,
    higher_timeframe: str,
) -> pd.DataFrame:
    if primary.empty or higher.empty:
        return pd.DataFrame(index=primary.index)

    left = primary.loc[:, ["date"]].copy()
    left["date"] = pd.to_datetime(left["date"], errors="coerce")

    right = higher.loc[:, ["date", *columns]].copy()
    right["date"] = pd.to_datetime(right["date"], errors="coerce")
    right = right.dropna(subset=["date"]).sort_values("date", kind="stable")

    if higher_timeframe.lower() in {"1d", "d", "day", "daily"}:
        right["date"] = right["date"] + timedelta(days=1)

    aligned = pd.merge_asof(
        left.sort_values("date", kind="stable"),
        right.sort_values("date", kind="stable"),
        on="date",
        direction="backward",
    )
    return aligned.loc[:, columns].set_index(primary.index)


def _load_timeframe_frame(
    symbol: str,
    timeframe: str,
    start_date: str | None,
    end_date: str | None,
    provider: str | None,
) -> pd.DataFrame:
    result = fetch_price_history(
        symbol_value=symbol,
        start_date_value=start_date,
        end_date_value=end_date,
        provider_value=provider,
        interval_value=timeframe,
        adjustment_value="splits_only",
        extended_hours_value=False,
    )
    frame = clean_price_history_frame(to_dataframe(result))
    if frame.empty:
        raise ValueError(f"No OHLCV data returned for {symbol} at timeframe {timeframe}.")
    return frame


def _unique_timeframes(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if normalized and normalized not in seen:
            output.append(normalized)
            seen.add(normalized)
    return output
