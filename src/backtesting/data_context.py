from __future__ import annotations

import re
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
    extended_hours: bool = False,
    data_requirements: dict | None = None,
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
            extended_hours=_resolve_timeframe_extended_hours(
                timeframe=timeframe,
                data_requirements=data_requirements,
                fallback=extended_hours,
            ),
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
    extended_hours: bool = False,
) -> pd.DataFrame:
    result = fetch_price_history(
        symbol_value=symbol,
        start_date_value=start_date,
        end_date_value=end_date,
        provider_value=provider,
        interval_value=timeframe,
        adjustment_value="splits_only",
        extended_hours_value=extended_hours,
    )
    frame = clean_price_history_frame(to_dataframe(result))
    if frame.empty:
        raise ValueError(f"No OHLCV data returned for {symbol} at timeframe {timeframe}.")
    return frame


def _resolve_timeframe_extended_hours(
    timeframe: str,
    data_requirements: dict | None,
    fallback: bool,
) -> bool:
    timeframe_requirements = (data_requirements or {}).get("timeframes", {})
    requirement = timeframe_requirements.get(timeframe)
    if requirement is None:
        requirement = timeframe_requirements.get(str(timeframe).lower())
    if isinstance(requirement, dict) and "extended_hours" in requirement:
        return bool(requirement["extended_hours"])
    return bool(fallback) if is_intraday_timeframe(timeframe) else False


def is_intraday_timeframe(timeframe: str) -> bool:
    """
    Return True for intraday intervals such as:
    1, 5, 15, 30, 60,
    1m, 5m, 15m, 30m,
    1min, 5min, 15min,
    1minute, 5minutes,
    1h, 2h, 4h,
    1hr, 2hrs,
    1hour, 2hours,
    hourly.

    Return False for daily or higher intervals such as:
    1d, d, day, daily,
    1w, w, week, weekly,
    1mo, month, monthly,
    1y, year, yearly.
    """
    normalized = str(timeframe or "").strip().lower()
    normalized = normalized.replace(" ", "")
    normalized = normalized.replace("_", "")
    normalized = normalized.replace("-", "")

    if not normalized:
        return False

    daily_or_higher = {
        "d",
        "1d",
        "day",
        "1day",
        "daily",
        "w",
        "1w",
        "week",
        "1week",
        "weekly",
        "mo",
        "1mo",
        "mon",
        "1mon",
        "month",
        "1month",
        "monthly",
        "q",
        "1q",
        "quarter",
        "1quarter",
        "quarterly",
        "y",
        "1y",
        "yr",
        "1yr",
        "year",
        "1year",
        "yearly",
        "annual",
        "annually",
    }

    if normalized in daily_or_higher:
        return False

    always_intraday = {
        "hourly",
        "intraday",
    }

    if normalized in always_intraday:
        return True

    if normalized.isdigit():
        return int(normalized) > 0

    match = re.fullmatch(
        r"(?P<num>\d+(?:\.\d+)?)(?P<unit>m|min|mins|minute|minutes|h|hr|hrs|hour|hours)",
        normalized,
    )

    if not match:
        return False

    value = float(match.group("num"))
    unit = match.group("unit")

    if value <= 0:
        return False

    return unit in {
        "m",
        "min",
        "mins",
        "minute",
        "minutes",
        "h",
        "hr",
        "hrs",
        "hour",
        "hours",
    }


def _unique_timeframes(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if normalized and normalized not in seen:
            output.append(normalized)
            seen.add(normalized)
    return output
