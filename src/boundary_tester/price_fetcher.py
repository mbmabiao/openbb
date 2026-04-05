from __future__ import annotations

from typing import Any

import pandas as pd
from openbb import obb

from .zone_engine import normalise_ohlcv_columns, to_dataframe


def is_intraday_interval(interval: str) -> bool:
    normalized = interval.strip().lower()
    return normalized not in {"1d", "1w", "1wk", "1mo", "1mth", "1month", "1q", "1y"}


def fetch_price_frame(
    symbol: str,
    start_date: str,
    end_date: str,
    provider: str | None,
    interval: str,
    adjustment: str,
    extended_hours: bool,
) -> pd.DataFrame:
    return _fetch_single_price_frame(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        interval=interval,
        adjustment=adjustment,
        extended_hours=extended_hours,
    )


def fetch_interval_history_for_dates(
    symbol: str,
    interval: str,
    needed_trading_dates: list[pd.Timestamp],
    trading_calendar_dates: list[pd.Timestamp],
    provider: str | None,
    adjustment: str,
    extended_hours: bool,
    max_span_days: int = 59,
) -> pd.DataFrame:
    if not needed_trading_dates:
        return pd.DataFrame()

    normalized_needed_dates = sorted({pd.Timestamp(d).normalize() for d in needed_trading_dates})
    normalized_calendar = sorted({pd.Timestamp(d).normalize() for d in trading_calendar_dates})
    date_ranges = compress_needed_trading_dates_to_ranges(normalized_needed_dates, normalized_calendar)
    if not date_ranges:
        return pd.DataFrame()

    request_windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for start_ts, end_ts in date_ranges:
        request_windows.extend(split_date_range_into_chunks(start_ts, end_ts, max_span_days=max_span_days))

    frames: list[pd.DataFrame] = []
    for chunk_start, chunk_end in request_windows:
        query_end_ts = chunk_end + pd.Timedelta(days=1)
        try:
            frame = _fetch_single_price_frame(
                symbol=symbol,
                start_date=str(chunk_start.date()),
                end_date=str(query_end_ts.date()),
                provider=provider,
                interval=interval,
                adjustment=adjustment,
                extended_hours=extended_hours,
            )
        except Exception as exc:
            raise ValueError(
                f"Price fetch failed for {symbol} at interval {interval} "
                f"for request window {chunk_start.date()} to {chunk_end.date()}."
            ) from exc

        frame = filter_frame_to_date_window(frame, start_ts=chunk_start, end_ts=chunk_end)
        if frame.empty:
            raise ValueError(
                f"Price fetch for {symbol} at interval {interval} returned no usable rows "
                f"after filtering request window {chunk_start.date()} to {chunk_end.date()}."
            )
        frames.append(frame)

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["date"], keep="last").sort_values("date", kind="stable").reset_index(drop=True)
    target_dates = set(normalized_needed_dates)
    normalized_dates = pd.to_datetime(out["date"]).dt.normalize()
    out = out.loc[normalized_dates.isin(target_dates)].copy().reset_index(drop=True)
    return out


def compress_needed_trading_dates_to_ranges(
    needed_trading_dates: list[pd.Timestamp],
    trading_calendar_dates: list[pd.Timestamp],
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if not needed_trading_dates or not trading_calendar_dates:
        return []

    calendar = [pd.Timestamp(d).normalize() for d in trading_calendar_dates]
    calendar_index = {date_value: idx for idx, date_value in enumerate(calendar)}
    positions = sorted(calendar_index[d] for d in needed_trading_dates if d in calendar_index)
    if not positions:
        return []

    ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    start_pos = positions[0]
    prev_pos = positions[0]

    for pos in positions[1:]:
        if pos == prev_pos + 1:
            prev_pos = pos
            continue
        ranges.append((calendar[start_pos], calendar[prev_pos]))
        start_pos = pos
        prev_pos = pos

    ranges.append((calendar[start_pos], calendar[prev_pos]))
    return ranges


def split_date_range_into_chunks(
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    max_span_days: int,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if end_ts < start_ts:
        return []

    max_span_days = max(int(max_span_days), 1)
    span = pd.Timedelta(days=max_span_days - 1)
    output: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    current_start = start_ts.normalize()

    while current_start <= end_ts:
        current_end = min(current_start + span, end_ts.normalize())
        output.append((current_start, current_end))
        if current_end >= end_ts:
            break
        current_start = current_end + pd.Timedelta(days=1)

    return output


def filter_frame_to_date_window(df: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df

    normalized_dates = pd.to_datetime(df["date"]).dt.normalize()
    mask = (normalized_dates >= start_ts.normalize()) & (normalized_dates <= end_ts.normalize())
    return df.loc[mask].copy().reset_index(drop=True)


def _fetch_single_price_frame(
    symbol: str,
    start_date: str,
    end_date: str,
    provider: str | None,
    interval: str,
    adjustment: str,
    extended_hours: bool,
) -> pd.DataFrame:
    kwargs: dict[str, Any] = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "interval": interval,
        "adjustment": adjustment,
        "extended_hours": extended_hours,
    }
    if provider:
        kwargs["provider"] = provider

    result = obb.equity.price.historical(**kwargs)
    df = to_dataframe(result)
    if df is None or df.empty:
        raise ValueError(f"No price data returned for {symbol} at interval {interval}.")

    out = normalise_ohlcv_columns(df, date_col_name="date")
    required_cols = {"date", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(set(out.columns)):
        raise ValueError(f"Price data for {symbol} at interval {interval} is missing OHLCV columns.")

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["date", "open", "high", "low", "close", "volume"]).copy()
    out = out.sort_values("date", kind="stable").reset_index(drop=True)
    if out.empty:
        raise ValueError(f"Price data for {symbol} at interval {interval} became empty after cleaning.")

    return out
