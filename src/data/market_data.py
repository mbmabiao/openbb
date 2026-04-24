from __future__ import annotations

from datetime import date, timedelta

import pandas as pd


REQUIRED_OHLC_COLUMNS = {"date", "open", "high", "low", "close"}


def _get_obb():
    from openbb import obb

    return obb


def get_start_date_from_range(range_label: str) -> str | None:
    today = date.today()
    if range_label == "1Y":
        return str(today - timedelta(days=365))
    if range_label == "3Y":
        return str(today - timedelta(days=365 * 3))
    if range_label == "5Y":
        return str(today - timedelta(days=365 * 5))
    if range_label == "10Y":
        return str(today - timedelta(days=365 * 10))
    if range_label == "Max":
        return None
    return str(today - timedelta(days=365 * 5))


def to_dataframe(result):
    if result is None:
        return None
    if hasattr(result, "to_dataframe"):
        return result.to_dataframe()
    if hasattr(result, "to_df"):
        return result.to_df()
    if isinstance(result, pd.DataFrame):
        return result
    try:
        return pd.DataFrame(result)
    except Exception:
        return None


def normalise_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(column).strip() for column in out.columns]

    if isinstance(out.index, pd.DatetimeIndex):
        index_name = str(out.index.name).strip() if out.index.name is not None else "date"
        out = out.reset_index()
        first_column = out.columns[0]
        if index_name not in out.columns:
            out = out.rename(columns={first_column: index_name})
    else:
        out = out.reset_index(drop=False)
        if "date" in out.columns and "index" in out.columns:
            out = out.drop(columns=["index"])

    rename_map: dict[str, str] = {}
    for column in out.columns:
        lower = str(column).lower().strip()
        if lower in {"date", "datetime", "timestamp", "time"}:
            rename_map[column] = "date"
        elif lower in {"open", "adj_open"}:
            rename_map[column] = "open"
        elif lower in {"high", "adj_high"}:
            rename_map[column] = "high"
        elif lower in {"low", "adj_low"}:
            rename_map[column] = "low"
        elif lower in {"close", "adj_close", "price"}:
            rename_map[column] = "close"
        elif lower in {"volume", "vol"}:
            rename_map[column] = "volume"

    out = out.rename(columns=rename_map)

    if "date" not in out.columns and len(out.columns) > 0:
        first_column = out.columns[0]
        trial = pd.to_datetime(out[first_column], errors="coerce")
        if trial.notna().sum() > 0:
            out["date"] = trial

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        try:
            if getattr(out["date"].dt, "tz", None) is not None:
                out["date"] = out["date"].dt.tz_localize(None)
        except (AttributeError, TypeError):
            pass

    out = out.reset_index(drop=True)
    out.index.name = None

    if "date" in out.columns:
        out = out.dropna(subset=["date"])
        out = out.sort_values(by="date", kind="stable").reset_index(drop=True)

    return out


def clean_price_history_frame(raw_df: pd.DataFrame | None) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    df = normalise_ohlcv_columns(raw_df).copy()
    df.index.name = None
    df = df.reset_index(drop=True)

    for column in ["open", "high", "low", "close", "volume"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "volume" not in df.columns:
        df["volume"] = pd.NA

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df.dropna(subset=["date", "open", "high", "low", "close"]).copy()


def get_missing_ohlc_columns(df: pd.DataFrame) -> set[str]:
    return REQUIRED_OHLC_COLUMNS - set(df.columns)


def fetch_price_history(
    symbol_value: str,
    start_date_value: str | None,
    provider_value: str | None,
    end_date_value: str | None = None,
    interval_value: str | None = None,
    adjustment_value: str | None = None,
    extended_hours_value: bool | None = None,
):
    obb = _get_obb()
    kwargs = {"symbol": symbol_value}
    if start_date_value is not None:
        kwargs["start_date"] = start_date_value
    if end_date_value is not None:
        kwargs["end_date"] = end_date_value
    if interval_value:
        kwargs["interval"] = interval_value
    if adjustment_value:
        kwargs["adjustment"] = adjustment_value
    if extended_hours_value is not None:
        kwargs["extended_hours"] = extended_hours_value
    if provider_value:
        kwargs["provider"] = provider_value
    return obb.equity.price.historical(**kwargs)


def load_price_history_frame(
    symbol_value: str,
    history_range: str,
    provider_value: str | None,
) -> tuple[pd.DataFrame | None, pd.DataFrame]:
    start_date_value = get_start_date_from_range(history_range)
    result = fetch_price_history(symbol_value, start_date_value, provider_value)
    raw_df = to_dataframe(result)
    return raw_df, clean_price_history_frame(raw_df)


def get_recent_trading_dates(df: pd.DataFrame, lookback_days: int) -> list[pd.Timestamp]:
    if df.empty:
        return []

    trading_dates = (
        pd.to_datetime(df["date"])
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    return [pd.Timestamp(value).normalize() for value in trading_dates[-lookback_days:]]


def get_recent_trading_dates_for_weekly_window(
    df: pd.DataFrame,
    weekly_lookback_bars: int,
) -> list[pd.Timestamp]:
    if df.empty or weekly_lookback_bars < 1:
        return []

    daily_dates = pd.to_datetime(df["date"]).dt.normalize()
    weekly_periods = daily_dates.dt.to_period("W-FRI")
    period_df = pd.DataFrame(
        {
            "date": daily_dates,
            "week_period": weekly_periods,
        }
    ).drop_duplicates(subset=["date"]).sort_values("date", kind="stable")

    if period_df.empty:
        return []

    recent_week_periods = period_df["week_period"].drop_duplicates().tolist()[-weekly_lookback_bars:]
    if not recent_week_periods:
        return []

    recent_period_set = set(recent_week_periods)
    selected = period_df.loc[period_df["week_period"].isin(recent_period_set), "date"].tolist()
    return [pd.Timestamp(value).normalize() for value in selected]


def fetch_interval_history_for_dates(
    symbol_value: str,
    trading_dates: list[pd.Timestamp],
    provider_value: str | None,
    interval_value: str,
) -> pd.DataFrame:
    if not trading_dates:
        return pd.DataFrame()

    start_ts = pd.Timestamp(trading_dates[0]).normalize()
    end_ts = pd.Timestamp(trading_dates[-1]).normalize()
    query_end_ts = end_ts + timedelta(days=1)

    result = fetch_price_history(
        symbol_value=symbol_value,
        start_date_value=str(start_ts.date()),
        end_date_value=str(query_end_ts.date()),
        provider_value=provider_value,
        interval_value=interval_value,
        adjustment_value="splits_only",
        extended_hours_value=False,
    )
    interval_df = to_dataframe(result)
    if interval_df is None or interval_df.empty:
        return pd.DataFrame()

    interval_df = normalise_ohlcv_columns(interval_df)
    required_cols = {"date", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(set(interval_df.columns)):
        return pd.DataFrame()

    for column in ["open", "high", "low", "close", "volume"]:
        interval_df[column] = pd.to_numeric(interval_df[column], errors="coerce")

    interval_df = interval_df.dropna(subset=["date", "open", "high", "low", "close", "volume"]).copy()
    if interval_df.empty:
        return pd.DataFrame()

    target_dates = {pd.Timestamp(value).normalize() for value in trading_dates}
    interval_dates = pd.to_datetime(interval_df["date"]).dt.normalize()
    interval_df = interval_df.loc[interval_dates.isin(target_dates)].copy().reset_index(drop=True)
    return interval_df


def fetch_income_statement(symbol_value: str, provider_value: str | None = None):
    obb = _get_obb()
    if provider_value:
        return obb.equity.fundamental.income(symbol_value, provider=provider_value)
    return obb.equity.fundamental.income(symbol_value)


def fetch_balance_sheet(symbol_value: str, provider_value: str | None = None):
    obb = _get_obb()
    if provider_value:
        return obb.equity.fundamental.balance(symbol_value, provider=provider_value)
    return obb.equity.fundamental.balance(symbol_value)


def fetch_cash_flow(symbol_value: str, provider_value: str | None = None):
    obb = _get_obb()
    if provider_value:
        return obb.equity.fundamental.cash(symbol_value, provider=provider_value)
    return obb.equity.fundamental.cash(symbol_value)


def fetch_ratios(symbol_value: str, provider_value: str | None = None):
    obb = _get_obb()
    if provider_value:
        try:
            return obb.equity.fundamental.ratios(symbol_value, provider=provider_value)
        except TypeError:
            pass
    return obb.equity.fundamental.ratios(symbol_value)


def fetch_company_news(
    symbol_value: str,
    limit: int,
    provider_value: str | None = None,
):
    obb = _get_obb()
    if provider_value:
        return obb.news.company(symbol_value, limit=limit, provider=provider_value)
    return obb.news.company(symbol_value, limit=limit)
