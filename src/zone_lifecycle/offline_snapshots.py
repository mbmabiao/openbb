from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from data.market_data import clean_price_history_frame, fetch_price_history, normalise_ohlcv_columns, to_dataframe
from engines.zone_generation import ZoneGenerationConfig, make_preloaded_zone_provider
from .models import BreakoutEvent, SymbolLifecycleState, Zone, ZoneDailySnapshot
from .repository import create_session_factory
from .warmup import LifecycleWarmupResult, ensure_symbol_lifecycle_ready


@dataclass(frozen=True, slots=True)
class OfflineSnapshotBuildResult:
    symbol: str
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    lifecycle: LifecycleWarmupResult


def build_zone_snapshots_offline(
    *,
    symbol: str,
    start_date,
    end_date,
    config: ZoneGenerationConfig,
    provider: str | None = None,
    database_url: str | None = None,
    lookback_years: int = 2,
    force: bool = True,
    reset: bool = False,
) -> OfflineSnapshotBuildResult:
    normalized_symbol = str(symbol).strip().upper()
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    if end_ts < start_ts:
        raise ValueError("end_date must be on or after start_date")

    fetch_start = start_ts - pd.DateOffset(years=max(int(lookback_years), 1))
    query_end_ts = end_ts + pd.DateOffset(days=1)
    raw = fetch_price_history(
        symbol_value=normalized_symbol,
        start_date_value=str(fetch_start.date()),
        end_date_value=str(query_end_ts.date()),
        provider_value=provider,
    )
    price_df = clean_price_history_frame(to_dataframe(raw))
    if price_df.empty:
        raise ValueError(f"No price history returned for {normalized_symbol}")

    interval_cache = _build_interval_cache(
        symbol=normalized_symbol,
        provider=provider,
        fetch_start=fetch_start,
        query_end_ts=query_end_ts,
        daily_price_df=price_df,
    )
    zone_provider = make_preloaded_zone_provider(
        symbol=normalized_symbol,
        provider=provider,
        config=config,
        interval_frames=interval_cache,
        include_all_candidates=True,
    )
    Session = create_session_factory(database_url)
    with Session() as session:
        if reset:
            reset_symbol_lifecycle_data(session, normalized_symbol)
            session.flush()
        lifecycle = ensure_symbol_lifecycle_ready(
            session,
            symbol=normalized_symbol,
            price_df=price_df,
            zone_provider=zone_provider,
            lookback_years=lookback_years,
            timeframe="1d",
            as_of_date=end_ts,
            snapshot_start_date=start_ts,
            snapshot_end_date=end_ts,
            force=force,
        )
        session.commit()

    return OfflineSnapshotBuildResult(
        symbol=normalized_symbol,
        start_date=start_ts,
        end_date=end_ts,
        lifecycle=lifecycle,
    )


def reset_symbol_lifecycle_data(session: Session, symbol: str) -> None:
    normalized_symbol = str(symbol).strip().upper()
    session.execute(delete(ZoneDailySnapshot).where(ZoneDailySnapshot.symbol == normalized_symbol))
    session.execute(delete(BreakoutEvent).where(BreakoutEvent.symbol == normalized_symbol))
    session.execute(delete(Zone).where(Zone.symbol == normalized_symbol))
    session.execute(delete(SymbolLifecycleState).where(SymbolLifecycleState.symbol == normalized_symbol))


def _build_interval_cache(
    *,
    symbol: str,
    provider: str | None,
    fetch_start: pd.Timestamp,
    query_end_ts: pd.Timestamp,
    daily_price_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    return {
        "5m": _fetch_interval_frame(
            symbol=symbol,
            provider=provider,
            start_ts=fetch_start,
            end_ts=query_end_ts,
            interval="5m",
        ),
        "1d": _prepare_daily_interval_frame(daily_price_df),
    }


def _fetch_interval_frame(
    *,
    symbol: str,
    provider: str | None,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    interval: str,
) -> pd.DataFrame:
    try:
        raw = fetch_price_history(
            symbol_value=symbol,
            start_date_value=str(pd.Timestamp(start_ts).date()),
            end_date_value=str(pd.Timestamp(end_ts).date()),
            provider_value=provider,
            interval_value=interval,
            adjustment_value="splits_only",
            extended_hours_value=False,
        )
    except Exception:
        return pd.DataFrame()

    frame = _prepare_interval_frame(to_dataframe(raw))
    if frame.empty:
        return frame

    start_norm = pd.Timestamp(start_ts).normalize()
    end_norm = pd.Timestamp(end_ts).normalize()
    row_dates = pd.to_datetime(frame["date"]).dt.normalize()
    return frame.loc[(row_dates >= start_norm) & (row_dates <= end_norm)].copy().reset_index(drop=True)


def _prepare_daily_interval_frame(price_df: pd.DataFrame) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame()
    columns = ["date", "open", "high", "low", "close", "volume"]
    frame = price_df.copy()
    for column in columns:
        if column not in frame.columns:
            frame[column] = 0.0 if column == "volume" else pd.NA
    return _prepare_interval_frame(frame.loc[:, columns])


def _prepare_interval_frame(raw_df: pd.DataFrame | None) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    frame = normalise_ohlcv_columns(raw_df)
    required_columns = {"date", "open", "high", "low", "close", "volume"}
    if not required_columns.issubset(set(frame.columns)):
        return pd.DataFrame()

    frame = frame.loc[:, ["date", "open", "high", "low", "close", "volume"]].copy()
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date", "open", "high", "low", "close", "volume"]).copy()
    if frame.empty:
        return pd.DataFrame()
    return frame.sort_values("date", kind="stable").reset_index(drop=True)
