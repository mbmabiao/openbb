from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from data.market_data import clean_price_history_frame, fetch_price_history, to_dataframe
from engines.zone_generation import ZoneGenerationConfig, make_replay_zone_provider
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

    zone_provider = make_replay_zone_provider(
        symbol=normalized_symbol,
        provider=provider,
        config=config,
        include_all_candidates=True,
    )
    Session = create_session_factory(database_url)
    with Session() as session:
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
