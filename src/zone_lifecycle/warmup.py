from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from .adapters import upsert_dashboard_zone
from .breakout_state_machine import process_zone_bar
from .constants import ACTIVE_ZONE_STATUSES
from .lifecycle import BarInput, expire_event_zones
from .models import SymbolLifecycleState, Zone
from .service import ZoneSnapshotInput, record_zone_snapshot


ZoneProvider = Callable[[pd.DataFrame, BarInput], Iterable[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class LifecycleWarmupResult:
    symbol: str
    timeframe: str
    processed_bars: int
    upserted_zones: int
    snapshots: int
    zone_bar_updates: int
    breakout_updates: int
    warmup_start_ts: datetime | None
    last_processed_ts: datetime | None


def ensure_symbol_lifecycle_ready(
    session: Session,
    *,
    symbol: str,
    price_df: pd.DataFrame,
    zone_provider: ZoneProvider,
    lookback_years: int = 2,
    timeframe: str = "1d",
    as_of_date=None,
    force: bool = False,
) -> LifecycleWarmupResult:
    """Warm up or incrementally advance lifecycle state for one symbol.

    Replay controls should call read/query paths. This writer is intended for
    symbol load or scheduled daily refresh, and only processes bars beyond the
    stored high-water mark unless force=True.
    """
    normalized_symbol = str(symbol).strip().upper()
    normalized_timeframe = str(timeframe).strip().lower()
    bars = _normalize_price_frame(price_df)
    if bars.empty:
        return LifecycleWarmupResult(
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            processed_bars=0,
            upserted_zones=0,
            snapshots=0,
            zone_bar_updates=0,
            breakout_updates=0,
            warmup_start_ts=None,
            last_processed_ts=None,
        )

    as_of_ts = _coerce_timestamp(as_of_date) or bars["timestamp"].max()
    bars = bars[bars["timestamp"] <= as_of_ts].copy()
    if bars.empty:
        return LifecycleWarmupResult(
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            processed_bars=0,
            upserted_zones=0,
            snapshots=0,
            zone_bar_updates=0,
            breakout_updates=0,
            warmup_start_ts=None,
            last_processed_ts=None,
        )

    state = _get_state(session, normalized_symbol, normalized_timeframe)
    warmup_start_ts = _warmup_start(bars, as_of_ts, lookback_years)
    if state is None or force:
        start_ts = warmup_start_ts
    else:
        start_ts = state.last_processed_ts

    if state is None or force:
        bars_to_process = bars[bars["timestamp"] >= start_ts].copy()
    else:
        bars_to_process = bars[bars["timestamp"] > start_ts].copy()

    if bars_to_process.empty:
        return LifecycleWarmupResult(
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            processed_bars=0,
            upserted_zones=0,
            snapshots=0,
            zone_bar_updates=0,
            breakout_updates=0,
            warmup_start_ts=state.warmup_start_ts if state is not None else warmup_start_ts,
            last_processed_ts=state.last_processed_ts if state is not None else None,
        )

    upserted_zone_ids: set[str] = set()
    snapshot_count = 0
    zone_bar_updates = 0
    breakout_updates = 0
    last_processed_ts: datetime | None = None

    for row in bars_to_process.itertuples(index=False):
        bar = _row_to_bar(row)
        history = bars[bars["timestamp"] <= bar.timestamp]
        dashboard_zones = list(zone_provider(history.copy(), bar))
        selected_zones: list[Zone] = []
        for dashboard_zone in dashboard_zones:
            zone = upsert_dashboard_zone(
                session,
                symbol=normalized_symbol,
                zone=dashboard_zone,
                observed_ts=bar.timestamp,
            )
            upserted_zone_ids.add(zone.zone_id)
            selected_zones.append(zone)

        active_zones = session.scalars(
            select(Zone)
            .where(Zone.symbol == normalized_symbol)
            .where(Zone.status.in_(ACTIVE_ZONE_STATUSES))
        ).all()
        matching_active_zones = [
            zone for zone in active_zones if _timeframes_match(zone.timeframe, normalized_timeframe)
        ]
        for zone in matching_active_zones:
            event = process_zone_bar(session, zone, bar)
            zone_bar_updates += 1
            if event is not None:
                breakout_updates += 1
        expire_event_zones(
            session,
            current_ts=bar.timestamp,
            bars_since_created_by_zone_id=_bars_since_origin_by_zone_id(matching_active_zones, history),
        )

        for zone in selected_zones:
            record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=zone.zone_id,
                    snapshot_ts=bar.timestamp,
                    current_price=bar.close,
                    atr=bar.atr,
                ),
            )
            snapshot_count += 1

        last_processed_ts = bar.timestamp

    if last_processed_ts is not None:
        state = _upsert_state(
            session,
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            warmup_start_ts=warmup_start_ts,
            last_processed_ts=last_processed_ts,
            lookback_years=lookback_years,
        )

    session.flush()
    return LifecycleWarmupResult(
        symbol=normalized_symbol,
        timeframe=normalized_timeframe,
        processed_bars=len(bars_to_process),
        upserted_zones=len(upserted_zone_ids),
        snapshots=snapshot_count,
        zone_bar_updates=zone_bar_updates,
        breakout_updates=breakout_updates,
        warmup_start_ts=state.warmup_start_ts if state is not None else warmup_start_ts,
        last_processed_ts=last_processed_ts,
    )


def _normalize_price_frame(price_df: pd.DataFrame) -> pd.DataFrame:
    if price_df is None or price_df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "atr"])

    frame = price_df.copy()
    timestamp_column = _first_existing_column(frame, ("timestamp", "date", "datetime", "time"))
    if timestamp_column is None:
        if isinstance(frame.index, pd.DatetimeIndex):
            frame = frame.reset_index().rename(columns={frame.index.name or "index": "timestamp"})
            timestamp_column = "timestamp"
        else:
            raise ValueError("price_df must contain a timestamp/date column or a DatetimeIndex")

    rename_map = {timestamp_column: "timestamp"}
    for target in ("open", "high", "low", "close"):
        source = _first_existing_column(frame, (target, target.capitalize(), target.upper()))
        if source is None:
            raise ValueError(f"price_df missing required column: {target}")
        rename_map[source] = target

    atr_source = _first_existing_column(frame, ("atr", "ATR", "atr20", "ATR20"))
    if atr_source is not None:
        rename_map[atr_source] = "atr"
    volume_source = _first_existing_column(frame, ("volume", "Volume", "VOLUME"))
    if volume_source is not None:
        rename_map[volume_source] = "volume"

    frame = frame.rename(columns=rename_map)
    if "atr" not in frame.columns:
        frame["atr"] = None
    if "volume" not in frame.columns:
        frame["volume"] = 0.0

    frame = frame[["timestamp", "open", "high", "low", "close", "volume", "atr"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(None)
    for column in ("open", "high", "low", "close", "volume", "atr"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=["timestamp", "open", "high", "low", "close"])
    return frame.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)


def _first_existing_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    return None


def _row_to_bar(row) -> BarInput:
    atr = None if pd.isna(row.atr) else float(row.atr)
    return BarInput(
        timestamp=pd.Timestamp(row.timestamp).to_pydatetime(),
        open=float(row.open),
        high=float(row.high),
        low=float(row.low),
        close=float(row.close),
        atr=atr,
    )


def _warmup_start(bars: pd.DataFrame, as_of_ts: pd.Timestamp, lookback_years: int) -> datetime:
    requested_start = as_of_ts - pd.DateOffset(years=max(int(lookback_years), 1))
    available = bars[bars["timestamp"] >= requested_start]
    if available.empty:
        return pd.Timestamp(bars["timestamp"].min()).to_pydatetime()
    return pd.Timestamp(available["timestamp"].min()).to_pydatetime()


def _coerce_timestamp(value) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        return timestamp.tz_convert(None)
    return timestamp


def _timeframes_match(left: str, right: str) -> bool:
    return _normalize_timeframe_alias(left) == _normalize_timeframe_alias(right)


def _normalize_timeframe_alias(value: str) -> str:
    normalized = str(value).strip().lower()
    return {"d": "1d", "day": "1d", "daily": "1d", "w": "1w", "week": "1w", "weekly": "1w"}.get(
        normalized,
        normalized,
    )


def _bars_since_origin_by_zone_id(zones: list[Zone], history: pd.DataFrame) -> dict[str, int]:
    result: dict[str, int] = {}
    if history.empty:
        return result
    timestamps = list(history["timestamp"])
    for zone in zones:
        if zone.origin_bar is None:
            continue
        origin_ts = pd.Timestamp(zone.origin_bar)
        bars_since = sum(1 for timestamp in timestamps if timestamp >= origin_ts)
        result[zone.zone_id] = max(bars_since, 0)
    return result


def _get_state(session: Session, symbol: str, timeframe: str) -> SymbolLifecycleState | None:
    return session.scalars(
        select(SymbolLifecycleState)
        .where(SymbolLifecycleState.symbol == symbol)
        .where(SymbolLifecycleState.timeframe == timeframe)
    ).one_or_none()


def _upsert_state(
    session: Session,
    *,
    symbol: str,
    timeframe: str,
    warmup_start_ts: datetime,
    last_processed_ts: datetime,
    lookback_years: int,
) -> SymbolLifecycleState:
    state = _get_state(session, symbol, timeframe)
    now = last_processed_ts
    if state is None:
        state = SymbolLifecycleState(
            state_id=_state_id(symbol, timeframe),
            symbol=symbol,
            timeframe=timeframe,
            warmup_start_ts=warmup_start_ts,
            last_processed_ts=last_processed_ts,
            lookback_years=int(lookback_years),
            created_ts=now,
            updated_ts=now,
            metadata_json={},
        )
        session.add(state)
        session.flush()
        return state

    state.warmup_start_ts = warmup_start_ts
    state.last_processed_ts = last_processed_ts
    state.lookback_years = int(lookback_years)
    state.updated_ts = now
    return state


def _state_id(symbol: str, timeframe: str) -> str:
    payload = {"symbol": symbol, "timeframe": timeframe}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"symbol_lifecycle_{digest}"
