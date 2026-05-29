from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from config.warmup_config import WarmupThresholdConfig, load_warmup_config
from features.zone_strength import zone_strength_from_interval

from .adapters import upsert_dashboard_zone
from .breakout_state_machine import BreakoutStateConfig, process_zone_bar
from .constants import ACTIVE_ZONE_STATUSES, DEPRECATED_ZONE_SOURCES, ZoneKind
from .divergence_events import detect_macd_divergence_events_for_latest_bar
from .lifecycle import BarInput, expire_event_zones
from .models import SymbolLifecycleState, Zone
from .pattern_events import detect_pattern_events_for_latest_bar
from .service import (
    MarketObservationInput,
    ZoneSnapshotInput,
    record_market_observation,
    record_divergence_event,
    record_pattern_event,
    record_zone_snapshot,
)


ZoneProvider = Callable[[pd.DataFrame, BarInput], Iterable[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class LifecycleWarmupResult:
    symbol: str
    timeframe: str
    processed_bars: int
    upserted_zones: int
    snapshots: int
    observations: int
    pattern_events: int
    divergence_events: int
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
    lookback_years: int | None = None,
    timeframe: str = "1d",
    as_of_date=None,
    snapshot_start_date=None,
    snapshot_end_date=None,
    force: bool = False,
    warmup_config: WarmupThresholdConfig | None = None,
) -> LifecycleWarmupResult:
    """Warm up or incrementally advance lifecycle state for one symbol.

    Replay controls should call read/query paths. This writer is intended for
    symbol load or scheduled daily refresh, and only processes bars beyond the
    stored high-water mark unless force=True.
    """
    normalized_symbol = str(symbol).strip().upper()
    normalized_timeframe = str(timeframe).strip().lower()
    warmup_config = warmup_config or load_warmup_config()
    breakout_config = BreakoutStateConfig(**asdict(warmup_config.breakout))
    lookback_years = warmup_config.lifecycle.default_lookback_years if lookback_years is None else lookback_years
    bars = _normalize_price_frame(price_df)
    if bars.empty:
        return LifecycleWarmupResult(
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            processed_bars=0,
            upserted_zones=0,
            snapshots=0,
            observations=0,
            pattern_events=0,
            divergence_events=0,
            zone_bar_updates=0,
            breakout_updates=0,
            warmup_start_ts=None,
            last_processed_ts=None,
        )

    as_of_ts = _coerce_timestamp(as_of_date) or bars["timestamp"].max()
    snapshot_start_ts = _coerce_timestamp(snapshot_start_date)
    snapshot_end_ts = _coerce_timestamp(snapshot_end_date) or as_of_ts
    bars = bars[bars["timestamp"] <= as_of_ts].copy()
    if bars.empty:
        return LifecycleWarmupResult(
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            processed_bars=0,
            upserted_zones=0,
            snapshots=0,
            observations=0,
            pattern_events=0,
            divergence_events=0,
            zone_bar_updates=0,
            breakout_updates=0,
            warmup_start_ts=None,
            last_processed_ts=None,
        )

    state = _get_state(session, normalized_symbol, normalized_timeframe)
    warmup_start_ts = (
        _warmup_start_from_snapshot_start(bars, snapshot_start_ts)
        if int(lookback_years) <= 0 and snapshot_start_ts is not None
        else _warmup_start(bars, as_of_ts, lookback_years)
    )
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
            observations=0,
            pattern_events=0,
            divergence_events=0,
            zone_bar_updates=0,
            breakout_updates=0,
            warmup_start_ts=state.warmup_start_ts if state is not None else warmup_start_ts,
            last_processed_ts=state.last_processed_ts if state is not None else None,
        )

    upserted_zone_ids: set[str] = set()
    snapshot_count = 0
    observation_count = 0
    pattern_event_count = 0
    divergence_event_count = 0
    zone_bar_updates = 0
    breakout_updates = 0
    last_processed_ts: datetime | None = None

    for row in bars_to_process.itertuples(index=False):
        bar = _row_to_bar(row)
        history = bars[bars["timestamp"] <= bar.timestamp]
        dashboard_zones = list(zone_provider(history.copy(), bar))
        for observation in list(getattr(zone_provider, "latest_observations", []) or []):
            record_market_observation(
                session,
                MarketObservationInput(
                    symbol=normalized_symbol,
                    timeframe=str(observation.get("timeframe", normalized_timeframe)),
                    snapshot_ts=bar.timestamp,
                    observation_type=str(observation.get("observation_type", "observation")),
                    label=str(observation.get("label", "")),
                    value=float(observation.get("value")),
                    metadata=observation.get("metadata") or {},
                ),
            )
            observation_count += 1

        for pattern_event in detect_pattern_events_for_latest_bar(
            history,
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            config=warmup_config.pattern_events,
        ):
            record_pattern_event(session, pattern_event)
            pattern_event_count += 1

        for divergence_event in detect_macd_divergence_events_for_latest_bar(
            history,
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            config=warmup_config.macd_divergence,
        ):
            record_divergence_event(session, divergence_event)
            divergence_event_count += 1

        selected_zones: list[Zone] = []
        for dashboard_zone in dashboard_zones:
            if _dashboard_zone_has_deprecated_source(dashboard_zone):
                continue
            zone = upsert_dashboard_zone(
                session,
                symbol=normalized_symbol,
                zone=dashboard_zone,
                observed_ts=bar.timestamp,
            )
            upserted_zone_ids.add(zone.zone_id)
            selected_zones.append(zone)

        expire_event_zones(
            session,
            current_ts=bar.timestamp,
            bars_since_created_by_zone_id=_bars_since_origin_by_zone_id(selected_zones, history),
            ttl_by_timeframe=warmup_config.lifecycle.event_zone_ttl_bars,
            weekly_swing_expiration_days=warmup_config.lifecycle.weekly_swing_expiration_days,
        )

        active_zones = session.scalars(
            select(Zone)
            .where(Zone.symbol == normalized_symbol)
            .where(Zone.status.in_(ACTIVE_ZONE_STATUSES))
        ).all()
        matching_active_zones = [
            zone
            for zone in active_zones
            if not _has_deprecated_source(zone)
        ]
        for zone in matching_active_zones:
            event = process_zone_bar(session, zone, bar, config=breakout_config)
            zone_bar_updates += 1
            if event is not None:
                breakout_updates += 1
        expire_event_zones(
            session,
            current_ts=bar.timestamp,
            bars_since_created_by_zone_id=_bars_since_origin_by_zone_id(matching_active_zones, history),
            ttl_by_timeframe=warmup_config.lifecycle.event_zone_ttl_bars,
            weekly_swing_expiration_days=warmup_config.lifecycle.weekly_swing_expiration_days,
        )
        for zone in matching_active_zones:
            if zone.status in ACTIVE_ZONE_STATUSES:
                zone.zone_strength_pct = _zone_strength_pct(
                    zone,
                    history,
                    lookback_weeks=warmup_config.zone_generation.strength_lookback_weeks,
                )

        snapshot_zones_by_id: dict[str, Zone] = {}
        for zone in matching_active_zones:
            if (
                zone.status in ACTIVE_ZONE_STATUSES
                and (snapshot_start_ts is None or pd.Timestamp(bar.timestamp) >= snapshot_start_ts)
                and pd.Timestamp(bar.timestamp) <= snapshot_end_ts
            ):
                snapshot_zones_by_id[zone.zone_id] = zone

        for zone in snapshot_zones_by_id.values():
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
        observations=observation_count,
        pattern_events=pattern_event_count,
        divergence_events=divergence_event_count,
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
    frame = frame.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    frame["previous_close"] = frame["close"].shift(1)
    frame["volume_p80_20"] = frame["volume"].shift(1).rolling(window=20, min_periods=20).quantile(0.80)
    frame["bar_index"] = range(len(frame))
    return frame


def _first_existing_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    return None


def _row_to_bar(row) -> BarInput:
    atr = None if pd.isna(row.atr) else float(row.atr)
    previous_close = None if pd.isna(row.previous_close) else float(row.previous_close)
    volume_p80_20 = None if pd.isna(row.volume_p80_20) else float(row.volume_p80_20)
    return BarInput(
        timestamp=pd.Timestamp(row.timestamp).to_pydatetime(),
        open=float(row.open),
        high=float(row.high),
        low=float(row.low),
        close=float(row.close),
        atr=atr,
        previous_close=previous_close,
        volume=float(row.volume),
        volume_p80_20=volume_p80_20,
        bar_index=int(row.bar_index),
    )


def _warmup_start(bars: pd.DataFrame, as_of_ts: pd.Timestamp, lookback_years: int) -> datetime:
    requested_start = as_of_ts - pd.DateOffset(years=max(int(lookback_years), 0))
    available = bars[bars["timestamp"] >= requested_start]
    if available.empty:
        return pd.Timestamp(bars["timestamp"].min()).to_pydatetime()
    return pd.Timestamp(available["timestamp"].min()).to_pydatetime()


def _warmup_start_from_snapshot_start(bars: pd.DataFrame, snapshot_start_ts: pd.Timestamp) -> datetime:
    available = bars[bars["timestamp"] >= snapshot_start_ts]
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


def _has_deprecated_source(zone: Zone) -> bool:
    if zone.zone_kind == ZoneKind.COMPOSITE:
        return True
    sources = {str(source).strip().lower() for source in zone.source or []}
    return _has_deprecated_source_values(sources)


def _dashboard_zone_has_deprecated_source(zone: dict[str, Any]) -> bool:
    if zone.get("zone_kind") == ZoneKind.COMPOSITE:
        return True
    return _has_deprecated_source_values(_coerce_string_set(zone.get("source_types")))


def _has_deprecated_source_values(sources: set[str]) -> bool:
    if "swing_w" not in sources:
        return True
    return bool(sources & DEPRECATED_ZONE_SOURCES) or any(
        source.startswith("avwap_") or source.startswith("vp_")
        for source in sources
    )


def _coerce_string_set(value) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {item.strip().lower() for item in value.split(",") if item.strip()}
    return {str(item).strip().lower() for item in value if str(item).strip()}


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


def _zone_strength_pct(zone: Zone, history: pd.DataFrame, *, lookback_weeks: int = 52) -> float:
    return zone_strength_from_interval(
        price_history=history,
        price_low=float(zone.price_low),
        price_high=float(zone.price_high),
        date_column="timestamp",
        lookback_weeks=lookback_weeks,
    )["zone_strength_pct"]


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
