from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import math
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from .constants import ZoneKind, ZoneRole, ZoneStatus
from .identity import ZoneIdentityInput, generate_zone_id, infer_zone_kind
from .divergence_events import DivergenceEventInput
from .models import DivergenceEvent, MarketObservation, PatternEvent, Zone, ZoneDailySnapshot
from .pattern_events import PatternEventInput


@dataclass(frozen=True, slots=True)
class ZoneSnapshotInput:
    zone_id: str
    snapshot_ts: datetime | pd.Timestamp | str
    current_price: float
    atr: float | None = None


@dataclass(frozen=True, slots=True)
class MarketObservationInput:
    symbol: str
    timeframe: str
    snapshot_ts: datetime | pd.Timestamp | str
    observation_type: str
    label: str
    value: float
    metadata: dict[str, Any] | None = None


def upsert_zone(
    session: Session,
    *,
    symbol: str,
    timeframe: str,
    source: list[str] | tuple[str, ...] | set[str],
    price_low: float,
    price_high: float,
    current_role: str,
    zone_kind: str | None = None,
    zone_id: str | None = None,
    origin_bar=None,
    origin_event_id: str | None = None,
    origin_event_type: str | None = None,
    vp_window_type: str | None = None,
    vp_structure_key: str | None = None,
    metadata: dict[str, Any] | None = None,
    zone_strength_pct: float | None = None,
    observed_ts=None,
) -> Zone:
    normalized_source = _normalize_source(source)
    resolved_kind = zone_kind or infer_zone_kind(normalized_source)
    low = float(price_low)
    high = float(price_high)
    center = (low + high) / 2.0
    resolved_vp_structure_key = (
        vp_structure_key
        or origin_event_id
        or (
            _vp_price_bucket_key(vp_window_type or timeframe, center)
            if resolved_kind == ZoneKind.VP
            else None
        )
    )
    now = _coerce_datetime(observed_ts) or datetime.now(UTC).replace(tzinfo=None)
    strength_pct = _normalize_strength(zone_strength_pct)
    resolved_zone_id = zone_id
    if not resolved_zone_id:
        resolved_zone_id = generate_zone_id(
            ZoneIdentityInput(
                symbol=symbol,
                timeframe=timeframe,
                zone_kind=resolved_kind,
                source=tuple(normalized_source),
                price_low=low,
                price_high=high,
                origin_bar=origin_bar,
                origin_event_id=origin_event_id,
                vp_window_type=vp_window_type,
                vp_structure_key=resolved_vp_structure_key,
            )
        )

    zone = session.get(Zone, resolved_zone_id)
    if zone is None:
        zone = Zone(
            zone_id=resolved_zone_id,
            symbol=str(symbol).strip().upper(),
            timeframe=str(timeframe).strip().lower(),
            zone_kind=resolved_kind,
            source=normalized_source,
            price_center=center,
            price_low=low,
            price_high=high,
            current_role=_normalize_role(current_role),
            status=ZoneStatus.ACTIVE,
            zone_strength_pct=strength_pct,
            origin_bar=_coerce_datetime(origin_bar),
            origin_event_id=origin_event_id,
            origin_event_type=origin_event_type,
            created_ts=now,
            updated_ts=now,
            vp_window_type=vp_window_type,
            metadata_json=metadata or {},
        )
        session.add(zone)
        session.flush()
        return zone

    zone.price_low = low
    zone.price_high = high
    zone.price_center = center
    zone.current_role = _normalize_role(current_role)
    zone.timeframe = str(timeframe).strip().lower()
    zone.zone_kind = resolved_kind
    zone.source = normalized_source
    zone.zone_strength_pct = strength_pct
    if _is_weekly_swing_event(resolved_kind, normalized_source):
        zone.status = ZoneStatus.ACTIVE
        zone.expired_ts = None
        zone.invalidated_ts = None
    zone.updated_ts = now
    zone.vp_window_type = vp_window_type
    zone.metadata_json = {**(zone.metadata_json or {}), **(metadata or {})}
    return zone


def record_zone_snapshot(
    session: Session,
    snapshot: ZoneSnapshotInput,
) -> ZoneDailySnapshot:
    zone = session.get(Zone, snapshot.zone_id)
    if zone is None:
        raise ValueError(f"Zone not found: {snapshot.zone_id}")

    snapshot_ts = _coerce_datetime(snapshot.snapshot_ts)
    if snapshot_ts is None:
        raise ValueError("snapshot_ts is required")

    distance = distance_to_zone(
        current_price=snapshot.current_price,
        price_low=zone.price_low,
        price_high=zone.price_high,
    )
    distance_atr = None
    if snapshot.atr is not None and float(snapshot.atr) > 0:
        distance_atr = distance / float(snapshot.atr)

    snapshot_id = _snapshot_id(zone.zone_id, snapshot_ts)
    existing = session.scalars(
        select(ZoneDailySnapshot).where(ZoneDailySnapshot.snapshot_id == snapshot_id)
    ).one_or_none()
    if existing is None:
        existing = ZoneDailySnapshot(
            snapshot_id=snapshot_id,
            zone_id=zone.zone_id,
            symbol=zone.symbol,
            timeframe=zone.timeframe,
            snapshot_ts=snapshot_ts,
            current_price=float(snapshot.current_price),
            price_low=zone.price_low,
            price_high=zone.price_high,
            price_center=zone.price_center,
            distance_to_price=distance,
            distance_atr=distance_atr,
            zone_status=zone.status,
            current_role=zone.current_role,
            zone_strength_pct=zone.zone_strength_pct,
        )
        session.add(existing)
        session.flush()
        return existing

    existing.current_price = float(snapshot.current_price)
    existing.price_low = zone.price_low
    existing.price_high = zone.price_high
    existing.price_center = zone.price_center
    existing.distance_to_price = distance
    existing.distance_atr = distance_atr
    existing.zone_status = zone.status
    existing.current_role = zone.current_role
    existing.zone_strength_pct = zone.zone_strength_pct
    return existing


def record_market_observation(
    session: Session,
    observation: MarketObservationInput,
) -> MarketObservation:
    snapshot_ts = _coerce_datetime(observation.snapshot_ts)
    if snapshot_ts is None:
        raise ValueError("snapshot_ts is required")

    normalized_symbol = str(observation.symbol).strip().upper()
    observation_id = _market_observation_id(
        symbol=normalized_symbol,
        snapshot_ts=snapshot_ts,
        observation_type=observation.observation_type,
        label=observation.label,
    )
    existing = session.get(MarketObservation, observation_id)
    if existing is None:
        existing = MarketObservation(
            observation_id=observation_id,
            symbol=normalized_symbol,
            timeframe=str(observation.timeframe).strip().lower(),
            snapshot_ts=snapshot_ts,
            observation_type=str(observation.observation_type).strip().lower(),
            label=str(observation.label).strip(),
            value=float(observation.value),
            metadata_json=observation.metadata or {},
        )
        session.add(existing)
        session.flush()
        return existing

    existing.timeframe = str(observation.timeframe).strip().lower()
    existing.value = float(observation.value)
    existing.metadata_json = observation.metadata or {}
    return existing


def record_pattern_event(
    session: Session,
    event: PatternEventInput,
) -> PatternEvent:
    now = datetime.now(UTC).replace(tzinfo=None)
    existing = session.get(PatternEvent, event.event_id)
    if existing is None:
        existing = PatternEvent(
            event_id=event.event_id,
            symbol=str(event.symbol).strip().upper(),
            timeframe=str(event.timeframe).strip().lower(),
            event_time=_coerce_datetime(event.event_time),
            event_type=str(event.event_type).strip().lower(),
            direction=str(event.direction).strip().lower(),
            price_open=float(event.price_open),
            price_high=float(event.price_high),
            price_low=float(event.price_low),
            price_close=float(event.price_close),
            previous_close=float(event.previous_close),
            volume=float(event.volume),
            body_ratio=float(event.body_ratio),
            upper_wick_ratio=float(event.upper_wick_ratio),
            lower_wick_ratio=float(event.lower_wick_ratio),
            close_position=float(event.close_position),
            price_change_pct=float(event.price_change_pct),
            intrabar_return_pct=float(event.intrabar_return_pct),
            gap_pct=float(event.gap_pct),
            abs_price_change_pct=float(event.abs_price_change_pct),
            volume_percentile_20=float(event.volume_percentile_20),
            abs_price_change_percentile_20=float(event.abs_price_change_percentile_20),
            lookback_bars=int(event.lookback_bars),
            related_zone_id=event.related_zone_id,
            metadata_json=event.metadata or {},
            created_at=now,
        )
        session.add(existing)
        session.flush()
        return existing

    existing.direction = str(event.direction).strip().lower()
    existing.price_open = float(event.price_open)
    existing.price_high = float(event.price_high)
    existing.price_low = float(event.price_low)
    existing.price_close = float(event.price_close)
    existing.previous_close = float(event.previous_close)
    existing.volume = float(event.volume)
    existing.body_ratio = float(event.body_ratio)
    existing.upper_wick_ratio = float(event.upper_wick_ratio)
    existing.lower_wick_ratio = float(event.lower_wick_ratio)
    existing.close_position = float(event.close_position)
    existing.price_change_pct = float(event.price_change_pct)
    existing.intrabar_return_pct = float(event.intrabar_return_pct)
    existing.gap_pct = float(event.gap_pct)
    existing.abs_price_change_pct = float(event.abs_price_change_pct)
    existing.volume_percentile_20 = float(event.volume_percentile_20)
    existing.abs_price_change_percentile_20 = float(event.abs_price_change_percentile_20)
    existing.lookback_bars = int(event.lookback_bars)
    existing.related_zone_id = event.related_zone_id
    existing.metadata_json = event.metadata or {}
    return existing


def record_divergence_event(
    session: Session,
    event: DivergenceEventInput,
) -> DivergenceEvent:
    now = datetime.now(UTC).replace(tzinfo=None)
    existing = session.get(DivergenceEvent, event.event_id)
    timestamp = _coerce_datetime(event.timestamp)
    if timestamp is None:
        raise ValueError("timestamp is required")
    if existing is None:
        existing = DivergenceEvent(
            event_id=event.event_id,
            symbol=str(event.symbol).strip().upper(),
            timeframe=str(event.timeframe).strip().lower(),
            event_type=str(event.event_type).strip().lower(),
            event_name=str(event.event_name).strip(),
            timestamp=timestamp,
            price=float(event.price),
            direction=str(event.direction).strip().lower(),
            strength_score=float(event.strength_score),
            source=str(event.source).strip().lower(),
            metadata_json=event.metadata or {},
            created_at=now,
        )
        session.add(existing)
        session.flush()
        return existing

    existing.event_name = str(event.event_name).strip()
    existing.price = float(event.price)
    existing.direction = str(event.direction).strip().lower()
    existing.strength_score = float(event.strength_score)
    existing.source = str(event.source).strip().lower()
    existing.metadata_json = event.metadata or {}
    return existing


def distance_to_zone(current_price: float, price_low: float, price_high: float) -> float:
    current = float(current_price)
    low = float(price_low)
    high = float(price_high)
    if low <= current <= high:
        return 0.0
    if current < low:
        return low - current
    return current - high


def _snapshot_id(zone_id: str, snapshot_ts: datetime) -> str:
    payload = {
        "zone_id": zone_id,
        "snapshot_ts": pd.Timestamp(snapshot_ts).isoformat(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"snapshot_{digest}"


def _market_observation_id(
    *,
    symbol: str,
    snapshot_ts: datetime,
    observation_type: str,
    label: str,
) -> str:
    payload = {
        "symbol": symbol,
        "snapshot_ts": pd.Timestamp(snapshot_ts).isoformat(),
        "observation_type": str(observation_type).strip().lower(),
        "label": str(label).strip(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"observation_{digest}"


def _normalize_source(values) -> list[str]:
    return sorted({str(value).strip().lower() for value in values if str(value).strip()})


def _vp_price_bucket_key(window_name: str, center: float, bucket_pct: float = 0.005) -> str:
    normalized_window = str(window_name).strip().lower() or "vp"
    center_value = max(float(center), 1e-9)
    log_bucket = round(math.log(center_value) / math.log1p(max(float(bucket_pct), 1e-9)))
    return f"{normalized_window}:bucket_{log_bucket}"


def _normalize_role(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized in {ZoneRole.SUPPORT, ZoneRole.RESISTANCE, ZoneRole.NEUTRAL}:
        return normalized
    return ZoneRole.NEUTRAL


def _normalize_strength(value: float | None) -> float:
    if value is None:
        return 0.0
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(numeric):
        return 0.0
    return max(numeric, 0.0)


def _is_weekly_swing_event(zone_kind: str, sources: list[str]) -> bool:
    return zone_kind == ZoneKind.EVENT and any(source == "swing_w" for source in sources)


def _coerce_datetime(value) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).to_pydatetime().replace(tzinfo=None)
