from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from .constants import ZoneRole, ZoneStatus
from .identity import ZoneIdentityInput, generate_zone_id, infer_zone_kind
from .models import Zone, ZoneDailySnapshot


@dataclass(frozen=True, slots=True)
class ZoneSnapshotInput:
    zone_id: str
    snapshot_ts: datetime | pd.Timestamp | str
    current_price: float
    atr: float | None = None


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
    merged_from_zone_ids: list[str] | tuple[str, ...] | None = None,
    metadata: dict[str, Any] | None = None,
    observed_ts=None,
) -> Zone:
    normalized_source = _normalize_source(source)
    normalized_merged_from = _normalize_source(merged_from_zone_ids or ())
    resolved_kind = zone_kind or infer_zone_kind(normalized_source, normalized_merged_from)
    low = float(price_low)
    high = float(price_high)
    center = (low + high) / 2.0
    now = _coerce_datetime(observed_ts) or datetime.now(UTC).replace(tzinfo=None)
    resolved_zone_id = zone_id or generate_zone_id(
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
            merged_from_zone_ids=tuple(normalized_merged_from),
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
            origin_bar=_coerce_datetime(origin_bar),
            origin_event_id=origin_event_id,
            origin_event_type=origin_event_type,
            created_ts=now,
            updated_ts=now,
            vp_window_type=vp_window_type,
            merged_from_zone_ids=normalized_merged_from or None,
            metadata_json=metadata or {},
        )
        session.add(zone)
        session.flush()
        return zone

    zone.price_low = low
    zone.price_high = high
    zone.price_center = center
    zone.current_role = _normalize_role(current_role)
    zone.source = normalized_source
    zone.updated_ts = now
    zone.vp_window_type = vp_window_type
    zone.merged_from_zone_ids = normalized_merged_from or None
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


def _normalize_source(values) -> list[str]:
    return sorted({str(value).strip().lower() for value in values if str(value).strip()})


def _normalize_role(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized in {ZoneRole.SUPPORT, ZoneRole.RESISTANCE, ZoneRole.NEUTRAL}:
        return normalized
    return ZoneRole.NEUTRAL


def _coerce_datetime(value) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).to_pydatetime().replace(tzinfo=None)
