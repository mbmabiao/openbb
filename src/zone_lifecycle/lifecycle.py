from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from .constants import EVENT_ZONE_TTL_BARS, ZoneKind, ZoneStatus
from .models import Zone


@dataclass(frozen=True, slots=True)
class BarInput:
    timestamp: datetime | pd.Timestamp | str
    open: float
    high: float
    low: float
    close: float
    atr: float | None = None


def expire_event_zones(
    session: Session,
    *,
    current_ts,
    bars_since_created_by_zone_id: Mapping[str, int],
    ttl_by_timeframe: Mapping[str, int] | None = None,
) -> int:
    ttl_lookup = {**EVENT_ZONE_TTL_BARS, **(ttl_by_timeframe or {})}
    current_timestamp = _coerce_timestamp(current_ts)
    expired_count = 0

    zones = session.scalars(
        select(Zone).where(
            Zone.zone_kind == ZoneKind.EVENT,
            Zone.status.notin_([ZoneStatus.EXPIRED, ZoneStatus.INVALIDATED]),
        )
    ).all()
    for zone in zones:
        ttl = ttl_lookup.get(_normalize_timeframe(zone.timeframe))
        bars_since_created = bars_since_created_by_zone_id.get(zone.zone_id)
        if ttl is None or bars_since_created is None:
            continue
        if int(bars_since_created) >= int(ttl):
            zone.status = ZoneStatus.EXPIRED
            zone.expired_ts = current_timestamp
            zone.updated_ts = current_timestamp
            expired_count += 1
    return expired_count


def apply_composite_lifecycle(session: Session, *, current_ts) -> int:
    current_timestamp = _coerce_timestamp(current_ts)
    changed_count = 0
    composites = session.scalars(
        select(Zone).where(
            Zone.zone_kind == ZoneKind.COMPOSITE,
            Zone.status.notin_([ZoneStatus.EXPIRED, ZoneStatus.INVALIDATED]),
        )
    ).all()

    for composite in composites:
        source_ids = composite.merged_from_zone_ids or []
        if not source_ids:
            continue
        sources = session.scalars(select(Zone).where(Zone.zone_id.in_(source_ids))).all()
        if not sources:
            continue

        has_active_vp = any(
            source.zone_kind == ZoneKind.VP
            and source.status not in {ZoneStatus.EXPIRED, ZoneStatus.INVALIDATED}
            for source in sources
        )
        all_expired = all(source.status == ZoneStatus.EXPIRED for source in sources)
        any_invalidated = any(source.status == ZoneStatus.INVALIDATED for source in sources)

        if all_expired:
            composite.status = ZoneStatus.EXPIRED
            composite.expired_ts = current_timestamp
            composite.updated_ts = current_timestamp
            changed_count += 1
        elif any_invalidated and not has_active_vp:
            composite.status = ZoneStatus.INVALIDATED
            composite.invalidated_ts = current_timestamp
            composite.updated_ts = current_timestamp
            changed_count += 1
    return changed_count


def update_zone_interaction_counts(
    zone: Zone,
    bar: BarInput,
    *,
    breakout_buffer: float,
) -> None:
    high = float(bar.high)
    low = float(bar.low)
    close = float(bar.close)
    price_low = float(zone.price_low)
    price_high = float(zone.price_high)
    price_center = float(zone.price_center)

    close_inside = price_low <= close <= price_high
    confirmed_up = close > price_high + float(breakout_buffer)
    confirmed_down = close < price_low - float(breakout_buffer)

    if close_inside:
        zone.close_inside_count += 1
    if close_inside and not confirmed_up and not confirmed_down:
        zone.touch_count += 1
    if low <= price_center <= high:
        zone.break_count += 1
    false_break = (
        (high > price_high + float(breakout_buffer) or low < price_low - float(breakout_buffer))
        and close_inside
    )
    if false_break:
        zone.false_break_count += 1
    if confirmed_up or confirmed_down:
        zone.confirmed_breakout_count += 1
    zone.updated_ts = _coerce_timestamp(bar.timestamp)


def _normalize_timeframe(value: str) -> str:
    return str(value).strip().lower()


def _coerce_timestamp(value) -> datetime:
    return pd.Timestamp(value).to_pydatetime().replace(tzinfo=None)
