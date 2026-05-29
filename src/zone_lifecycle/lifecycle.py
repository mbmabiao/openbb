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
    previous_close: float | None = None
    volume: float | None = None
    volume_p80_20: float | None = None
    bar_index: int | None = None


def expire_event_zones(
    session: Session,
    *,
    current_ts,
    bars_since_created_by_zone_id: Mapping[str, int],
    ttl_by_timeframe: Mapping[str, int] | None = None,
    weekly_swing_expiration_days: int = 182,
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
        if _is_weekly_swing_zone(zone):
            if _is_weekly_swing_expired(zone, current_timestamp, expiration_days=weekly_swing_expiration_days):
                zone.status = ZoneStatus.EXPIRED
                zone.expired_ts = current_timestamp
                zone.updated_ts = current_timestamp
                expired_count += 1
            continue
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
    previous_close = _previous_close(bar)

    close_inside = price_low <= close <= price_high
    confirmed_up = previous_close is not None and previous_close < price_center and close > price_center
    confirmed_down = previous_close is not None and previous_close > price_center and close < price_center

    if close_inside:
        zone.close_inside_count += 1
    if close_inside and not confirmed_up and not confirmed_down:
        zone.touch_count += 1
    if low <= price_center <= high:
        zone.break_count += 1
    if confirmed_up or confirmed_down:
        zone.confirmed_breakout_count += 1
    zone.updated_ts = _coerce_timestamp(bar.timestamp)


def _previous_close(bar: BarInput) -> float | None:
    value = getattr(bar, "previous_close", None)
    if value is None or pd.isna(value):
        return None
    return float(value)


def _normalize_timeframe(value: str) -> str:
    return str(value).strip().lower()


def _is_weekly_swing_zone(zone: Zone) -> bool:
    return any(str(source).strip().lower() == "swing_w" for source in zone.source or [])


def _is_weekly_swing_expired(zone: Zone, current_timestamp: datetime, *, expiration_days: int) -> bool:
    if zone.origin_bar is None:
        return False
    age = pd.Timestamp(current_timestamp) - pd.Timestamp(zone.origin_bar)
    return age >= pd.Timedelta(days=max(int(expiration_days), 0))


def _coerce_timestamp(value) -> datetime:
    return pd.Timestamp(value).to_pydatetime().replace(tzinfo=None)
