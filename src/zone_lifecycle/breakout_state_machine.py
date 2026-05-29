from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session, object_session

from .constants import BREAKOUT_TERMINAL_STATUSES, BreakoutEventStatus, ZoneRole, ZoneStatus
from .lifecycle import BarInput, update_zone_interaction_counts
from .models import BreakoutEvent, Zone


@dataclass(frozen=True, slots=True)
class BreakoutStateConfig:
    breakout_confirm_buffer_atr: float = 0.0
    failure_buffer_atr: float = 0.0
    strong_follow_through_atr: float = 1.00
    weak_follow_through_atr: float = 0.30
    follow_through_window_bars: int = 5
    fast_failure_window_bars: int = 3
    failure_window_bars: int = 5
    retest_window_bars: int = 3


def process_zone_bar(
    session: Session,
    zone: Zone,
    bar: BarInput,
    config: BreakoutStateConfig | None = None,
) -> BreakoutEvent | None:
    config = config or BreakoutStateConfig()
    atr = _valid_atr(bar.atr)
    breakout_buffer = config.breakout_confirm_buffer_atr * atr
    timestamp = _coerce_timestamp(bar.timestamp)

    update_zone_interaction_counts(zone, bar, breakout_buffer=breakout_buffer)

    active_event = _find_active_breakout_event(session, zone.zone_id)
    if active_event is not None:
        return _advance_breakout_event(
            event=active_event,
            zone=zone,
            bar=bar,
            timestamp=timestamp,
            config=config,
        )

    if zone.status in {ZoneStatus.EXPIRED, ZoneStatus.INVALIDATED}:
        return None

    status = _initial_breakout_status(
        zone=zone,
        bar=bar,
    )
    if status is None:
        return None
    direction, status = status

    event = BreakoutEvent(
        breakout_event_id=_breakout_event_id(zone.zone_id, status, timestamp),
        zone_id=zone.zone_id,
        symbol=zone.symbol,
        timeframe=zone.timeframe,
        direction=direction,
        status=status,
        breakout_bar=timestamp,
        breakout_close=float(bar.close),
        atr_at_breakout=atr,
        max_high_after_breakout=float(bar.high),
        min_low_after_breakout=float(bar.low),
        follow_through_atr=0.0,
        created_ts=timestamp,
        updated_ts=timestamp,
        metadata_json=_event_metadata(bar),
    )
    session.add(event)
    _sync_zone_for_event_status(zone, event, timestamp)
    return event


def _advance_breakout_event(
    *,
    event: BreakoutEvent,
    zone: Zone,
    bar: BarInput,
    timestamp: datetime,
    config: BreakoutStateConfig,
) -> BreakoutEvent:
    previous_status = event.status
    event.max_high_after_breakout = max(
        float(event.max_high_after_breakout or bar.high),
        float(bar.high),
    )
    event.min_low_after_breakout = min(
        float(event.min_low_after_breakout or bar.low),
        float(bar.low),
    )
    event.follow_through_atr = _follow_through_atr(event)
    bars_since_confirmed = _bars_since_confirmed(event, bar)
    open_price = float(bar.open)
    close = float(bar.close)
    high = float(bar.high)
    low = float(bar.low)

    if 1 <= bars_since_confirmed <= _effective_retest_window(config) and _is_retest_success(event, zone, high, low, open_price, close):
        event.updated_ts = timestamp
        session = object_session(event)
        existing_retest = _find_retest_event_for_parent(session=session, parent_event_id=event.breakout_event_id)
        if existing_retest is not None:
            _sync_zone_for_event_status(zone, existing_retest, timestamp, previous_status=previous_status)
            return existing_retest
        retest_event = _create_retest_event(
            parent=event,
            zone=zone,
            bar=bar,
            timestamp=timestamp,
            atr=_valid_atr(bar.atr),
        )
        if session is not None:
            session.add(retest_event)
        _sync_zone_for_event_status(zone, retest_event, timestamp, previous_status=previous_status)
        return retest_event

    event.updated_ts = timestamp
    _sync_zone_for_event_status(zone, event, timestamp, previous_status=previous_status)
    return event


def _initial_breakout_status(
    *,
    zone: Zone,
    bar: BarInput,
) -> tuple[str, str] | None:
    close = float(bar.close)
    previous_close = _previous_close(bar)
    center = float(zone.price_center)
    if previous_close is None:
        return None
    if previous_close < center and close > center:
        status = BreakoutEventStatus.CONFIRMED if _has_volume_confirmation(bar) else BreakoutEventStatus.TRUE_BREAKOUT_WEAK
        return "up", status
    return None


def _sync_zone_for_event_status(
    zone: Zone,
    event: BreakoutEvent,
    timestamp: datetime,
    previous_status: str | None = None,
) -> None:
    if event.status in {BreakoutEventStatus.CONFIRMED, BreakoutEventStatus.TRUE_BREAKOUT_WEAK}:
        zone.status = ZoneStatus.FLIPPED
        zone.current_role = ZoneRole.SUPPORT
    elif event.status == BreakoutEventStatus.RETEST_SUCCESS:
        zone.status = ZoneStatus.RETESTED
        zone.current_role = ZoneRole.SUPPORT
    elif event.status == BreakoutEventStatus.RETESTING:
        zone.status = ZoneStatus.FLIPPED
        zone.current_role = ZoneRole.SUPPORT
    zone.updated_ts = timestamp


def _find_active_breakout_event(session: Session, zone_id: str) -> BreakoutEvent | None:
    return session.scalars(
        select(BreakoutEvent)
        .where(
            BreakoutEvent.zone_id == zone_id,
            BreakoutEvent.status.notin_(BREAKOUT_TERMINAL_STATUSES),
        )
        .order_by(BreakoutEvent.created_ts.desc())
    ).first()


def _effective_retest_window(config: BreakoutStateConfig) -> int:
    return min(max(int(config.retest_window_bars), 0), 3)


def _find_retest_event_for_parent(session: Session | None, parent_event_id: str) -> BreakoutEvent | None:
    if session is None:
        return None
    return session.scalars(
        select(BreakoutEvent)
        .where(BreakoutEvent.status == BreakoutEventStatus.RETEST_SUCCESS)
        .where(BreakoutEvent.metadata_json["parent_breakout_event_id"].as_string() == parent_event_id)
    ).first()


def _create_retest_event(
    *,
    parent: BreakoutEvent,
    zone: Zone,
    bar: BarInput,
    timestamp: datetime,
    atr: float,
) -> BreakoutEvent:
    metadata = _event_metadata(bar)
    metadata.update(
        {
            "parent_breakout_event_id": parent.breakout_event_id,
            "parent_breakout_bar": pd.Timestamp(parent.breakout_bar).isoformat(),
            "parent_breakout_close": float(parent.breakout_close),
            "parent_breakout_status": parent.status,
            "zone_center": float(zone.price_center),
        }
    )
    return BreakoutEvent(
        breakout_event_id=_breakout_event_id(zone.zone_id, BreakoutEventStatus.RETEST_SUCCESS, timestamp),
        zone_id=zone.zone_id,
        symbol=zone.symbol,
        timeframe=zone.timeframe,
        direction=parent.direction,
        status=BreakoutEventStatus.RETEST_SUCCESS,
        breakout_bar=timestamp,
        breakout_close=float(bar.close),
        atr_at_breakout=atr,
        max_high_after_breakout=float(bar.high),
        min_low_after_breakout=float(bar.low),
        follow_through_atr=parent.follow_through_atr,
        created_ts=timestamp,
        updated_ts=timestamp,
        metadata_json=metadata,
    )


def _is_retest_success(
    event: BreakoutEvent,
    zone: Zone,
    high: float,
    low: float,
    open_price: float,
    close: float,
) -> bool:
    if event.status not in {BreakoutEventStatus.CONFIRMED, BreakoutEventStatus.TRUE_BREAKOUT_WEAK}:
        return False
    if event.direction != "up":
        return False
    center = float(zone.price_center)
    return low <= center and open_price > center and close > center and float(event.breakout_close) > center


def _follow_through_atr(event: BreakoutEvent) -> float:
    atr = max(float(event.atr_at_breakout), 1e-9)
    return (float(event.max_high_after_breakout or event.breakout_close) - event.breakout_close) / atr


def _previous_close(bar: BarInput) -> float | None:
    value = getattr(bar, "previous_close", None)
    if value is None or pd.isna(value):
        return None
    return float(value)


def _has_volume_confirmation(bar: BarInput) -> bool:
    volume = getattr(bar, "volume", None)
    threshold = getattr(bar, "volume_p80_20", None)
    if volume is None or threshold is None or pd.isna(volume) or pd.isna(threshold):
        return False
    return float(volume) >= float(threshold)


def _event_metadata(bar: BarInput) -> dict:
    metadata = {
        "previous_close": _previous_close(bar),
        "volume": _optional_float(getattr(bar, "volume", None)),
        "volume_p80_20": _optional_float(getattr(bar, "volume_p80_20", None)),
        "volume_confirmed": _has_volume_confirmation(bar),
    }
    if getattr(bar, "bar_index", None) is not None:
        metadata["bar_index"] = int(bar.bar_index)
    return metadata


def _optional_float(value: float | None) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _bars_since_confirmed(event: BreakoutEvent, bar: BarInput) -> int:
    bar_index = getattr(bar, "bar_index", None)
    event_index = (event.metadata_json or {}).get("bar_index")
    if bar_index is not None and event_index is not None:
        return max(int(bar_index) - int(event_index), 0)
    current_ts = pd.Timestamp(bar.timestamp).normalize()
    breakout_ts = pd.Timestamp(event.breakout_bar).normalize()
    return max(int((current_ts - breakout_ts) / pd.Timedelta(days=1)), 0)


def _breakout_event_id(zone_id: str, status: str, timestamp: datetime) -> str:
    payload = {
        "zone_id": zone_id,
        "status": status,
        "timestamp": pd.Timestamp(timestamp).isoformat(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"breakout_{digest}"


def _valid_atr(value: float | None) -> float:
    if value is None or float(value) <= 0:
        return 1.0
    return float(value)


def _coerce_timestamp(value) -> datetime:
    return pd.Timestamp(value).to_pydatetime().replace(tzinfo=None)
