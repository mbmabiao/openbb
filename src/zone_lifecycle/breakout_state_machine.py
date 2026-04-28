from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from .constants import BREAKOUT_TERMINAL_STATUSES, BreakoutEventStatus, ZoneRole, ZoneStatus
from .lifecycle import BarInput, update_zone_interaction_counts
from .models import BreakoutEvent, Zone


@dataclass(frozen=True, slots=True)
class BreakoutStateConfig:
    breakout_confirm_buffer_atr: float = 0.10
    failure_buffer_atr: float = 0.10
    strong_follow_through_atr: float = 1.00
    weak_follow_through_atr: float = 0.30
    follow_through_window_bars: int = 5
    fast_failure_window_bars: int = 3
    failure_window_bars: int = 10
    retest_window_bars: int = 10


def process_zone_bar(
    session: Session,
    zone: Zone,
    bar: BarInput,
    config: BreakoutStateConfig | None = None,
) -> BreakoutEvent | None:
    config = config or BreakoutStateConfig()
    atr = _valid_atr(bar.atr)
    breakout_buffer = config.breakout_confirm_buffer_atr * atr
    failure_buffer = config.failure_buffer_atr * atr
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
            failure_buffer=failure_buffer,
        )

    if zone.status in {ZoneStatus.EXPIRED, ZoneStatus.INVALIDATED}:
        return None

    direction = _breakout_direction(zone)
    status = _initial_breakout_status(
        zone=zone,
        bar=bar,
        breakout_buffer=breakout_buffer,
        direction=direction,
    )
    if status is None:
        return None

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
        metadata_json={},
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
    failure_buffer: float,
) -> BreakoutEvent:
    previous_status = event.status
    if event.status == BreakoutEventStatus.ATTEMPT:
        next_status = _initial_breakout_status(
            zone=zone,
            bar=bar,
            breakout_buffer=config.breakout_confirm_buffer_atr * event.atr_at_breakout,
            direction=event.direction,
        )
        if next_status in {BreakoutEventStatus.CONFIRMED, BreakoutEventStatus.FALSE_BREAKOUT}:
            event.status = next_status
            event.breakout_bar = timestamp
            event.breakout_close = float(bar.close)
            event.atr_at_breakout = _valid_atr(bar.atr)
            event.max_high_after_breakout = float(bar.high)
            event.min_low_after_breakout = float(bar.low)
            event.follow_through_atr = 0.0
            event.updated_ts = timestamp
            _sync_zone_for_event_status(zone, event, timestamp, previous_status=previous_status)
        return event

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
    close = float(bar.close)
    high = float(bar.high)
    low = float(bar.low)

    if _is_retest_failed(event, zone, close, failure_buffer):
        event.status = BreakoutEventStatus.RETEST_FAILED
    elif bars_since_confirmed <= config.failure_window_bars and _is_failed_breakout(event, zone, close, failure_buffer):
        event.status = BreakoutEventStatus.FAILED_BREAKOUT
    elif _is_fast_false_breakout(event, zone, close, bars_since_confirmed, config):
        event.status = BreakoutEventStatus.FALSE_BREAKOUT
    elif bars_since_confirmed <= config.retest_window_bars and _is_retest_success(event, zone, high, low, close, failure_buffer):
        event.status = BreakoutEventStatus.RETEST_SUCCESS
    elif _is_reclaimed(zone, close):
        event.status = BreakoutEventStatus.RECLAIMED
    elif bars_since_confirmed <= config.retest_window_bars and _is_retesting(event, zone, high, low, close):
        event.status = BreakoutEventStatus.RETESTING
        zone.retest_num += 1
    elif bars_since_confirmed <= config.follow_through_window_bars and event.follow_through_atr is not None:
        if event.follow_through_atr >= config.strong_follow_through_atr:
            event.status = BreakoutEventStatus.TRUE_BREAKOUT_STRONG
        elif event.follow_through_atr >= config.weak_follow_through_atr:
            event.status = BreakoutEventStatus.TRUE_BREAKOUT_WEAK

    event.updated_ts = timestamp
    _sync_zone_for_event_status(zone, event, timestamp, previous_status=previous_status)
    return event


def _initial_breakout_status(
    *,
    zone: Zone,
    bar: BarInput,
    breakout_buffer: float,
    direction: str,
) -> str | None:
    high = float(bar.high)
    low = float(bar.low)
    close = float(bar.close)
    if direction == "up":
        if high > zone.price_high + breakout_buffer and close <= zone.price_high:
            return BreakoutEventStatus.FALSE_BREAKOUT
        if close > zone.price_high + breakout_buffer:
            return BreakoutEventStatus.CONFIRMED
        if high > zone.price_high and close <= zone.price_high + breakout_buffer:
            return BreakoutEventStatus.ATTEMPT
    else:
        if low < zone.price_low - breakout_buffer and close >= zone.price_low:
            return BreakoutEventStatus.FALSE_BREAKOUT
        if close < zone.price_low - breakout_buffer:
            return BreakoutEventStatus.CONFIRMED
        if low < zone.price_low and close >= zone.price_low - breakout_buffer:
            return BreakoutEventStatus.ATTEMPT
    return None


def _sync_zone_for_event_status(
    zone: Zone,
    event: BreakoutEvent,
    timestamp: datetime,
    previous_status: str | None = None,
) -> None:
    if event.status == BreakoutEventStatus.CONFIRMED:
        zone.status = ZoneStatus.FLIPPED
        zone.current_role = ZoneRole.SUPPORT if event.direction == "up" else ZoneRole.RESISTANCE
    elif event.status == BreakoutEventStatus.RETEST_SUCCESS:
        zone.status = ZoneStatus.RETESTED
        zone.current_role = ZoneRole.SUPPORT if event.direction == "up" else ZoneRole.RESISTANCE
    elif event.status in {BreakoutEventStatus.FAILED_BREAKOUT, BreakoutEventStatus.RETEST_FAILED}:
        zone.status = ZoneStatus.INVALIDATED
        zone.invalidated_ts = timestamp
        zone.failed_breakout_count += 1
    elif event.status == BreakoutEventStatus.FALSE_BREAKOUT and previous_status is not None:
        zone.false_break_count += 1
    elif event.status == BreakoutEventStatus.RETESTING:
        zone.status = ZoneStatus.FLIPPED
        zone.current_role = ZoneRole.SUPPORT if event.direction == "up" else ZoneRole.RESISTANCE
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


def _breakout_direction(zone: Zone) -> str:
    if zone.current_role == ZoneRole.SUPPORT:
        return "down"
    return "up"


def _is_failed_breakout(event: BreakoutEvent, zone: Zone, close: float, failure_buffer: float) -> bool:
    if event.direction == "up":
        return close < zone.price_low - failure_buffer
    return close > zone.price_high + failure_buffer


def _is_retest_failed(event: BreakoutEvent, zone: Zone, close: float, failure_buffer: float) -> bool:
    if event.status != BreakoutEventStatus.RETESTING:
        return False
    return _is_failed_breakout(event, zone, close, failure_buffer)


def _is_fast_false_breakout(
    event: BreakoutEvent,
    zone: Zone,
    close: float,
    bars_since_confirmed: int,
    config: BreakoutStateConfig,
) -> bool:
    if bars_since_confirmed > config.fast_failure_window_bars:
        return False
    follow = float(event.follow_through_atr or 0.0)
    if follow >= config.weak_follow_through_atr:
        return False
    if event.direction == "up":
        return close <= zone.price_high
    return close >= zone.price_low


def _is_retest_success(
    event: BreakoutEvent,
    zone: Zone,
    high: float,
    low: float,
    close: float,
    failure_buffer: float,
) -> bool:
    if event.direction == "up":
        return low <= zone.price_high and close >= zone.price_high and close >= zone.price_low - failure_buffer
    return high >= zone.price_low and close <= zone.price_low and close <= zone.price_high + failure_buffer


def _is_retesting(event: BreakoutEvent, zone: Zone, high: float, low: float, close: float) -> bool:
    if event.direction == "up":
        return low <= zone.price_high and close >= zone.price_low
    return high >= zone.price_low and close <= zone.price_high


def _is_reclaimed(zone: Zone, close: float) -> bool:
    return zone.price_low <= close <= zone.price_high


def _follow_through_atr(event: BreakoutEvent) -> float:
    atr = max(float(event.atr_at_breakout), 1e-9)
    if event.direction == "up":
        return (float(event.max_high_after_breakout or event.breakout_close) - event.breakout_close) / atr
    return (event.breakout_close - float(event.min_low_after_breakout or event.breakout_close)) / atr


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
