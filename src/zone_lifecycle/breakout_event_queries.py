from __future__ import annotations

import pandas as pd
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from .models import BreakoutEvent


BREAKOUT_EVENT_DISPLAY_NAMES = {
    "confirmed": "\u7a81\u7834",
    "true_breakout_weak": "\u5f31\u7a81\u7834",
    "retest_success": "\u56de\u8e29\u786e\u8ba4",
}


def load_breakout_events(
    session: Session,
    *,
    symbol: str,
    start_time,
    end_time,
    timeframe: str | None = None,
) -> list[dict]:
    start_ts = pd.Timestamp(start_time).to_pydatetime().replace(tzinfo=None)
    end_ts = pd.Timestamp(end_time).to_pydatetime().replace(tzinfo=None)
    query = (
        select(BreakoutEvent)
        .where(BreakoutEvent.symbol == str(symbol).strip().upper())
    )
    # Breakout events are detected on the daily bar stream but tagged with the
    # owning zone's timeframe (e.g. weekly swing zones -> 'w'). The daily chart
    # must show them all, so only filter by timeframe when one is explicitly given.
    if timeframe is not None:
        query = query.where(BreakoutEvent.timeframe == str(timeframe).strip().lower())
    rows = session.scalars(
        query.where(
            BreakoutEvent.status.in_(BREAKOUT_EVENT_DISPLAY_NAMES),
            BreakoutEvent.direction == "up",
            or_(
                BreakoutEvent.breakout_bar.between(start_ts, end_ts),
                BreakoutEvent.updated_ts.between(start_ts, end_ts),
            )
        )
        .order_by(BreakoutEvent.breakout_bar, BreakoutEvent.status)
    ).all()
    return [
        {
            "event_id": event.breakout_event_id,
            "event_time": _display_time(event),
            "event_type": event.status,
            "event_name": BREAKOUT_EVENT_DISPLAY_NAMES.get(event.status, event.status),
            "direction": event.direction,
            "price": float(event.breakout_close),
            "source": "breakout",
            "metadata": {
                **(event.metadata_json or {}),
                "zone_id": event.zone_id,
                "follow_through_atr": float(event.follow_through_atr or 0.0),
            },
        }
        for event in rows
        if start_ts <= _display_time(event) <= end_ts
        and _is_displayable_breakout_event(event)
    ]


def _display_time(event: BreakoutEvent):
    if event.status == "retest_success":
        return event.updated_ts
    return event.breakout_bar


def _is_displayable_breakout_event(event: BreakoutEvent) -> bool:
    if event.status != "retest_success":
        return True
    return bool((event.metadata_json or {}).get("parent_breakout_event_id"))
