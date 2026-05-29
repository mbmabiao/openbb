from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session
import pandas as pd

from .models import PatternEvent


def load_pattern_events(
    session: Session,
    *,
    symbol: str,
    start_time,
    end_time,
    timeframe: str = "1d",
) -> list[dict]:
    start_ts = pd.Timestamp(start_time).to_pydatetime().replace(tzinfo=None)
    end_ts = pd.Timestamp(end_time).to_pydatetime().replace(tzinfo=None)
    rows = session.scalars(
        select(PatternEvent)
        .where(PatternEvent.symbol == str(symbol).strip().upper())
        .where(PatternEvent.timeframe == str(timeframe).strip().lower())
        .where(PatternEvent.event_time >= start_ts)
        .where(PatternEvent.event_time <= end_ts)
        .order_by(PatternEvent.event_time, PatternEvent.event_type)
    ).all()
    return [
        {
            "event_id": event.event_id,
            "event_time": event.event_time,
            "event_type": event.event_type,
            "direction": event.direction,
            "price_close": float(event.price_close),
            "volume_percentile_20": float(event.volume_percentile_20),
            "abs_price_change_percentile_20": float(event.abs_price_change_percentile_20),
        }
        for event in rows
    ]
