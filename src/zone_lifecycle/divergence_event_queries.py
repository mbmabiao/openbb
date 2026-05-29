from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session
import pandas as pd

from .models import DivergenceEvent


def load_divergence_events(
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
        select(DivergenceEvent)
        .where(DivergenceEvent.symbol == str(symbol).strip().upper())
        .where(DivergenceEvent.timeframe == str(timeframe).strip().lower())
        .where(DivergenceEvent.timestamp >= start_ts)
        .where(DivergenceEvent.timestamp <= end_ts)
        .order_by(DivergenceEvent.timestamp, DivergenceEvent.event_type)
    ).all()
    events = [
        {
            "event_id": event.event_id,
            "event_time": event.timestamp,
            "event_type": event.event_type,
            "event_name": event.event_name,
            "direction": event.direction,
            "price": float(event.price),
            "strength_score": float(event.strength_score),
            "source": event.source,
            "metadata": event.metadata_json or {},
        }
        for event in rows
    ]
    return [_normalize_risk_divergence(event) for event in _prefer_confirmed_over_risk(events)]


def _prefer_confirmed_over_risk(events: list[dict]) -> list[dict]:
    confirmed_keys = {
        _risk_resolution_key(event)
        for event in events
        if _is_confirmed_divergence(event)
    }
    if not confirmed_keys:
        return events
    return [
        event
        for event in events
        if not (_is_risk_divergence(event) and _risk_resolution_key(event) in confirmed_keys)
    ]


def _risk_resolution_key(event: dict) -> tuple:
    return (
        event.get("event_time"),
        str(event.get("direction", "")).strip().lower(),
        str(event.get("source", "")).strip().lower(),
    )


def _is_confirmed_divergence(event: dict) -> bool:
    event_type = str(event.get("event_type", "")).strip().lower()
    return event_type in {"macd_bullish_divergence", "macd_bearish_divergence"}


def _is_risk_divergence(event: dict) -> bool:
    event_type = str(event.get("event_type", "")).strip().lower()
    return event_type in {"macd_bullish_divergence_risk", "macd_bearish_divergence_risk"}


def _normalize_risk_divergence(event: dict) -> dict:
    event_type = str(event.get("event_type", "")).strip().lower()
    if event_type == "macd_bullish_divergence_risk":
        normalized = event.copy()
        normalized["event_type"] = "macd_bullish_divergence"
        normalized["event_name"] = "底背离"
        metadata = dict(normalized.get("metadata") or {})
        metadata["is_risk"] = False
        normalized["metadata"] = metadata
        return normalized
    if event_type == "macd_bearish_divergence_risk":
        normalized = event.copy()
        normalized["event_type"] = "macd_bearish_divergence"
        normalized["event_name"] = "顶背离"
        metadata = dict(normalized.get("metadata") or {})
        metadata["is_risk"] = False
        normalized["metadata"] = metadata
        return normalized
    return event
