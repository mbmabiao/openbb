from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from .identity import infer_zone_kind
from .models import Zone
from .service import upsert_zone


def upsert_dashboard_zone(
    session: Session,
    *,
    symbol: str,
    zone: dict[str, Any],
    observed_ts=None,
) -> Zone:
    source_types = _coerce_string_set(zone.get("source_types"))
    timeframe_values = _coerce_string_set(zone.get("timeframes"))
    timeframe = zone.get("timeframe_sources") or zone.get("primary_timeframe")
    if not timeframe:
        timeframe = ",".join(sorted(timeframe_values)) if timeframe_values else "1d"

    zone_kind = zone.get("zone_kind") or infer_zone_kind(source_types)
    zone_id = zone.get("zone_id")

    metadata = {
        "dashboard_type": zone.get("type"),
        "source_label": zone.get("source_label", ""),
        "source_types_label": zone.get("source_types_label", ""),
        "timeframes": sorted(timeframe_values),
        "raw_zone_id": zone.get("zone_id"),
    }

    return upsert_zone(
        session,
        symbol=symbol,
        timeframe=str(timeframe),
        source=source_types,
        price_low=float(zone["lower"]),
        price_high=float(zone["upper"]),
        current_role=str(zone.get("side", "neutral")),
        zone_kind=zone_kind,
        zone_id=zone_id,
        origin_bar=zone.get("origin_bar") or zone.get("anchor_start_date"),
        origin_event_id=zone.get("origin_event_id") or zone.get("anchor_name"),
        origin_event_type=zone.get("origin_event_type") or zone.get("anchor_family"),
        vp_window_type=zone.get("vp_window_type") or zone.get("source_label"),
        vp_structure_key=zone.get("vp_structure_key") or zone.get("origin_event_id"),
        metadata=metadata,
        zone_strength_pct=zone.get("zone_strength_pct"),
        observed_ts=observed_ts,
    )


def _coerce_string_set(value) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {item.strip() for item in value.split(",") if item.strip()}
    return {str(item).strip() for item in value if str(item).strip()}
