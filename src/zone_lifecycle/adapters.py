from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy.orm import Session

from .constants import ZoneKind
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

    merged_from_zone_ids = zone.get("merged_from_zone_ids")
    zone_kind = zone.get("zone_kind") or infer_zone_kind(source_types, merged_from_zone_ids)
    zone_id = zone.get("zone_id")
    if not zone_id and zone_kind == ZoneKind.COMPOSITE and not merged_from_zone_ids:
        zone_id = _fallback_dashboard_composite_zone_id(
            symbol=symbol,
            timeframe=str(timeframe),
            source_types=source_types,
            zone=zone,
        )

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
        merged_from_zone_ids=merged_from_zone_ids,
        metadata=metadata,
        observed_ts=observed_ts,
    )


def _coerce_string_set(value) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {item.strip() for item in value.split(",") if item.strip()}
    return {str(item).strip() for item in value if str(item).strip()}


def _fallback_dashboard_composite_zone_id(
    *,
    symbol: str,
    timeframe: str,
    source_types: set[str],
    zone: dict[str, Any],
) -> str:
    payload = {
        "symbol": str(symbol).strip().upper(),
        "timeframe": str(timeframe).strip().lower(),
        "zone_kind": ZoneKind.COMPOSITE,
        "source": sorted(str(source).strip().lower() for source in source_types),
        "price_low": round(float(zone["lower"]), 4),
        "price_high": round(float(zone["upper"]), 4),
        "current_role": str(zone.get("side", "neutral")).strip().lower(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"zone_dashboard_composite_{digest}"
