from __future__ import annotations

import pandas as pd

from zone_lifecycle.constants import ZoneKind
from zone_lifecycle.identity import ZoneIdentityInput, generate_zone_id


def create_candidate_zones_from_swing_points(
    swing_points: list[dict],
    zone_expand_pct: float,
    current_price: float,
    symbol: str | None = None,
) -> list[dict]:
    zones: list[dict] = []
    for point in swing_points:
        center = float(point["price"])
        expand = center * float(zone_expand_pct)
        side = "resistance" if center >= float(current_price) else "support"
        timeframe = str(point.get("timeframe") or "W")
        anchor_name = str(point.get("anchor_name") or f"{timeframe}_swing_{side}")
        origin_bar = point.get("origin_bar")
        zone = {
            "type": f"weekly_swing_{side}",
            "side": side,
            "lower": center - expand,
            "upper": center + expand,
            "center": center,
            "anchor_name": anchor_name,
            "anchor_start_date": origin_bar,
            "anchor_family": "swing",
            "timeframes": {timeframe},
            "source_types": {f"swing_{timeframe}"},
            "primary_timeframe": timeframe,
            "source_label": f"Swing ({timeframe})",
            "zone_kind": ZoneKind.EVENT,
            "origin_bar": origin_bar,
            "origin_event_id": anchor_name,
            "origin_event_type": "swing",
        }
        zones.append(_with_identity_metadata(zone, symbol=symbol))
    return zones


def format_zone_source_types(
    source_types: set[str] | list[str] | tuple[str, ...] | None,
) -> str:
    if not source_types:
        return ""

    formatted: list[str] = []
    for source_type in sorted(set(source_types)):
        parts = str(source_type).split("_", 1)
        if len(parts) == 2:
            family, timeframe = parts
            formatted.append(f"{family.upper()}_{timeframe.upper()}")
        else:
            formatted.append(str(source_type).upper())
    return ",".join(formatted)


def assign_zone_display_labels(zones: list[dict], prefix: str) -> list[dict]:
    if not zones:
        return []

    labeled: list[dict] = []
    for index, zone in enumerate(zones, start=1):
        zone_copy = zone.copy()
        zone_copy["display_label"] = f"{prefix}{index}"
        labeled.append(zone_copy)
    return labeled


def zones_to_dataframe(zones: list[dict]) -> pd.DataFrame:
    if not zones:
        return pd.DataFrame(
            columns=[
                "side",
                "type",
                "lower",
                "upper",
                "center",
                "timeframe_sources",
                "source_types_label",
                "center_volume",
                "zone_strength_pct",
                "touch_count",
                "width_pct",
            ]
        )

    frame = pd.DataFrame(zones).copy()
    for column in ["timeframes", "source_types"]:
        if column in frame.columns:
            frame[column] = frame[column].apply(
                lambda value: ",".join(sorted(value)) if isinstance(value, set) else value
            )
    return frame


def _with_identity_metadata(zone: dict, symbol: str | None) -> dict:
    zone_copy = zone.copy()
    if symbol and not zone_copy.get("zone_id"):
        zone_copy["zone_id"] = _generate_zone_id(symbol=symbol, zone=zone_copy)
    return zone_copy


def _generate_zone_id(symbol: str, zone: dict) -> str:
    zone_kind = zone.get("zone_kind") or ZoneKind.EVENT
    return generate_zone_id(
        ZoneIdentityInput(
            symbol=symbol,
            timeframe=str(zone.get("primary_timeframe") or _format_timeframe(zone.get("timeframes")) or "1d"),
            zone_kind=str(zone_kind),
            source=tuple(sorted(set(zone.get("source_types", set())))),
            price_low=float(zone["lower"]),
            price_high=float(zone["upper"]),
            origin_bar=zone.get("origin_bar") or zone.get("anchor_start_date"),
            origin_event_id=zone.get("origin_event_id") or zone.get("anchor_name"),
        )
    )


def _format_timeframe(value) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return ",".join(sorted(str(item) for item in value))
