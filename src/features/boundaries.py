from __future__ import annotations

import pandas as pd

from zone_lifecycle.constants import ZoneKind
from zone_lifecycle.identity import ZoneIdentityInput, generate_zone_id


def create_candidate_zones_from_avwap(
    df: pd.DataFrame,
    anchor_meta: dict,
    zone_expand_pct: float,
    symbol: str | None = None,
) -> list[dict]:
    zones: list[dict] = []
    if df.empty:
        return zones

    current_price = float(df["close"].iloc[-1])
    for column_name, meta in anchor_meta.items():
        latest_values = df[column_name].dropna()
        if latest_values.empty:
            continue

        avwap_now = float(latest_values.iloc[-1])
        center = avwap_now
        expand = center * zone_expand_pct
        timeframe = meta["timeframe"]
        anchor_family = str(meta.get("anchor_family", "rolling"))
        source_type = f"avwap_{timeframe}_{anchor_family}"

        if center >= current_price:
            zone_side = "resistance"
            avwap_strength = max((center - current_price) / max(current_price, 1e-9), 0.0) + 0.5
        else:
            zone_side = "support"
            avwap_strength = max((current_price - center) / max(current_price, 1e-9), 0.0) + 0.5

        zone = {
            "type": f"avwap_{zone_side}_{timeframe}",
            "side": zone_side,
            "lower": center - expand,
            "upper": center + expand,
            "center": center,
            "vp_volume": 0.0,
            "anchor_count": 1,
            "avwap_strength": avwap_strength,
            "anchor_name": meta["anchor_name"],
            "anchor_start_date": meta["start_date"],
            "anchor_family": anchor_family,
            "timeframes": {timeframe},
            "source_types": {source_type},
            "primary_timeframe": timeframe,
            "source_label": f"AVWAP ({timeframe}, {anchor_family})",
            "zone_kind": ZoneKind.AVWAP,
            "origin_bar": meta["start_date"],
            "origin_event_id": meta["anchor_name"],
            "origin_event_type": anchor_family,
        }
        zones.append(_with_identity_metadata(zone, symbol=symbol))

    return zones


def create_candidate_zones_from_vp(
    df: pd.DataFrame,
    vp_zones: list[dict],
    symbol: str | None = None,
) -> list[dict]:
    if df.empty:
        return []

    current_price = float(df["close"].iloc[-1])
    output: list[dict] = []
    for zone in vp_zones:
        enriched = zone.copy()
        enriched["anchor_count"] = 0
        enriched["avwap_strength"] = 0.0
        enriched["side"] = "resistance" if zone["center"] >= current_price else "support"
        enriched["zone_kind"] = ZoneKind.VP
        enriched["vp_window_type"] = str(
            zone.get("vp_window_type") or zone.get("source_label") or zone.get("type") or "vp"
        )
        output.append(_with_identity_metadata(enriched, symbol=symbol))
    return output


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


def merge_close_zones(
    zones: list[dict],
    merge_pct: float = 0.006,
    symbol: str | None = None,
) -> list[dict]:
    if not zones:
        return []

    zones_sorted = sorted(zones, key=lambda zone: (zone["side"], zone["center"]))
    merged = [zones_sorted[0].copy()]

    for zone in zones_sorted[1:]:
        previous = merged[-1]
        if zone["side"] != previous["side"]:
            merged.append(zone.copy())
            continue

        overlap = not (zone["lower"] > previous["upper"] or zone["upper"] < previous["lower"])
        close_center = abs(zone["center"] - previous["center"]) / max(previous["center"], 1e-9) <= merge_pct

        if overlap or close_center:
            new_lower = min(previous["lower"], zone["lower"])
            new_upper = max(previous["upper"], zone["upper"])
            timeframes = set(previous.get("timeframes", set())) | set(zone.get("timeframes", set()))
            source_types = set(previous.get("source_types", set())) | set(zone.get("source_types", set()))
            inherited_zone_id = _existing_composite_zone_id(previous, zone)
            merged_zone = {
                "type": f"merged_{previous['side']}",
                "side": previous["side"],
                "lower": float(new_lower),
                "upper": float(new_upper),
                "center": float((new_lower + new_upper) / 2.0),
                "vp_volume": float(previous.get("vp_volume", 0.0) + zone.get("vp_volume", 0.0)),
                "anchor_count": int(previous.get("anchor_count", 0) + zone.get("anchor_count", 0)),
                "avwap_strength": float(
                    previous.get("avwap_strength", 0.0) + zone.get("avwap_strength", 0.0)
                ),
                "timeframes": timeframes,
                "source_types": source_types,
                "primary_timeframe": "W" if "W" in timeframes else "D",
                "source_label": format_zone_source_types(source_types),
                "zone_kind": ZoneKind.COMPOSITE,
                "merged_from_zone_ids": _collect_source_zone_ids(previous, zone),
                "source_components": _collect_source_components(previous, zone),
            }
            if inherited_zone_id:
                merged_zone["zone_id"] = inherited_zone_id
            merged[-1] = _with_identity_metadata(merged_zone, symbol=symbol)
        else:
            merged.append(zone.copy())

    return merged


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
                "vp_volume",
                "anchor_count",
                "avwap_strength",
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
    if "zone_id" in zone_copy:
        zone_copy.setdefault("source_zone_ids", [zone_copy["zone_id"]])
    zone_copy.setdefault("source_components", [_component_payload(zone_copy)])
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
            vp_window_type=zone.get("vp_window_type") or zone.get("source_label"),
            vp_structure_key=zone.get("vp_structure_key") or zone.get("origin_event_id"),
            merged_from_zone_ids=tuple(zone.get("merged_from_zone_ids") or ()),
        )
    )


def _format_timeframe(value) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return ",".join(sorted(str(item) for item in value))


def _collect_source_zone_ids(*zones: dict) -> list[str]:
    ids: list[str] = []
    for zone in zones:
        if zone.get("zone_kind") == ZoneKind.COMPOSITE and zone.get("merged_from_zone_ids"):
            zone_ids = zone.get("merged_from_zone_ids") or []
        else:
            zone_ids = zone.get("source_zone_ids") or ([zone["zone_id"]] if zone.get("zone_id") else [])
        ids.extend(str(zone_id) for zone_id in zone_ids if str(zone_id).strip())
    return sorted(set(ids))


def _collect_source_components(*zones: dict) -> list[dict]:
    components_by_id: dict[str, dict] = {}
    anonymous_components: list[dict] = []
    for zone in zones:
        components = zone.get("source_components") or [_component_payload(zone)]
        for component in components:
            component_id = component.get("zone_id")
            if component_id:
                components_by_id[str(component_id)] = component
            else:
                anonymous_components.append(component)
    return [*anonymous_components, *[components_by_id[key] for key in sorted(components_by_id)]]


def _existing_composite_zone_id(*zones: dict) -> str | None:
    for zone in zones:
        if zone.get("zone_kind") == ZoneKind.COMPOSITE and zone.get("zone_id"):
            return str(zone["zone_id"])
    return None


def _component_payload(zone: dict) -> dict:
    keys = [
        "zone_id",
        "zone_kind",
        "type",
        "side",
        "lower",
        "upper",
        "center",
        "timeframes",
        "source_types",
        "primary_timeframe",
        "source_label",
        "anchor_name",
        "anchor_start_date",
        "anchor_family",
        "origin_bar",
        "origin_event_id",
        "origin_event_type",
        "vp_window_type",
        "vp_structure_key",
        "vp_source_interval",
    ]
    return {key: zone[key] for key in keys if key in zone}
