from __future__ import annotations

import pandas as pd


def create_candidate_zones_from_avwap(
    df: pd.DataFrame,
    anchor_meta: dict,
    zone_expand_pct: float,
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

        zones.append(
            {
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
                "timeframes": {timeframe},
                "source_types": {source_type},
                "primary_timeframe": timeframe,
                "source_label": f"AVWAP ({timeframe}, {anchor_family})",
            }
        )

    return zones


def create_candidate_zones_from_vp(df: pd.DataFrame, vp_zones: list[dict]) -> list[dict]:
    if df.empty:
        return []

    current_price = float(df["close"].iloc[-1])
    output: list[dict] = []
    for zone in vp_zones:
        enriched = zone.copy()
        enriched["anchor_count"] = 0
        enriched["avwap_strength"] = 0.0
        enriched["side"] = "resistance" if zone["center"] >= current_price else "support"
        output.append(enriched)
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


def merge_close_zones(zones: list[dict], merge_pct: float = 0.006) -> list[dict]:
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
            merged[-1] = {
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
            }
        else:
            merged.append(zone.copy())

    return merged


def compute_inventory_zone_score(
    zone: dict,
    current_price: float,
    vp_df_daily: pd.DataFrame,
    vp_df_weekly: pd.DataFrame,
) -> float:
    zone_volume = float(zone.get("vp_volume", 0.0))
    daily_max = _max_profile_volume(vp_df_daily)
    weekly_max = _max_profile_volume(vp_df_weekly)
    max_volume = max(daily_max, weekly_max, 1.0)

    volume_score = zone_volume / max_volume
    distance_pct = abs(zone["center"] - current_price) / max(current_price, 1e-9)
    proximity_score = 1.0 / max(distance_pct, 0.01)
    weekly_bonus = 0.25 if "W" in zone.get("timeframes", set()) else 0.0
    multi_tf_bonus = 0.35 if len(zone.get("timeframes", set())) >= 2 else 0.0
    return 0.5 * volume_score + 0.3 * proximity_score + weekly_bonus + multi_tf_bonus


def assign_zone_display_labels(zones: list[dict], prefix: str) -> list[dict]:
    if not zones:
        return []

    ranked_by_distance = sorted(
        zones,
        key=lambda zone: (
            zone.get("distance_pct", float("inf")),
            abs(float(zone.get("center", 0.0))),
        ),
    )
    label_map = {id(zone): f"{prefix}{index}" for index, zone in enumerate(ranked_by_distance, start=1)}

    labeled: list[dict] = []
    for zone in zones:
        zone_copy = zone.copy()
        zone_copy["display_label"] = label_map[id(zone)]
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
                "confluence_count",
                "vp_volume",
                "anchor_count",
                "avwap_strength",
                "touch_count",
                "first_touch_score",
                "strong_reaction_rate",
                "reclaim_rate",
                "reaction_score",
                "distance_pct",
                "width_pct",
                "vp_strength",
                "inventory_score",
                "weekly_bonus",
                "multi_tf_bonus",
                "structural_score",
                "institutional_score",
            ]
        )

    frame = pd.DataFrame(zones).copy()
    for column in ["timeframes", "source_types"]:
        if column in frame.columns:
            frame[column] = frame[column].apply(
                lambda value: ",".join(sorted(value)) if isinstance(value, set) else value
            )
    return frame


def _max_profile_volume(vp_df: pd.DataFrame) -> float:
    if vp_df.empty or "volume" not in vp_df.columns:
        return 1.0
    values = pd.to_numeric(vp_df["volume"], errors="coerce")
    max_value = values.max()
    if pd.isna(max_value):
        return 1.0
    return max(float(max_value), 1e-9)
