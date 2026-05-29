from __future__ import annotations

import pandas as pd

from features.boundaries import format_zone_source_types
from features.zone_strength import zone_strength_from_center


def rank_zones_for_side(
    zones: list[dict],
    current_price: float,
    side: str,
    max_zones: int,
    price_history: pd.DataFrame,
    center_volume_pct: float,
    strength_lookback_weeks: int = 52,
) -> list[dict]:
    ranked: list[dict] = []

    for zone in zones:
        if zone.get("side") != side:
            continue
        if side == "resistance" and zone["upper"] < current_price:
            continue
        if side == "support" and zone["lower"] > current_price:
            continue

        timeframes = set(zone.get("timeframes", set()))
        enriched_zone = zone.copy()
        enriched_zone["timeframe_sources"] = ",".join(sorted(timeframes))
        enriched_zone["source_types_label"] = format_zone_source_types(zone.get("source_types", set()))
        if "center_volume" not in enriched_zone or "zone_strength_pct" not in enriched_zone:
            strength = zone_strength_from_center(
                price_history=price_history,
                center=float(zone["center"]),
                band_pct=center_volume_pct,
                lookback_weeks=strength_lookback_weeks,
            )
            enriched_zone["center_volume"] = strength["zone_volume"]
            enriched_zone["zone_strength_pct"] = strength["zone_strength_pct"]
        ranked.append(enriched_zone)

    ranked = sorted(
        ranked,
        key=lambda item: (
            item["center_volume"],
            str(item.get("zone_id", "")),
        ),
        reverse=True,
    )
    return ranked[:max_zones]


def enrich_zones_with_strength(
    zones: list[dict],
    *,
    price_history: pd.DataFrame,
    center_volume_pct: float,
    strength_lookback_weeks: int = 52,
) -> list[dict]:
    output: list[dict] = []
    for zone in zones:
        zone_copy = zone.copy()
        strength = zone_strength_from_center(
            price_history=price_history,
            center=float(zone["center"]),
            band_pct=center_volume_pct,
            lookback_weeks=strength_lookback_weeks,
        )
        zone_copy["center_volume"] = strength["zone_volume"]
        zone_copy["zone_strength_pct"] = strength["zone_strength_pct"]
        output.append(zone_copy)
    return output
