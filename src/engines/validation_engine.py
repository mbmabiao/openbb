from __future__ import annotations

import pandas as pd

from features.boundaries import format_zone_source_types


def rank_zones_for_side(
    zones: list[dict],
    current_price: float,
    side: str,
    max_zones: int,
    price_history: pd.DataFrame,
    center_volume_pct: float,
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
        enriched_zone["center_volume"] = _center_band_volume(
            df=price_history,
            center=float(zone["center"]),
            band_pct=center_volume_pct,
        )
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


def _center_band_volume(df: pd.DataFrame, center: float, band_pct: float) -> float:
    if df.empty or "volume" not in df.columns or "high" not in df.columns or "low" not in df.columns or center <= 0:
        return 0.0

    pct = max(float(band_pct), 0.0)
    lower = center * (1.0 - pct)
    upper = center * (1.0 + pct)
    high = pd.to_numeric(df.get("high"), errors="coerce")
    low = pd.to_numeric(df.get("low"), errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    touched = (low <= upper) & (high >= lower)
    return float(volume.where(touched, 0.0).sum())
