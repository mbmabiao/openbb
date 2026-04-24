from __future__ import annotations

import pandas as pd

from features.boundaries import compute_inventory_zone_score, format_zone_source_types


def validate_zone_reaction(
    df: pd.DataFrame,
    zone: dict,
    lookahead: int,
    return_threshold: float,
    min_gap: int,
) -> dict:
    empty_result = {
        "touch_count": 0,
        "first_touch_score": 0.0,
        "strong_reaction_rate": 0.0,
        "reclaim_rate": 0.0,
        "reaction_score": 0.0,
        "last_reaction_date": pd.NaT,
    }
    if df.empty or lookahead < 1:
        return empty_result

    lower = float(zone["lower"])
    upper = float(zone["upper"])
    center = float(zone["center"])
    side = zone["side"]

    touched_indexes: list[int] = []
    last_touch = -10_000

    for row_index in range(len(df) - lookahead):
        row = df.iloc[row_index]
        touched = float(row["low"]) <= upper and float(row["high"]) >= lower
        if touched and (row_index - last_touch) >= min_gap:
            touched_indexes.append(row_index)
            last_touch = row_index

    if not touched_indexes:
        return empty_result

    strong_reactions = 0
    reclaims = 0
    reactions: list[float] = []

    for row_index in touched_indexes:
        row = df.iloc[row_index]
        base_close = float(row["close"])
        future = df.iloc[row_index + 1 : row_index + 1 + lookahead].copy()
        if future.empty:
            continue

        if side == "support":
            best_forward = float(future["high"].max())
            forward_return = (best_forward - base_close) / max(base_close, 1e-9)
            pierced = float(row["low"]) < lower
            reclaimed_same_bar = pierced and float(row["close"]) >= lower
            reclaimed_future = bool((future["close"] >= lower).any())
            reclaimed = reclaimed_same_bar or reclaimed_future
            strong = (
                forward_return >= return_threshold
                or (float(row["close"]) > center and (float(row["close"]) - float(row["open"])) > 0)
                or reclaimed
            )
        else:
            best_forward = float(future["low"].min())
            forward_return = (base_close - best_forward) / max(base_close, 1e-9)
            pierced = float(row["high"]) > upper
            reclaimed_same_bar = pierced and float(row["close"]) <= upper
            reclaimed_future = bool((future["close"] <= upper).any())
            reclaimed = reclaimed_same_bar or reclaimed_future
            strong = (
                forward_return >= return_threshold
                or (float(row["close"]) < center and (float(row["open"]) - float(row["close"])) > 0)
                or reclaimed
            )

        if strong:
            strong_reactions += 1
        if reclaimed:
            reclaims += 1

        touch_quality = 0.0
        if strong:
            touch_quality += 1.0
        if reclaimed:
            touch_quality += 0.75
        reactions.append(touch_quality)

    effective_touches = max(len(reactions), 1)
    strong_reaction_rate = strong_reactions / effective_touches
    reclaim_rate = reclaims / effective_touches
    first_touch_score = reactions[0] if reactions else 0.0
    touch_count = len(touched_indexes)
    repeated_test_decay = max(touch_count - 2, 0) * 0.12
    reaction_score = (
        1.2 * first_touch_score
        + 1.0 * strong_reaction_rate
        + 0.8 * reclaim_rate
        - repeated_test_decay
    )
    last_reaction_date = df.iloc[touched_indexes[-1]]["date"]

    return {
        "touch_count": touch_count,
        "first_touch_score": float(first_touch_score),
        "strong_reaction_rate": float(strong_reaction_rate),
        "reclaim_rate": float(reclaim_rate),
        "reaction_score": float(reaction_score),
        "last_reaction_date": last_reaction_date,
    }


def rank_zones_for_side(
    zones: list[dict],
    vp_df_daily: pd.DataFrame,
    vp_df_weekly: pd.DataFrame,
    current_price: float,
    side: str,
    max_zones: int,
    df_reaction: pd.DataFrame,
    lookahead: int,
    reaction_threshold: float,
    min_gap: int,
) -> list[dict]:
    ranked: list[dict] = []
    max_vp_daily = _max_profile_volume(vp_df_daily)
    max_vp_weekly = _max_profile_volume(vp_df_weekly)
    max_vp = max(max_vp_daily, max_vp_weekly, 1.0)

    for zone in zones:
        if zone.get("side") != side:
            continue
        if side == "resistance" and zone["upper"] < current_price:
            continue
        if side == "support" and zone["lower"] > current_price:
            continue

        reaction = validate_zone_reaction(
            df=df_reaction,
            zone=zone,
            lookahead=lookahead,
            return_threshold=reaction_threshold,
            min_gap=min_gap,
        )

        distance_pct = abs(zone["center"] - current_price) / max(current_price, 1e-9)
        width_pct = (zone["upper"] - zone["lower"]) / max(zone["center"], 1e-9)
        vp_strength = float(zone.get("vp_volume", 0.0)) / max_vp
        inventory_score = compute_inventory_zone_score(zone, current_price, vp_df_daily, vp_df_weekly)
        avwap_strength = float(zone.get("avwap_strength", 0.0))
        anchor_count = int(zone.get("anchor_count", 0))
        proximity_score = 1.0 / max(distance_pct, 0.01)
        width_penalty = width_pct * 20.0

        timeframes = set(zone.get("timeframes", set()))
        weekly_bonus = 1.0 if "W" in timeframes else 0.0
        multi_tf_bonus = 1.2 if len(timeframes) >= 2 else 0.0
        confluence_count = len(set(zone.get("source_types", set())))

        structural_score = (
            2.4 * vp_strength
            + 1.9 * inventory_score
            + 1.3 * avwap_strength
            + 0.7 * anchor_count
            + 0.9 * weekly_bonus
            + 1.1 * multi_tf_bonus
            + 0.3 * confluence_count
            + 1.0 * proximity_score
            - width_penalty
        )
        institutional_score = structural_score + 2.0 * reaction["reaction_score"]

        enriched_zone = zone.copy()
        enriched_zone["distance_pct"] = distance_pct
        enriched_zone["width_pct"] = width_pct
        enriched_zone["vp_strength"] = vp_strength
        enriched_zone["inventory_score"] = inventory_score
        enriched_zone["weekly_bonus"] = weekly_bonus
        enriched_zone["multi_tf_bonus"] = multi_tf_bonus
        enriched_zone["confluence_count"] = confluence_count
        enriched_zone["timeframe_sources"] = ",".join(sorted(timeframes))
        enriched_zone["source_types_label"] = format_zone_source_types(zone.get("source_types", set()))
        enriched_zone.update(reaction)
        enriched_zone["structural_score"] = structural_score
        enriched_zone["institutional_score"] = institutional_score
        ranked.append(enriched_zone)

    ranked = sorted(
        ranked,
        key=lambda item: (
            item["institutional_score"],
            item["reaction_score"],
            item["multi_tf_bonus"],
            item["vp_strength"],
            -item["distance_pct"],
        ),
        reverse=True,
    )
    ranked = ranked[:max_zones]
    return sorted(ranked, key=lambda item: item["center"])


def _max_profile_volume(vp_df: pd.DataFrame) -> float:
    if vp_df.empty or "volume" not in vp_df.columns:
        return 1.0
    values = pd.to_numeric(vp_df["volume"], errors="coerce")
    max_value = values.max()
    if pd.isna(max_value):
        return 1.0
    return max(float(max_value), 1e-9)
