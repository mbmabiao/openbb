from __future__ import annotations

import numpy as np
import pandas as pd

from .config import BoundaryTesterConfig
from .schema import DEFENSE_EVENT_TYPES


def label_zone_defense_events(
    events_df: pd.DataFrame,
    price_df: pd.DataFrame,
    zone_df: pd.DataFrame,
    config: BoundaryTesterConfig,
) -> pd.DataFrame:
    if events_df.empty:
        out = events_df.copy()
        for col in [
            "defense_label",
            "hold_flag",
            "failed_hold_flag",
            "defense_unresolved_flag",
            "reversal_strength_pct",
            "max_rejection_excursion",
            "max_penetration_excursion",
            "bars_to_reversal",
            "close_back_from_edge_flag",
            "zone_defense_score",
        ]:
            out[col] = pd.Series(dtype="object")
        return out

    zone_lookup = zone_df.set_index("zone_id").to_dict(orient="index")
    labeled_rows: list[dict] = []

    for _, event in events_df.iterrows():
        row = event.to_dict()
        if row["event_type"] not in DEFENSE_EVENT_TYPES:
            row.update(_non_defense_payload())
            labeled_rows.append(row)
            continue

        zone = zone_lookup.get(row["zone_id"])
        if zone is None:
            row.update(_unresolved_payload())
            labeled_rows.append(row)
            continue

        ticker_prices = price_df[price_df["ticker"] == row["ticker"]].copy() if "ticker" in price_df.columns else price_df.copy()
        ticker_prices = ticker_prices.sort_values("timestamp", kind="stable").reset_index(drop=True)
        event_idx = _locate_event_bar_index(ticker_prices=ticker_prices, event=row)
        if event_idx is None:
            row.update(_unresolved_payload())
            labeled_rows.append(row)
            continue

        future = ticker_prices.iloc[event_idx + 1: event_idx + 1 + config.lookahead_bars].copy()
        if future.empty:
            row.update(_unresolved_payload())
            labeled_rows.append(row)
            continue

        row.update(_label_single_defense(event=row, future_df=future, zone=zone, config=config))
        labeled_rows.append(row)

    return pd.DataFrame(labeled_rows)


def _label_single_defense(event: dict, future_df: pd.DataFrame, zone: dict, config: BoundaryTesterConfig) -> dict:
    side = str(zone["side"]).lower()
    event_price = float(event["price_at_event"])
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])

    if side == "resistance":
        rejection_path = (event_price - future_df["low"]) / max(abs(event_price), 1e-9)
        penetration_path = (future_df["high"] - boundary) / max(abs(boundary), 1e-9)
        close_back_from_edge_flag = bool((future_df["close"] < boundary).any())
    else:
        rejection_path = (future_df["high"] - event_price) / max(abs(event_price), 1e-9)
        penetration_path = (boundary - future_df["low"]) / max(abs(boundary), 1e-9)
        close_back_from_edge_flag = bool((future_df["close"] > boundary).any())

    max_rejection_excursion = float(rejection_path.max()) if not rejection_path.empty else np.nan
    max_penetration_excursion = float(max(penetration_path.max(), 0.0)) if not penetration_path.empty else np.nan
    reversal_hits = rejection_path >= config.defense_reversal_pct
    bars_to_reversal = int(reversal_hits.idxmax() - future_df.index[0] + 1) if bool(reversal_hits.any()) else np.nan

    if bool(reversal_hits.any()) and max_penetration_excursion <= config.breakout_buffer_pct:
        defense_label = "hold"
    elif max_penetration_excursion > config.breakout_buffer_pct:
        defense_label = "failed_hold"
    else:
        defense_label = "unresolved"

    defense_score = (
        max(0.0, max_rejection_excursion) * 100.0
        - max(0.0, max_penetration_excursion) * 80.0
        + (0.5 if close_back_from_edge_flag else 0.0)
    )

    return {
        "defense_label": defense_label,
        "hold_flag": int(defense_label == "hold"),
        "failed_hold_flag": int(defense_label == "failed_hold"),
        "defense_unresolved_flag": int(defense_label == "unresolved"),
        "reversal_strength_pct": max_rejection_excursion,
        "max_rejection_excursion": max_rejection_excursion,
        "max_penetration_excursion": max_penetration_excursion,
        "bars_to_reversal": bars_to_reversal,
        "close_back_from_edge_flag": int(close_back_from_edge_flag),
        "zone_defense_score": float(defense_score),
    }


def _locate_event_bar_index(ticker_prices: pd.DataFrame, event: dict) -> int | None:
    event_ts = pd.Timestamp(event["event_timestamp"])
    timestamps = pd.to_datetime(ticker_prices["timestamp"])
    matches = ticker_prices.index[timestamps == event_ts]
    if len(matches) > 0:
        return int(matches[0])

    global_bar_index = event.get("global_bar_index")
    if pd.notna(global_bar_index):
        idx = int(global_bar_index)
        if 0 <= idx < len(ticker_prices):
            candidate_ts = pd.Timestamp(ticker_prices.iloc[idx]["timestamp"])
            if candidate_ts == event_ts:
                return idx
    return None


def _non_defense_payload() -> dict:
    return {
        "defense_label": "not_applicable",
        "hold_flag": 0,
        "failed_hold_flag": 0,
        "defense_unresolved_flag": 0,
        "reversal_strength_pct": np.nan,
        "max_rejection_excursion": np.nan,
        "max_penetration_excursion": np.nan,
        "bars_to_reversal": np.nan,
        "close_back_from_edge_flag": 0,
        "zone_defense_score": np.nan,
    }


def _unresolved_payload() -> dict:
    payload = _non_defense_payload()
    payload.update({"defense_label": "unresolved", "defense_unresolved_flag": 1})
    return payload
