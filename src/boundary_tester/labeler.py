from __future__ import annotations

import numpy as np
import pandas as pd

from .config import BoundaryTesterConfig
from .schema import BREAKOUT_EVENT_TYPES


def label_breakout_events(
    events_df: pd.DataFrame,
    price_df: pd.DataFrame,
    zone_df: pd.DataFrame,
    config: BoundaryTesterConfig,
) -> pd.DataFrame:
    if events_df.empty:
        out = events_df.copy()
        for col in [
            "label",
            "success_flag",
            "failure_flag",
            "unresolved_flag",
            "max_favorable_excursion",
            "max_adverse_excursion",
            "follow_through_pct",
            "reentry_flag",
            "bars_to_reentry",
            "bars_to_target",
            "outcome_window_end",
        ]:
            out[col] = pd.Series(dtype="object")
        return out

    zone_lookup = zone_df.set_index("zone_id").to_dict(orient="index")
    labeled_rows: list[dict] = []

    for _, event in events_df.iterrows():
        row = event.to_dict()
        if row["event_type"] not in BREAKOUT_EVENT_TYPES:
            row.update(_non_breakout_label_payload())
            labeled_rows.append(row)
            continue

        zone = zone_lookup.get(row["zone_id"])
        if zone is None:
            row.update(_unresolved_payload())
            labeled_rows.append(row)
            continue

        ticker_prices = price_df[price_df["ticker"] == row["ticker"]].copy() if "ticker" in price_df.columns else price_df.copy()
        ticker_prices = ticker_prices.reset_index(drop=True)
        breakout_idx = int(row["bar_index"])
        future = ticker_prices.iloc[breakout_idx + 1: breakout_idx + 1 + config.lookahead_bars].copy()

        if future.empty or len(future) < config.min_close_outside_zone:
            row.update(_unresolved_payload())
            labeled_rows.append(row)
            continue

        row.update(_label_single_breakout(event=row, future_df=future, zone=zone, config=config))
        labeled_rows.append(row)

    return pd.DataFrame(labeled_rows)


def _label_single_breakout(event: dict, future_df: pd.DataFrame, zone: dict, config: BoundaryTesterConfig) -> dict:
    side = str(zone["side"]).lower()
    breakout_close = float(event["price_at_event"])
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])

    success_threshold = config.success_move_pct
    if config.use_atr_filter and "atr" in future_df.columns and pd.notna(future_df["atr"].iloc[0]):
        atr_based = config.atr_multiple_success * float(future_df["atr"].iloc[0]) / max(abs(breakout_close), 1e-9)
        success_threshold = max(success_threshold, atr_based)

    if side == "resistance":
        favorable_path = (future_df["high"] - breakout_close) / max(abs(breakout_close), 1e-9)
        adverse_path = (breakout_close - future_df["low"]) / max(abs(breakout_close), 1e-9)
        final_follow_through = (float(future_df["close"].iloc[-1]) - breakout_close) / max(abs(breakout_close), 1e-9)
        reentry_mask = future_df["close"] <= boundary
        outside_mask = future_df["close"] > boundary
    else:
        favorable_path = (breakout_close - future_df["low"]) / max(abs(breakout_close), 1e-9)
        adverse_path = (future_df["high"] - breakout_close) / max(abs(breakout_close), 1e-9)
        final_follow_through = (breakout_close - float(future_df["close"].iloc[-1])) / max(abs(breakout_close), 1e-9)
        reentry_mask = future_df["close"] >= boundary
        outside_mask = future_df["close"] < boundary

    max_favorable_excursion = float(favorable_path.max()) if not favorable_path.empty else np.nan
    max_adverse_excursion = float(adverse_path.max()) if not adverse_path.empty else np.nan
    target_hits = favorable_path >= success_threshold
    bars_to_target = int(target_hits.idxmax() - future_df.index[0] + 1) if bool(target_hits.any()) else np.nan

    early_window = future_df.iloc[: config.failure_reentry_bars]
    early_reentry_mask = reentry_mask.loc[early_window.index]
    reentry_flag = bool(early_reentry_mask.any())
    bars_to_reentry = int(early_reentry_mask.idxmax() - future_df.index[0] + 1) if reentry_flag else np.nan

    hold_outside = bool(outside_mask.iloc[: config.min_close_outside_zone].all())
    success = bool(target_hits.any()) and (not reentry_flag) and hold_outside

    if success:
        label = "success"
    elif reentry_flag:
        label = "failure"
    elif len(future_df) < config.lookahead_bars:
        label = "unresolved"
    elif final_follow_through <= 0 and max_favorable_excursion < success_threshold:
        label = "failure"
    else:
        label = "unresolved"

    return {
        "label": label,
        "success_flag": int(label == "success"),
        "failure_flag": int(label == "failure"),
        "unresolved_flag": int(label == "unresolved"),
        "max_favorable_excursion": max_favorable_excursion,
        "max_adverse_excursion": max_adverse_excursion,
        "follow_through_pct": float(final_follow_through),
        "reentry_flag": int(reentry_flag),
        "bars_to_reentry": bars_to_reentry,
        "bars_to_target": bars_to_target,
        "outcome_window_end": pd.Timestamp(future_df["timestamp"].iloc[-1]),
    }


def _non_breakout_label_payload() -> dict:
    return {
        "label": "not_applicable",
        "success_flag": 0,
        "failure_flag": 0,
        "unresolved_flag": 0,
        "max_favorable_excursion": np.nan,
        "max_adverse_excursion": np.nan,
        "follow_through_pct": np.nan,
        "reentry_flag": 0,
        "bars_to_reentry": np.nan,
        "bars_to_target": np.nan,
        "outcome_window_end": pd.NaT,
    }


def _unresolved_payload() -> dict:
    payload = _non_breakout_label_payload()
    payload.update({"label": "unresolved", "unresolved_flag": 1})
    return payload
