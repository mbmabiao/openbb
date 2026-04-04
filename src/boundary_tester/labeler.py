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
            "false_breakout_flag",
            "failed_follow_through_flag",
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
        ticker_prices = ticker_prices.sort_values("timestamp", kind="stable").reset_index(drop=True)
        breakout_idx = _locate_breakout_bar_index(ticker_prices=ticker_prices, event=row)
        if breakout_idx is None:
            row.update(_unresolved_payload())
            labeled_rows.append(row)
            continue

        future = ticker_prices.iloc[breakout_idx + 1: breakout_idx + 1 + config.lookahead_bars].copy()

        if future.empty:
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
    else:
        favorable_path = (breakout_close - future_df["low"]) / max(abs(breakout_close), 1e-9)
        adverse_path = (future_df["high"] - breakout_close) / max(abs(breakout_close), 1e-9)
        final_follow_through = (breakout_close - float(future_df["close"].iloc[-1])) / max(abs(breakout_close), 1e-9)

    max_favorable_excursion = float(favorable_path.max()) if not favorable_path.empty else np.nan
    max_adverse_excursion = float(adverse_path.max()) if not adverse_path.empty else np.nan
    target_hits = favorable_path >= success_threshold
    bars_to_target = int(target_hits.idxmax() - future_df.index[0] + 1) if bool(target_hits.any()) else np.nan

    early_window = future_df.iloc[: config.failure_reentry_bars]
    bars_to_reentry = _detect_failed_breakout_offset(early_window=early_window, zone=zone, config=config)
    reentry_flag = not np.isnan(bars_to_reentry)

    success = bool(target_hits.any()) and (not reentry_flag)

    if success:
        label = "success"
        false_breakout_flag = 0
        failed_follow_through_flag = 0
    elif reentry_flag:
        label = "failure"
        false_breakout_flag = 1
        failed_follow_through_flag = 0
    elif len(future_df) < config.lookahead_bars:
        label = "unresolved"
        false_breakout_flag = 0
        failed_follow_through_flag = 0
    elif final_follow_through <= 0 and max_favorable_excursion < success_threshold:
        label = "failure"
        false_breakout_flag = 0
        failed_follow_through_flag = 1
    else:
        label = "unresolved"
        false_breakout_flag = 0
        failed_follow_through_flag = 0

    return {
        "label": label,
        "success_flag": int(label == "success"),
        "failure_flag": int(label == "failure"),
        "unresolved_flag": int(label == "unresolved"),
        "false_breakout_flag": int(false_breakout_flag),
        "failed_follow_through_flag": int(failed_follow_through_flag),
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
        "false_breakout_flag": 0,
        "failed_follow_through_flag": 0,
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


def _locate_breakout_bar_index(ticker_prices: pd.DataFrame, event: dict) -> int | None:
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


def _detect_failed_breakout_offset(
    early_window: pd.DataFrame,
    zone: dict,
    config: BoundaryTesterConfig,
) -> float:
    if early_window.empty:
        return np.nan

    side = str(zone["side"]).lower()
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])
    zone_width = max(float(zone["upper"]) - float(zone["lower"]), 0.0)
    depth_frac = max(float(config.failed_breakout_reentry_depth_frac), 0.0)
    min_consecutive = max(int(config.failed_breakout_min_consecutive_inside_bars), 1)
    consecutive_inside = 0

    for offset, (_, row) in enumerate(early_window.iterrows(), start=1):
        close = float(row["close"])
        if side == "resistance":
            inside_boundary = close <= boundary
            deep_reentry = zone_width > 0 and close <= float(zone["upper"]) - zone_width * depth_frac
        else:
            inside_boundary = close >= boundary
            deep_reentry = zone_width > 0 and close >= float(zone["lower"]) + zone_width * depth_frac

        consecutive_inside = consecutive_inside + 1 if inside_boundary else 0
        if deep_reentry or consecutive_inside >= min_consecutive:
            return float(offset)

    return np.nan
