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
            "breakout_confirmed_flag",
            "hold_outside_flag",
            "target_hit_flag",
            "retest_success_flag",
            "max_favorable_excursion",
            "max_adverse_excursion",
            "follow_through_pct",
            "reentry_flag",
            "bars_to_reentry",
            "bars_to_target",
            "failure_subtype",
            "bars_to_failure",
            "reentry_depth_pct",
            "post_breakout_best_close_pct",
            "move_over_atr",
            "move_over_zone_width",
            "hold_outside_bars",
            "breakout_quality_score",
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
    zone_width_pct = max(float(event.get("zone_width_pct", 0.0)), 0.0)
    atr_value = float(future_df["atr"].iloc[0]) if "atr" in future_df.columns and pd.notna(future_df["atr"].iloc[0]) else np.nan
    success_threshold = _compute_success_threshold(
        breakout_close=breakout_close,
        zone_width_pct=zone_width_pct,
        atr_value=atr_value,
        config=config,
    )

    if side == "resistance":
        favorable_path = (future_df["high"] - breakout_close) / max(abs(breakout_close), 1e-9)
        adverse_path = (breakout_close - future_df["low"]) / max(abs(breakout_close), 1e-9)
        final_follow_through = (float(future_df["close"].iloc[-1]) - breakout_close) / max(abs(breakout_close), 1e-9)
        reentry_mask = future_df["close"] <= boundary
        outside_mask = future_df["close"] > boundary
        close_path = (future_df["close"] - breakout_close) / max(abs(breakout_close), 1e-9)
    else:
        favorable_path = (breakout_close - future_df["low"]) / max(abs(breakout_close), 1e-9)
        adverse_path = (future_df["high"] - breakout_close) / max(abs(breakout_close), 1e-9)
        final_follow_through = (breakout_close - float(future_df["close"].iloc[-1])) / max(abs(breakout_close), 1e-9)
        reentry_mask = future_df["close"] >= boundary
        outside_mask = future_df["close"] < boundary
        close_path = (breakout_close - future_df["close"]) / max(abs(breakout_close), 1e-9)

    max_favorable_excursion = float(favorable_path.max()) if not favorable_path.empty else np.nan
    max_adverse_excursion = float(adverse_path.max()) if not adverse_path.empty else np.nan
    target_hits = favorable_path >= success_threshold
    bars_to_target = int(target_hits.idxmax() - future_df.index[0] + 1) if bool(target_hits.any()) else np.nan

    early_window = future_df.iloc[: config.failure_reentry_bars]
    bars_to_reentry = _detect_failed_breakout_offset(early_window=early_window, zone=zone, config=config)
    late_reentry_offset = _detect_failed_breakout_offset(early_window=future_df, zone=zone, config=config)
    reentry_flag = not np.isnan(late_reentry_offset)
    fast_fail_flag = not np.isnan(bars_to_reentry)

    breakout_distance_pct = abs(float(event.get("close_distance_pct", 0.0)))
    breakout_confirmed_flag = breakout_distance_pct >= config.min_breakout_distance_pct
    if not np.isnan(atr_value) and config.min_breakout_distance_atr > 0:
        breakout_confirmed_flag = breakout_confirmed_flag and (
            breakout_distance_pct >= (config.min_breakout_distance_atr * atr_value / max(abs(boundary), 1e-9))
        )

    hold_outside_bars = 1 + _count_initial_true(outside_mask)
    hold_outside_flag = hold_outside_bars >= max(config.hold_outside_bars_required, 1)
    target_hit_flag = bool(target_hits.any())
    retest_success_flag = _detect_retest_success(future_df=future_df, zone=zone, config=config, side=side)
    success = (
        breakout_confirmed_flag
        and hold_outside_flag
        and (target_hit_flag or retest_success_flag)
        and ((not config.require_retest_success) or retest_success_flag)
        and (not fast_fail_flag)
    )

    if success and (target_hit_flag or retest_success_flag):
        label = "success"
        false_breakout_flag = 0
        failed_follow_through_flag = 0
        failure_subtype = "none"
        bars_to_failure = np.nan
    elif fast_fail_flag:
        label = "failure"
        false_breakout_flag = 1
        failed_follow_through_flag = 0
        failure_subtype = "fast_false_breakout"
        bars_to_failure = bars_to_reentry
    elif reentry_flag:
        label = "failure"
        false_breakout_flag = 0
        failed_follow_through_flag = 0
        failure_subtype = "late_failure"
        bars_to_failure = late_reentry_offset
    elif len(future_df) < config.lookahead_bars:
        label = "unresolved"
        false_breakout_flag = 0
        failed_follow_through_flag = 0
        failure_subtype = "unresolved"
        bars_to_failure = np.nan
    elif final_follow_through <= 0 and max_favorable_excursion < success_threshold:
        label = "failure"
        false_breakout_flag = 0
        failed_follow_through_flag = 1
        failure_subtype = "failed_follow_through"
        bars_to_failure = np.nan
    else:
        label = "unresolved"
        false_breakout_flag = 0
        failed_follow_through_flag = 0
        failure_subtype = "unresolved"
        bars_to_failure = np.nan

    post_breakout_best_close_pct = float(close_path.max()) if not close_path.empty else np.nan
    reentry_depth_pct = _compute_reentry_depth_pct(future_df=future_df, zone=zone, side=side)
    move_over_atr = np.nan if np.isnan(atr_value) else float(max_favorable_excursion / max(atr_value / max(abs(breakout_close), 1e-9), 1e-9))
    move_over_zone_width = np.nan if zone_width_pct <= 0 else float(max_favorable_excursion / max(zone_width_pct, 1e-9))
    breakout_quality_score = (
        1.2 * float(breakout_confirmed_flag)
        + 1.0 * float(hold_outside_flag)
        + 1.0 * float(target_hit_flag)
        + 0.8 * float(retest_success_flag)
        + 10.0 * max(0.0, max_favorable_excursion)
        - 8.0 * max(0.0, reentry_depth_pct)
    )

    return {
        "label": label,
        "success_flag": int(label == "success"),
        "failure_flag": int(label == "failure"),
        "unresolved_flag": int(label == "unresolved"),
        "false_breakout_flag": int(false_breakout_flag),
        "failed_follow_through_flag": int(failed_follow_through_flag),
        "breakout_confirmed_flag": int(breakout_confirmed_flag),
        "hold_outside_flag": int(hold_outside_flag),
        "target_hit_flag": int(target_hit_flag),
        "retest_success_flag": int(retest_success_flag),
        "max_favorable_excursion": max_favorable_excursion,
        "max_adverse_excursion": max_adverse_excursion,
        "follow_through_pct": float(final_follow_through),
        "reentry_flag": int(reentry_flag),
        "bars_to_reentry": late_reentry_offset,
        "bars_to_target": bars_to_target,
        "failure_subtype": failure_subtype,
        "bars_to_failure": bars_to_failure,
        "reentry_depth_pct": reentry_depth_pct,
        "post_breakout_best_close_pct": post_breakout_best_close_pct,
        "move_over_atr": move_over_atr,
        "move_over_zone_width": move_over_zone_width,
        "hold_outside_bars": int(hold_outside_bars),
        "breakout_quality_score": float(breakout_quality_score),
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
        "breakout_confirmed_flag": 0,
        "hold_outside_flag": 0,
        "target_hit_flag": 0,
        "retest_success_flag": 0,
        "max_favorable_excursion": np.nan,
        "max_adverse_excursion": np.nan,
        "follow_through_pct": np.nan,
        "reentry_flag": 0,
        "bars_to_reentry": np.nan,
        "bars_to_target": np.nan,
        "failure_subtype": "not_applicable",
        "bars_to_failure": np.nan,
        "reentry_depth_pct": np.nan,
        "post_breakout_best_close_pct": np.nan,
        "move_over_atr": np.nan,
        "move_over_zone_width": np.nan,
        "hold_outside_bars": np.nan,
        "breakout_quality_score": np.nan,
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


def _compute_success_threshold(
    breakout_close: float,
    zone_width_pct: float,
    atr_value: float,
    config: BoundaryTesterConfig,
) -> float:
    fixed_pct_threshold = config.success_move_pct
    atr_threshold = (
        config.atr_multiple_success * atr_value / max(abs(breakout_close), 1e-9)
        if not np.isnan(atr_value) else np.nan
    )
    zone_width_threshold = zone_width_pct * max(config.success_move_zone_width_multiple, 0.0)
    mode = str(config.success_move_mode).strip().lower()

    if mode == "atr":
        return atr_threshold if not np.isnan(atr_threshold) else fixed_pct_threshold
    if mode == "zone_width":
        return zone_width_threshold if zone_width_threshold > 0 else fixed_pct_threshold
    if mode == "hybrid":
        candidates = [fixed_pct_threshold]
        if not np.isnan(atr_threshold):
            candidates.append(atr_threshold)
        if zone_width_threshold > 0:
            candidates.append(zone_width_threshold)
        return max(candidates)
    return fixed_pct_threshold


def _count_initial_true(mask: pd.Series) -> int:
    count = 0
    for value in mask.tolist():
        if bool(value):
            count += 1
        else:
            break
    return count


def _detect_retest_success(
    future_df: pd.DataFrame,
    zone: dict,
    config: BoundaryTesterConfig,
    side: str,
) -> bool:
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])
    retest_buffer = config.retest_buffer_pct

    for pos, (_, row) in enumerate(future_df.iterrows()):
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        if side == "resistance":
            is_retest = low <= boundary * (1.0 + retest_buffer) and close > boundary
            if is_retest:
                later = future_df.iloc[pos + 1:]
                if later.empty:
                    return False
                return bool((later["high"] > high).any())
        else:
            is_retest = high >= boundary * (1.0 - retest_buffer) and close < boundary
            if is_retest:
                later = future_df.iloc[pos + 1:]
                if later.empty:
                    return False
                return bool((later["low"] < low).any())
    return False


def _compute_reentry_depth_pct(future_df: pd.DataFrame, zone: dict, side: str) -> float:
    zone_width = max(float(zone["upper"]) - float(zone["lower"]), 0.0)
    if zone_width <= 0 or future_df.empty:
        return np.nan

    if side == "resistance":
        reentry_depth = max(float(zone["upper"]) - float(future_df["close"].min()), 0.0)
    else:
        reentry_depth = max(float(future_df["close"].max()) - float(zone["lower"]), 0.0)

    return float(reentry_depth / zone_width)
