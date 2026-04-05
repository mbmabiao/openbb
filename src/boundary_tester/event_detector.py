from __future__ import annotations

import json
from collections import defaultdict

import numpy as np
import pandas as pd

from .config import BoundaryTesterConfig


COMMON_COLUMNS = [
    "ticker",
    "zone_id",
    "structural_zone_key",
    "event_timestamp",
    "event_type",
    "direction",
    "price_at_event",
    "boundary_price",
    "close_distance_pct",
    "bar_index",
    "local_bar_index",
    "global_bar_index",
    "touch_count_before_event",
    "meaningful_touch_count_before_event",
    "is_first_meaningful_test",
    "bars_since_last_touch",
    "days_since_last_touch",
    "zone_class",
    "zone_side",
    "zone_timeframe",
    "zone_width_pct",
    "confluence_count",
    "metadata_json",
]


def detect_boundary_interactions(
    price_df: pd.DataFrame,
    zone_df: pd.DataFrame,
    config: BoundaryTesterConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_interactions: list[dict] = []
    emitted_events: list[dict] = []

    for _, zone in zone_df.iterrows():
        zone_raw, zone_events = _detect_zone_interactions(price_df, zone, config)
        raw_interactions.extend(zone_raw)
        emitted_events.extend(zone_events)

    raw_df = _to_sorted_dataframe(raw_interactions, id_column="raw_interaction_id")
    events_df = _to_sorted_dataframe(emitted_events, id_column="event_id")
    return raw_df, events_df


def detect_boundary_events(price_df: pd.DataFrame, zone_df: pd.DataFrame, config: BoundaryTesterConfig) -> pd.DataFrame:
    _, events_df = detect_boundary_interactions(price_df, zone_df, config)
    return events_df


def _to_sorted_dataframe(records: list[dict], id_column: str) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=[id_column, *COMMON_COLUMNS])
    return (
        pd.DataFrame(records)
        .sort_values(["ticker", "event_timestamp", "zone_id", "event_type"], kind="stable")
        .reset_index(drop=True)
    )


def _detect_zone_interactions(
    price_df: pd.DataFrame,
    zone: pd.Series,
    config: BoundaryTesterConfig,
) -> tuple[list[dict], list[dict]]:
    ticker_prices = price_df[price_df["ticker"] == zone["ticker"]].copy() if "ticker" in price_df.columns else price_df.copy()
    ticker_prices = ticker_prices.sort_values("timestamp", kind="stable").reset_index(drop=True)
    ticker_prices["global_bar_index"] = ticker_prices.index.astype(int)
    ticker_prices = ticker_prices[ticker_prices["timestamp"] >= zone["valid_from"]].copy()
    if pd.notna(zone.get("valid_to")):
        ticker_prices = ticker_prices[ticker_prices["timestamp"] <= zone["valid_to"]].copy()
    ticker_prices = ticker_prices.reset_index(drop=True)
    ticker_prices["local_bar_index"] = ticker_prices.index.astype(int)

    if ticker_prices.empty:
        return [], []

    raw_interactions, emitted_pre = _detect_pre_breakout_interactions(ticker_prices, zone, config)
    breakout_events = [event for event in emitted_pre if event["event_type"] in {"breakout_up", "breakout_down"}]
    raw_post, emitted_post = _detect_post_breakout_events(ticker_prices, zone, breakout_events, config)
    return raw_interactions + raw_post, emitted_pre + emitted_post


def _detect_pre_breakout_interactions(
    price_df: pd.DataFrame,
    zone: pd.Series,
    config: BoundaryTesterConfig,
) -> tuple[list[dict], list[dict]]:
    side = str(zone["side"]).lower()
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])
    breakout_buffer = config.breakout_buffer_pct
    probe_buffer = config.probe_buffer_pct
    min_outside = max(config.min_close_outside_zone, 1)
    emission_gap = max(config.event_emission_gap_bars, 0)
    touch_gap = max(config.touch_merge_gap_bars, 0)

    raw_interactions: list[dict] = []
    emitted_events: list[dict] = []
    last_emitted_idx: dict[str, int] = defaultdict(lambda: -10_000)
    touch_count = 0
    meaningful_touch_count = 0
    last_touch_idx: int | None = None
    last_touch_ts: pd.Timestamp | None = None
    last_meaningful_touch_idx: int | None = None
    outside_count = 0

    for idx, row in price_df.iterrows():
        close = float(row["close"])
        high = float(row["high"])
        low = float(row["low"])

        if side == "resistance":
            close_outside = close > boundary * (1.0 + breakout_buffer)
            outside_count = outside_count + 1 if close_outside else 0
            breakout_ready = outside_count >= min_outside and _breakout_started_from_inside(
                price_df=price_df,
                idx=idx,
                min_outside=min_outside,
                boundary=boundary,
                side=side,
                breakout_buffer=breakout_buffer,
            )
            qualifies_test = high >= boundary and close <= boundary * (1.0 + probe_buffer)
            qualifies_probe = high > boundary and not qualifies_test
        else:
            close_outside = close < boundary * (1.0 - breakout_buffer)
            outside_count = outside_count + 1 if close_outside else 0
            breakout_ready = outside_count >= min_outside and _breakout_started_from_inside(
                price_df=price_df,
                idx=idx,
                min_outside=min_outside,
                boundary=boundary,
                side=side,
                breakout_buffer=breakout_buffer,
            )
            qualifies_test = low <= boundary and close >= boundary * (1.0 - probe_buffer)
            qualifies_probe = low < boundary and not qualifies_test

        event_type = None
        if breakout_ready:
            event_type = "breakout_up" if side == "resistance" else "breakout_down"
        elif qualifies_test:
            event_type = "test"
        elif qualifies_probe:
            event_type = "probe"

        if event_type is None:
            continue

        bars_since_last_touch = np.nan if last_touch_idx is None else float(idx - last_touch_idx)
        days_since_last_touch = np.nan
        if last_touch_ts is not None:
            days_since_last_touch = float((pd.Timestamp(row["timestamp"]) - last_touch_ts) / pd.Timedelta(days=1))

        stats_before = {
            "touch_count_before_event": int(touch_count),
            "meaningful_touch_count_before_event": int(meaningful_touch_count),
            "is_first_meaningful_test": bool(meaningful_touch_count == 0),
            "bars_since_last_touch": bars_since_last_touch,
            "days_since_last_touch": days_since_last_touch,
        }

        raw_interactions.append(
            _build_event_dict(
                zone=zone,
                row=row,
                idx=idx,
                event_type=event_type,
                stats_before=stats_before,
                id_column="raw_interaction_id",
            )
        )

        touch_count += 1
        if last_meaningful_touch_idx is None or (idx - last_meaningful_touch_idx) > touch_gap:
            meaningful_touch_count += 1
            last_meaningful_touch_idx = idx
        last_touch_idx = idx
        last_touch_ts = pd.Timestamp(row["timestamp"])

        if idx - last_emitted_idx[event_type] > emission_gap:
            emitted_events.append(
                _build_event_dict(
                    zone=zone,
                    row=row,
                    idx=idx,
                    event_type=event_type,
                    stats_before=stats_before,
                    id_column="event_id",
                )
            )
            last_emitted_idx[event_type] = idx

    return raw_interactions, emitted_events


def _breakout_started_from_inside(
    price_df: pd.DataFrame,
    idx: int,
    min_outside: int,
    boundary: float,
    side: str,
    breakout_buffer: float,
) -> bool:
    start_idx = idx - min_outside + 1
    if start_idx < 0:
        return False
    if start_idx == 0:
        return True

    previous_close = float(price_df.iloc[start_idx - 1]["close"])
    if side == "resistance":
        return previous_close <= boundary * (1.0 + breakout_buffer)
    return previous_close >= boundary * (1.0 - breakout_buffer)


def _detect_post_breakout_events(
    price_df: pd.DataFrame,
    zone: pd.Series,
    breakout_events: list[dict],
    config: BoundaryTesterConfig,
) -> tuple[list[dict], list[dict]]:
    side = str(zone["side"]).lower()
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])
    retest_buffer = config.retest_buffer_pct
    raw_interactions: list[dict] = []
    emitted_events: list[dict] = []

    for breakout_event in breakout_events:
        breakout_idx = int(breakout_event["local_bar_index"])
        future = price_df.iloc[
            breakout_idx + 1: breakout_idx + 1 + max(config.lookahead_bars, config.failure_reentry_bars)
        ].copy()
        if future.empty:
            continue

        failed_idx = None
        retest_idx = None
        consecutive_inside = 0

        for offset, (_, row) in enumerate(future.iterrows(), start=1):
            close = float(row["close"])
            high = float(row["high"])
            low = float(row["low"])
            inside_boundary = _is_inside_boundary(close=close, boundary=boundary, side=side)
            deep_reentry = _is_deep_reentry(close=close, zone=zone, side=side, config=config)

            if offset <= config.failure_reentry_bars:
                consecutive_inside = consecutive_inside + 1 if inside_boundary else 0
                if (
                    failed_idx is None
                    and (
                        deep_reentry
                        or consecutive_inside >= max(config.failed_breakout_min_consecutive_inside_bars, 1)
                    )
                ):
                    failed_idx = breakout_idx + offset

            if side == "resistance":
                if retest_idx is None and low <= boundary * (1.0 + retest_buffer) and close > boundary:
                    retest_idx = breakout_idx + offset
            else:
                if retest_idx is None and high >= boundary * (1.0 - retest_buffer) and close < boundary:
                    retest_idx = breakout_idx + offset

            if failed_idx is not None and retest_idx is not None:
                break

        stats_before = {
            "touch_count_before_event": int(breakout_event["touch_count_before_event"] + 1),
            "meaningful_touch_count_before_event": int(breakout_event["meaningful_touch_count_before_event"] + 1),
            "is_first_meaningful_test": bool(breakout_event["is_first_meaningful_test"]),
            "bars_since_last_touch": np.nan,
            "days_since_last_touch": np.nan,
        }

        if failed_idx is not None:
            failed_row = price_df.iloc[failed_idx]
            event_type = "failed_breakout_up" if side == "resistance" else "failed_breakout_down"
            record = _build_event_dict(zone=zone, row=failed_row, idx=failed_idx, event_type=event_type, stats_before=stats_before, id_column="event_id")
            emitted_events.append(record)
            raw_interactions.append({**record, "raw_interaction_id": record["event_id"]})

        if retest_idx is not None and (failed_idx is None or retest_idx < failed_idx):
            retest_row = price_df.iloc[retest_idx]
            event_type = "retest_up" if side == "resistance" else "retest_down"
            record = _build_event_dict(zone=zone, row=retest_row, idx=retest_idx, event_type=event_type, stats_before=stats_before, id_column="event_id")
            emitted_events.append(record)
            raw_interactions.append({**record, "raw_interaction_id": record["event_id"]})

    return raw_interactions, emitted_events


def _is_inside_boundary(close: float, boundary: float, side: str) -> bool:
    if side == "resistance":
        return close <= boundary
    return close >= boundary


def _is_deep_reentry(close: float, zone: pd.Series, side: str, config: BoundaryTesterConfig) -> bool:
    zone_width = max(float(zone["upper"]) - float(zone["lower"]), 0.0)
    depth_frac = max(float(config.failed_breakout_reentry_depth_frac), 0.0)
    if zone_width <= 0:
        return False
    if side == "resistance":
        threshold = float(zone["upper"]) - zone_width * depth_frac
        return close <= threshold
    threshold = float(zone["lower"]) + zone_width * depth_frac
    return close >= threshold


def _build_event_dict(
    zone: pd.Series,
    row: pd.Series,
    idx: int,
    event_type: str,
    stats_before: dict,
    id_column: str,
) -> dict:
    side = str(zone["side"]).lower()
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])
    close = float(row["close"])
    zone_width_pct = (float(zone["upper"]) - float(zone["lower"])) / max(abs(float(zone["center"])), 1e-9)
    timestamp = pd.Timestamp(row["timestamp"])

    metadata = {
        "source_reason": zone.get("source_reason"),
        "zone_valid_from": pd.Timestamp(zone["valid_from"]).isoformat(),
        "zone_valid_to": pd.Timestamp(zone["valid_to"]).isoformat() if pd.notna(zone.get("valid_to")) else None,
        "trigger_rule": event_type,
    }

    return {
        id_column: f"{zone['zone_id']}::{event_type}::{timestamp.isoformat()}",
        "ticker": zone["ticker"],
        "zone_id": zone["zone_id"],
        "structural_zone_key": zone.get("structural_zone_key", zone["zone_id"]),
        "event_timestamp": timestamp,
        "event_type": event_type,
        "direction": "up" if side == "resistance" else "down",
        "price_at_event": close,
        "boundary_price": boundary,
        "close_distance_pct": (close - boundary) / max(abs(boundary), 1e-9),
        "bar_index": int(row.get("global_bar_index", idx)),
        "local_bar_index": int(row.get("local_bar_index", idx)),
        "global_bar_index": int(row.get("global_bar_index", idx)),
        "touch_count_before_event": int(stats_before["touch_count_before_event"]),
        "meaningful_touch_count_before_event": int(stats_before["meaningful_touch_count_before_event"]),
        "is_first_meaningful_test": bool(stats_before["is_first_meaningful_test"]),
        "bars_since_last_touch": stats_before["bars_since_last_touch"],
        "days_since_last_touch": stats_before["days_since_last_touch"],
        "zone_class": zone["zone_class"],
        "zone_side": zone["side"],
        "zone_timeframe": zone["timeframe"],
        "zone_width_pct": float(zone_width_pct),
        "confluence_count": int(zone.get("confluence_count", 1)),
        "metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True),
    }
