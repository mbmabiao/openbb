from __future__ import annotations

import json
from collections import defaultdict

import pandas as pd

from .config import BoundaryTesterConfig


def detect_boundary_events(price_df: pd.DataFrame, zone_df: pd.DataFrame, config: BoundaryTesterConfig) -> pd.DataFrame:
    events: list[dict] = []

    for _, zone in zone_df.iterrows():
        events.extend(_detect_zone_events(price_df, zone, config))

    if not events:
        return pd.DataFrame(
            columns=[
                "event_id",
                "ticker",
                "zone_id",
                "event_timestamp",
                "event_type",
                "direction",
                "price_at_event",
                "boundary_price",
                "close_distance_pct",
                "bar_index",
                "is_first_test",
                "prior_test_count",
                "zone_class",
                "zone_side",
                "zone_timeframe",
                "zone_width_pct",
                "confluence_count",
                "metadata_json",
            ]
        )

    return (
        pd.DataFrame(events)
        .sort_values(["ticker", "event_timestamp", "zone_id", "event_type"], kind="stable")
        .reset_index(drop=True)
    )


def _detect_zone_events(price_df: pd.DataFrame, zone: pd.Series, config: BoundaryTesterConfig) -> list[dict]:
    ticker_prices = price_df[price_df["ticker"] == zone["ticker"]].copy() if "ticker" in price_df.columns else price_df.copy()
    ticker_prices = ticker_prices[ticker_prices["timestamp"] >= zone["valid_from"]].copy()
    if pd.notna(zone.get("valid_to")):
        ticker_prices = ticker_prices[ticker_prices["timestamp"] <= zone["valid_to"]].copy()
    ticker_prices = ticker_prices.reset_index(drop=True)

    if ticker_prices.empty:
        return []

    pre_breakout_events = _detect_pre_breakout_events(ticker_prices, zone, config)
    breakout_events = [event for event in pre_breakout_events if event["event_type"] in {"breakout_up", "breakout_down"}]
    post_breakout_events = _detect_post_breakout_events(ticker_prices, zone, breakout_events, config)
    return sorted(pre_breakout_events + post_breakout_events, key=lambda item: (item["event_timestamp"], item["event_type"]))


def _detect_pre_breakout_events(price_df: pd.DataFrame, zone: pd.Series, config: BoundaryTesterConfig) -> list[dict]:
    side = str(zone["side"]).lower()
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])
    breakout_buffer = config.breakout_buffer_pct
    probe_buffer = config.probe_buffer_pct
    min_outside = max(config.min_close_outside_zone, 1)
    max_gap = max(config.max_event_gap, 0)

    events: list[dict] = []
    last_emitted_idx: dict[str, int] = defaultdict(lambda: -10_000)
    prior_interaction_count = 0
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

            if breakout_ready and idx - last_emitted_idx["breakout_up"] > max_gap:
                events.append(_build_event_dict(price_df, zone, row, idx, "breakout_up", prior_interaction_count))
                last_emitted_idx["breakout_up"] = idx
                prior_interaction_count += 1
                continue

            qualifies_test = high >= boundary and close <= boundary * (1.0 + probe_buffer)
            qualifies_probe = high > boundary and not qualifies_test

            if qualifies_test and idx - last_emitted_idx["test"] > max_gap:
                events.append(_build_event_dict(price_df, zone, row, idx, "test", prior_interaction_count))
                last_emitted_idx["test"] = idx
                prior_interaction_count += 1
            elif qualifies_probe and idx - last_emitted_idx["probe"] > max_gap:
                events.append(_build_event_dict(price_df, zone, row, idx, "probe", prior_interaction_count))
                last_emitted_idx["probe"] = idx
                prior_interaction_count += 1
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

            if breakout_ready and idx - last_emitted_idx["breakout_down"] > max_gap:
                events.append(_build_event_dict(price_df, zone, row, idx, "breakout_down", prior_interaction_count))
                last_emitted_idx["breakout_down"] = idx
                prior_interaction_count += 1
                continue

            qualifies_test = low <= boundary and close >= boundary * (1.0 - probe_buffer)
            qualifies_probe = low < boundary and not qualifies_test

            if qualifies_test and idx - last_emitted_idx["test"] > max_gap:
                events.append(_build_event_dict(price_df, zone, row, idx, "test", prior_interaction_count))
                last_emitted_idx["test"] = idx
                prior_interaction_count += 1
            elif qualifies_probe and idx - last_emitted_idx["probe"] > max_gap:
                events.append(_build_event_dict(price_df, zone, row, idx, "probe", prior_interaction_count))
                last_emitted_idx["probe"] = idx
                prior_interaction_count += 1

    return events


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
) -> list[dict]:
    side = str(zone["side"]).lower()
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])
    retest_buffer = config.retest_buffer_pct
    events: list[dict] = []

    for breakout_event in breakout_events:
        breakout_idx = int(breakout_event["bar_index"])
        future = price_df.iloc[
            breakout_idx + 1: breakout_idx + 1 + max(config.lookahead_bars, config.failure_reentry_bars)
        ].copy()
        if future.empty:
            continue

        failed_idx = None
        retest_idx = None

        for offset, (_, row) in enumerate(future.iterrows(), start=1):
            close = float(row["close"])
            high = float(row["high"])
            low = float(row["low"])

            if side == "resistance":
                if failed_idx is None and offset <= config.failure_reentry_bars and close <= boundary:
                    failed_idx = breakout_idx + offset
                if retest_idx is None and low <= boundary * (1.0 + retest_buffer) and close > boundary:
                    retest_idx = breakout_idx + offset
            else:
                if failed_idx is None and offset <= config.failure_reentry_bars and close >= boundary:
                    failed_idx = breakout_idx + offset
                if retest_idx is None and high >= boundary * (1.0 - retest_buffer) and close < boundary:
                    retest_idx = breakout_idx + offset

            if failed_idx is not None and retest_idx is not None:
                break

        if failed_idx is not None:
            failed_row = price_df.iloc[failed_idx]
            event_type = "failed_breakout_up" if side == "resistance" else "failed_breakout_down"
            events.append(_build_event_dict(price_df, zone, failed_row, failed_idx, event_type, breakout_event["prior_test_count"] + 1))

        if retest_idx is not None and (failed_idx is None or retest_idx < failed_idx):
            retest_row = price_df.iloc[retest_idx]
            event_type = "retest_up" if side == "resistance" else "retest_down"
            events.append(_build_event_dict(price_df, zone, retest_row, retest_idx, event_type, breakout_event["prior_test_count"] + 1))

    return events


def _build_event_dict(
    price_df: pd.DataFrame,
    zone: pd.Series,
    row: pd.Series,
    idx: int,
    event_type: str,
    prior_test_count: int,
) -> dict:
    side = str(zone["side"]).lower()
    boundary = float(zone["upper"] if side == "resistance" else zone["lower"])
    close = float(row["close"])
    zone_width_pct = (float(zone["upper"]) - float(zone["lower"])) / max(abs(float(zone["center"])), 1e-9)

    metadata = {
        "source_reason": zone.get("source_reason"),
        "zone_valid_from": pd.Timestamp(zone["valid_from"]).isoformat(),
        "zone_valid_to": pd.Timestamp(zone["valid_to"]).isoformat() if pd.notna(zone.get("valid_to")) else None,
        "trigger_rule": event_type,
    }

    return {
        "event_id": f"{zone['zone_id']}::{event_type}::{pd.Timestamp(row['timestamp']).isoformat()}",
        "ticker": zone["ticker"],
        "zone_id": zone["zone_id"],
        "event_timestamp": pd.Timestamp(row["timestamp"]),
        "event_type": event_type,
        "direction": "up" if side == "resistance" else "down",
        "price_at_event": close,
        "boundary_price": boundary,
        "close_distance_pct": (close - boundary) / max(abs(boundary), 1e-9),
        "bar_index": int(idx),
        "is_first_test": bool(prior_test_count == 0),
        "prior_test_count": int(prior_test_count),
        "zone_class": zone["zone_class"],
        "zone_side": zone["side"],
        "zone_timeframe": zone["timeframe"],
        "zone_width_pct": float(zone_width_pct),
        "confluence_count": int(zone.get("confluence_count", 1)),
        "metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True),
    }
