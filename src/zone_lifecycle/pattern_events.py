from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
from typing import Any

import pandas as pd

from config.warmup_config import PatternEventThresholdConfig, load_warmup_config

DEFAULT_PATTERN_EVENT_CONFIG = load_warmup_config().pattern_events
MIN_WICK_EVENT_BODY_RETURN = 0.003


@dataclass(frozen=True, slots=True)
class PatternEventInput:
    event_id: str
    symbol: str
    timeframe: str
    event_time: datetime
    event_type: str
    direction: str
    price_open: float
    price_high: float
    price_low: float
    price_close: float
    previous_close: float
    volume: float
    body_ratio: float
    upper_wick_ratio: float
    lower_wick_ratio: float
    close_position: float
    price_change_pct: float
    intrabar_return_pct: float
    gap_pct: float
    abs_price_change_pct: float
    volume_percentile_20: float
    abs_price_change_percentile_20: float
    lookback_bars: int
    related_zone_id: str | None = None
    metadata: dict[str, Any] | None = None


def detect_pattern_events_for_latest_bar(
    history: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    related_zone_id: str | None = None,
    config: PatternEventThresholdConfig | None = None,
) -> list[PatternEventInput]:
    config = config or DEFAULT_PATTERN_EVENT_CONFIG
    lookback_bars = int(config.lookback_bars)
    if history.empty or len(history) <= lookback_bars:
        return []

    required = {"timestamp", "open", "high", "low", "close", "volume"}
    if not required.issubset(history.columns):
        return []

    frame = history.sort_values("timestamp", kind="stable").reset_index(drop=True)
    current = frame.iloc[-1]
    previous = frame.iloc[-2]

    price_open = _to_float(current["open"])
    price_high = _to_float(current["high"])
    price_low = _to_float(current["low"])
    price_close = _to_float(current["close"])
    previous_close = _to_float(previous["close"])
    volume = _to_float(current["volume"])
    event_time = pd.Timestamp(current["timestamp"]).to_pydatetime().replace(tzinfo=None)

    if (
        not all(math.isfinite(value) for value in [price_open, price_high, price_low, price_close, previous_close, volume])
        or previous_close <= 0
        or price_open <= 0
    ):
        return []

    candle_range = price_high - price_low
    if candle_range <= 0:
        return []

    volume_history = pd.to_numeric(frame["volume"].iloc[-lookback_bars - 1:-1], errors="coerce").dropna()
    if len(volume_history) < lookback_bars:
        return []

    previous_closes = pd.to_numeric(frame["close"].shift(1), errors="coerce")
    closes = pd.to_numeric(frame["close"], errors="coerce")
    abs_change_history = ((closes - previous_closes) / previous_closes.where(previous_closes != 0)).abs()
    abs_change_history = abs_change_history.iloc[-lookback_bars - 1:-1].dropna()
    if len(abs_change_history) < lookback_bars:
        return []

    body_size = abs(price_close - price_open)
    upper_wick_size = max(price_high - max(price_open, price_close), 0.0)
    lower_wick_size = max(min(price_open, price_close) - price_low, 0.0)
    atr20 = _latest_atr(frame, period=int(config.atr_period))
    if not math.isfinite(atr20) or atr20 <= 0:
        return []
    min_wick_size = atr20 * float(config.long_wick_atr_multiple)

    price_change_pct = (price_close - previous_close) / previous_close
    abs_price_change_pct = abs(price_change_pct)
    metrics = {
        "body_ratio": body_size / candle_range,
        "upper_wick_ratio": upper_wick_size / candle_range,
        "lower_wick_ratio": lower_wick_size / candle_range,
        "close_position": (price_close - price_low) / candle_range,
        "price_change_pct": price_change_pct,
        "intrabar_return_pct": (price_close - price_open) / price_open,
        "gap_pct": (price_open - previous_close) / previous_close,
        "abs_price_change_pct": abs_price_change_pct,
        "volume_percentile_20": _percentile_rank(volume_history, volume),
        "abs_price_change_percentile_20": _percentile_rank(abs_change_history, abs_price_change_pct),
    }
    if not all(math.isfinite(value) for value in metrics.values()):
        return []

    is_high_volume = metrics["volume_percentile_20"] >= float(config.high_volume_percentile)
    is_low_price_movement = metrics["abs_price_change_percentile_20"] <= float(config.low_price_movement_percentile)
    close_return = price_change_pct
    open_return = (price_open - previous_close) / previous_close
    has_wick_event_body = abs(close_return - open_return) >= MIN_WICK_EVENT_BODY_RETURN
    has_long_upper_wick = (
        metrics["upper_wick_ratio"] >= float(config.long_wick_ratio)
        and upper_wick_size >= min_wick_size
    )
    has_long_lower_wick = (
        metrics["lower_wick_ratio"] >= float(config.long_wick_ratio)
        and lower_wick_size >= min_wick_size
    )

    event_specs: list[tuple[str, str, bool]] = [
        ("volume_stall_up", "bearish", is_high_volume and is_low_price_movement and price_change_pct > 0),
        ("volume_hold_down", "bullish", is_high_volume and is_low_price_movement and price_change_pct <= 0),
        (
            "volume_long_upper_wick",
            "bearish",
            is_high_volume
            and has_wick_event_body
            and has_long_upper_wick,
        ),
        (
            "volume_long_lower_wick",
            "bullish",
            is_high_volume
            and has_wick_event_body
            and has_long_lower_wick,
        ),
    ]

    output: list[PatternEventInput] = []
    for event_type, direction, matched in event_specs:
        if not matched:
            continue
        output.append(
            PatternEventInput(
                event_id=_pattern_event_id(symbol=symbol, timeframe=timeframe, event_time=event_time, event_type=event_type),
                symbol=str(symbol).strip().upper(),
                timeframe=str(timeframe).strip().lower(),
                event_time=event_time,
                event_type=event_type,
                direction=direction,
                price_open=price_open,
                price_high=price_high,
                price_low=price_low,
                price_close=price_close,
                previous_close=previous_close,
                volume=volume,
                lookback_bars=lookback_bars,
                related_zone_id=related_zone_id,
                metadata={
                    "is_high_volume": is_high_volume,
                    "is_low_price_movement": is_low_price_movement,
                    "volume_threshold_percentile": float(config.high_volume_percentile),
                    "price_movement_threshold_percentile": float(config.low_price_movement_percentile),
                    "long_wick_ratio_threshold": float(config.long_wick_ratio),
                    "long_wick_atr_multiple_threshold": float(config.long_wick_atr_multiple),
                    "long_wick_min_size": min_wick_size,
                    "upper_wick_size": upper_wick_size,
                    "lower_wick_size": lower_wick_size,
                    "atr20": atr20,
                    "wick_event_min_body_return": MIN_WICK_EVENT_BODY_RETURN,
                    "wick_event_body_return": abs(close_return - open_return),
                },
                **metrics,
            )
        )
    return output


def _percentile_rank(history: pd.Series, value: float) -> float:
    numeric = pd.to_numeric(history, errors="coerce").dropna()
    if numeric.empty:
        return math.nan
    return float((numeric <= float(value)).sum()) / float(len(numeric))


def _latest_atr(frame: pd.DataFrame, *, period: int) -> float:
    period = max(int(period), 1)
    if len(frame) < period + 1:
        return math.nan
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.rolling(window=period, min_periods=period).mean()
    return _to_float(atr.iloc[-1])


def _pattern_event_id(*, symbol: str, timeframe: str, event_time: datetime, event_type: str) -> str:
    payload = {
        "symbol": str(symbol).strip().upper(),
        "timeframe": str(timeframe).strip().lower(),
        "event_time": pd.Timestamp(event_time).isoformat(),
        "event_type": str(event_type).strip().lower(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"pattern_{digest}"


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan
