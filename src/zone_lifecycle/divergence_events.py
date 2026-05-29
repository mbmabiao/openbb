from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
from typing import Any

import pandas as pd

from config.warmup_config import MacdDivergenceConfig, load_warmup_config


DEFAULT_MACD_DIVERGENCE_CONFIG = load_warmup_config().macd_divergence


@dataclass(frozen=True, slots=True)
class DivergenceEventInput:
    event_id: str
    symbol: str
    timeframe: str
    event_type: str
    event_name: str
    timestamp: datetime
    price: float
    direction: str
    strength_score: float
    source: str
    metadata: dict[str, Any]


def detect_macd_divergence_events_for_latest_bar(
    history: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    config: MacdDivergenceConfig | None = None,
) -> list[DivergenceEventInput]:
    config = config or DEFAULT_MACD_DIVERGENCE_CONFIG
    if not bool(config.enabled):
        return []
    required = {"timestamp", "high", "low", "close"}
    if history.empty or not required.issubset(history.columns):
        return []

    frame = history.sort_values("timestamp", kind="stable").reset_index(drop=True).copy()
    min_bars = (
        max(int(config.slow_period), int(config.signal_period))
        + int(config.swing_left_bars)
    )
    if len(frame) < min_bars:
        return []

    close = pd.to_numeric(frame["close"], errors="coerce")
    if close.dropna().empty:
        return []
    dif, dea, histogram = _macd(
        close,
        fast_period=int(config.fast_period),
        slow_period=int(config.slow_period),
        signal_period=int(config.signal_period),
    )
    frame["dif"] = dif
    frame["dea"] = dea
    frame["histogram"] = histogram

    left_bars = int(config.swing_left_bars)
    right_bars = int(config.swing_right_bars)
    min_bar_distance = int(config.min_bar_distance)

    output: list[DivergenceEventInput] = []
    side_specs = [
        {
            "side": "high",
            "price_column": "high",
            "event_type": "macd_bearish_divergence",
            "event_name": "顶背离",
            "direction": "bearish",
        },
        {
            "side": "low",
            "price_column": "low",
            "event_type": "macd_bullish_divergence",
            "event_name": "底背离",
            "direction": "bullish",
        },
    ]
    for spec in side_specs:
        output.extend(
            _detect_latest_side(
                frame,
                symbol=symbol,
                timeframe=timeframe,
                left_bars=left_bars,
                right_bars=right_bars,
                min_bar_distance=min_bar_distance,
                **spec,
            )
        )
    return output


def _detect_latest_side(
    frame: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    side: str,
    price_column: str,
    event_type: str,
    event_name: str,
    direction: str,
    left_bars: int,
    right_bars: int,
    min_bar_distance: int,
) -> list[DivergenceEventInput]:
    current_idx = len(frame) - 1
    pivots = _swing_indices(
        frame,
        price_column=price_column,
        mode=side,
        left_bars=left_bars,
        right_bars=right_bars,
    )
    previous_pivots = [index for index in pivots if index < current_idx]
    if not previous_pivots:
        return []
    previous_idx = previous_pivots[-1]

    return _build_divergence_event(
        frame,
        symbol=symbol,
        timeframe=timeframe,
        side=side,
        price_column=price_column,
        event_type=event_type,
        event_name=event_name,
        direction=direction,
        previous_idx=previous_idx,
        current_idx=current_idx,
        left_bars=left_bars,
        right_bars=right_bars,
        min_bar_distance=min_bar_distance,
    )


def _build_divergence_event(
    frame: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    side: str,
    price_column: str,
    event_type: str,
    event_name: str,
    direction: str,
    previous_idx: int,
    current_idx: int,
    left_bars: int,
    right_bars: int,
    min_bar_distance: int,
) -> list[DivergenceEventInput]:
    bar_distance = current_idx - previous_idx
    if bar_distance < min_bar_distance:
        return []

    previous_price = _to_float(frame.at[previous_idx, price_column])
    current_price = _to_float(frame.at[current_idx, price_column])
    previous_dif = _to_float(frame.at[previous_idx, "dif"])
    current_dif = _to_float(frame.at[current_idx, "dif"])
    previous_histogram = _to_float(frame.at[previous_idx, "histogram"])
    current_histogram = _to_float(frame.at[current_idx, "histogram"])
    if not all(math.isfinite(value) for value in [previous_price, current_price, previous_dif, current_dif]):
        return []

    prior_price_window = pd.to_numeric(frame.loc[previous_idx: current_idx - 1, price_column], errors="coerce").dropna()
    prior_dif_window = pd.to_numeric(frame.loc[previous_idx: current_idx - 1, "dif"], errors="coerce").dropna()
    if prior_price_window.empty or prior_dif_window.empty:
        return []

    if side == "high":
        prior_extreme_price = float(prior_price_window.max())
        prior_extreme_dif = float(prior_dif_window.max())
        matched = current_price > prior_extreme_price and current_dif <= prior_extreme_dif
        pivot_distance = current_price - prior_extreme_price
        dif_distance = prior_extreme_dif - current_dif
    else:
        prior_extreme_price = float(prior_price_window.min())
        prior_extreme_dif = float(prior_dif_window.min())
        matched = current_price < prior_extreme_price and current_dif >= prior_extreme_dif
        pivot_distance = prior_extreme_price - current_price
        dif_distance = current_dif - prior_extreme_dif
    if not matched:
        return []

    timestamp = pd.Timestamp(frame.at[current_idx, "timestamp"]).to_pydatetime().replace(tzinfo=None)
    swing_strength = _swing_strength(
        frame,
        index=current_idx,
        price_column=price_column,
        side=side,
        left_bars=left_bars,
        right_bars=0,
    )
    strength_score = _strength_score(
        pivot_distance=pivot_distance,
        reference_price=current_price,
        dif_distance=dif_distance,
        reference_dif=previous_dif,
        swing_strength=swing_strength,
    )
    metadata = {
        "previous_price": previous_price,
        "current_price": current_price,
        "prior_extreme_price": prior_extreme_price,
        "previous_dif": previous_dif,
        "current_dif": current_dif,
        "prior_extreme_dif": prior_extreme_dif,
        "previous_dea": _to_float(frame.at[previous_idx, "dea"]),
        "current_dea": _to_float(frame.at[current_idx, "dea"]),
        "previous_histogram": previous_histogram,
        "current_histogram": current_histogram,
        "histogram_values": {
            "previous": previous_histogram,
            "current": current_histogram,
        },
        "pivot_distance": pivot_distance,
        "bar_distance": bar_distance,
        "swing_strength": swing_strength,
        "swing_left_bars": left_bars,
        "swing_right_bars": right_bars,
        "observed_right_bars": 0,
        "remaining_confirmation_bars": 0,
        "previous_pivot_time": pd.Timestamp(frame.at[previous_idx, "timestamp"]).isoformat(),
        "current_pivot_time": pd.Timestamp(frame.at[current_idx, "timestamp"]).isoformat(),
        "uses_future_bars": False,
        "is_risk": False,
    }
    return [
        DivergenceEventInput(
            event_id=_divergence_event_id(symbol=symbol, timeframe=timeframe, timestamp=timestamp, event_type=event_type),
            symbol=str(symbol).strip().upper(),
            timeframe=str(timeframe).strip().lower(),
            event_type=event_type,
            event_name=event_name,
            timestamp=timestamp,
            price=current_price,
            direction=direction,
            strength_score=strength_score,
            source="macd_divergence",
            metadata=metadata,
        )
    ]


def _macd(close: pd.Series, *, fast_period: int, slow_period: int, signal_period: int) -> tuple[pd.Series, pd.Series, pd.Series]:
    fast = close.ewm(span=max(int(fast_period), 1), adjust=False, min_periods=max(int(fast_period), 1)).mean()
    slow = close.ewm(span=max(int(slow_period), 1), adjust=False, min_periods=max(int(slow_period), 1)).mean()
    dif = fast - slow
    dea = dif.ewm(span=max(int(signal_period), 1), adjust=False, min_periods=max(int(signal_period), 1)).mean()
    histogram = dif - dea
    return dif, dea, histogram


def _swing_indices(
    df: pd.DataFrame,
    *,
    price_column: str,
    mode: str,
    left_bars: int,
    right_bars: int,
) -> list[int]:
    if df.empty or price_column not in df.columns or len(df) <= left_bars + right_bars:
        return []
    values = pd.to_numeric(df[price_column], errors="coerce")
    output: list[int] = []
    for idx in range(left_bars, len(df) - right_bars):
        value = values.iloc[idx]
        if pd.isna(value):
            continue
        left = values.iloc[idx - left_bars:idx].dropna()
        right = values.iloc[idx + 1:idx + 1 + right_bars].dropna()
        if len(left) < left_bars or len(right) < right_bars:
            continue
        if mode == "high" and float(value) > float(left.max()) and float(value) > float(right.max()):
            output.append(idx)
        elif mode == "low" and float(value) < float(left.min()) and float(value) < float(right.min()):
            output.append(idx)
    return output


def _swing_strength(
    df: pd.DataFrame,
    *,
    index: int,
    price_column: str,
    side: str,
    left_bars: int,
    right_bars: int,
) -> float:
    values = pd.to_numeric(df[price_column], errors="coerce")
    value = _to_float(values.iloc[index])
    left = values.iloc[index - left_bars:index].dropna()
    right = values.iloc[index + 1:index + 1 + right_bars].dropna()
    if not math.isfinite(value) or left.empty:
        return 0.0
    if side == "high":
        comparison = float(left.max()) if right.empty else max(float(left.max()), float(right.max()))
        return max(value - comparison, 0.0)
    comparison = float(left.min()) if right.empty else min(float(left.min()), float(right.min()))
    return max(comparison - value, 0.0)


def _strength_score(*, pivot_distance: float, reference_price: float, dif_distance: float, reference_dif: float, swing_strength: float) -> float:
    price_component = abs(float(pivot_distance)) / max(abs(float(reference_price)), 1e-9)
    dif_component = abs(float(dif_distance)) / max(abs(float(reference_dif)), 1e-9)
    swing_component = abs(float(swing_strength)) / max(abs(float(reference_price)), 1e-9)
    return float(min((price_component + dif_component + swing_component) * 100.0, 100.0))


def _divergence_event_id(*, symbol: str, timeframe: str, timestamp: datetime, event_type: str) -> str:
    payload = {
        "symbol": str(symbol).strip().upper(),
        "timeframe": str(timeframe).strip().lower(),
        "timestamp": pd.Timestamp(timestamp).isoformat(),
        "event_type": str(event_type).strip().lower(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"divergence_{digest}"


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan
