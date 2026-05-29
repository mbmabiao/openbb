from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "open", "high", "low", "close"}
    if df.empty or not required.issubset(df.columns):
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    frame = df.copy()
    if "volume" not in frame.columns:
        frame["volume"] = 0.0

    weekly = (
        frame.set_index("date")
        .sort_index()
        .resample("W-FRI")
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    return weekly


def compute_atr(df: pd.DataFrame, period: int = 21) -> pd.Series:
    required = {"high", "low", "close"}
    if df.empty or period < 1 or not required.issubset(df.columns):
        return pd.Series(index=df.index, dtype=float)

    high_low = df["high"] - df["low"]
    high_prev_close = (df["high"] - df["close"].shift(1)).abs()
    low_prev_close = (df["low"] - df["close"].shift(1)).abs()
    true_range = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    return true_range.rolling(window=period, min_periods=period).mean()


def _volume_qualifies(
    df: pd.DataFrame,
    index: int,
    lookback_bars: int = 60,
    quantile: float = 0.8,
) -> bool:
    if "volume" not in df.columns or index <= 0 or not 0 <= index < len(df):
        return False

    event_volume = pd.to_numeric(pd.Series([df["volume"].iloc[index]]), errors="coerce").iloc[0]
    if pd.isna(event_volume):
        return False

    start = max(0, index - lookback_bars)
    history = pd.to_numeric(df["volume"].iloc[start:index], errors="coerce").dropna()
    if history.empty:
        return False

    return float(event_volume) >= float(history.quantile(quantile))


def find_recent_swing_points(
    df: pd.DataFrame,
    *,
    timeframe: str,
    max_points_per_side: int = 3,
    left_bars: int = 2,
    right_bars: int = 2,
    volume_lookback_bars: int = 20,
    volume_quantile: float = 0.75,
) -> list[dict[str, Any]]:
    """Return recent confirmed high-volume swing high/low points."""
    if df.empty:
        return []

    swing_highs = _find_basic_swing_points(
        df,
        price_column="high",
        mode="high",
        left_bars=left_bars,
        right_bars=right_bars,
    )
    swing_lows = _find_basic_swing_points(
        df,
        price_column="low",
        mode="low",
        left_bars=left_bars,
        right_bars=right_bars,
    )
    points: list[dict[str, Any]] = []
    max_count = max(int(max_points_per_side), 0)
    if max_count <= 0:
        return points

    qualified_highs = [
        index
        for index in swing_highs
        if _volume_qualifies(
            df,
            index,
            lookback_bars=volume_lookback_bars,
            quantile=volume_quantile,
        )
    ]
    qualified_lows = [
        index
        for index in swing_lows
        if _volume_qualifies(
            df,
            index,
            lookback_bars=volume_lookback_bars,
            quantile=volume_quantile,
        )
    ]

    for index in qualified_highs[-max_count:]:
        points.append(_swing_point_payload(df, index=index, side="resistance", price_column="high", timeframe=timeframe))
    for index in qualified_lows[-max_count:]:
        points.append(_swing_point_payload(df, index=index, side="support", price_column="low", timeframe=timeframe))

    return sorted(points, key=lambda point: (pd.Timestamp(point["origin_bar"]), point["side"], point["price"]))


def _find_basic_swing_points(
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
    extrema: list[int] = []
    for idx in range(left_bars, len(df) - right_bars):
        value = values.iloc[idx]
        if pd.isna(value):
            continue
        left = values.iloc[idx - left_bars:idx].dropna()
        right = values.iloc[idx + 1:idx + 1 + right_bars].dropna()
        if len(left) < left_bars or len(right) < right_bars:
            continue
        if mode == "high" and float(value) > float(left.max()) and float(value) > float(right.max()):
            extrema.append(idx)
        elif mode == "low" and float(value) < float(left.min()) and float(value) < float(right.min()):
            extrema.append(idx)
    return extrema


def _swing_point_payload(
    df: pd.DataFrame,
    *,
    index: int,
    side: str,
    price_column: str,
    timeframe: str,
) -> dict[str, Any]:
    timestamp = pd.Timestamp(df.loc[index, "date"])
    price = float(df.loc[index, price_column])
    return {
        "index": int(index),
        "side": side,
        "price": price,
        "origin_bar": timestamp,
        "timeframe": timeframe,
        "anchor_family": "swing",
        "anchor_name": f"weekly_swing_{'high' if side == 'resistance' else 'low'}_{timestamp.strftime('%Y%m%d')}",
        "price_column": price_column,
    }


def build_composite_interval_volume_profile_zones(
    interval_df: pd.DataFrame,
    bins: int,
    timeframe: str,
    source_mode: str = "composite",
) -> pd.DataFrame:
    if interval_df.empty or bins < 1:
        return pd.DataFrame()

    source = interval_df.copy()
    low_min = float(source["low"].min())
    high_max = float(source["high"].max())
    if not np.isfinite(low_min) or not np.isfinite(high_max):
        return pd.DataFrame()

    if high_max <= low_min:
        high_max = low_min * (1.0 + 1e-6) if low_min != 0 else 1e-6

    bin_edges = np.linspace(low_min, high_max, bins + 1)
    bin_left = bin_edges[:-1]
    bin_right = bin_edges[1:]
    bin_centers = (bin_left + bin_right) / 2.0
    volume_bins = np.zeros(bins, dtype=float)
    buy_volume_bins = np.zeros(bins, dtype=float)
    sell_volume_bins = np.zeros(bins, dtype=float)

    for row in source.itertuples(index=False):
        low = float(row.low)
        high = float(row.high)
        volume = float(row.volume)
        open_price = float(row.open)
        close_price = float(row.close)

        if (
            not np.isfinite(low)
            or not np.isfinite(high)
            or not np.isfinite(volume)
            or not np.isfinite(open_price)
            or not np.isfinite(close_price)
            or volume <= 0
        ):
            continue

        if high < low:
            low, high = high, low

        is_buy_bar = close_price >= open_price

        low = min(max(low, low_min), high_max)
        high = min(max(high, low_min), high_max)

        if high <= low:
            index = int(np.searchsorted(bin_edges, low, side="right") - 1)
            index = int(np.clip(index, 0, bins - 1))
            volume_bins[index] += volume
            if is_buy_bar:
                buy_volume_bins[index] += volume
            else:
                sell_volume_bins[index] += volume
            continue

        overlap_left = np.maximum(bin_left, low)
        overlap_right = np.minimum(bin_right, high)
        overlaps = np.maximum(overlap_right - overlap_left, 0.0)
        total_overlap = float(overlaps.sum())

        if total_overlap <= 0:
            index = int(np.searchsorted(bin_edges, low, side="right") - 1)
            index = int(np.clip(index, 0, bins - 1))
            volume_bins[index] += volume
            if is_buy_bar:
                buy_volume_bins[index] += volume
            else:
                sell_volume_bins[index] += volume
            continue

        distributed_volume = volume * (overlaps / total_overlap)
        volume_bins += distributed_volume
        if is_buy_bar:
            buy_volume_bins += distributed_volume
        else:
            sell_volume_bins += distributed_volume

    profile_df = pd.DataFrame(
        {
            "bin_left": bin_left,
            "bin_right": bin_right,
            "bin_center": bin_centers,
            "volume": volume_bins,
            "buy_volume": buy_volume_bins,
            "sell_volume": sell_volume_bins,
            "timeframe": timeframe,
            "source_bars": len(source),
            "source_mode": source_mode,
        }
    )

    profile_df["volume"] = pd.to_numeric(profile_df["volume"], errors="coerce").fillna(0.0)
    return profile_df

