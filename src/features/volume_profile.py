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


def compute_atr(df: pd.DataFrame, period: int = 20) -> pd.Series:
    required = {"high", "low", "close"}
    if df.empty or period < 1 or not required.issubset(df.columns):
        return pd.Series(index=df.index, dtype=float)

    high_low = df["high"] - df["low"]
    high_prev_close = (df["high"] - df["close"].shift(1)).abs()
    low_prev_close = (df["low"] - df["close"].shift(1)).abs()
    true_range = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    return true_range.rolling(window=period, min_periods=period).mean()


def compute_vwap(df: pd.DataFrame, start_idx: int) -> pd.Series:
    if df.empty or not (0 <= start_idx < len(df)):
        return pd.Series(index=df.index, dtype=float)

    subset = df.iloc[start_idx:].copy()
    typical_price = (subset["high"] + subset["low"] + subset["close"]) / 3.0
    volume = subset["volume"].fillna(0.0)

    cumulative_price_volume = (typical_price * volume).cumsum()
    cumulative_volume = volume.cumsum().replace(0, np.nan)
    avwap = cumulative_price_volume / cumulative_volume

    output = pd.Series(index=df.index, dtype=float)
    output.loc[subset.index] = avwap.values
    return output


def _find_confirmed_swing_points(
    df: pd.DataFrame,
    left_bars: int = 3,
    right_bars: int = 3,
    min_reversal_atr: float = 1.0,
    atr_period: int = 20,
) -> tuple[list[int], list[int]]:
    if df.empty or len(df) <= (left_bars + right_bars):
        return [], []

    atr_series = compute_atr(df, period=atr_period)
    highs = pd.to_numeric(df["high"], errors="coerce")
    lows = pd.to_numeric(df["low"], errors="coerce")

    swing_highs: list[int] = []
    swing_lows: list[int] = []

    for idx in range(left_bars, len(df) - right_bars):
        atr_value = float(atr_series.iloc[idx]) if pd.notna(atr_series.iloc[idx]) else np.nan
        if not np.isfinite(atr_value) or atr_value <= 0:
            continue

        high_value = float(highs.iloc[idx])
        low_value = float(lows.iloc[idx])
        if not np.isfinite(high_value) or not np.isfinite(low_value):
            continue

        left_highs = highs.iloc[idx - left_bars:idx]
        right_highs = highs.iloc[idx + 1:idx + 1 + right_bars]
        left_lows = lows.iloc[idx - left_bars:idx]
        right_lows = lows.iloc[idx + 1:idx + 1 + right_bars]

        if (
            left_highs.notna().all()
            and right_highs.notna().all()
            and high_value > float(left_highs.max())
            and high_value > float(right_highs.max())
        ):
            reversal_down = high_value - float(right_lows.min())
            if reversal_down >= (min_reversal_atr * atr_value):
                swing_highs.append(idx)

        if (
            left_lows.notna().all()
            and right_lows.notna().all()
            and low_value < float(left_lows.min())
            and low_value < float(right_lows.min())
        ):
            reversal_up = float(right_highs.max()) - low_value
            if reversal_up >= (min_reversal_atr * atr_value):
                swing_lows.append(idx)

    return swing_highs, swing_lows


def find_anchor_points(
    df: pd.DataFrame,
    timeframe: str,
    rolling_window_bars: tuple[int, ...] | None = None,
    swing_search_bars: int = 60,
    event_search_bars: int = 60,
) -> dict[str, dict[str, Any]]:
    anchors: dict[str, dict[str, Any]] = {}
    if df.empty:
        return anchors

    rolling_window_bars = rolling_window_bars or (20, 60)
    swing_highs, swing_lows = _find_confirmed_swing_points(
        df,
        left_bars=3,
        right_bars=3,
        min_reversal_atr=1.0,
        atr_period=20,
    )

    for window_bars in rolling_window_bars:
        if len(df) < window_bars:
            continue
        window_slice = df.iloc[-window_bars:]
        anchors[f"rolling_{window_bars}_high"] = {
            "index": int(window_slice["high"].idxmax()),
            "anchor_family": "rolling",
            "anchor_window_bars": window_bars,
            "anchor_search_bars": None,
            "timeframe": timeframe,
        }
        anchors[f"rolling_{window_bars}_low"] = {
            "index": int(window_slice["low"].idxmin()),
            "anchor_family": "rolling",
            "anchor_window_bars": window_bars,
            "anchor_search_bars": None,
            "timeframe": timeframe,
        }

    swing_search_bars = min(max(int(swing_search_bars), 1), len(df))
    swing_window_start = len(df) - swing_search_bars
    recent_swing_highs = [idx for idx in swing_highs if idx >= swing_window_start]
    recent_swing_lows = [idx for idx in swing_lows if idx >= swing_window_start]

    if recent_swing_highs:
        anchors["recent_swing_high"] = {
            "index": int(recent_swing_highs[-1]),
            "anchor_family": "swing",
            "anchor_window_bars": None,
            "anchor_search_bars": swing_search_bars,
            "timeframe": timeframe,
        }
    if len(recent_swing_highs) >= 2:
        anchors["previous_swing_high"] = {
            "index": int(recent_swing_highs[-2]),
            "anchor_family": "swing",
            "anchor_window_bars": None,
            "anchor_search_bars": swing_search_bars,
            "timeframe": timeframe,
        }
    if recent_swing_lows:
        anchors["recent_swing_low"] = {
            "index": int(recent_swing_lows[-1]),
            "anchor_family": "swing",
            "anchor_window_bars": None,
            "anchor_search_bars": swing_search_bars,
            "timeframe": timeframe,
        }
    if len(recent_swing_lows) >= 2:
        anchors["previous_swing_low"] = {
            "index": int(recent_swing_lows[-2]),
            "anchor_family": "swing",
            "anchor_window_bars": None,
            "anchor_search_bars": swing_search_bars,
            "timeframe": timeframe,
        }

    event_search_bars = min(max(int(event_search_bars), 1), len(df))
    previous_close = df["close"].shift(1)
    gap_pct = (df["open"] - previous_close) / previous_close.replace(0, np.nan)
    gap_slice = gap_pct.iloc[-event_search_bars:]
    if gap_slice.notna().sum() > 0:
        anchors["gap_down"] = {
            "index": int(gap_slice.idxmin()),
            "anchor_family": "event",
            "anchor_window_bars": None,
            "anchor_search_bars": event_search_bars,
            "timeframe": timeframe,
        }
        anchors["gap_up"] = {
            "index": int(gap_slice.idxmax()),
            "anchor_family": "event",
            "anchor_window_bars": None,
            "anchor_search_bars": event_search_bars,
            "timeframe": timeframe,
        }

    body_return = (df["close"] - df["open"]) / df["open"].replace(0, np.nan)
    body_slice = body_return.iloc[-event_search_bars:]
    if body_slice.notna().sum() > 0:
        anchors["big_down"] = {
            "index": int(body_slice.idxmin()),
            "anchor_family": "event",
            "anchor_window_bars": None,
            "anchor_search_bars": event_search_bars,
            "timeframe": timeframe,
        }
        anchors["big_up"] = {
            "index": int(body_slice.idxmax()),
            "anchor_family": "event",
            "anchor_window_bars": None,
            "anchor_search_bars": event_search_bars,
            "timeframe": timeframe,
        }

    filtered: dict[str, dict[str, Any]] = {}
    for name, meta in anchors.items():
        index = int(meta["index"])
        if 0 <= index < len(df) - 1:
            filtered[name] = meta
    return filtered


def build_vp_zones_from_profile(
    vp_df: pd.DataFrame,
    zone_expand: float,
    hv_quantile: float,
    timeframe: str,
    source_label: str,
) -> tuple[list[dict], pd.DataFrame]:
    if vp_df.empty:
        return [], vp_df

    profile_df = vp_df.copy()
    profile_df["volume"] = pd.to_numeric(profile_df["volume"], errors="coerce").fillna(0.0)
    if not (profile_df["volume"] > 0).any():
        return [], profile_df

    threshold = profile_df["volume"].quantile(hv_quantile)
    high_volume_nodes = profile_df.loc[profile_df["volume"] >= threshold].copy()
    if high_volume_nodes.empty:
        return [], profile_df

    high_volume_nodes = high_volume_nodes.sort_values("bin_center").reset_index(drop=True)
    zones: list[dict] = []

    current_left = float(high_volume_nodes.loc[0, "bin_left"])
    current_right = float(high_volume_nodes.loc[0, "bin_right"])
    current_volume = float(high_volume_nodes.loc[0, "volume"])

    for row_index in range(1, len(high_volume_nodes)):
        left = float(high_volume_nodes.loc[row_index, "bin_left"])
        right = float(high_volume_nodes.loc[row_index, "bin_right"])
        volume = float(high_volume_nodes.loc[row_index, "volume"])

        width_reference = max(current_right - current_left, 1e-9)
        close_enough = (left - current_right) <= (width_reference * 0.8)
        if close_enough:
            current_right = right
            current_volume += volume
            continue

        center = (current_left + current_right) / 2.0
        expand = center * zone_expand
        zones.append(
            {
                "type": f"vp_zone_{timeframe}",
                "lower": current_left - expand,
                "upper": current_right + expand,
                "center": center,
                "vp_volume": current_volume,
                "timeframes": {timeframe},
                "source_types": {f"vp_{timeframe}"},
                "primary_timeframe": timeframe,
                "source_label": source_label,
            }
        )

        current_left = left
        current_right = right
        current_volume = volume

    center = (current_left + current_right) / 2.0
    expand = center * zone_expand
    zones.append(
        {
            "type": f"vp_zone_{timeframe}",
            "lower": current_left - expand,
            "upper": current_right + expand,
            "center": center,
            "vp_volume": current_volume,
            "timeframes": {timeframe},
            "source_types": {f"vp_{timeframe}"},
            "primary_timeframe": timeframe,
            "source_label": source_label,
        }
    )

    return zones, profile_df


def build_composite_interval_volume_profile_zones(
    interval_df: pd.DataFrame,
    bins: int,
    zone_expand: float,
    hv_quantile: float,
    timeframe: str,
    source_label: str | None = None,
    source_mode: str = "composite",
) -> tuple[list[dict], pd.DataFrame]:
    if interval_df.empty or bins < 1:
        return [], pd.DataFrame()

    source = interval_df.copy()
    low_min = float(source["low"].min())
    high_max = float(source["high"].max())
    if not np.isfinite(low_min) or not np.isfinite(high_max):
        return [], pd.DataFrame()

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

    return build_vp_zones_from_profile(
        vp_df=profile_df,
        zone_expand=zone_expand,
        hv_quantile=hv_quantile,
        timeframe=timeframe,
        source_label=source_label or f"VP ({timeframe}, composite)",
    )


def build_avwap_features(df: pd.DataFrame, timeframe: str) -> tuple[pd.DataFrame, dict]:
    anchors = find_anchor_points(
        df,
        timeframe=timeframe,
        rolling_window_bars=(20, 60),
        swing_search_bars=60,
        event_search_bars=60,
    )
    avwap_columns: dict[str, pd.Series] = {}
    anchor_meta: dict[str, dict] = {}

    for anchor_name, source_meta in anchors.items():
        index = int(source_meta["index"])
        column_name = f"avwap_{timeframe}_{anchor_name}"
        avwap_columns[column_name] = compute_vwap(df, index)
        anchor_meta[column_name] = {
            "anchor_name": anchor_name,
            "anchor_family": source_meta["anchor_family"],
            "start_idx": index,
            "start_date": df.loc[index, "date"],
            "start_price": float(df.loc[index, "close"]),
            "timeframe": timeframe,
            "anchor_window_bars": source_meta.get("anchor_window_bars"),
            "anchor_search_bars": source_meta.get("anchor_search_bars"),
        }

    output = df.copy()
    for column_name, values in avwap_columns.items():
        output[column_name] = values

    return output, anchor_meta
