from __future__ import annotations

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


def find_anchor_points(df: pd.DataFrame, recent_window_cap: int = 126) -> dict[str, int]:
    anchors: dict[str, int] = {}
    if len(df) < 30:
        return anchors

    recent_window = min(recent_window_cap, len(df))
    recent_slice = df.iloc[-recent_window:]

    anchors["major_high"] = int(recent_slice["high"].idxmax())
    anchors["major_low"] = int(recent_slice["low"].idxmin())

    previous_close = df["close"].shift(1)
    gap_pct = (df["open"] - previous_close) / previous_close.replace(0, np.nan)
    gap_slice = gap_pct.iloc[-recent_window:]
    if gap_slice.notna().sum() > 0:
        anchors["gap_down"] = int(gap_slice.idxmin())
        anchors["gap_up"] = int(gap_slice.idxmax())

    body_return = (df["close"] - df["open"]) / df["open"].replace(0, np.nan)
    body_slice = body_return.iloc[-recent_window:]
    if body_slice.notna().sum() > 0:
        anchors["big_down"] = int(body_slice.idxmin())
        anchors["big_up"] = int(body_slice.idxmax())

    deduped: dict[str, int] = {}
    seen_indexes: set[int] = set()
    for name, index in anchors.items():
        if index not in seen_indexes and 0 <= index < len(df) - 1:
            deduped[name] = index
            seen_indexes.add(index)
    return deduped


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

    for row in source.itertuples(index=False):
        low = float(row.low)
        high = float(row.high)
        volume = float(row.volume)

        if not np.isfinite(low) or not np.isfinite(high) or not np.isfinite(volume) or volume <= 0:
            continue

        if high < low:
            low, high = high, low

        low = min(max(low, low_min), high_max)
        high = min(max(high, low_min), high_max)

        if high <= low:
            index = int(np.searchsorted(bin_edges, low, side="right") - 1)
            index = int(np.clip(index, 0, bins - 1))
            volume_bins[index] += volume
            continue

        overlap_left = np.maximum(bin_left, low)
        overlap_right = np.minimum(bin_right, high)
        overlaps = np.maximum(overlap_right - overlap_left, 0.0)
        total_overlap = float(overlaps.sum())

        if total_overlap <= 0:
            index = int(np.searchsorted(bin_edges, low, side="right") - 1)
            index = int(np.clip(index, 0, bins - 1))
            volume_bins[index] += volume
            continue

        volume_bins += volume * (overlaps / total_overlap)

    profile_df = pd.DataFrame(
        {
            "bin_left": bin_left,
            "bin_right": bin_right,
            "bin_center": bin_centers,
            "volume": volume_bins,
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
    recent_window_cap = 126 if timeframe == "D" else 52
    anchors = find_anchor_points(df, recent_window_cap=recent_window_cap)
    avwap_columns: dict[str, pd.Series] = {}
    anchor_meta: dict[str, dict] = {}

    for anchor_name, index in anchors.items():
        column_name = f"avwap_{timeframe}_{anchor_name}"
        avwap_columns[column_name] = compute_vwap(df, index)
        anchor_meta[column_name] = {
            "anchor_name": anchor_name,
            "start_idx": index,
            "start_date": df.loc[index, "date"],
            "start_price": float(df.loc[index, "close"]),
            "timeframe": timeframe,
        }

    output = df.copy()
    for column_name, values in avwap_columns.items():
        output[column_name] = values

    return output, anchor_meta
