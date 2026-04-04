from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd


@dataclass(slots=True)
class ZoneEngineConfig:
    daily_vp_lookback_days: int = 30
    daily_vp_bins: int = 48
    weekly_vp_lookback_weeks: int = 26
    weekly_vp_bins: int = 24
    zone_expand_bp: int = 50
    hv_node_quantile_pct: int = 75
    merge_pct_bp: int = 60
    max_resistance_zones: int = 3
    max_support_zones: int = 3
    reaction_lookahead_bars: int = 5
    reaction_threshold_bp: int = 150
    min_touch_gap: int = 3
    zone_refresh_every_n_bars: int = 5
    min_history_bars: int = 90

    @property
    def zone_expand_pct(self) -> float:
        return self.zone_expand_bp / 10000.0

    @property
    def hv_node_quantile(self) -> float:
        return self.hv_node_quantile_pct / 100.0

    @property
    def merge_pct(self) -> float:
        return self.merge_pct_bp / 10000.0

    @property
    def reaction_return_threshold(self) -> float:
        return self.reaction_threshold_bp / 10000.0

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "ZoneEngineConfig":
        if not payload:
            return cls()
        valid_fields = {field.name for field in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in payload.items() if k in valid_fields}
        return cls(**filtered)


def to_dataframe(result):
    if result is None:
        return None
    if hasattr(result, "to_dataframe"):
        return result.to_dataframe()
    if hasattr(result, "to_df"):
        return result.to_df()
    if isinstance(result, pd.DataFrame):
        return result
    try:
        return pd.DataFrame(result)
    except Exception:
        return None


def normalise_ohlcv_columns(df: pd.DataFrame, date_col_name: str = "date") -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if isinstance(out.index, pd.DatetimeIndex):
        index_name = out.index.name if out.index.name is not None else date_col_name
        if index_name in out.columns:
            index_name = f"__index_{date_col_name}__"
        out = out.reset_index(names=index_name)
    else:
        out = out.reset_index(drop=False)
        if date_col_name in out.columns and "index" in out.columns:
            out = out.drop(columns=["index"])

    rename_map = {}
    for col in out.columns:
        lower = str(col).lower().strip()
        if lower in ("date", "datetime", "timestamp", "time", f"__index_{date_col_name}__"):
            rename_map[col] = date_col_name
        elif lower in ("open", "adj_open"):
            rename_map[col] = "open"
        elif lower in ("high", "adj_high"):
            rename_map[col] = "high"
        elif lower in ("low", "adj_low"):
            rename_map[col] = "low"
        elif lower in ("close", "adj_close", "price"):
            rename_map[col] = "close"
        elif lower in ("volume", "vol"):
            rename_map[col] = "volume"

    out = out.rename(columns=rename_map)

    if date_col_name not in out.columns and len(out.columns) > 0:
        first_col = out.columns[0]
        trial = pd.to_datetime(out[first_col], errors="coerce")
        if trial.notna().sum() > 0:
            out[date_col_name] = trial

    if date_col_name in out.columns:
        out[date_col_name] = pd.to_datetime(out[date_col_name], errors="coerce")
        try:
            if getattr(out[date_col_name].dt, "tz", None) is not None:
                out[date_col_name] = out[date_col_name].dt.tz_localize(None)
        except (AttributeError, TypeError):
            pass

    out = out.reset_index(drop=True)
    if date_col_name in out.columns:
        out = out.dropna(subset=[date_col_name])
        out = out.sort_values(by=date_col_name, kind="stable").reset_index(drop=True)

    return out


def get_recent_trading_dates(df: pd.DataFrame, lookback_days: int, date_col: str = "date") -> list[pd.Timestamp]:
    if df.empty:
        return []
    trading_dates = (
        pd.to_datetime(df[date_col]).dt.normalize().drop_duplicates().sort_values().tolist()
    )
    return [pd.Timestamp(d).normalize() for d in trading_dates[-lookback_days:]]


def get_recent_trading_dates_for_weekly_window(
    df: pd.DataFrame,
    weekly_lookback_bars: int,
    date_col: str = "date",
) -> list[pd.Timestamp]:
    if df.empty or weekly_lookback_bars < 1:
        return []
    daily_dates = pd.to_datetime(df[date_col]).dt.normalize()
    weekly_periods = daily_dates.dt.to_period("W-FRI")
    period_df = pd.DataFrame({"date": daily_dates, "week_period": weekly_periods})
    period_df = period_df.drop_duplicates(subset=["date"]).sort_values("date", kind="stable")
    recent_periods = period_df["week_period"].drop_duplicates().tolist()[-weekly_lookback_bars:]
    if not recent_periods:
        return []
    selected = period_df.loc[period_df["week_period"].isin(set(recent_periods)), "date"].tolist()
    return [pd.Timestamp(d).normalize() for d in selected]


def filter_frame_by_dates(df: pd.DataFrame, trading_dates: list[pd.Timestamp], date_col: str = "date") -> pd.DataFrame:
    if df.empty or not trading_dates:
        return df.iloc[0:0].copy()
    target_dates = {pd.Timestamp(d).normalize() for d in trading_dates}
    normalized = pd.to_datetime(df[date_col]).dt.normalize()
    return df.loc[normalized.isin(target_dates)].copy().reset_index(drop=True)


def resample_to_weekly(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    if df.empty:
        return df.copy()
    x = df.copy().set_index(date_col).sort_index()
    weekly = pd.DataFrame(
        {
            "date": x["open"].resample("W-FRI").first().index,
            "open": x["open"].resample("W-FRI").first().values,
            "high": x["high"].resample("W-FRI").max().values,
            "low": x["low"].resample("W-FRI").min().values,
            "close": x["close"].resample("W-FRI").last().values,
            "volume": x["volume"].resample("W-FRI").sum().values,
        }
    )
    return weekly.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)


def compute_vwap(df: pd.DataFrame, start_idx: int) -> pd.Series:
    sub = df.iloc[start_idx:].copy()
    typical_price = (sub["high"] + sub["low"] + sub["close"]) / 3.0
    vol = sub["volume"].fillna(0.0)
    cum_pv = (typical_price * vol).cumsum()
    cum_v = vol.cumsum().replace(0, np.nan)
    avwap = cum_pv / cum_v
    full = pd.Series(index=df.index, dtype=float)
    full.loc[sub.index] = avwap.values
    return full


def find_anchor_points(df: pd.DataFrame, recent_window_cap: int = 126) -> dict[str, int]:
    out: dict[str, int] = {}
    n = len(df)
    if n < 30:
        return out
    recent_window = min(recent_window_cap, n)
    recent_slice = df.iloc[-recent_window:]
    out["major_high"] = int(recent_slice["high"].idxmax())
    out["major_low"] = int(recent_slice["low"].idxmin())

    prev_close = df["close"].shift(1)
    gap_pct = (df["open"] - prev_close) / prev_close.replace(0, np.nan)
    gap_slice = gap_pct.iloc[-recent_window:]
    if gap_slice.notna().sum() > 0:
        out["gap_down"] = int(gap_slice.idxmin())
        out["gap_up"] = int(gap_slice.idxmax())

    body_return = (df["close"] - df["open"]) / df["open"].replace(0, np.nan)
    body_slice = body_return.iloc[-recent_window:]
    if body_slice.notna().sum() > 0:
        out["big_down"] = int(body_slice.idxmin())
        out["big_up"] = int(body_slice.idxmax())

    cleaned: dict[str, int] = {}
    seen = set()
    for key, value in out.items():
        if value is not None and value not in seen and 0 <= value < len(df) - 1:
            cleaned[key] = value
            seen.add(value)
    return cleaned


def build_avwap_features(df: pd.DataFrame, timeframe: str) -> tuple[pd.DataFrame, dict]:
    recent_window_cap = 126 if timeframe == "D" else 52
    anchors = find_anchor_points(df, recent_window_cap=recent_window_cap)
    avwap_cols = {}
    anchor_meta = {}
    for anchor_name, idx in anchors.items():
        col_name = f"avwap_{timeframe}_{anchor_name}"
        avwap_cols[col_name] = compute_vwap(df, idx)
        anchor_meta[col_name] = {
            "anchor_name": anchor_name,
            "start_idx": idx,
            "start_date": df.loc[idx, "date"],
            "start_price": float(df.loc[idx, "close"]),
            "timeframe": timeframe,
        }
    out = df.copy()
    for key, series in avwap_cols.items():
        out[key] = series
    return out, anchor_meta


def build_vp_zones_from_profile(
    vp_df: pd.DataFrame,
    zone_expand: float,
    hv_quantile: float,
    timeframe: str,
    source_label: str,
) -> tuple[list[dict], pd.DataFrame]:
    if vp_df.empty:
        return [], vp_df
    if not (pd.to_numeric(vp_df["volume"], errors="coerce").fillna(0.0) > 0).any():
        return [], vp_df
    threshold = vp_df["volume"].quantile(hv_quantile)
    hv_nodes = vp_df[vp_df["volume"] >= threshold].copy()
    if hv_nodes.empty:
        return [], vp_df
    hv_nodes = hv_nodes.sort_values("bin_center").reset_index(drop=True)
    zones = []
    current_left = float(hv_nodes.loc[0, "bin_left"])
    current_right = float(hv_nodes.loc[0, "bin_right"])
    current_vol = float(hv_nodes.loc[0, "volume"])

    for i in range(1, len(hv_nodes)):
        left = float(hv_nodes.loc[i, "bin_left"])
        right = float(hv_nodes.loc[i, "bin_right"])
        vol = float(hv_nodes.loc[i, "volume"])
        width_ref = max(current_right - current_left, 1e-9)
        close_enough = (left - current_right) <= (width_ref * 0.8)
        if close_enough:
            current_right = right
            current_vol += vol
        else:
            center = (current_left + current_right) / 2.0
            expand = center * zone_expand
            zones.append(
                {
                    "type": f"vp_zone_{timeframe}",
                    "lower": current_left - expand,
                    "upper": current_right + expand,
                    "center": center,
                    "vp_volume": current_vol,
                    "timeframes": {timeframe},
                    "source_types": {f"vp_{timeframe}"},
                    "primary_timeframe": timeframe,
                    "source_label": source_label,
                }
            )
            current_left = left
            current_right = right
            current_vol = vol

    center = (current_left + current_right) / 2.0
    expand = center * zone_expand
    zones.append(
        {
            "type": f"vp_zone_{timeframe}",
            "lower": current_left - expand,
            "upper": current_right + expand,
            "center": center,
            "vp_volume": current_vol,
            "timeframes": {timeframe},
            "source_types": {f"vp_{timeframe}"},
            "primary_timeframe": timeframe,
            "source_label": source_label,
        }
    )
    return zones, vp_df


def build_composite_interval_volume_profile_zones(
    interval_df: pd.DataFrame,
    bins: int,
    zone_expand: float,
    hv_quantile: float,
    timeframe: str,
    source_label: str,
    source_mode: str,
) -> tuple[list[dict], pd.DataFrame]:
    if interval_df.empty:
        return [], pd.DataFrame()
    sub = interval_df.copy()
    low_min = float(sub["low"].min())
    high_max = float(sub["high"].max())
    if not np.isfinite(low_min) or not np.isfinite(high_max):
        return [], pd.DataFrame()
    if high_max <= low_min:
        high_max = low_min * (1.0 + 1e-6) if low_min != 0 else 1e-6
    bin_edges = np.linspace(low_min, high_max, bins + 1)
    bin_left = bin_edges[:-1]
    bin_right = bin_edges[1:]
    bin_centers = (bin_left + bin_right) / 2.0
    vol_bins = np.zeros(bins, dtype=float)

    for row in sub.itertuples(index=False):
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
            idx = int(np.searchsorted(bin_edges, low, side="right") - 1)
            idx = int(np.clip(idx, 0, bins - 1))
            vol_bins[idx] += volume
            continue
        overlap_left = np.maximum(bin_left, low)
        overlap_right = np.minimum(bin_right, high)
        overlaps = np.maximum(overlap_right - overlap_left, 0.0)
        total_overlap = float(overlaps.sum())
        if total_overlap <= 0:
            idx = int(np.searchsorted(bin_edges, low, side="right") - 1)
            idx = int(np.clip(idx, 0, bins - 1))
            vol_bins[idx] += volume
            continue
        vol_bins += volume * (overlaps / total_overlap)

    vp_df = pd.DataFrame(
        {
            "bin_left": bin_left,
            "bin_right": bin_right,
            "bin_center": bin_centers,
            "volume": vol_bins,
            "timeframe": timeframe,
            "source_bars": len(sub),
            "source_mode": source_mode,
        }
    )
    return build_vp_zones_from_profile(vp_df, zone_expand, hv_quantile, timeframe, source_label)


def create_candidate_zones_from_vp(df: pd.DataFrame, vp_zones: list[dict]) -> list[dict]:
    if df.empty:
        return []
    current_price = float(df["close"].iloc[-1])
    out = []
    for zone in vp_zones:
        zone_copy = zone.copy()
        zone_copy["anchor_count"] = 0
        zone_copy["avwap_strength"] = 0.0
        zone_copy["side"] = "resistance" if zone["center"] >= current_price else "support"
        out.append(zone_copy)
    return out


def create_candidate_zones_from_avwap(df: pd.DataFrame, anchor_meta: dict, zone_expand_pct: float) -> list[dict]:
    if df.empty:
        return []
    current_price = float(df["close"].iloc[-1])
    zones = []
    for col, meta in anchor_meta.items():
        latest_val = df[col].dropna()
        if latest_val.empty:
            continue
        center = float(latest_val.iloc[-1])
        expand = center * zone_expand_pct
        timeframe = meta["timeframe"]
        if center >= current_price:
            zone_side = "resistance"
            avwap_strength = max((center - current_price) / max(current_price, 1e-9), 0.0) + 0.5
        else:
            zone_side = "support"
            avwap_strength = max((current_price - center) / max(current_price, 1e-9), 0.0) + 0.5
        zones.append(
            {
                "type": f"avwap_{zone_side}_{timeframe}",
                "side": zone_side,
                "lower": center - expand,
                "upper": center + expand,
                "center": center,
                "vp_volume": 0.0,
                "anchor_count": 1,
                "avwap_strength": avwap_strength,
                "anchor_name": meta["anchor_name"],
                "anchor_start_date": meta["start_date"],
                "timeframes": {timeframe},
                "source_types": {f"avwap_{timeframe}"},
                "primary_timeframe": timeframe,
                "source_label": f"AVWAP ({timeframe})",
            }
        )
    return zones


def merge_close_zones(zones: list[dict], merge_pct: float) -> list[dict]:
    if not zones:
        return []
    zones_sorted = sorted(zones, key=lambda z: (z["side"], z["center"]))
    merged = [zones_sorted[0].copy()]
    for zone in zones_sorted[1:]:
        last = merged[-1]
        if zone["side"] != last["side"]:
            merged.append(zone.copy())
            continue
        overlap = not (zone["lower"] > last["upper"] or zone["upper"] < last["lower"])
        close_center = abs(zone["center"] - last["center"]) / max(last["center"], 1e-9) <= merge_pct
        if overlap or close_center:
            new_lower = min(last["lower"], zone["lower"])
            new_upper = max(last["upper"], zone["upper"])
            timeframes = set(last.get("timeframes", set())) | set(zone.get("timeframes", set()))
            source_types = set(last.get("source_types", set())) | set(zone.get("source_types", set()))
            merged[-1] = {
                "type": f"merged_{last['side']}",
                "side": last["side"],
                "lower": float(new_lower),
                "upper": float(new_upper),
                "center": float((new_lower + new_upper) / 2.0),
                "vp_volume": float(last.get("vp_volume", 0.0) + zone.get("vp_volume", 0.0)),
                "anchor_count": int(last.get("anchor_count", 0) + zone.get("anchor_count", 0)),
                "avwap_strength": float(last.get("avwap_strength", 0.0) + zone.get("avwap_strength", 0.0)),
                "timeframes": timeframes,
                "source_types": source_types,
                "primary_timeframe": "W" if "W" in timeframes else "D",
                "source_label": ", ".join(sorted(source_types)),
            }
        else:
            merged.append(zone.copy())
    return merged


def compute_inventory_zone_score(zone: dict, current_price: float, vp_df_daily: pd.DataFrame, vp_df_weekly: pd.DataFrame) -> float:
    zone_vol = float(zone.get("vp_volume", 0.0))
    max_daily = max(float(vp_df_daily["volume"].max()), 1e-9) if not vp_df_daily.empty else 1.0
    max_weekly = max(float(vp_df_weekly["volume"].max()), 1e-9) if not vp_df_weekly.empty else 1.0
    max_vol = max(max_daily, max_weekly, 1.0)
    vol_score = zone_vol / max_vol
    distance_pct = abs(zone["center"] - current_price) / max(current_price, 1e-9)
    proximity_score = 1.0 / max(distance_pct, 0.01)
    weekly_bonus = 0.25 if "W" in zone.get("timeframes", set()) else 0.0
    multi_tf_bonus = 0.35 if len(zone.get("timeframes", set())) >= 2 else 0.0
    return 0.5 * vol_score + 0.3 * proximity_score + weekly_bonus + multi_tf_bonus


def validate_zone_reaction(
    df: pd.DataFrame,
    zone: dict,
    lookahead: int,
    return_threshold: float,
    min_gap: int,
) -> dict:
    if df.empty or lookahead < 1:
        return {
            "touch_count": 0,
            "first_touch_score": 0.0,
            "strong_reaction_rate": 0.0,
            "reclaim_rate": 0.0,
            "reaction_score": 0.0,
            "last_reaction_date": pd.NaT,
        }
    lower = float(zone["lower"])
    upper = float(zone["upper"])
    center = float(zone["center"])
    side = zone["side"]
    touched_idx = []
    last_touch = -10_000
    for i in range(len(df) - lookahead):
        row = df.iloc[i]
        touched = float(row["low"]) <= upper and float(row["high"]) >= lower
        if touched and (i - last_touch) >= min_gap:
            touched_idx.append(i)
            last_touch = i
    if not touched_idx:
        return {
            "touch_count": 0,
            "first_touch_score": 0.0,
            "strong_reaction_rate": 0.0,
            "reclaim_rate": 0.0,
            "reaction_score": 0.0,
            "last_reaction_date": pd.NaT,
        }
    strong_reactions = 0
    reclaims = 0
    reactions = []
    for i in touched_idx:
        row = df.iloc[i]
        base_close = float(row["close"])
        future = df.iloc[i + 1:i + 1 + lookahead].copy()
        if future.empty:
            continue
        if side == "support":
            best_forward = float(future["high"].max())
            forward_ret = (best_forward - base_close) / max(base_close, 1e-9)
            pierced = float(row["low"]) < lower
            reclaimed = (pierced and float(row["close"]) >= lower) or bool((future["close"] >= lower).any())
            strong = forward_ret >= return_threshold or reclaimed or (
                float(row["close"]) > center and (float(row["close"]) - float(row["open"])) > 0
            )
        else:
            best_forward = float(future["low"].min())
            forward_ret = (base_close - best_forward) / max(base_close, 1e-9)
            pierced = float(row["high"]) > upper
            reclaimed = (pierced and float(row["close"]) <= upper) or bool((future["close"] <= upper).any())
            strong = forward_ret >= return_threshold or reclaimed or (
                float(row["close"]) < center and (float(row["open"]) - float(row["close"])) > 0
            )
        if strong:
            strong_reactions += 1
        if reclaimed:
            reclaims += 1
        touch_quality = 0.0
        if strong:
            touch_quality += 1.0
        if reclaimed:
            touch_quality += 0.75
        reactions.append(touch_quality)
    effective_touches = max(len(reactions), 1)
    strong_reaction_rate = strong_reactions / effective_touches
    reclaim_rate = reclaims / effective_touches
    first_touch_score = reactions[0] if reactions else 0.0
    touch_count = len(touched_idx)
    repeated_test_decay = max(touch_count - 2, 0) * 0.12
    reaction_score = 1.2 * first_touch_score + 1.0 * strong_reaction_rate + 0.8 * reclaim_rate - repeated_test_decay
    return {
        "touch_count": touch_count,
        "first_touch_score": float(first_touch_score),
        "strong_reaction_rate": float(strong_reaction_rate),
        "reclaim_rate": float(reclaim_rate),
        "reaction_score": float(reaction_score),
        "last_reaction_date": df.iloc[touched_idx[-1]]["date"],
    }


def rank_zones_for_side(
    zones: list[dict],
    vp_df_daily: pd.DataFrame,
    vp_df_weekly: pd.DataFrame,
    current_price: float,
    side: str,
    max_zones: int,
    df_reaction: pd.DataFrame,
    lookahead: int,
    reaction_threshold: float,
    min_gap: int,
) -> list[dict]:
    ranked = []
    max_vp_daily = max(float(vp_df_daily["volume"].max()), 1e-9) if not vp_df_daily.empty else 1.0
    max_vp_weekly = max(float(vp_df_weekly["volume"].max()), 1e-9) if not vp_df_weekly.empty else 1.0
    max_vp = max(max_vp_daily, max_vp_weekly, 1.0)
    for zone in zones:
        if zone.get("side") != side:
            continue
        if side == "resistance" and zone["upper"] < current_price:
            continue
        if side == "support" and zone["lower"] > current_price:
            continue
        reaction = validate_zone_reaction(df_reaction, zone, lookahead, reaction_threshold, min_gap)
        distance_pct = abs(zone["center"] - current_price) / max(current_price, 1e-9)
        width_pct = (zone["upper"] - zone["lower"]) / max(zone["center"], 1e-9)
        vp_strength = float(zone.get("vp_volume", 0.0)) / max_vp
        inventory_score = compute_inventory_zone_score(zone, current_price, vp_df_daily, vp_df_weekly)
        avwap_strength = float(zone.get("avwap_strength", 0.0))
        anchor_count = int(zone.get("anchor_count", 0))
        proximity_score = 1.0 / max(distance_pct, 0.01)
        width_penalty = width_pct * 20.0
        timeframes = zone.get("timeframes", set())
        weekly_bonus = 1.0 if "W" in timeframes else 0.0
        multi_tf_bonus = 1.2 if len(timeframes) >= 2 else 0.0
        confluence_count = len(set(zone.get("source_types", set())))
        structural_score = (
            2.4 * vp_strength
            + 1.9 * inventory_score
            + 1.3 * avwap_strength
            + 0.7 * anchor_count
            + 0.9 * weekly_bonus
            + 1.1 * multi_tf_bonus
            + 0.3 * confluence_count
            + 1.0 * proximity_score
            - width_penalty
        )
        institutional_score = structural_score + 2.0 * reaction["reaction_score"]
        zone_copy = zone.copy()
        zone_copy["distance_pct"] = distance_pct
        zone_copy["width_pct"] = width_pct
        zone_copy["vp_strength"] = vp_strength
        zone_copy["inventory_score"] = inventory_score
        zone_copy["weekly_bonus"] = weekly_bonus
        zone_copy["multi_tf_bonus"] = multi_tf_bonus
        zone_copy["confluence_count"] = confluence_count
        zone_copy["timeframe_sources"] = ",".join(sorted(timeframes))
        zone_copy["source_types_label"] = ",".join(sorted(zone.get("source_types", set())))
        zone_copy.update(reaction)
        zone_copy["structural_score"] = structural_score
        zone_copy["institutional_score"] = institutional_score
        ranked.append(zone_copy)
    ranked = sorted(
        ranked,
        key=lambda x: (
            x["institutional_score"],
            x["reaction_score"],
            x["multi_tf_bonus"],
            x["vp_strength"],
            -x["distance_pct"],
        ),
        reverse=True,
    )
    return sorted(ranked[:max_zones], key=lambda x: x["center"])


def build_zone_rows_from_snapshot(
    ticker: str,
    valid_from: pd.Timestamp,
    valid_to: pd.Timestamp | None,
    selected_zones: list[dict],
) -> list[dict]:
    rows = []
    for i, zone in enumerate(selected_zones, start=1):
        rows.append(
            {
                "zone_id": f"{ticker}::{pd.Timestamp(valid_from).strftime('%Y%m%d')}::{zone['side']}::{i}",
                "ticker": ticker,
                "valid_from": pd.Timestamp(valid_from),
                "valid_to": valid_to,
                "zone_class": _zone_class_from_source_types(zone.get("source_types", set())),
                "side": zone["side"],
                "lower": float(zone["lower"]),
                "upper": float(zone["upper"]),
                "center": float(zone["center"]),
                "timeframe": zone.get("timeframe_sources") or ",".join(sorted(zone.get("timeframes", set()))),
                "source_reason": zone.get("source_types_label") or zone.get("source_label", ""),
                "confluence_count": max(len(set(zone.get("source_types", set()))), 1),
                "metadata": {
                    "institutional_score": float(zone.get("institutional_score", 0.0)),
                    "structural_score": float(zone.get("structural_score", 0.0)),
                    "touch_count": int(zone.get("touch_count", 0)),
                    "reaction_score": float(zone.get("reaction_score", 0.0)),
                    "source_label": zone.get("source_label", ""),
                    "source_types": sorted(zone.get("source_types", set())),
                    "timeframes": sorted(zone.get("timeframes", set())),
                },
            }
        )
    return rows


def _zone_class_from_source_types(source_types: set[str]) -> str:
    if not source_types:
        return "composite"
    normalized = set(source_types)
    if len(normalized) > 1:
        return "composite"
    source = next(iter(normalized))
    if source.startswith("vp_"):
        return "inventory"
    if source.startswith("avwap_"):
        return "cost"
    return "structural"
