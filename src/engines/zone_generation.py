from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from config.warmup_config import ZoneGenerationThresholdConfig, load_warmup_config
from data.market_data import (
    fetch_interval_history_for_dates,
    get_recent_trading_dates,
)
from engines.validation_engine import enrich_zones_with_strength, rank_zones_for_side
from features.boundaries import (
    assign_zone_display_labels,
    create_candidate_zones_from_swing_points,
)
from features.volume_profile import (
    build_composite_interval_volume_profile_zones,
    compute_atr,
    find_recent_swing_points,
    resample_to_weekly,
)


IntervalHistoryLoader = Callable[
    [str, list[pd.Timestamp], str | None, str],
    pd.DataFrame,
]


@dataclass(frozen=True, slots=True, init=False)
class ZoneGenerationConfig:
    long_vp_lookback_days: int
    long_vp_bins: int
    zone_expand_pct: float
    max_resistance_zones: int
    max_support_zones: int
    thresholds: ZoneGenerationThresholdConfig

    def __init__(
        self,
        *,
        long_vp_lookback_days: int | None = None,
        long_vp_bins: int | None = None,
        zone_expand_pct: float,
        max_resistance_zones: int,
        max_support_zones: int,
        thresholds: ZoneGenerationThresholdConfig | None = None,
    ) -> None:
        resolved_thresholds = thresholds or load_warmup_config().zone_generation
        object.__setattr__(self, "long_vp_lookback_days", int(long_vp_lookback_days or 63))
        object.__setattr__(self, "long_vp_bins", int(long_vp_bins or 48))
        object.__setattr__(self, "zone_expand_pct", float(zone_expand_pct))
        object.__setattr__(self, "max_resistance_zones", int(max_resistance_zones))
        object.__setattr__(self, "max_support_zones", int(max_support_zones))
        object.__setattr__(self, "thresholds", resolved_thresholds)

@dataclass(frozen=True, slots=True)
class GeneratedZoneSet:
    df_calc_daily_with_features: pd.DataFrame
    daily_anchor_meta: dict
    long_vp_profile_df: pd.DataFrame
    all_candidate_zones: list[dict]
    resistance_zones: list[dict]
    support_zones: list[dict]
    current_price: float
    atr20_series: pd.Series
    atr20_value: float
    weekly_swing_points: list[dict]
    observation_rows: list[dict]

def make_replay_zone_provider(
    *,
    symbol: str,
    provider: str | None,
    config: ZoneGenerationConfig,
    interval_history_loader: IntervalHistoryLoader | None = None,
    include_all_candidates: bool = False,
):
    def zone_provider(history: pd.DataFrame, _bar) -> list[dict]:
        if history.empty:
            zone_provider.latest_observations = []
            return []
        generated = generate_zones_for_replay(
            symbol=symbol,
            provider=provider,
            df_calc_daily=_ensure_date_column(history),
            config=config,
            interval_history_loader=interval_history_loader,
        )
        zone_provider.latest_observations = generated.observation_rows
        if include_all_candidates:
            return generated.all_candidate_zones
        return generated.support_zones + generated.resistance_zones

    zone_provider.latest_observations = []
    return zone_provider


def make_preloaded_zone_provider(
    *,
    symbol: str,
    provider: str | None,
    config: ZoneGenerationConfig,
    interval_frames: dict[str, pd.DataFrame],
    include_all_candidates: bool = False,
):
    """Create a replay provider that never performs network interval loads.

    The warmup path should preload all interval data once, then use this provider
    so each historical bar only slices local DataFrames.
    """
    return make_replay_zone_provider(
        symbol=symbol,
        provider=provider,
        config=config,
        interval_history_loader=make_preloaded_interval_history_loader(interval_frames),
        include_all_candidates=include_all_candidates,
    )


def make_preloaded_interval_history_loader(
    interval_frames: dict[str, pd.DataFrame],
) -> IntervalHistoryLoader:
    normalized_frames = {
        str(interval).strip().lower(): _prepare_preloaded_interval_frame(frame)
        for interval, frame in interval_frames.items()
    }

    def load_interval_history(
        symbol: str,
        trading_dates: list[pd.Timestamp],
        provider: str | None,
        interval: str,
    ) -> pd.DataFrame:
        del symbol, provider
        frame = normalized_frames.get(str(interval).strip().lower(), pd.DataFrame())
        if frame.empty or not trading_dates:
            return pd.DataFrame()

        target_dates = {pd.Timestamp(value).normalize() for value in trading_dates}
        row_dates = pd.to_datetime(frame["date"]).dt.normalize()
        return frame.loc[row_dates.isin(target_dates)].copy().reset_index(drop=True)

    return load_interval_history


def config_from_controls(controls) -> ZoneGenerationConfig:
    return ZoneGenerationConfig(
        long_vp_lookback_days=controls.long_vp_lookback_days,
        long_vp_bins=controls.long_vp_bins,
        zone_expand_pct=controls.zone_expand_pct,
        max_resistance_zones=getattr(controls, "max_resistance_zones", 999),
        max_support_zones=getattr(controls, "max_support_zones", 999),
    )


def generate_zones_for_replay(
    *,
    symbol: str,
    provider: str | None,
    df_calc_daily: pd.DataFrame,
    config: ZoneGenerationConfig,
    interval_history_loader: IntervalHistoryLoader | None = None,
) -> GeneratedZoneSet:
    interval_history_loader = interval_history_loader or _default_interval_history_loader
    current_price = float(df_calc_daily["close"].iloc[-1])
    atr20_series = compute_atr(df_calc_daily, period=config.thresholds.atr_period)
    atr20_value = (
        float(atr20_series.iloc[-1])
        if not atr20_series.empty and pd.notna(atr20_series.iloc[-1])
        else np.nan
    )

    df_calc_weekly = resample_to_weekly(df_calc_daily)
    weekly_swing_points = find_recent_swing_points(
        df_calc_weekly,
        timeframe="W",
        max_points_per_side=config.thresholds.weekly_swing_max_points_per_side,
        left_bars=config.thresholds.swing_left_bars,
        right_bars=config.thresholds.swing_right_bars,
        volume_lookback_bars=config.thresholds.swing_volume_lookback_bars,
        volume_quantile=config.thresholds.swing_volume_quantile,
    )
    long_vp_dates = get_recent_trading_dates(
        df_calc_daily,
        config.long_vp_lookback_days,
    )
    long_vp_profile_df = _load_window_volume_profile(
        symbol=symbol,
        provider=provider,
        df_calc_daily=df_calc_daily,
        trading_dates=long_vp_dates,
        window_name="long",
        lookback_days=config.long_vp_lookback_days,
        bins=config.long_vp_bins,
        interval_history_loader=interval_history_loader,
    )
    swing_zones = create_candidate_zones_from_swing_points(
        swing_points=weekly_swing_points,
        zone_expand_pct=config.zone_expand_pct,
        current_price=current_price,
        symbol=symbol,
    )

    all_candidate_zones = enrich_zones_with_strength(
        swing_zones,
        price_history=df_calc_daily,
        center_volume_pct=config.zone_expand_pct,
        strength_lookback_weeks=config.thresholds.strength_lookback_weeks,
    )
    resistance_zones = assign_zone_display_labels(
        rank_zones_for_side(
            zones=all_candidate_zones,
            current_price=current_price,
            side="resistance",
            max_zones=config.max_resistance_zones,
            price_history=df_calc_daily,
            center_volume_pct=config.zone_expand_pct,
            strength_lookback_weeks=config.thresholds.strength_lookback_weeks,
        ),
        prefix="R",
    )
    support_zones = assign_zone_display_labels(
        rank_zones_for_side(
            zones=all_candidate_zones,
            current_price=current_price,
            side="support",
            max_zones=config.max_support_zones,
            price_history=df_calc_daily,
            center_volume_pct=config.zone_expand_pct,
            strength_lookback_weeks=config.thresholds.strength_lookback_weeks,
        ),
        prefix="S",
    )
    observation_rows = _build_observation_rows(
        long_vp_profile_df=long_vp_profile_df,
    )

    return GeneratedZoneSet(
        df_calc_daily_with_features=df_calc_daily,
        daily_anchor_meta={},
        long_vp_profile_df=long_vp_profile_df,
        all_candidate_zones=all_candidate_zones,
        resistance_zones=resistance_zones,
        support_zones=support_zones,
        current_price=current_price,
        atr20_series=atr20_series,
        atr20_value=atr20_value,
        weekly_swing_points=weekly_swing_points,
        observation_rows=observation_rows,
    )


def _build_observation_rows(
    *,
    long_vp_profile_df: pd.DataFrame,
) -> list[dict]:
    rows: list[dict] = []
    profile_df = long_vp_profile_df
    if not profile_df.empty and "volume" in profile_df.columns:
        volume = pd.to_numeric(profile_df["volume"], errors="coerce")
        if volume.notna().any():
            poc_row = profile_df.loc[volume.idxmax()]
            rows.append(
                {
                    "observation_type": "vp_poc",
                    "label": "long_vp_poc",
                    "timeframe": "long",
                    "value": float(poc_row["bin_center"]),
                    "metadata": {
                        "source_mode": str(poc_row.get("source_mode", "")),
                        "source_bars": int(poc_row.get("source_bars", 0)),
                    },
                }
            )
    return rows


def _default_interval_history_loader(
    symbol: str,
    trading_dates: list[pd.Timestamp],
    provider: str | None,
    interval: str,
) -> pd.DataFrame:
    return fetch_interval_history_for_dates(
        symbol_value=symbol,
        trading_dates=trading_dates,
        provider_value=provider,
        interval_value=interval,
    )


def _load_window_volume_profile(
    *,
    symbol: str,
    provider: str | None,
    df_calc_daily: pd.DataFrame,
    trading_dates: list[pd.Timestamp],
    window_name: str,
    lookback_days: int,
    bins: int,
    interval_history_loader: IntervalHistoryLoader,
) -> pd.DataFrame:
    required = {"date", "open", "high", "low", "close", "volume"}
    normalized_window = str(window_name).strip().lower()
    if df_calc_daily.empty or not required.issubset(set(df_calc_daily.columns)) or not trading_dates:
        return pd.DataFrame()

    target_dates = {pd.Timestamp(value).normalize() for value in trading_dates}
    daily_dates = pd.to_datetime(df_calc_daily["date"], errors="coerce").dt.normalize()
    daily_source = df_calc_daily.loc[daily_dates.isin(target_dates)].copy()
    if daily_source.empty:
        return pd.DataFrame()

    sorted_target_dates = sorted(target_dates)

    source_df = daily_source
    source_mode = f"{normalized_window}_vp_1d"

    if sorted_target_dates and sorted_target_dates[-1] <= pd.Timestamp.today().normalize():
        interval_source = pd.DataFrame()
        try:
            interval_source = interval_history_loader(symbol, sorted_target_dates, provider, "5m")
        except Exception:
            interval_source = pd.DataFrame()

        if not interval_source.empty:
            interval_dates = pd.to_datetime(interval_source["date"], errors="coerce").dt.normalize()
            interval_date_set = {pd.Timestamp(value).normalize() for value in interval_dates.dropna().unique()}
            if target_dates.issubset(interval_date_set):
                source_df = interval_source
                source_mode = f"{normalized_window}_vp_5m"

    try:
        profile_df = build_composite_interval_volume_profile_zones(
            interval_df=source_df,
            bins=bins,
            timeframe=normalized_window,
            source_mode=source_mode,
        )
    except Exception as error:
        del error
        return pd.DataFrame()

    if profile_df.empty:
        return pd.DataFrame()

    return profile_df


def _ensure_date_column(frame: pd.DataFrame) -> pd.DataFrame:
    if "date" in frame.columns:
        return frame.copy()
    if "timestamp" not in frame.columns:
        return frame.copy()
    output = frame.copy()
    output["date"] = pd.to_datetime(output["timestamp"]).dt.tz_localize(None)
    return output


def _prepare_preloaded_interval_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    if "date" not in frame.columns:
        return pd.DataFrame()

    output = frame.copy()
    output["date"] = pd.to_datetime(output["date"], errors="coerce")
    output = output.dropna(subset=["date"]).copy()
    return output.sort_values("date", kind="stable").reset_index(drop=True)
