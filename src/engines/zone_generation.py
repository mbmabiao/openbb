from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from data.market_data import (
    fetch_interval_history_for_dates,
    get_recent_trading_dates,
    get_recent_trading_dates_for_weekly_window,
)
from engines.validation_engine import rank_zones_for_side
from features.boundaries import (
    assign_zone_display_labels,
    create_candidate_zones_from_avwap,
    create_candidate_zones_from_vp,
    merge_close_zones,
)
from features.volume_profile import (
    build_avwap_features,
    build_composite_interval_volume_profile_zones,
    compute_atr,
    resample_to_weekly,
)


IntervalHistoryLoader = Callable[
    [str, list[pd.Timestamp], str | None, str],
    pd.DataFrame,
]


@dataclass(frozen=True, slots=True)
class ZoneGenerationConfig:
    vp_lookback_days: int
    vp_bins: int
    weekly_vp_lookback: int
    weekly_vp_bins: int
    zone_expand_pct: float
    hv_node_quantile: float
    merge_pct: float
    max_resistance_zones: int
    max_support_zones: int
    reaction_lookahead: int
    reaction_return_threshold: float
    min_touch_gap: int


@dataclass(frozen=True, slots=True)
class VolumeProfileContext:
    mode: str
    note: str
    source_df: pd.DataFrame
    zones_raw: list[dict]
    profile_df: pd.DataFrame


@dataclass(frozen=True, slots=True)
class GeneratedZoneSet:
    df_calc_daily_with_features: pd.DataFrame
    df_calc_weekly_with_features: pd.DataFrame
    daily_anchor_meta: dict
    weekly_anchor_meta: dict
    daily_vp_context: VolumeProfileContext
    weekly_vp_context: VolumeProfileContext
    all_candidate_zones: list[dict]
    resistance_zones: list[dict]
    support_zones: list[dict]
    current_price: float
    atr20_series: pd.Series
    atr20_value: float


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
            return []
        generated = generate_zones_for_replay(
            symbol=symbol,
            provider=provider,
            df_calc_daily=_ensure_date_column(history),
            config=config,
            interval_history_loader=interval_history_loader,
        )
        if include_all_candidates:
            return generated.all_candidate_zones
        return generated.support_zones + generated.resistance_zones

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
        vp_lookback_days=controls.vp_lookback_days,
        vp_bins=controls.vp_bins,
        weekly_vp_lookback=controls.weekly_vp_lookback,
        weekly_vp_bins=controls.weekly_vp_bins,
        zone_expand_pct=controls.zone_expand_pct,
        hv_node_quantile=controls.hv_node_quantile,
        merge_pct=controls.merge_pct,
        max_resistance_zones=controls.max_resistance_zones,
        max_support_zones=controls.max_support_zones,
        reaction_lookahead=controls.reaction_lookahead,
        reaction_return_threshold=controls.reaction_return_threshold,
        min_touch_gap=controls.min_touch_gap,
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
    atr20_series = compute_atr(df_calc_daily, period=20)
    atr20_value = (
        float(atr20_series.iloc[-1])
        if not atr20_series.empty and pd.notna(atr20_series.iloc[-1])
        else np.nan
    )

    df_calc_daily_with_features, daily_anchor_meta = build_avwap_features(df_calc_daily, timeframe="D")
    daily_vp_dates = get_recent_trading_dates(df_calc_daily, config.vp_lookback_days)
    daily_vp_context = _load_interval_volume_profile_context(
        symbol=symbol,
        provider=provider,
        trading_dates=daily_vp_dates,
        interval="5m",
        bins=config.vp_bins,
        zone_expand_pct=config.zone_expand_pct,
        hv_node_quantile=config.hv_node_quantile,
        timeframe="D",
        source_label="VP (D, 5m composite)",
        source_mode="5m_composite",
        default_mode="5m composite",
        unavailable_mode="5m unavailable",
        source_error_note="5m history could not be loaded for the selected replay window, so daily VP was omitted.",
        empty_source_note="No 5m history was returned for the selected replay window, so daily VP was omitted.",
        empty_profile_note="5m history was returned, but no valid composite daily VP could be built, so daily VP was omitted.",
        build_error_note="5m composite daily VP construction failed for the selected replay window, so daily VP was omitted.",
        success_note_builder=lambda source_df: (
            f"Daily VP uses {len(daily_vp_dates)} trading days / {len(source_df)} bars of 5m OHLCV."
        ),
        interval_history_loader=interval_history_loader,
    )
    daily_vp_zones = create_candidate_zones_from_vp(
        df=df_calc_daily_with_features,
        vp_zones=daily_vp_context.zones_raw,
        symbol=symbol,
    )
    daily_avwap_zones = create_candidate_zones_from_avwap(
        df=df_calc_daily_with_features,
        anchor_meta=daily_anchor_meta,
        zone_expand_pct=config.zone_expand_pct,
        symbol=symbol,
    )

    df_calc_weekly = resample_to_weekly(df_calc_daily)
    df_calc_weekly_with_features, weekly_anchor_meta = build_avwap_features(df_calc_weekly, timeframe="W")
    weekly_vp_dates = get_recent_trading_dates_for_weekly_window(
        df_calc_daily,
        config.weekly_vp_lookback,
    )
    weekly_vp_context = _load_interval_volume_profile_context(
        symbol=symbol,
        provider=provider,
        trading_dates=weekly_vp_dates,
        interval="1d",
        bins=config.weekly_vp_bins,
        zone_expand_pct=config.zone_expand_pct,
        hv_node_quantile=config.hv_node_quantile,
        timeframe="W",
        source_label="VP (W, 1d higher-timeframe composite)",
        source_mode="1d_higher_timeframe_composite",
        default_mode="1d higher-timeframe composite",
        unavailable_mode="1d unavailable",
        source_error_note="1d higher-timeframe history could not be loaded for the selected replay window, so higher-timeframe VP was omitted.",
        empty_source_note="No 1d higher-timeframe history was returned for the selected replay window, so higher-timeframe VP was omitted.",
        empty_profile_note="1d higher-timeframe history was returned, but no valid composite VP could be built, so higher-timeframe VP was omitted.",
        build_error_note="1d higher-timeframe VP construction failed for the selected replay window, so higher-timeframe VP was omitted.",
        success_note_builder=lambda source_df: (
            f"Weekly VP uses {len(weekly_vp_dates)} trading days / {len(source_df)} bars of 1d OHLCV."
        ),
        interval_history_loader=interval_history_loader,
    )
    weekly_vp_zones = create_candidate_zones_from_vp(
        df=df_calc_weekly_with_features,
        vp_zones=weekly_vp_context.zones_raw,
        symbol=symbol,
    )
    weekly_avwap_zones = create_candidate_zones_from_avwap(
        df=df_calc_weekly_with_features,
        anchor_meta=weekly_anchor_meta,
        zone_expand_pct=config.zone_expand_pct,
        symbol=symbol,
    )

    all_candidate_zones = merge_close_zones(
        daily_vp_zones + daily_avwap_zones + weekly_vp_zones + weekly_avwap_zones,
        merge_pct=config.merge_pct,
        symbol=symbol,
    )
    resistance_zones = assign_zone_display_labels(
        rank_zones_for_side(
            zones=all_candidate_zones,
            vp_df_daily=daily_vp_context.profile_df,
            vp_df_weekly=weekly_vp_context.profile_df,
            current_price=current_price,
            side="resistance",
            max_zones=config.max_resistance_zones,
            df_reaction=df_calc_daily,
            lookahead=config.reaction_lookahead,
            reaction_threshold=config.reaction_return_threshold,
            min_gap=config.min_touch_gap,
        ),
        prefix="R",
    )
    support_zones = assign_zone_display_labels(
        rank_zones_for_side(
            zones=all_candidate_zones,
            vp_df_daily=daily_vp_context.profile_df,
            vp_df_weekly=weekly_vp_context.profile_df,
            current_price=current_price,
            side="support",
            max_zones=config.max_support_zones,
            df_reaction=df_calc_daily,
            lookahead=config.reaction_lookahead,
            reaction_threshold=config.reaction_return_threshold,
            min_gap=config.min_touch_gap,
        ),
        prefix="S",
    )

    return GeneratedZoneSet(
        df_calc_daily_with_features=df_calc_daily_with_features,
        df_calc_weekly_with_features=df_calc_weekly_with_features,
        daily_anchor_meta=daily_anchor_meta,
        weekly_anchor_meta=weekly_anchor_meta,
        daily_vp_context=daily_vp_context,
        weekly_vp_context=weekly_vp_context,
        all_candidate_zones=all_candidate_zones,
        resistance_zones=resistance_zones,
        support_zones=support_zones,
        current_price=current_price,
        atr20_series=atr20_series,
        atr20_value=atr20_value,
    )


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


def _load_interval_volume_profile_context(
    *,
    symbol: str,
    provider: str | None,
    trading_dates: list[pd.Timestamp],
    interval: str,
    bins: int,
    zone_expand_pct: float,
    hv_node_quantile: float,
    timeframe: str,
    source_label: str,
    source_mode: str,
    default_mode: str,
    unavailable_mode: str,
    source_error_note: str,
    empty_source_note: str,
    empty_profile_note: str,
    build_error_note: str,
    success_note_builder: Callable[[pd.DataFrame], str],
    interval_history_loader: IntervalHistoryLoader,
) -> VolumeProfileContext:
    source_df = pd.DataFrame()
    try:
        source_df = interval_history_loader(symbol, trading_dates, provider, interval)
    except Exception as error:
        return VolumeProfileContext(
            mode=unavailable_mode,
            note=f"{source_error_note} Details: {error}",
            source_df=pd.DataFrame(),
            zones_raw=[],
            profile_df=pd.DataFrame(),
        )

    if source_df.empty:
        return VolumeProfileContext(
            mode=unavailable_mode,
            note=empty_source_note,
            source_df=source_df,
            zones_raw=[],
            profile_df=pd.DataFrame(),
        )

    try:
        zones_raw, profile_df = build_composite_interval_volume_profile_zones(
            interval_df=source_df,
            bins=bins,
            zone_expand=zone_expand_pct,
            hv_quantile=hv_node_quantile,
            timeframe=timeframe,
            source_label=source_label,
            source_mode=source_mode,
        )
    except Exception as error:
        return VolumeProfileContext(
            mode=unavailable_mode,
            note=f"{build_error_note} Details: {error}",
            source_df=source_df,
            zones_raw=[],
            profile_df=pd.DataFrame(),
        )

    if profile_df.empty:
        return VolumeProfileContext(
            mode=unavailable_mode,
            note=empty_profile_note,
            source_df=source_df,
            zones_raw=[],
            profile_df=pd.DataFrame(),
        )

    return VolumeProfileContext(
        mode=default_mode,
        note=success_note_builder(source_df),
        source_df=source_df,
        zones_raw=zones_raw,
        profile_df=profile_df,
    )


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
