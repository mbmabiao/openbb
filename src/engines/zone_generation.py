from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from data.market_data import (
    fetch_interval_history_for_dates,
    get_recent_trading_dates,
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


@dataclass(frozen=True, slots=True, init=False)
class ZoneGenerationConfig:
    short_vp_lookback_days: int
    short_vp_bins: int
    long_vp_lookback_days: int
    long_vp_bins: int
    zone_expand_pct: float
    hv_node_quantile: float
    merge_pct: float
    max_resistance_zones: int
    max_support_zones: int
    reaction_lookahead: int
    reaction_return_threshold: float
    min_touch_gap: int

    def __init__(
        self,
        *,
        short_vp_lookback_days: int | None = None,
        short_vp_bins: int | None = None,
        long_vp_lookback_days: int | None = None,
        long_vp_bins: int | None = None,
        vp_lookback_days: int | None = None,
        vp_bins: int | None = None,
        weekly_vp_lookback: int | None = None,
        weekly_vp_bins: int | None = None,
        zone_expand_pct: float,
        hv_node_quantile: float,
        merge_pct: float,
        max_resistance_zones: int,
        max_support_zones: int,
        reaction_lookahead: int,
        reaction_return_threshold: float,
        min_touch_gap: int,
    ) -> None:
        object.__setattr__(self, "short_vp_lookback_days", int(short_vp_lookback_days or vp_lookback_days or 21))
        object.__setattr__(self, "short_vp_bins", int(short_vp_bins or vp_bins or 48))
        object.__setattr__(self, "long_vp_lookback_days", int(long_vp_lookback_days or weekly_vp_lookback or vp_lookback_days or 63))
        object.__setattr__(self, "long_vp_bins", int(long_vp_bins or weekly_vp_bins or vp_bins or self.short_vp_bins))
        object.__setattr__(self, "zone_expand_pct", float(zone_expand_pct))
        object.__setattr__(self, "hv_node_quantile", float(hv_node_quantile))
        object.__setattr__(self, "merge_pct", float(merge_pct))
        object.__setattr__(self, "max_resistance_zones", int(max_resistance_zones))
        object.__setattr__(self, "max_support_zones", int(max_support_zones))
        object.__setattr__(self, "reaction_lookahead", int(reaction_lookahead))
        object.__setattr__(self, "reaction_return_threshold", float(reaction_return_threshold))
        object.__setattr__(self, "min_touch_gap", int(min_touch_gap))

    @property
    def vp_lookback_days(self) -> int:
        return self.long_vp_lookback_days

    @property
    def vp_bins(self) -> int:
        return self.long_vp_bins

    @property
    def weekly_vp_lookback(self) -> int:
        return self.long_vp_lookback_days

    @property
    def weekly_vp_bins(self) -> int:
        return self.long_vp_bins


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
    short_vp_context: VolumeProfileContext
    long_vp_context: VolumeProfileContext
    all_candidate_zones: list[dict]
    resistance_zones: list[dict]
    support_zones: list[dict]
    current_price: float
    atr20_series: pd.Series
    atr20_value: float

    @property
    def daily_vp_context(self) -> VolumeProfileContext:
        return self.long_vp_context

    @property
    def weekly_vp_context(self) -> VolumeProfileContext:
        return self.long_vp_context


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
        short_vp_lookback_days=_control_value(controls, "short_vp_lookback_days", "vp_lookback_days"),
        short_vp_bins=_control_value(controls, "short_vp_bins", "vp_bins"),
        long_vp_lookback_days=_control_value(controls, "long_vp_lookback_days", "weekly_vp_lookback"),
        long_vp_bins=_control_value(controls, "long_vp_bins", "weekly_vp_bins"),
        zone_expand_pct=controls.zone_expand_pct,
        hv_node_quantile=controls.hv_node_quantile,
        merge_pct=controls.merge_pct,
        max_resistance_zones=controls.max_resistance_zones,
        max_support_zones=controls.max_support_zones,
        reaction_lookahead=controls.reaction_lookahead,
        reaction_return_threshold=controls.reaction_return_threshold,
        min_touch_gap=controls.min_touch_gap,
    )


def _control_value(controls, preferred_name: str, legacy_name: str):
    if hasattr(controls, preferred_name):
        return getattr(controls, preferred_name)
    return getattr(controls, legacy_name)


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

    df_calc_daily_with_features, daily_anchor_meta = build_avwap_features(
        df_calc_daily,
        timeframe="D",
        rolling_window_bars=(
            (config.long_vp_lookback_days, "long"),
        ),
    )
    short_vp_context = _disabled_volume_profile_context("short")
    long_vp_dates = get_recent_trading_dates(
        df_calc_daily,
        config.long_vp_lookback_days,
    )
    long_vp_context = _load_window_volume_profile_context(
        symbol=symbol,
        provider=provider,
        df_calc_daily=df_calc_daily,
        trading_dates=long_vp_dates,
        window_name="long",
        lookback_days=config.long_vp_lookback_days,
        bins=config.long_vp_bins,
        zone_expand_pct=config.zone_expand_pct,
        hv_node_quantile=config.hv_node_quantile,
        interval_history_loader=interval_history_loader,
    )
    long_vp_zones = create_candidate_zones_from_vp(
        df=df_calc_daily_with_features,
        vp_zones=long_vp_context.zones_raw,
        symbol=symbol,
    )
    daily_avwap_zones = create_candidate_zones_from_avwap(
        df=df_calc_daily_with_features,
        anchor_meta=daily_anchor_meta,
        zone_expand_pct=config.zone_expand_pct,
        symbol=symbol,
    )

    df_calc_weekly = resample_to_weekly(df_calc_daily)
    df_calc_weekly_with_features, weekly_anchor_meta = build_avwap_features(
        df_calc_weekly,
        timeframe="W",
        rolling_window_bars=(),
        swing_search_bars=52,
        event_search_bars=52,
    )
    weekly_avwap_zones = create_candidate_zones_from_avwap(
        df=df_calc_weekly_with_features,
        anchor_meta=weekly_anchor_meta,
        zone_expand_pct=config.zone_expand_pct,
        symbol=symbol,
    )

    all_candidate_zones = merge_close_zones(
        long_vp_zones + daily_avwap_zones + weekly_avwap_zones,
        merge_pct=config.merge_pct,
        symbol=symbol,
    )
    resistance_zones = assign_zone_display_labels(
        rank_zones_for_side(
            zones=all_candidate_zones,
            current_price=current_price,
            side="resistance",
            max_zones=config.max_resistance_zones,
            price_history=df_calc_daily,
            center_volume_pct=config.zone_expand_pct,
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
        ),
        prefix="S",
    )

    return GeneratedZoneSet(
        df_calc_daily_with_features=df_calc_daily_with_features,
        df_calc_weekly_with_features=df_calc_weekly_with_features,
        daily_anchor_meta=daily_anchor_meta,
        weekly_anchor_meta=weekly_anchor_meta,
        short_vp_context=short_vp_context,
        long_vp_context=long_vp_context,
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


def _load_window_volume_profile_context(
    *,
    symbol: str,
    provider: str | None,
    df_calc_daily: pd.DataFrame,
    trading_dates: list[pd.Timestamp],
    window_name: str,
    lookback_days: int,
    bins: int,
    zone_expand_pct: float,
    hv_node_quantile: float,
    interval_history_loader: IntervalHistoryLoader,
) -> VolumeProfileContext:
    required = {"date", "open", "high", "low", "close", "volume"}
    normalized_window = str(window_name).strip().lower()
    window_label = f"{normalized_window} {int(lookback_days)} trading days"
    unavailable_mode = f"{normalized_window} unavailable"
    if df_calc_daily.empty or not required.issubset(set(df_calc_daily.columns)) or not trading_dates:
        return VolumeProfileContext(
            mode=unavailable_mode,
            note=f"{window_label} VP input history is unavailable, so this VP window was omitted.",
            source_df=pd.DataFrame(),
            zones_raw=[],
            profile_df=pd.DataFrame(),
        )

    target_dates = {pd.Timestamp(value).normalize() for value in trading_dates}
    daily_dates = pd.to_datetime(df_calc_daily["date"], errors="coerce").dt.normalize()
    daily_source = df_calc_daily.loc[daily_dates.isin(target_dates)].copy()
    if daily_source.empty:
        return VolumeProfileContext(
            mode=unavailable_mode,
            note=f"No daily OHLCV rows matched the {window_label} VP window, so it was omitted.",
            source_df=pd.DataFrame(),
            zones_raw=[],
            profile_df=pd.DataFrame(),
        )

    sorted_target_dates = sorted(target_dates)

    source_df = daily_source
    source_interval = "1d"
    source_mode = f"{normalized_window}_vp_1d"
    mode = f"{window_label} 1d"

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
                source_interval = "5m"
                source_mode = f"{normalized_window}_vp_5m"
                mode = f"{window_label} 5m"

    try:
        zones_raw, profile_df = build_composite_interval_volume_profile_zones(
            interval_df=source_df,
            bins=bins,
            zone_expand=zone_expand_pct,
            hv_quantile=hv_node_quantile,
            timeframe=normalized_window,
            source_label=f"VP ({window_label}, {source_interval})",
            source_mode=source_mode,
            vp_window_type=f"{normalized_window}_{int(lookback_days)}d",
        )
    except Exception as error:
        return VolumeProfileContext(
            mode=unavailable_mode,
            note=f"{window_label} VP construction failed, so this VP window was omitted. Details: {error}",
            source_df=source_df,
            zones_raw=[],
            profile_df=pd.DataFrame(),
        )

    if profile_df.empty:
        return VolumeProfileContext(
            mode=unavailable_mode,
            note=f"{window_label} VP input rows were available, but no valid profile could be built.",
            source_df=source_df,
            zones_raw=[],
            profile_df=pd.DataFrame(),
        )

    if source_interval == "5m":
        note = (
            f"{window_label} VP uses 5m OHLCV because complete intraday data was available "
            f"for every trading day in the window: {len(source_df)} bars / {len(profile_df)} bins."
        )
    else:
        note = (
            f"{window_label} VP uses 1d OHLCV because complete 5m data was unavailable "
            f"for the full trading-day window: "
            f"{int(profile_df['source_bars'].max())} bars / {len(profile_df)} bins."
        )

    return VolumeProfileContext(
        mode=mode,
        note=note,
        source_df=source_df,
        zones_raw=zones_raw,
        profile_df=profile_df,
    )


def _disabled_volume_profile_context(window_name: str) -> VolumeProfileContext:
    normalized_window = str(window_name).strip().lower() or "vp"
    return VolumeProfileContext(
        mode=f"{normalized_window} disabled",
        note=f"{normalized_window.title()} VP is disabled; zone generation uses only the fixed long VP window.",
        source_df=pd.DataFrame(),
        zones_raw=[],
        profile_df=pd.DataFrame(),
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
