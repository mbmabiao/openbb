from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd
import streamlit as st

from data.market_data import (
    fetch_interval_history_for_dates,
    get_missing_ohlc_columns,
    get_recent_trading_dates,
    get_recent_trading_dates_for_weekly_window,
    load_price_history_frame,
)
from engines.replay_engine import prepare_plot_and_calc_frames, prepare_replay_frame
from engines.validation_engine import rank_zones_for_side
from features.boundaries import (
    assign_zone_display_labels,
    create_candidate_zones_from_avwap,
    create_candidate_zones_from_vp,
    merge_close_zones,
    zones_to_dataframe,
)
from features.volume_profile import (
    build_avwap_features,
    build_composite_interval_volume_profile_zones,
    compute_atr,
    resample_to_weekly,
)
from plotting.chart_builder import (
    build_volume_profile_overlay_data,
    build_chart_options,
    build_lwc_series,
    render_lwc_chart_with_focus_header,
    render_zone_left_panel,
)
from ui.panels import show_definitions
from ui.sidebar import DashboardControls
from ui.state import get_replay_date_state, render_replay_controls


@dataclass(frozen=True)
class VolumeProfileContext:
    mode: str
    note: str
    source_df: pd.DataFrame
    zones_raw: list[dict]
    profile_df: pd.DataFrame


def render_historical_price_tab(controls: DashboardControls) -> None:
    st.subheader(f"Historical Price - {controls.symbol}")

    try:
        raw_df, price_df = load_price_history_frame(
            symbol_value=controls.symbol,
            history_range=controls.history_range,
            provider_value=controls.price_provider,
        )
        if raw_df is None or raw_df.empty:
            st.info("No historical price data returned.")
            return

        missing_columns = get_missing_ohlc_columns(price_df)
        if missing_columns:
            st.error(
                f"Price data does not contain required OHLC columns: {sorted(missing_columns)}. "
                f"Available columns: {list(price_df.columns)}"
            )
            st.markdown("### Raw Price Data")
            st.dataframe(raw_df, use_container_width=True)
            st.markdown("### Normalized Price Data")
            st.dataframe(price_df, use_container_width=True)
            return

        if price_df.empty:
            st.info("No valid OHLC rows available after cleaning.")
            return

        df_plot, df_calc_daily_base = prepare_plot_and_calc_frames(
            df=price_df,
            exclude_last_bar_for_calc=controls.exclude_last_unclosed_bar,
            show_last_bar_on_chart=controls.show_live_last_bar_on_chart,
        )
        if df_calc_daily_base.empty:
            st.warning("Calculation frame is empty after excluding the latest bar.")
            return

        replay_date = get_replay_date_state(df_calc_daily_base, controls.symbol)
        df_plot_replay, df_calc_daily = prepare_replay_frame(df_plot, df_calc_daily_base, replay_date)
        if df_calc_daily.empty:
            st.warning("No calculation data available on or before the selected replay date.")
            return
        if df_plot_replay.empty:
            st.warning("No chart data available on or before the selected replay date.")
            return

        df_plot_replay = df_plot_replay.copy()
        df_plot_replay["prev_close"] = df_plot_replay["close"].shift(1)
        df_plot_replay["change_pct"] = (
            (df_plot_replay["close"] - df_plot_replay["prev_close"])
            / df_plot_replay["prev_close"].replace(0, np.nan)
        )
        df_plot_display = df_plot_replay.tail(controls.initial_visible_bars).copy()

        current_price = float(df_calc_daily["close"].iloc[-1])
        atr20_series = compute_atr(df_calc_daily, period=20)
        atr20_value = (
            float(atr20_series.iloc[-1])
            if not atr20_series.empty and pd.notna(atr20_series.iloc[-1])
            else np.nan
        )
        df_calc_daily_with_features, daily_anchor_meta = build_avwap_features(df_calc_daily, timeframe="D")
        atr_overlay = _build_atr_overlay(
            df_calc_daily_with_features=df_calc_daily_with_features,
            atr20_series=atr20_series,
            show_atr_bands=controls.show_atr_bands,
            atr_multiplier=controls.atr_multiplier,
        )
        daily_vp_dates = get_recent_trading_dates(df_calc_daily, controls.vp_lookback_days)
        daily_vp_context = _load_interval_volume_profile_context(
            symbol=controls.symbol,
            provider=controls.price_provider,
            trading_dates=daily_vp_dates,
            interval="5m",
            bins=controls.vp_bins,
            zone_expand_pct=controls.zone_expand_pct,
            hv_node_quantile=controls.hv_node_quantile,
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
        )
        daily_vp_zones = create_candidate_zones_from_vp(
            df=df_calc_daily_with_features,
            vp_zones=daily_vp_context.zones_raw,
        )
        daily_avwap_zones = create_candidate_zones_from_avwap(
            df=df_calc_daily_with_features,
            anchor_meta=daily_anchor_meta,
            zone_expand_pct=controls.zone_expand_pct,
        )

        df_calc_weekly = resample_to_weekly(df_calc_daily)
        df_calc_weekly_with_features, weekly_anchor_meta = build_avwap_features(df_calc_weekly, timeframe="W")
        weekly_vp_dates = get_recent_trading_dates_for_weekly_window(
            df_calc_daily,
            controls.weekly_vp_lookback,
        )
        weekly_vp_context = _load_interval_volume_profile_context(
            symbol=controls.symbol,
            provider=controls.price_provider,
            trading_dates=weekly_vp_dates,
            interval="1d",
            bins=controls.weekly_vp_bins,
            zone_expand_pct=controls.zone_expand_pct,
            hv_node_quantile=controls.hv_node_quantile,
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
        )
        weekly_vp_zones = create_candidate_zones_from_vp(
            df=df_calc_weekly_with_features,
            vp_zones=weekly_vp_context.zones_raw,
        )
        weekly_avwap_zones = create_candidate_zones_from_avwap(
            df=df_calc_weekly_with_features,
            anchor_meta=weekly_anchor_meta,
            zone_expand_pct=controls.zone_expand_pct,
        )

        all_candidate_zones = merge_close_zones(
            daily_vp_zones + daily_avwap_zones + weekly_vp_zones + weekly_avwap_zones,
            merge_pct=controls.merge_pct,
        )

        resistance_zones = rank_zones_for_side(
            zones=all_candidate_zones,
            vp_df_daily=daily_vp_context.profile_df,
            vp_df_weekly=weekly_vp_context.profile_df,
            current_price=current_price,
            side="resistance",
            max_zones=controls.max_resistance_zones,
            df_reaction=df_calc_daily,
            lookahead=controls.reaction_lookahead,
            reaction_threshold=controls.reaction_return_threshold,
            min_gap=controls.min_touch_gap,
        )
        resistance_zones = assign_zone_display_labels(resistance_zones, prefix="R")

        support_zones = rank_zones_for_side(
            zones=all_candidate_zones,
            vp_df_daily=daily_vp_context.profile_df,
            vp_df_weekly=weekly_vp_context.profile_df,
            current_price=current_price,
            side="support",
            max_zones=controls.max_support_zones,
            df_reaction=df_calc_daily,
            lookahead=controls.reaction_lookahead,
            reaction_threshold=controls.reaction_return_threshold,
            min_gap=controls.min_touch_gap,
        )
        support_zones = assign_zone_display_labels(support_zones, prefix="S")

        chart_series = build_lwc_series(
            df_plot=df_plot_display,
            df_calc_daily_with_features=df_calc_daily_with_features,
            support_zones=support_zones,
            resistance_zones=resistance_zones,
            daily_anchor_meta=daily_anchor_meta,
            show_avwap_lines=controls.show_avwap_lines,
            atr_overlay=atr_overlay,
        )

        replay_date = render_replay_controls(df_calc_daily_base, controls.symbol)

        left_col, right_col = st.columns([1.15, 6.2], vertical_alignment="top")
        with left_col:
            render_zone_left_panel(
                support_zones=support_zones,
                resistance_zones=resistance_zones,
                current_price=current_price,
            )
        with right_col:
            render_lwc_chart_with_focus_header(
                chart_options=build_chart_options(),
                series=chart_series,
                chart_key=f"lwc_{controls.symbol}_{pd.Timestamp(replay_date).strftime('%Y%m%d')}",
                volume_profile_data=build_volume_profile_overlay_data(daily_vp_context.profile_df),
            )

        st.caption(f"Daily VP mode: {daily_vp_context.mode}. {daily_vp_context.note}")
        st.caption(f"Weekly VP mode: {weekly_vp_context.mode}. {weekly_vp_context.note}")
        if controls.show_atr_bands:
            if np.isfinite(atr20_value):
                st.caption(
                    f"ATR overlay: ATR20 = {atr20_value:.2f}; "
                    f"band distance = {atr20_value * controls.atr_multiplier:.2f} "
                    f"({controls.atr_multiplier:.1f}x)."
                )
            else:
                st.caption("ATR overlay: insufficient daily bars to compute ATR20.")

        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
        _render_summary_metrics(
            replay_date=replay_date,
            current_price=current_price,
            resistance_zones=resistance_zones,
            support_zones=support_zones,
            show_atr_bands=controls.show_atr_bands,
            atr20_value=atr20_value,
            atr_multiplier=controls.atr_multiplier,
        )

        show_definitions(controls)

        st.markdown("### Selected Resistance Zones")
        if resistance_zones:
            st.dataframe(zones_to_dataframe(resistance_zones), use_container_width=True)
        else:
            st.info("No important resistance zones found.")

        st.markdown("### Selected Support Zones")
        if support_zones:
            st.dataframe(zones_to_dataframe(support_zones), use_container_width=True)
        else:
            st.info("No important support zones found.")

        if controls.show_all_candidate_zones:
            st.markdown("### All Candidate Zones")
            if all_candidate_zones:
                st.dataframe(zones_to_dataframe(all_candidate_zones), use_container_width=True)
            else:
                st.info("No candidate zones detected.")

        st.markdown("### Daily AVWAP Anchor Points")
        daily_anchor_rows = _build_anchor_rows(df_calc_daily_with_features, daily_anchor_meta)
        if daily_anchor_rows:
            st.dataframe(pd.DataFrame(daily_anchor_rows), use_container_width=True)
        else:
            st.info("No daily AVWAP anchors available.")

        st.markdown("### Weekly AVWAP Anchor Points")
        weekly_anchor_rows = _build_anchor_rows(df_calc_weekly_with_features, weekly_anchor_meta)
        if weekly_anchor_rows:
            st.dataframe(pd.DataFrame(weekly_anchor_rows), use_container_width=True)
        else:
            st.info("No weekly AVWAP anchors available.")

        st.markdown("### Daily Composite Volume Profile Bins")
        if not daily_vp_context.profile_df.empty:
            st.dataframe(daily_vp_context.profile_df, use_container_width=True)
        else:
            st.info("No daily composite volume profile data available.")

        st.markdown("### Weekly / Higher-Timeframe Volume Profile Bins")
        if not weekly_vp_context.profile_df.empty:
            st.dataframe(weekly_vp_context.profile_df, use_container_width=True)
        else:
            st.info("No weekly or higher-timeframe volume profile data available.")

        st.markdown("### Data Frames Used")
        st.markdown(f"- Plot rows (replay): **{len(df_plot_replay)}**")
        st.markdown(f"- Daily calc rows: **{len(df_calc_daily_with_features)}**")
        st.markdown(f"- Weekly calc rows: **{len(df_calc_weekly_with_features)}**")

        st.markdown("### Historical Price Data (Replay Plot Frame)")
        st.dataframe(df_plot_replay, use_container_width=True)
    except Exception as error:
        st.error(f"Error: {error}")


def _build_atr_overlay(
    df_calc_daily_with_features: pd.DataFrame,
    atr20_series: pd.Series,
    show_atr_bands: bool,
    atr_multiplier: float,
) -> dict | None:
    if not show_atr_bands or df_calc_daily_with_features.empty or atr20_series.empty:
        return None

    atr_frame = df_calc_daily_with_features.loc[:, ["date", "close"]].copy()
    atr_frame["atr20"] = pd.to_numeric(atr20_series, errors="coerce")
    atr_frame = atr_frame.dropna(subset=["date", "close", "atr20"]).copy()
    if atr_frame.empty:
        return None

    atr_distance = atr_frame["atr20"] * atr_multiplier
    atr_frame["upper"] = atr_frame["close"] + atr_distance
    atr_frame["lower"] = atr_frame["close"] - atr_distance

    return {
        "label": f"ATR20x{atr_multiplier:.1f}",
        "color": "#6d28d9",
        "upper_data": [
            {"time": pd.Timestamp(row.date).strftime("%Y-%m-%d"), "value": float(row.upper)}
            for row in atr_frame.itertuples(index=False)
        ],
        "lower_data": [
            {"time": pd.Timestamp(row.date).strftime("%Y-%m-%d"), "value": float(row.lower)}
            for row in atr_frame.itertuples(index=False)
        ],
    }


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
) -> VolumeProfileContext:
    source_df = pd.DataFrame()
    try:
        source_df = fetch_interval_history_for_dates(
            symbol_value=symbol,
            trading_dates=trading_dates,
            provider_value=provider,
            interval_value=interval,
        )
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


def _render_summary_metrics(
    *,
    replay_date: pd.Timestamp,
    current_price: float,
    resistance_zones: list[dict],
    support_zones: list[dict],
    show_atr_bands: bool,
    atr20_value: float,
    atr_multiplier: float,
) -> None:
    nearest_resistance = (
        min(resistance_zones, key=lambda zone: abs(zone["center"] - current_price))
        if resistance_zones
        else None
    )
    nearest_support = (
        min(support_zones, key=lambda zone: abs(zone["center"] - current_price))
        if support_zones
        else None
    )

    if show_atr_bands and np.isfinite(atr20_value):
        col1, col2, col3, col4 = st.columns(4)
    else:
        col1, col2, col3 = st.columns(3)
        col4 = None

    col1.metric("Replay Date", str(pd.Timestamp(replay_date).date()))
    col2.metric("Nearest Resistance", _format_zone_metric(nearest_resistance))
    col3.metric("Nearest Support", _format_zone_metric(nearest_support))
    if col4 is not None:
        col4.metric("ATR20", f"{atr20_value:.2f}", f"{atr_multiplier:.1f}x = {atr20_value * atr_multiplier:.2f}")


def _build_anchor_rows(df_with_features: pd.DataFrame, anchor_meta: dict) -> list[dict]:
    rows: list[dict] = []
    for column_name, meta in anchor_meta.items():
        latest_avwap = (
            df_with_features[column_name].dropna()
            if column_name in df_with_features.columns
            else pd.Series(dtype=float)
        )
        avwap_now = float(latest_avwap.iloc[-1]) if not latest_avwap.empty else np.nan
        rows.append(
            {
                "timeframe": meta["timeframe"],
                "avwap_column": column_name,
                "anchor_name": meta["anchor_name"],
                "anchor_window_trading_days": meta.get("anchor_window_trading_days"),
                "start_date": meta["start_date"],
                "start_price": meta["start_price"],
                "latest_avwap": avwap_now,
            }
        )
    return rows


def _format_zone_metric(zone: dict | None) -> str:
    if zone is None:
        return "N/A"
    return (
        f"{zone['lower']:.2f} - {zone['upper']:.2f} "
        f"[{zone.get('source_types_label', '')}]"
    )
