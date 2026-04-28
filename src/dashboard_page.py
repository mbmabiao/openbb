from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

from data.market_data import (
    get_missing_ohlc_columns,
    load_price_history_frame,
)
from engines.replay_engine import prepare_plot_and_calc_frames, prepare_replay_frame
from engines.zone_generation import config_from_controls, generate_zones_for_replay
from features.boundaries import zones_to_dataframe
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
from zone_lifecycle import persist_dashboard_zones_safely


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

        generated_zones = generate_zones_for_replay(
            symbol=controls.symbol,
            provider=controls.price_provider,
            df_calc_daily=df_calc_daily,
            config=config_from_controls(controls),
        )
        current_price = generated_zones.current_price
        atr20_value = generated_zones.atr20_value
        atr_overlay = _build_atr_overlay(
            df_calc_daily_with_features=generated_zones.df_calc_daily_with_features,
            atr20_series=generated_zones.atr20_series,
            show_atr_bands=controls.show_atr_bands,
            atr_multiplier=controls.atr_multiplier,
        )
        support_zones = generated_zones.support_zones
        resistance_zones = generated_zones.resistance_zones
        all_candidate_zones = generated_zones.all_candidate_zones

        persist_dashboard_zones_safely(
            symbol=controls.symbol,
            replay_date=replay_date,
            current_price=current_price,
            atr_value=atr20_value,
            support_zones=support_zones,
            resistance_zones=resistance_zones,
        )

        chart_series = build_lwc_series(
            df_plot=df_plot_display,
            df_calc_daily_with_features=generated_zones.df_calc_daily_with_features,
            support_zones=support_zones,
            resistance_zones=resistance_zones,
            daily_anchor_meta=generated_zones.daily_anchor_meta,
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
                volume_profile_data=build_volume_profile_overlay_data(generated_zones.daily_vp_context.profile_df),
            )

        st.caption(f"Daily VP mode: {generated_zones.daily_vp_context.mode}. {generated_zones.daily_vp_context.note}")
        st.caption(f"Weekly VP mode: {generated_zones.weekly_vp_context.mode}. {generated_zones.weekly_vp_context.note}")
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
        daily_anchor_rows = _build_anchor_rows(
            generated_zones.df_calc_daily_with_features,
            generated_zones.daily_anchor_meta,
        )
        if daily_anchor_rows:
            st.dataframe(pd.DataFrame(daily_anchor_rows), use_container_width=True)
        else:
            st.info("No daily AVWAP anchors available.")

        st.markdown("### Weekly AVWAP Anchor Points")
        weekly_anchor_rows = _build_anchor_rows(
            generated_zones.df_calc_weekly_with_features,
            generated_zones.weekly_anchor_meta,
        )
        if weekly_anchor_rows:
            st.dataframe(pd.DataFrame(weekly_anchor_rows), use_container_width=True)
        else:
            st.info("No weekly AVWAP anchors available.")

        st.markdown("### Daily Composite Volume Profile Bins")
        if not generated_zones.daily_vp_context.profile_df.empty:
            st.dataframe(generated_zones.daily_vp_context.profile_df, use_container_width=True)
        else:
            st.info("No daily composite volume profile data available.")

        st.markdown("### Weekly / Higher-Timeframe Volume Profile Bins")
        if not generated_zones.weekly_vp_context.profile_df.empty:
            st.dataframe(generated_zones.weekly_vp_context.profile_df, use_container_width=True)
        else:
            st.info("No weekly or higher-timeframe volume profile data available.")

        st.markdown("### Data Frames Used")
        st.markdown(f"- Plot rows (replay): **{len(df_plot_replay)}**")
        st.markdown(f"- Daily calc rows: **{len(generated_zones.df_calc_daily_with_features)}**")
        st.markdown(f"- Weekly calc rows: **{len(generated_zones.df_calc_weekly_with_features)}**")

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
                "anchor_family": meta.get("anchor_family"),
                "anchor_window_bars": meta.get("anchor_window_bars"),
                "anchor_search_bars": meta.get("anchor_search_bars"),
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
