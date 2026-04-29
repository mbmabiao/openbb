from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st

from data.market_data import (
    get_missing_ohlc_columns,
    load_price_history_frame,
)
from engines.replay_engine import prepare_plot_and_calc_frames, prepare_replay_frame
from features.boundaries import zones_to_dataframe
from features.volume_profile import build_composite_interval_volume_profile_zones, compute_atr
from plotting.chart_builder import (
    build_chart_options,
    build_lwc_series,
    build_volume_profile_overlay_data,
    render_lwc_chart_with_focus_header,
    render_zone_left_panel,
)
from ui.sidebar import DashboardControls
from ui.state import get_replay_date_state, render_replay_controls
from zone_lifecycle import create_session_factory, load_replay_zone_snapshots


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

        get_replay_date_state(df_calc_daily_base, controls.symbol)
        replay_date = render_replay_controls(df_calc_daily_base, controls.symbol)
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
        atr_overlay = _build_atr_overlay(
            df_calc_daily_with_features=df_calc_daily,
            atr20_series=atr20_series,
            show_atr_bands=controls.show_atr_bands,
            atr_multiplier=controls.atr_multiplier,
        )
        vap_profile_df = _build_local_vap_profile(
            df_calc_daily=df_calc_daily,
            lookback_bars=controls.vp_lookback_days,
            bins=controls.vp_bins,
        )

        Session = create_session_factory()
        with Session() as session:
            snapshot_zones = load_replay_zone_snapshots(
                session,
                symbol=controls.symbol,
                replay_date=replay_date,
                max_support_zones=controls.max_support_zones,
                max_resistance_zones=controls.max_resistance_zones,
            )
        support_zones = snapshot_zones.support_zones
        resistance_zones = snapshot_zones.resistance_zones
        all_snapshot_zones = snapshot_zones.all_zones
        visible_snapshot_zones = _filter_zones_by_visible_price_range(all_snapshot_zones, df_plot_display)

        chart_series = build_lwc_series(
            df_plot=df_plot_display,
            df_calc_daily_with_features=df_calc_daily,
            support_zones=support_zones,
            resistance_zones=resistance_zones,
            daily_anchor_meta={},
            show_avwap_lines=controls.show_avwap_lines,
            atr_overlay=atr_overlay,
        )

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
                volume_profile_data=build_volume_profile_overlay_data(vap_profile_df),
            )

        if not support_zones and not resistance_zones:
            st.info(
                "No zone snapshots were found for this replay date. "
                "Run the offline snapshot task for the symbol/date range to populate them."
            )
        else:
            st.caption("Zones are loaded from zone_daily_snapshots. The dashboard does not warm up or write zones.")
        if not vap_profile_df.empty:
            st.caption(
                f"VAP/POC overlay uses local daily OHLCV: "
                f"{int(vap_profile_df['source_bars'].max())} bars / {len(vap_profile_df)} bins."
            )

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

        _render_zone_profile(
            zones=visible_snapshot_zones,
            replay_date=replay_date,
            visible_start=df_plot_display["date"].iloc[0],
            visible_end=df_plot_display["date"].iloc[-1],
            visible_price_low=float(df_plot_display["low"].min()),
            visible_price_high=float(df_plot_display["high"].max()),
            total_snapshot_count=len(all_snapshot_zones),
        )
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


def _build_local_vap_profile(
    *,
    df_calc_daily: pd.DataFrame,
    lookback_bars: int,
    bins: int,
) -> pd.DataFrame:
    required = {"date", "open", "high", "low", "close", "volume"}
    if df_calc_daily.empty or not required.issubset(set(df_calc_daily.columns)):
        return pd.DataFrame()

    source = df_calc_daily.tail(max(int(lookback_bars), 1)).copy()
    for column in ["open", "high", "low", "close", "volume"]:
        source[column] = pd.to_numeric(source[column], errors="coerce")
    source = source.dropna(subset=["open", "high", "low", "close", "volume"]).copy()
    source = source.loc[source["volume"] > 0].copy()
    if source.empty:
        return pd.DataFrame()

    _, profile_df = build_composite_interval_volume_profile_zones(
        interval_df=source,
        bins=max(int(bins), 1),
        zone_expand=0.0,
        hv_quantile=0.80,
        timeframe="D",
        source_label="VAP (local daily)",
        source_mode="local_daily_replay",
    )
    return profile_df


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


def _filter_zones_by_visible_price_range(zones: list[dict], df_plot_display: pd.DataFrame) -> list[dict]:
    if not zones or df_plot_display.empty:
        return []

    visible_low = float(pd.to_numeric(df_plot_display["low"], errors="coerce").min())
    visible_high = float(pd.to_numeric(df_plot_display["high"], errors="coerce").max())
    if not np.isfinite(visible_low) or not np.isfinite(visible_high):
        return zones

    return [
        zone
        for zone in zones
        if float(zone.get("lower", np.inf)) <= visible_high
        and float(zone.get("upper", -np.inf)) >= visible_low
    ]


def _render_zone_profile(
    *,
    zones: list[dict],
    replay_date,
    visible_start,
    visible_end,
    visible_price_low: float,
    visible_price_high: float,
    total_snapshot_count: int,
) -> None:
    st.markdown("### Zone Profile")
    st.caption(
        "Visible chart window: "
        f"{pd.Timestamp(visible_start).date()} to {pd.Timestamp(visible_end).date()} · "
        f"price {visible_price_low:.2f} - {visible_price_high:.2f} · "
        f"replay {pd.Timestamp(replay_date).date()}"
    )

    if not zones:
        st.info(
            f"No zone snapshots intersect the current visible price range. "
            f"Total snapshots loaded for this replay date: {total_snapshot_count}."
        )
        return

    profile_frame = _zone_profile_frame(zones)
    total_touches = int(profile_frame["touch_count"].sum())
    total_breaks = int(profile_frame["break_count"].sum())
    total_confirmed = int(profile_frame["confirmed_breakout_count"].sum())
    total_failed = int(profile_frame["failed_breakout_count"].sum())

    metric_cols = st.columns(5)
    metric_cols[0].metric("Visible Zones", str(len(profile_frame)), f"{total_snapshot_count} loaded")
    metric_cols[1].metric("Touches", str(total_touches))
    metric_cols[2].metric("Breaks", str(total_breaks))
    metric_cols[3].metric("Confirmed", str(total_confirmed))
    metric_cols[4].metric("Failed", str(total_failed))

    status_counts = (
        profile_frame.groupby(["zone_status", "current_role"], dropna=False)
        .size()
        .reset_index(name="zone_count")
        .sort_values(["zone_status", "current_role"], kind="stable")
    )
    count_totals = pd.DataFrame(
        [
            {
                "touch_count": total_touches,
                "break_count": total_breaks,
                "false_break_count": int(profile_frame["false_break_count"].sum()),
                "close_inside_count": int(profile_frame["close_inside_count"].sum()),
                "confirmed_breakout_count": total_confirmed,
                "failed_breakout_count": total_failed,
                "retest_num": int(profile_frame["retest_num"].sum()),
            }
        ]
    )

    status_col, count_col = st.columns([1.2, 2.0])
    with status_col:
        st.markdown("#### Status")
        st.dataframe(status_counts, use_container_width=True, hide_index=True)
    with count_col:
        st.markdown("#### Counts")
        st.dataframe(count_totals, use_container_width=True, hide_index=True)

    st.markdown("#### Zones")
    st.dataframe(profile_frame, use_container_width=True, hide_index=True)


def _zone_profile_frame(zones: list[dict]) -> pd.DataFrame:
    frame = zones_to_dataframe(zones)
    if frame.empty:
        return frame

    columns = [
        "zone_status",
        "current_role",
        "zone_kind",
        "lower",
        "upper",
        "center",
        "distance_atr",
        "source_types_label",
        "timeframe_sources",
        "touch_count",
        "break_count",
        "false_break_count",
        "close_inside_count",
        "confirmed_breakout_count",
        "failed_breakout_count",
        "retest_num",
        "zone_id",
    ]
    for column in columns:
        if column not in frame.columns:
            frame[column] = 0 if column.endswith("_count") or column == "retest_num" else ""

    numeric_columns = [
        "lower",
        "upper",
        "center",
        "distance_atr",
        "touch_count",
        "break_count",
        "false_break_count",
        "close_inside_count",
        "confirmed_breakout_count",
        "failed_breakout_count",
        "retest_num",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.loc[:, columns].sort_values(
        ["distance_atr", "zone_status", "current_role"],
        kind="stable",
        na_position="last",
    )
    return frame.reset_index(drop=True)


def _format_zone_metric(zone: dict | None) -> str:
    if zone is None:
        return "N/A"
    return (
        f"{zone['lower']:.2f} - {zone['upper']:.2f} "
        f"[{zone.get('source_types_label', '')}]"
    )
