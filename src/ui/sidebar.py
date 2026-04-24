from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from config.settings import HISTORY_RANGE_OPTIONS, SidebarDefaults


@dataclass(frozen=True)
class DashboardControls:
    symbol: str
    price_provider: str | None
    fund_provider: str | None
    news_provider: str | None
    history_range: str
    news_limit: int
    vp_lookback_days: int
    vp_bins: int
    weekly_vp_lookback: int
    weekly_vp_bins: int
    zone_expand_bp: int
    zone_expand_pct: float
    hv_node_quantile_pct: int
    hv_node_quantile: float
    merge_pct_bp: int
    merge_pct: float
    max_resistance_zones: int
    max_support_zones: int
    show_avwap_lines: bool
    show_all_candidate_zones: bool
    show_atr_bands: bool
    atr_multiplier: float
    reaction_lookahead: int
    reaction_threshold_bp: int
    reaction_return_threshold: float
    min_touch_gap: int
    exclude_last_unclosed_bar: bool
    show_live_last_bar_on_chart: bool
    initial_visible_bars: int


def render_sidebar(defaults: SidebarDefaults | None = None) -> DashboardControls:
    defaults = defaults or SidebarDefaults()

    symbol = st.sidebar.text_input("Symbol", value=defaults.symbol).strip().upper()
    price_provider = st.sidebar.text_input("Price provider (optional)", value="").strip() or None
    fund_provider = st.sidebar.text_input("Fundamentals provider (optional)", value="").strip() or None
    news_provider = st.sidebar.text_input("News provider (optional)", value="").strip() or None

    history_range = st.sidebar.selectbox(
        "Price history range",
        options=HISTORY_RANGE_OPTIONS,
        index=HISTORY_RANGE_OPTIONS.index(defaults.history_range),
    )
    news_limit = st.sidebar.slider("News items", min_value=5, max_value=50, value=defaults.news_limit, step=5)

    st.sidebar.markdown("---")
    st.sidebar.subheader("Institutional Zone Settings")

    vp_bins = st.sidebar.slider(
        "Composite VP price bins",
        min_value=20,
        max_value=120,
        value=defaults.vp_bins,
        step=4,
    )
    weekly_vp_lookback = st.sidebar.slider(
        "Weekly volume profile lookback bars",
        min_value=20,
        max_value=156,
        value=defaults.weekly_vp_lookback,
        step=4,
    )
    weekly_vp_bins = st.sidebar.slider(
        "Weekly volume profile price bins",
        min_value=10,
        max_value=60,
        value=defaults.weekly_vp_bins,
        step=2,
    )
    zone_expand_bp = st.sidebar.slider(
        "Zone expand (bp)",
        min_value=10,
        max_value=300,
        value=defaults.zone_expand_bp,
        step=10,
    )
    hv_node_quantile_pct = st.sidebar.slider(
        "High-volume node quantile (%)",
        min_value=50,
        max_value=95,
        value=defaults.hv_node_quantile_pct,
        step=5,
    )
    merge_pct_bp = st.sidebar.slider(
        "Merge nearby zones (bp)",
        min_value=10,
        max_value=200,
        value=defaults.merge_pct_bp,
        step=10,
    )
    max_resistance_zones = st.sidebar.slider(
        "Maximum resistance zones to display",
        min_value=1,
        max_value=8,
        value=defaults.max_resistance_zones,
        step=1,
    )
    max_support_zones = st.sidebar.slider(
        "Maximum support zones to display",
        min_value=1,
        max_value=8,
        value=defaults.max_support_zones,
        step=1,
    )
    show_avwap_lines = st.sidebar.checkbox("Show anchored VWAP lines", value=defaults.show_avwap_lines)
    show_all_candidate_zones = st.sidebar.checkbox(
        "Show all candidate zones table",
        value=defaults.show_all_candidate_zones,
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("ATR Overlay")

    show_atr_bands = st.sidebar.checkbox("Show recent 20-day ATR bands", value=defaults.show_atr_bands)
    atr_multiplier = st.sidebar.slider(
        "ATR multiple",
        min_value=1.5,
        max_value=3.0,
        value=defaults.atr_multiplier,
        step=0.1,
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Reaction Validation")

    reaction_lookahead = st.sidebar.slider(
        "Reaction lookahead bars",
        min_value=1,
        max_value=15,
        value=defaults.reaction_lookahead,
        step=1,
    )
    reaction_threshold_bp = st.sidebar.slider(
        "Strong reaction threshold (bp)",
        min_value=20,
        max_value=800,
        value=defaults.reaction_threshold_bp,
        step=10,
    )
    min_touch_gap = st.sidebar.slider(
        "Minimum bars between distinct touches",
        min_value=1,
        max_value=20,
        value=defaults.min_touch_gap,
        step=1,
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Bar Handling")

    exclude_last_unclosed_bar = st.sidebar.checkbox(
        "Exclude latest unclosed bar from calculations",
        value=defaults.exclude_last_unclosed_bar,
    )
    show_live_last_bar_on_chart = st.sidebar.checkbox(
        "Show latest live bar on chart",
        value=defaults.show_live_last_bar_on_chart,
    )

    return DashboardControls(
        symbol=symbol,
        price_provider=price_provider,
        fund_provider=fund_provider,
        news_provider=news_provider,
        history_range=history_range,
        news_limit=news_limit,
        vp_lookback_days=defaults.vp_lookback_days,
        vp_bins=vp_bins,
        weekly_vp_lookback=weekly_vp_lookback,
        weekly_vp_bins=weekly_vp_bins,
        zone_expand_bp=zone_expand_bp,
        zone_expand_pct=zone_expand_bp / 10000.0,
        hv_node_quantile_pct=hv_node_quantile_pct,
        hv_node_quantile=hv_node_quantile_pct / 100.0,
        merge_pct_bp=merge_pct_bp,
        merge_pct=merge_pct_bp / 10000.0,
        max_resistance_zones=max_resistance_zones,
        max_support_zones=max_support_zones,
        show_avwap_lines=show_avwap_lines,
        show_all_candidate_zones=show_all_candidate_zones,
        show_atr_bands=show_atr_bands,
        atr_multiplier=atr_multiplier,
        reaction_lookahead=reaction_lookahead,
        reaction_threshold_bp=reaction_threshold_bp,
        reaction_return_threshold=reaction_threshold_bp / 10000.0,
        min_touch_gap=min_touch_gap,
        exclude_last_unclosed_bar=exclude_last_unclosed_bar,
        show_live_last_bar_on_chart=show_live_last_bar_on_chart,
        initial_visible_bars=defaults.initial_visible_bars,
    )
