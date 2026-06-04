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
    long_vp_lookback_days: int
    long_vp_bins: int
    zone_expand_bp: int
    zone_expand_pct: float
    show_ema20_line: bool
    show_ema50_line: bool
    show_atr_bands: bool
    atr_multiplier: float
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

    long_vp_bins = st.sidebar.slider(
        "VP price bins",
        min_value=20,
        max_value=120,
        value=defaults.long_vp_bins,
        step=4,
    )
    zone_expand_bp = st.sidebar.slider(
        "Zone expand (bp)",
        min_value=10,
        max_value=300,
        value=defaults.zone_expand_bp,
        step=10,
    )
    show_ema20_line = st.sidebar.checkbox("Show EMA20", value=defaults.show_ema20_line)
    show_ema50_line = st.sidebar.checkbox("Show EMA50", value=defaults.show_ema50_line)

    st.sidebar.markdown("---")
    st.sidebar.subheader("ATR Overlay")

    show_atr_bands = st.sidebar.checkbox("Show recent 20-day ATR bands", value=defaults.show_atr_bands)
    atr_multiplier = st.sidebar.slider(
        "ATR multiple",
        min_value=0.5,
        max_value=3.0,
        value=defaults.atr_multiplier,
        step=0.1,
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Bar Handling")

    include_latest_bar_in_calculations = st.sidebar.checkbox(
        "Include latest bar in calculations",
        value=not defaults.exclude_last_unclosed_bar,
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
        long_vp_lookback_days=defaults.long_vp_lookback_days,
        long_vp_bins=long_vp_bins,
        zone_expand_bp=zone_expand_bp,
        zone_expand_pct=zone_expand_bp / 10000.0,
        show_ema20_line=show_ema20_line,
        show_ema50_line=show_ema50_line,
        show_atr_bands=show_atr_bands,
        atr_multiplier=atr_multiplier,
        exclude_last_unclosed_bar=not include_latest_bar_in_calculations,
        show_live_last_bar_on_chart=show_live_last_bar_on_chart,
        initial_visible_bars=defaults.initial_visible_bars,
    )
