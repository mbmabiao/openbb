from __future__ import annotations

import os

os.environ["OPENBB_AUTO_BUILD"] = "false"

import streamlit as st

from config.settings import APP_TITLE, PAGE_TITLE, TAB_NAMES
from dashboard_page import render_historical_price_tab
from data.market_data import (
    fetch_balance_sheet,
    fetch_cash_flow,
    fetch_company_news,
    fetch_income_statement,
    fetch_ratios,
)
from ui.panels import show_dataframe_result, show_news
from ui.sidebar import render_sidebar


st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.title(APP_TITLE)

controls = render_sidebar()
if not controls.symbol:
    st.warning("Enter a symbol in the sidebar.")
    st.stop()

tabs = st.tabs(list(TAB_NAMES))

with tabs[0]:
    render_historical_price_tab(controls)

with tabs[1]:
    show_dataframe_result(
        f"Income Statement - {controls.symbol}",
        lambda: fetch_income_statement(controls.symbol, controls.fund_provider),
        empty_message="No income statement data returned.",
    )

with tabs[2]:
    show_dataframe_result(
        f"Balance Sheet - {controls.symbol}",
        lambda: fetch_balance_sheet(controls.symbol, controls.fund_provider),
        empty_message="No balance sheet data returned.",
    )

with tabs[3]:
    show_dataframe_result(
        f"Cash Flow - {controls.symbol}",
        lambda: fetch_cash_flow(controls.symbol, controls.fund_provider),
        empty_message="No cash flow data returned.",
    )

with tabs[4]:
    show_dataframe_result(
        f"Ratios - {controls.symbol}",
        lambda: fetch_ratios(controls.symbol, controls.fund_provider),
        empty_message="No ratios data returned.",
    )

with tabs[5]:
    show_news(
        f"Company News - {controls.symbol}",
        lambda: fetch_company_news(
            controls.symbol,
            limit=controls.news_limit,
            provider_value=controls.news_provider,
        ),
        news_limit=controls.news_limit,
    )
