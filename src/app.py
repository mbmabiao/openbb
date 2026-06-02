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
from market_dashboard import render_market_dashboard
from ui.panels import show_dataframe_result, show_news
from ui.sidebar import render_sidebar
from ui.strategy_backtest_page import render_strategy_backtest_page


def _app_shell_css() -> str:
    return """
    <style>
    :root {
        --qd-bg: #05070d;
        --qd-panel: #0d1420;
        --qd-border: rgba(148, 163, 184, 0.18);
        --qd-text: #ffffff;
        --qd-muted: #d7e2ee;
        --qd-accent: #38d5b5;
        --qd-accent-2: #5aa7ff;
    }
    .stApp {
        background:
            radial-gradient(circle at 16% 0%, rgba(56, 213, 181, 0.16), transparent 28rem),
            radial-gradient(circle at 82% 2%, rgba(90, 167, 255, 0.14), transparent 30rem),
            var(--qd-bg);
        color: var(--qd-text);
    }
    .stApp p,
    .stApp span,
    .stApp label,
    .stApp h1,
    .stApp h2,
    .stApp h3,
    .stApp h4,
    .stApp h5,
    .stApp h6,
    .stApp [data-testid="stMarkdownContainer"],
    .stApp [data-testid="stMarkdownContainer"] p {
        color: var(--qd-text);
    }
    .stApp [data-testid="stCaptionContainer"],
    .stApp [data-testid="stMarkdownContainer"] li {
        color: var(--qd-muted);
    }
    header[data-testid="stHeader"] {
        background: rgba(5, 7, 13, 0.78);
        backdrop-filter: blur(16px);
        border-bottom: 1px solid var(--qd-border);
    }
    section[data-testid="stSidebar"] {
        background: rgba(8, 13, 22, 0.94);
        border-right: 1px solid var(--qd-border);
    }
    section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] p {
        color: var(--qd-text);
    }
    .block-container {
        max-width: 1440px;
        padding-top: 1.15rem;
    }
    [data-testid="stAppViewContainer"] h1:first-of-type {
        margin: 0;
        font-size: 1.04rem;
        line-height: 1.2;
        color: var(--qd-text);
    }
    .app-brand {
        color: var(--qd-text);
        font-size: 1.02rem;
        font-weight: 900;
        line-height: 1.2;
        margin-bottom: 0.35rem;
    }
    .app-brand span {
        color: var(--qd-accent);
    }
    div[role="radiogroup"] {
        display: inline-flex;
        gap: 0.35rem;
        padding: 0.28rem;
        margin: 0.65rem 0 1.05rem 0;
        border: 1px solid var(--qd-border);
        border-radius: 999px;
        background: rgba(13, 20, 32, 0.84);
        box-shadow: 0 18px 48px rgba(0, 0, 0, 0.28);
        backdrop-filter: blur(18px);
    }
    div[role="radiogroup"] label {
        min-height: 2.15rem;
        padding: 0 1rem;
        border-radius: 999px;
        border: 1px solid transparent;
        color: var(--qd-muted);
        font-weight: 800;
        transition: all 160ms ease;
    }
    div[role="radiogroup"] label p {
        color: inherit;
        font-weight: inherit;
    }
    .main [data-testid="stMarkdownContainer"] {
        color: var(--qd-text);
    }
    .main [data-testid="stMarkdownContainer"] p,
    .main [data-testid="stMarkdownContainer"] li {
        color: var(--qd-muted);
    }
    .main h1,
    .main h2,
    .main h3,
    .main h4 {
        color: var(--qd-text);
    }
    div[role="radiogroup"] label:has(input:checked) {
        color: #04110f;
        border-color: rgba(56, 213, 181, 0.86);
        background: linear-gradient(135deg, var(--qd-accent), #8bf5dd);
        box-shadow: 0 0 26px rgba(56, 213, 181, 0.22);
    }
    div[role="radiogroup"] label:hover {
        color: var(--qd-text);
        border-color: rgba(148, 163, 184, 0.28);
        background: rgba(255, 255, 255, 0.04);
    }
    div[role="radiogroup"] label:has(input:checked):hover {
        color: #04110f;
        background: linear-gradient(135deg, var(--qd-accent), #8bf5dd);
    }
    div[role="radiogroup"] input {
        display: none;
    }
    [data-testid="stTabs"] {
        border-bottom: 1px solid var(--qd-border);
    }
    [data-testid="stTabs"] button {
        color: var(--qd-muted);
        border: 0 !important;
        border-radius: 0 !important;
        background: transparent;
        margin-right: 0.25rem;
    }
    [data-testid="stTabs"] button[aria-selected="true"] {
        color: #ffffff;
        border: 0 !important;
        background: transparent;
    }
    [data-testid="stTabs"] button p {
        color: inherit !important;
        font-weight: 800;
    }
    [data-testid="baseButton-secondary"],
    [data-testid="baseButton-primary"] {
        border: 1px solid rgba(255, 255, 255, 0.72) !important;
        border-radius: 8px !important;
        box-shadow: none !important;
    }
    [data-testid="baseButton-secondary"] {
        background: rgba(13, 20, 32, 0.84) !important;
        color: var(--qd-text) !important;
    }
    [data-testid="baseButton-secondary"] button,
    [data-testid="baseButton-secondary"] > button,
    [data-testid="baseButton-secondary"] div {
        background: rgba(13, 20, 32, 0.84) !important;
        color: var(--qd-text) !important;
    }
    [data-testid="baseButton-secondary"]:hover {
        background: rgba(255, 255, 255, 0.08) !important;
        border-color: rgba(255, 255, 255, 0.9) !important;
        color: #ffffff !important;
    }
    [data-testid="baseButton-secondary"]:hover button,
    [data-testid="baseButton-secondary"]:hover div {
        background: rgba(255, 255, 255, 0.08) !important;
        color: #ffffff !important;
    }
    [data-testid="baseButton-primary"] {
        background: linear-gradient(135deg, var(--qd-accent), #8bf5dd) !important;
        border-color: rgba(56, 213, 181, 0.9) !important;
        color: #04110f !important;
    }
    [data-testid="baseButton-primary"] button,
    [data-testid="baseButton-primary"] > button,
    [data-testid="baseButton-primary"] div {
        background: linear-gradient(135deg, var(--qd-accent), #8bf5dd) !important;
        color: #04110f !important;
    }
    [data-testid="baseButton-primary"]:hover {
        background: linear-gradient(135deg, #5ee6cb, #a8fff0) !important;
        color: #04110f !important;
    }
    [data-testid="baseButton-secondary"] p,
    [data-testid="baseButton-secondary"] span,
    [data-testid="baseButton-primary"] p,
    [data-testid="baseButton-primary"] span {
        color: inherit !important;
    }
    [data-testid="stMetric"],
    [data-testid="stDataFrame"],
    [data-testid="stAlert"] {
        border-radius: 8px;
    }
    [data-testid="stMetric"] {
        border: 1px solid var(--qd-border);
        background: rgba(13, 20, 32, 0.78);
        padding: 0.85rem 0.95rem;
    }
    [data-testid="stMetric"] label,
    [data-testid="stMetric"] label p {
        color: var(--qd-muted);
    }
    [data-testid="stMetricValue"],
    [data-testid="stMetricValue"] div,
    [data-testid="stMetricValue"] p {
        color: var(--qd-text) !important;
    }
    [data-testid="stMetricDelta"],
    [data-testid="stMetricDelta"] div,
    [data-testid="stMetricDelta"] p,
    [data-testid="stMetricDelta"] span {
        color: var(--qd-accent) !important;
    }
    div[data-testid="stDateInput"] label p,
    div[data-testid="stSelectbox"] label p,
    div[data-testid="stSlider"] label p,
    div[data-testid="stCheckbox"] label p {
        color: var(--qd-text);
    }
    div[data-baseweb="select"] > div,
    div[data-baseweb="input"] > div,
    div[data-baseweb="base-input"] {
        border-color: var(--qd-border);
        background: rgba(13, 20, 32, 0.9);
        color: var(--qd-text);
    }
    input,
    textarea {
        color: var(--qd-text) !important;
    }
    </style>
    """


st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.markdown(_app_shell_css(), unsafe_allow_html=True)
st.markdown(f'<div class="app-brand">{APP_TITLE} <span>v1</span></div>', unsafe_allow_html=True)

top_page = st.radio(
    "Top navigation",
    ("个股", "市场", "回测"),
    horizontal=True,
    label_visibility="collapsed",
)

if top_page == "市场":
    render_market_dashboard()
    st.stop()

if top_page == "回测":
    render_strategy_backtest_page(default_symbol="MSFT")
    st.stop()

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
