from __future__ import annotations

import base64
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from backtesting.presenter import format_trade_table
from backtesting.runner import run_backtest
from plotting.backtest_lwc_adapter import build_backtest_lwc_series
from plotting.chart_builder import build_chart_options, render_lwc_chart_with_focus_header
from strategies.registry import discover_strategies, list_strategies


TIMEFRAME_OPTIONS = ["1d", "1h", "30m", "15m", "5m"]
STRATEGY_PROTOCOL_PATH = Path(__file__).resolve().parents[1] / "backtesting" / "README.md"


def render_strategy_backtest_page(default_symbol: str = "MSFT") -> None:
    st.subheader("Strategy Backtest")
    _render_strategy_protocol_download_link()

    strategies = list_strategies()
    if not strategies:
        st.warning("No strategies discovered in src/strategies.")
        return

    strategy_by_label = {
        f"{item['display_name']} ({item['name']})": item
        for item in strategies
    }

    env_col, strategy_col = st.columns([1, 1], gap="large")
    with env_col:
        st.markdown("#### Backtest Environment")
        symbol = st.text_input("Symbol", value=default_symbol, key="bt_symbol").strip().upper()
        strategy_select_col, refresh_col = st.columns([0.78, 0.22], gap="small")
        with strategy_select_col:
            selected_label = st.selectbox(
                "Strategy",
                options=list(strategy_by_label),
                key="bt_strategy",
                on_change=_reset_strategy_state,
            )
        with refresh_col:
            st.markdown("<div style='height: 1.75rem;'></div>", unsafe_allow_html=True)
            if st.button("Refresh", key="bt_refresh_strategies", use_container_width=True):
                discover_strategies.cache_clear()
                _reset_strategy_state()
                st.rerun()
        metadata = strategy_by_label[selected_label]
        required_timeframes = list(metadata.get("required_timeframes") or ["1d"])
        data_requirements = dict(metadata.get("data_requirements") or {})
        primary_default = (
            data_requirements.get("primary_timeframe")
            or metadata.get("preferred_primary_timeframe")
            or (required_timeframes[0] if required_timeframes else "1d")
        )
        timeframe_options = _ordered_unique([primary_default, *required_timeframes, *TIMEFRAME_OPTIONS])
        primary_timeframe = st.selectbox(
            "Primary timeframe",
            options=timeframe_options,
            index=timeframe_options.index(primary_default),
            key=f"bt_primary_{metadata['name']}",
        )
        today = date.today()
        start_date = st.date_input("Backtest start date", value=today - timedelta(days=365), key="bt_start")
        end_date = st.date_input("Backtest end date", value=today, key="bt_end")
        price_provider = st.text_input("Price provider", value="yfinance", key="bt_provider").strip() or None
        requires_extended_hours = bool(metadata.get("requires_extended_hours"))
        supports_extended_hours = bool(metadata.get("supports_extended_hours"))
        extended_hours = st.checkbox(
            "Extended Hours",
            value=True if requires_extended_hours else False,
            disabled=requires_extended_hours,
            help=_extended_hours_help(requires_extended_hours, supports_extended_hours),
        )
        if requires_extended_hours:
            st.caption("This strategy requires extended-hours intraday data.")

        st.markdown("#### Execution Settings")
        initial_capital = st.number_input("Initial capital", min_value=1.0, value=10_000.0, step=1_000.0)
        slippage_pct = st.number_input("Slippage (%)", min_value=0.0, value=0.05, step=0.01, format="%.4f")
        commission_pct = st.number_input("Commission per trade (%)", min_value=0.0, value=0.05, step=0.01, format="%.4f")
        position_size_pct = st.number_input("Position size (%)", min_value=0.0, max_value=100.0, value=100.0, step=5.0)
        allow_long = st.checkbox("Allow long", value=True)
        allow_short = st.checkbox("Allow short", value=True)
        exit_before_entry = st.checkbox("Exit before entry on same bar", value=True)

    with strategy_col:
        st.markdown("#### Strategy Settings")
        st.caption(metadata.get("description") or "")
        st.caption(f"Required timeframes: {', '.join(required_timeframes)}")
        strategy_config = _render_strategy_config(metadata)

    run_clicked = st.button("Run Backtest", type="primary", use_container_width=True)

    if run_clicked:
        if not symbol:
            st.error("Symbol is required.")
            return
        if start_date > end_date:
            st.error("Backtest start date must be before end date.")
            return
        required_missing = _missing_required_fields(metadata, strategy_config)
        if required_missing:
            st.error(f"Required strategy fields are missing: {', '.join(required_missing)}")
            return

        backtest_config = {
            "initial_capital": float(initial_capital),
            "slippage": float(slippage_pct) / 100.0,
            "commission_pct": float(commission_pct) / 100.0,
            "position_size_pct": float(position_size_pct) / 100.0,
            "allow_short": bool(allow_short),
            "allow_long": bool(allow_long),
            "exit_before_entry": bool(exit_before_entry),
            "price_col": "close",
            "primary_timeframe": primary_timeframe,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "price_provider": price_provider,
            "extended_hours": bool(extended_hours),
        }
        with st.spinner("Running strategy backtest..."):
            try:
                result = run_backtest(
                    symbol=symbol,
                    strategy_name=metadata["name"],
                    strategy_config=strategy_config,
                    backtest_config=backtest_config,
                )
            except Exception as exc:
                st.error(f"Backtest failed: {exc}")
                return
        _render_results(result)


def _render_strategy_config(metadata: dict) -> dict:
    default_config = dict(metadata.get("default_config") or {})
    schema = dict(metadata.get("config_schema") or {})
    if not schema:
        schema = _infer_schema(default_config)

    config: dict[str, Any] = {}
    for field_name, spec in schema.items():
        if field_name == "direction":
            continue
        field_type = str(spec.get("type", "str")).lower()
        label = str(spec.get("label") or field_name)
        help_text = spec.get("help")
        default = spec.get("default", default_config.get(field_name))
        key = f"strategy_{metadata['name']}_{field_name}"

        if field_type == "int":
            config[field_name] = int(
                st.number_input(
                    label,
                    value=int(default or 0),
                    min_value=spec.get("min"),
                    max_value=spec.get("max"),
                    step=int(spec.get("step", 1)),
                    help=help_text,
                    key=key,
                )
            )
        elif field_type == "float":
            config[field_name] = float(
                st.number_input(
                    label,
                    value=float(default or 0.0),
                    min_value=spec.get("min"),
                    max_value=spec.get("max"),
                    step=float(spec.get("step", 0.1)),
                    help=help_text,
                    key=key,
                )
            )
        elif field_type == "bool":
            config[field_name] = bool(st.checkbox(label, value=bool(default), help=help_text, key=key))
        elif field_type == "select":
            options = list(spec.get("options") or [])
            selected = default if default in options else (options[0] if options else "")
            config[field_name] = st.selectbox(
                label,
                options=options,
                index=options.index(selected) if selected in options else 0,
                help=help_text,
                key=key,
            )
        else:
            config[field_name] = st.text_input(label, value="" if default is None else str(default), help=help_text, key=key)
    return config


def _load_strategy_protocol_doc() -> str:
    try:
        return STRATEGY_PROTOCOL_PATH.read_text(encoding="utf-8")
    except OSError:
        return (
            "# Backtesting Strategy Protocol\n\n"
            "Required signal columns: open_long, close_long, open_short, close_short.\n"
            "Optional execution price columns: open_long_price, close_long_price, "
            "open_short_price, close_short_price.\n"
        )


def _render_strategy_protocol_download_link() -> None:
    encoded = base64.b64encode(_load_strategy_protocol_doc().encode("utf-8")).decode("ascii")
    href = (
        "data:text/markdown;charset=utf-8;base64,"
        f"{encoded}"
    )
    st.markdown(
        f"""
        <div class="strategy-protocol-link">
            <span aria-hidden="true">↓</span>
            <a href="{href}" download="backtesting_strategy_protocol.md">download develop protocol</a>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_results(result) -> None:
    st.markdown("#### Summary")
    metrics = result.metrics
    metric_items = [
        ("Total return", _format_pct(metrics.get("total_return"))),
        ("Annualised return", _format_pct(metrics.get("annualised_return"))),
        ("Max drawdown", _format_pct(metrics.get("max_drawdown"))),
        ("Sharpe", _format_number(metrics.get("sharpe_ratio"))),
        ("Win rate", _format_pct(metrics.get("win_rate"))),
        ("P/L ratio", _format_number(metrics.get("profit_loss_ratio"))),
        ("Trade count", str(metrics.get("trade_count", 0))),
        ("Final equity", _format_currency(metrics.get("final_equity"))),
    ]
    columns = st.columns(4)
    for idx, (label, value) in enumerate(metric_items):
        columns[idx % 4].metric(label, value)

    st.markdown("#### Executed Trades")
    render_lwc_chart_with_focus_header(
        chart_options=build_chart_options(),
        series=build_backtest_lwc_series(result),
        chart_key=f"backtest_lwc_{result.symbol}_{result.strategy_name}",
        volume_profile_data=[],
    )

    st.markdown("#### Equity Curve")
    st.plotly_chart(_build_equity_chart(result), use_container_width=True)

    st.markdown("#### Trade Details")
    st.dataframe(_trades_to_frame(result.trades), use_container_width=True, hide_index=True)



def _build_equity_chart(result) -> go.Figure:
    df = pd.DataFrame([point.__dict__ for point in result.equity_curve])
    fig = go.Figure()
    if not df.empty:
        fig.add_trace(go.Scatter(x=df["time"], y=df["equity"], mode="lines", name="Equity", line={"color": "#38d5b5"}))
    fig.update_layout(template="plotly_dark", height=560, margin={"l": 10, "r": 10, "t": 20, "b": 10})
    return fig


def _trades_to_frame(trades: list) -> pd.DataFrame:
    return pd.DataFrame(format_trade_table(trades))


def _infer_schema(default_config: dict) -> dict:
    schema: dict[str, dict] = {}
    for key, value in default_config.items():
        if isinstance(value, bool):
            field_type = "bool"
        elif isinstance(value, int):
            field_type = "int"
        elif isinstance(value, float):
            field_type = "float"
        else:
            field_type = "str"
        schema[key] = {"type": field_type, "label": key.replace("_", " ").title(), "default": value}
    return schema


def _missing_required_fields(metadata: dict, config: dict) -> list[str]:
    schema = dict(metadata.get("config_schema") or {})
    default_config = dict(metadata.get("default_config") or {})
    missing: list[str] = []
    for field_name, spec in schema.items():
        if not spec.get("required"):
            continue
        value = config.get(field_name)
        fallback = spec.get("default", default_config.get(field_name))
        if (value is None or value == "") and (fallback is None or fallback == ""):
            missing.append(field_name)
    return missing


def _reset_strategy_state() -> None:
    for key in list(st.session_state):
        if str(key).startswith("strategy_") or str(key).startswith("bt_primary_"):
            del st.session_state[key]


def _extended_hours_help(requires_extended_hours: bool, supports_extended_hours: bool) -> str:
    if requires_extended_hours:
        return "This strategy requires premarket/after-hours intraday candles, so the option is locked on."
    if supports_extended_hours:
        return "Enable to include premarket and after-hours candles for intraday timeframes."
    return "Optional framework-level data setting. Daily timeframes remain regular-session unless explicitly requested by a strategy."


def _ordered_unique(values: list[str]) -> list[str]:
    output: list[str] = []
    for value in values:
        if value not in output:
            output.append(value)
    return output


def _format_pct(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{float(value) * 100:.2f}%"


def _format_number(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.2f}"


def _format_currency(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"${float(value):,.2f}"
