from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st

from data.market_data import clean_price_history_frame
from data.vix_futures import (
    DEFAULT_VIX_FUTURES_CONTRACTS_CSV,
    VIX_FUTURES_UNAVAILABLE_MESSAGE,
    calculate_vx_term_structure,
    load_vix_futures_contracts,
    select_vx1_vx2,
)


PRICE_HISTORY_DAYS = 220
PRICE_CACHE_SECONDS = 15 * 60
RATIO_FLAT_BAND = 0.01


@dataclass(frozen=True)
class PriceSeries:
    symbol: str
    label: str
    data: pd.DataFrame
    source: str
    warning: str | None = None


@dataclass(frozen=True)
class VxContract:
    slot: str
    symbol: str
    expiry: str
    price: float | None
    data: pd.DataFrame
    source: str
    warning: str | None = None


@dataclass(frozen=True)
class VxTermStructure:
    vx1: VxContract
    vx2: VxContract
    ratio_df: pd.DataFrame
    status: str
    contango_pct: float | None
    backwardation_pct: float | None
    warning: str | None
    source: str


def render_vix_dashboard() -> None:
    st.markdown(_vix_css(), unsafe_allow_html=True)
    st.markdown("## VIX / UVIX Monitor")
    _render_manual_import_controls()

    today = date.today()
    start_date = today - timedelta(days=PRICE_HISTORY_DAYS)
    end_date = today + timedelta(days=1)
    vix = _load_symbol_history_cached("^VIX", "VIX Spot", start_date.isoformat(), end_date.isoformat())
    uvix = _load_symbol_history_cached("UVIX", "UVIX", start_date.isoformat(), end_date.isoformat())
    spy = _load_symbol_history_cached("SPY", "SPY", start_date.isoformat(), end_date.isoformat())
    ten_year = _load_symbol_history_cached("^TNX", "10Y Treasury Yield", start_date.isoformat(), end_date.isoformat())
    ten_year = _as_treasury_yield(ten_year)
    term_structure = _load_vx_term_structure_cached(today.isoformat())
    vx1 = term_structure.vx1
    vx2 = term_structure.vx2
    ratio_df = term_structure.ratio_df

    _render_vix_futures_warning(term_structure)
    _render_top_summary(
        vix=vix,
        uvix=uvix,
        ten_year=ten_year,
        vx1=vx1,
        vx2=vx2,
        term_structure=term_structure,
        ratio_df=ratio_df,
        spy=spy,
        today=today,
    )
    _render_data_warnings([vix, uvix, ten_year, spy, vx1, vx2])

    st.plotly_chart(
        _build_candlestick_chart(vix.data, "VIX spot", y_title="VIX"),
        use_container_width=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(
            _build_candlestick_chart(vx1.data, _vx_chart_title(vx1), y_title="VX1"),
            use_container_width=True,
        )
    with col2:
        st.plotly_chart(
            _build_candlestick_chart(vx2.data, _vx_chart_title(vx2), y_title="VX2"),
            use_container_width=True,
        )

    st.plotly_chart(_build_ratio_chart(ratio_df), use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(
            _build_candlestick_chart(uvix.data, "UVIX price", y_title="UVIX"),
            use_container_width=True,
        )
    with col4:
        st.plotly_chart(
            _build_line_or_candle_chart(ten_year.data, "10-year Treasury yield", y_title="Yield %"),
            use_container_width=True,
        )


def _render_top_summary(
    *,
    vix: PriceSeries,
    uvix: PriceSeries,
    ten_year: PriceSeries,
    vx1: VxContract,
    vx2: VxContract,
    term_structure: VxTermStructure,
    ratio_df: pd.DataFrame,
    spy: PriceSeries,
    today: date,
) -> None:
    ratio_value = _last_value(ratio_df, "ratio")
    status = term_structure.status
    regime_summary = _build_regime_summary(term_structure_status=status)

    metrics = [
        ("Current VIX", _format_number(_last_value(vix.data, "close")), vix.source),
        ("Current VX1", _format_vx_metric(vx1), _format_vx_caption(vx1)),
        ("Current VX2", _format_vx_metric(vx2), _format_vx_caption(vx2)),
        ("VX1 / VX2 ratio", _format_number(ratio_value, 3), _format_term_structure_caption(term_structure)),
        ("UVIX price", _format_number(_last_value(uvix.data, "close")), uvix.source),
        ("10Y yield", _format_pct_value(_last_value(ten_year.data, "close")), ten_year.source),
    ]

    st.markdown('<div class="vix-summary-grid">', unsafe_allow_html=True)
    for row_start in range(0, len(metrics), 4):
        columns = st.columns(4)
        for column, (label, value, caption) in zip(columns, metrics[row_start : row_start + 4], strict=False):
            with column:
                st.metric(label, value, caption)
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='regime-summary'><b>Regime summary</b><br>{regime_summary}</div>", unsafe_allow_html=True)


def _render_manual_import_controls() -> None:
    with st.expander("Manual CSV import", expanded=False):
        st.caption("VIX futures schema: symbol,expiry,last,open,high,low,close,source,timestamp")
        futures_upload = st.file_uploader(
            "Import vix_futures_contracts.csv",
            type=["csv"],
            key="vix_futures_upload",
        )
        if futures_upload is not None and st.button("Save vix_futures_contracts.csv"):
            DEFAULT_VIX_FUTURES_CONTRACTS_CSV.write_bytes(futures_upload.getvalue())
            st.cache_data.clear()
            st.success(f"Saved {DEFAULT_VIX_FUTURES_CONTRACTS_CSV}")


def _render_vix_futures_warning(term_structure: VxTermStructure) -> None:
    if term_structure.warning:
        st.warning(term_structure.warning)
    st.caption(f"VIX futures term structure source: {term_structure.source}")


def _render_data_warnings(items: list[PriceSeries | VxContract]) -> None:
    warnings = [item.warning for item in items if item.warning]
    if not warnings:
        return
    with st.expander("Data warnings", expanded=False):
        for warning in warnings:
            st.warning(warning)


@st.cache_data(ttl=PRICE_CACHE_SECONDS, show_spinner=False)
def _load_symbol_history_cached(symbol: str, label: str, start_date: str, end_date: str) -> PriceSeries:
    start = pd.Timestamp(start_date).date()
    end = pd.Timestamp(end_date).date()
    openbb_df, openbb_error = _fetch_openbb_price_history(symbol, start, end)
    if openbb_df is not None and not openbb_df.empty:
        return PriceSeries(symbol=symbol, label=label, data=openbb_df, source="OpenBB")

    yf_df, yf_error = _fetch_yfinance_history(symbol, start, end)
    warning_parts = [part for part in [openbb_error, yf_error] if part]
    warning = "; ".join(warning_parts) if warning_parts else None
    return PriceSeries(symbol=symbol, label=label, data=yf_df, source="yfinance" if not yf_df.empty else "empty", warning=warning)


@st.cache_data(ttl=PRICE_CACHE_SECONDS, show_spinner=False)
def _load_vx_term_structure_cached(today: str) -> VxTermStructure:
    return _load_vx_term_structure(pd.Timestamp(today).date())


def _fetch_openbb_price_history(symbol: str, start_date: date, end_date: date) -> tuple[pd.DataFrame | None, str | None]:
    try:
        from openbb import obb
    except Exception as error:
        return None, f"OpenBB price fetch unavailable for {symbol}: {error}"

    call_specs = [
        ("equity.price.historical", lambda: obb.equity.price.historical),
        ("index.price.historical", lambda: obb.index.price.historical),
    ]
    errors: list[str] = []
    for name, getter in call_specs:
        for kwargs in (
            {"provider": "yfinance"},
            {},
        ):
            try:
                func = getter()
                result = func(
                    symbol=symbol,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    interval="1d",
                    **kwargs,
                )
                df = _to_ohlcv_frame(result)
                if not df.empty:
                    return df, None
            except Exception as error:
                errors.append(f"{name} {kwargs or 'default'}: {error}")
    return None, f"OpenBB price fetch failed for {symbol}: {'; '.join(errors[:2])}"


def _fetch_yfinance_history(symbol: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str | None]:
    try:
        import yfinance as yf

        raw = yf.download(
            symbol,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            interval="1d",
            auto_adjust=False,
            progress=False,
            group_by="column",
            threads=False,
        )
        df = _normalize_yfinance_frame(raw)
        return df, None if not df.empty else f"yfinance returned no rows for {symbol}"
    except Exception as error:
        return pd.DataFrame(), f"yfinance fetch failed for {symbol}: {error}"


def _load_vx_term_structure(today: date) -> VxTermStructure:
    result = load_vix_futures_contracts(today=today)
    vx1_row, vx2_row = select_vx1_vx2(result.contracts, today=today)
    term = calculate_vx_term_structure(vx1_row, vx2_row)

    if vx1_row is None or vx2_row is None or term["vx1_vx2_ratio"] is None:
        warning = result.warning or VIX_FUTURES_UNAVAILABLE_MESSAGE
        return VxTermStructure(
            vx1=_unavailable_vx_contract("VX1", warning=warning),
            vx2=_unavailable_vx_contract("VX2", warning=warning),
            ratio_df=pd.DataFrame(columns=["date", "ratio", "vx1", "vx2"]),
            status="Term structure unavailable",
            contango_pct=None,
            backwardation_pct=None,
            warning=warning,
            source=result.source,
        )

    vx1 = _vx_contract_from_row(vx1_row, "VX1", result.source)
    vx2 = _vx_contract_from_row(vx2_row, "VX2", result.source)
    ratio_df = pd.DataFrame(
        {
            "date": [pd.Timestamp(today)],
            "ratio": [term["vx1_vx2_ratio"]],
            "vx1": [term["vx1_price"]],
            "vx2": [term["vx2_price"]],
        }
    )
    return VxTermStructure(
        vx1=vx1,
        vx2=vx2,
        ratio_df=ratio_df,
        status=str(term["status"]),
        contango_pct=_safe_float(term["contango_pct"]),
        backwardation_pct=_safe_float(term["backwardation_pct"]),
        warning=result.warning,
        source=result.source,
    )


def _build_candlestick_chart(df: pd.DataFrame, title: str, *, y_title: str):
    import plotly.graph_objects as go

    fig = go.Figure()
    if _has_ohlc(df):
        fig.add_trace(
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name=title,
                increasing_line_color="#38d5b5",
                decreasing_line_color="#ef4444",
            )
        )
    elif not df.empty and "close" in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df["close"], mode="lines", name=title, line={"color": "#38d5b5"}))
    else:
        fig.add_annotation(text="No data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)

    _apply_chart_layout(fig, title, y_title=y_title, rangeslider=False)
    return fig


def _build_line_or_candle_chart(df: pd.DataFrame, title: str, *, y_title: str):
    import plotly.graph_objects as go

    fig = go.Figure()
    if not df.empty and "close" in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df["close"], mode="lines", name=title, line={"color": "#5aa7ff", "width": 2}))
    else:
        fig.add_annotation(text="No data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
    _apply_chart_layout(fig, title, y_title=y_title, rangeslider=False)
    return fig


def _build_ratio_chart(ratio_df: pd.DataFrame):
    import plotly.graph_objects as go

    latest_ratio = _last_value(ratio_df, "ratio")
    status = _contango_status(latest_ratio)
    fig = go.Figure()
    if not ratio_df.empty:
        fig.add_trace(
            go.Scatter(
                x=ratio_df["date"],
                y=ratio_df["ratio"],
                mode="lines",
                name="VX1 / VX2",
                line={"color": "#f59e0b", "width": 2},
            )
        )
    else:
        fig.add_annotation(text="No ratio data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)

    fig.add_hline(y=1 - RATIO_FLAT_BAND, line_dash="dot", line_color="rgba(148,163,184,0.6)", annotation_text="Contango")
    fig.add_hline(y=1, line_dash="solid", line_color="rgba(255,255,255,0.45)", annotation_text="Flat")
    fig.add_hline(y=1 + RATIO_FLAT_BAND, line_dash="dot", line_color="rgba(239,68,68,0.65)", annotation_text="Backwardation")
    _apply_chart_layout(fig, f"VX1 / VX2 ratio - {status}", y_title="Ratio", rangeslider=False)
    return fig


def _apply_chart_layout(fig: Any, title: str, *, y_title: str, rangeslider: bool, height: int = 520) -> None:
    fig.update_layout(
        template="plotly_dark",
        title={"text": title, "x": 0.01, "xanchor": "left"},
        height=height,
        margin={"l": 42, "r": 18, "t": 58, "b": 38},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(5,7,13,0.78)",
        hovermode="closest",
        showlegend=False,
        font={"color": "#e5edf6"},
        xaxis={"showgrid": False, "rangeslider": {"visible": rangeslider}},
        yaxis={"title": y_title, "gridcolor": "rgba(148,163,184,0.14)", "zeroline": False},
    )


def _build_ratio_frame(vx1_df: pd.DataFrame, vx2_df: pd.DataFrame) -> pd.DataFrame:
    if vx1_df.empty or vx2_df.empty or "close" not in vx1_df.columns or "close" not in vx2_df.columns:
        return pd.DataFrame(columns=["date", "ratio"])
    left = vx1_df[["date", "close"]].rename(columns={"close": "vx1"})
    right = vx2_df[["date", "close"]].rename(columns={"close": "vx2"})
    merged = left.merge(right, on="date", how="inner")
    merged["ratio"] = merged["vx1"] / merged["vx2"].replace(0, np.nan)
    return merged.dropna(subset=["ratio"]).sort_values("date", kind="stable").reset_index(drop=True)


def _vx_contract_from_row(row: pd.Series, slot: str, source: str) -> VxContract:
    price = _safe_float(row.get("last"))
    if price is None:
        price = _safe_float(row.get("close"))
    timestamp = pd.to_datetime(row.get("timestamp"), errors="coerce")
    if pd.isna(timestamp):
        timestamp = pd.to_datetime(row.get("expiry"), errors="coerce")
    open_price = _safe_float(row.get("open"))
    high_price = _safe_float(row.get("high"))
    low_price = _safe_float(row.get("low"))
    data = pd.DataFrame(
        {
            "date": [timestamp],
            "open": [open_price if open_price is not None else price],
            "high": [high_price if high_price is not None else price],
            "low": [low_price if low_price is not None else price],
            "close": [price],
        }
    )
    return VxContract(
        slot=slot,
        symbol=str(row.get("symbol") or slot),
        expiry=_format_expiry(row.get("expiry")),
        price=price,
        data=data.dropna(subset=["date", "close"]),
        source=source,
    )


def _unavailable_vx_contract(slot: str, *, warning: str) -> VxContract:
    return VxContract(
        slot=slot,
        symbol="unavailable",
        expiry="unavailable",
        price=None,
        data=pd.DataFrame(columns=["date", "open", "high", "low", "close"]),
        source="unavailable",
        warning=warning,
    )


def _build_regime_summary(*, term_structure_status: str) -> str:
    return term_structure_status


def _as_treasury_yield(series: PriceSeries) -> PriceSeries:
    df = series.data.copy()
    if df.empty or "close" not in df.columns:
        return series
    median_close = pd.to_numeric(df["close"], errors="coerce").median()
    if pd.notna(median_close) and median_close > 15:
        for column in ["open", "high", "low", "close"]:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce") / 10.0
        return PriceSeries(series.symbol, series.label, df, series.source, series.warning)
    return series


def _normalize_yfinance_frame(raw: pd.DataFrame | None) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(column[0]) if str(column[0]) else str(column[-1]) for column in df.columns]
    df = clean_price_history_frame(df)
    return df


def _to_ohlcv_frame(result: Any) -> pd.DataFrame:
    raw = _to_dataframe(result)
    if raw is None or raw.empty:
        return pd.DataFrame()
    return clean_price_history_frame(raw)


def _to_dataframe(result: Any) -> pd.DataFrame | None:
    if result is None:
        return None
    if hasattr(result, "to_dataframe"):
        return result.to_dataframe()
    if hasattr(result, "to_df"):
        return result.to_df()
    if hasattr(result, "results"):
        return pd.DataFrame(_records_from_results(getattr(result, "results")))
    if isinstance(result, pd.DataFrame):
        return result
    try:
        return pd.DataFrame(result)
    except Exception:
        return None


def _records_from_results(results: Any) -> list[Any]:
    if results is None:
        return []
    try:
        rows = results if isinstance(results, list) else list(results)
    except TypeError:
        rows = [results]
    records: list[Any] = []
    for item in rows:
        if hasattr(item, "model_dump"):
            records.append(item.model_dump())
        elif hasattr(item, "dict"):
            records.append(item.dict())
        else:
            records.append(item)
    return records


def _has_ohlc(df: pd.DataFrame) -> bool:
    return not df.empty and {"date", "open", "high", "low", "close"}.issubset(df.columns)


def _contango_status(ratio: float | None) -> str:
    if ratio is None or pd.isna(ratio):
        return "Term structure unavailable"
    if ratio < 1 - RATIO_FLAT_BAND:
        return "Contango"
    if ratio > 1 + RATIO_FLAT_BAND:
        return "Backwardation"
    return "Flat"


def _vx_chart_title(contract: VxContract) -> str:
    if contract.symbol == "unavailable":
        return f"{contract.slot} unavailable"
    return f"{contract.slot} {contract.symbol} - expiry {contract.expiry}"


def _format_expiry(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "unavailable"
    return pd.Timestamp(ts).strftime("%Y-%m-%d")


def _last_value(df: pd.DataFrame, column: str) -> float | None:
    if df is None or df.empty or column not in df.columns:
        return None
    values = pd.to_numeric(df[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.iloc[-1])


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _format_vx_metric(contract: VxContract) -> str:
    if contract.symbol == "unavailable":
        return "unavailable"
    price = contract.price if contract.price is not None else _last_value(contract.data, "close")
    return f"{contract.symbol} {_format_number(price)}"


def _format_vx_caption(contract: VxContract) -> str:
    if contract.symbol == "unavailable":
        return "no contract-level data"
    return f"Expiry: {contract.expiry}"


def _format_term_structure_caption(term_structure: VxTermStructure) -> str:
    if term_structure.status == "Term structure unavailable":
        return term_structure.status
    contango = _format_number(term_structure.contango_pct, 2)
    backwardation = _format_number(term_structure.backwardation_pct, 2)
    return (
        f"{term_structure.status} | "
        f"contango {contango}% | "
        f"backwardation {backwardation}%"
    )


def _format_number(value: float | None, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.{decimals}f}"


def _format_pct_value(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2f}%"


def _vix_css() -> str:
    return """
    <style>
    .vix-summary-grid [data-testid="stMetric"] {
        min-height: 118px;
    }
    .regime-summary {
        border: 1px solid rgba(148, 163, 184, 0.22);
        background: rgba(13, 20, 32, 0.78);
        border-radius: 8px;
        padding: 0.9rem 1rem;
        margin: 0.3rem 0 1rem 0;
        color: #e5edf6;
        line-height: 1.55;
    }
    </style>
    """
