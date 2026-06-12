from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from data.market_data import clean_price_history_frame
from data.vix_futures import (
    VIX_FUTURES_UNAVAILABLE_MESSAGE,
    calculate_vx_term_structure,
    load_vix_futures_contracts,
    select_vx1_vx2,
)
from data.vx_ratio_history import (
    OBSERVED_LOOKBACK_DAYS,
    PAIR_LOOKBACK_DAYS,
    build_contract_pair_daily_rows,
    make_snapshot_timestamp,
    read_contract_pair_daily,
    read_observed_daily,
    write_contract_pair_daily,
    write_observed_daily,
)


PRICE_HISTORY_DAYS = 220
PRICE_CACHE_SECONDS = 15 * 60


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
    st.markdown(
        "<div class='vix-subtitle'>UVIX Structure &amp; Volatility Risk Snapshot</div>",
        unsafe_allow_html=True,
    )

    today = date.today()
    start_date = today - timedelta(days=PRICE_HISTORY_DAYS)
    end_date = today + timedelta(days=1)
    vix = _load_symbol_history_cached("^VIX", "VIX Spot", start_date.isoformat(), end_date.isoformat())
    uvix = _load_symbol_history_cached("UVIX", "UVIX", start_date.isoformat(), end_date.isoformat())
    ten_year = _load_symbol_history_cached("^TNX", "10Y Treasury Yield", start_date.isoformat(), end_date.isoformat())
    ten_year = _as_treasury_yield(ten_year)
    term_structure = _load_vx_term_structure_cached(today.isoformat())
    vx1 = term_structure.vx1
    vx2 = term_structure.vx2
    ratio = _last_value(term_structure.ratio_df, "ratio")

    _render_market_snapshot_cards(
        vix=vix,
        uvix=uvix,
        ten_year=ten_year,
        vx1=vx1,
        vx2=vx2,
        ratio=ratio,
    )
    _render_structure_signal_panel(term_structure, vx1, vx2)

    kind, history_df = _update_and_load_vx_ratio_history(term_structure, today)
    _render_vx_ratio_history(kind, history_df)

    st.plotly_chart(
        _build_candlestick_chart(vix.data, "VIX Spot", y_title="VIX"),
        use_container_width=True,
    )
    st.plotly_chart(
        _build_candlestick_chart(uvix.data, "UVIX", y_title="UVIX"),
        use_container_width=True,
    )
    st.plotly_chart(
        _build_line_or_candle_chart(ten_year.data, "10Y Treasury Yield", y_title="Yield %"),
        use_container_width=True,
    )


def _render_market_snapshot_cards(
    *,
    vix: PriceSeries,
    uvix: PriceSeries,
    ten_year: PriceSeries,
    vx1: VxContract,
    vx2: VxContract,
    ratio: float | None,
) -> None:
    vix_value = _last_value(vix.data, "close")
    uvix_value = _last_value(uvix.data, "close")
    yield_value = _last_value(ten_year.data, "close")

    cards = [
        ("VIX Spot", _format_number(vix_value), _classify_vix_level(vix_value)),
        ("VX1 Front Month", _format_vx_metric(vx1), _format_vx_caption(vx1)),
        ("VX2 Second Month", _format_vx_metric(vx2), _format_vx_caption(vx2)),
        ("VX1 / VX2", _format_number(ratio, 3), _classify_term_structure_label(ratio)),
        ("UVIX", _format_number(uvix_value), "2x short-term VIX futures"),
        ("10Y Yield", _format_pct_value(yield_value), _classify_yield_pressure(ten_year.data)),
    ]

    st.markdown('<div class="vix-summary-grid">', unsafe_allow_html=True)
    for row_start in range(0, len(cards), 3):
        columns = st.columns(3)
        for column, (label, value, caption) in zip(columns, cards[row_start : row_start + 3], strict=False):
            with column:
                st.metric(label, value, caption)
    st.markdown("</div>", unsafe_allow_html=True)


def _render_structure_signal_panel(term_structure: VxTermStructure, vx1: VxContract, vx2: VxContract) -> None:
    st.markdown("### UVIX Structure Signal")
    ratio = _last_value(term_structure.ratio_df, "ratio")
    if ratio is None or vx1.price is None or vx2.price is None:
        st.markdown(
            "<div class='signal-panel'><div class='signal-title tone-caution'>"
            "Term structure unavailable</div><div class='signal-explain'>"
            "<div class='en'>VIX futures term structure is currently unavailable.</div>"
            "<div class='zh'>当前 VIX 期货期限结构暂不可用。</div></div></div>",
            unsafe_allow_html=True,
        )
        return

    title = _build_structure_signal_title(ratio)
    tone = _signal_tone_class(ratio)
    explain_en, explain_zh = _build_structure_explanation(term_structure, vx1, vx2)
    summary = _build_roll_pressure_summary(term_structure, vx1, vx2)
    interpretation = _build_trading_interpretation(ratio)

    roll_cards = "".join(
        f"<div class='roll-card'><div class='roll-label'>{label}</div>"
        f"<div class='roll-value'>{value}</div></div>"
        for label, value in summary.items()
    )
    trade_rows = "".join(
        f"<div class='trade-row'><div class='trade-en'>{en}</div>"
        f"<div class='trade-zh'>{zh}</div></div>"
        for en, zh in interpretation
    )

    st.markdown(
        f"""
        <div class='signal-panel'>
            <div class='signal-title {tone}'>{title}</div>
            <div class='signal-explain'>
                <div class='en'>{explain_en}</div>
                <div class='zh'>{explain_zh}</div>
            </div>
            <div class='rollgrid'>{roll_cards}</div>
            <div class='trade-block'>
                <div class='trade-head'>Trading Interpretation · 交易解读</div>
                {trade_rows}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _classify_vix_level(vix_value: float | None) -> str:
    if vix_value is None:
        return "Unavailable"
    if vix_value < 15:
        return "Low volatility"
    if vix_value < 20:
        return "Mildly elevated"
    if vix_value < 30:
        return "Elevated risk"
    return "Stress / panic"


def _classify_term_structure_label(ratio: float | None) -> str:
    if ratio is None:
        return "Unavailable"
    if ratio < 0.95:
        return "Strong Contango"
    if ratio < 0.99:
        return "Contango"
    if ratio <= 1.01:
        return "Flat"
    return "Backwardation"


def _classify_yield_pressure(ten_year_df: pd.DataFrame) -> str:
    if ten_year_df is None or ten_year_df.empty or "close" not in ten_year_df.columns:
        return "Rates stable"
    closes = pd.to_numeric(ten_year_df["close"], errors="coerce").dropna()
    if len(closes) < 6:
        return "Rates stable"
    latest = float(closes.iloc[-1])
    prior = float(closes.iloc[-6])
    if latest > prior:
        return "Rates pressure rising"
    if latest < prior:
        return "Rates pressure easing"
    return "Rates stable"


def _build_structure_signal_title(ratio: float | None) -> str:
    if ratio is None:
        return "Term structure unavailable"
    if ratio < 0.95:
        return "Strong Contango: UVIX structure is unfavourable"
    if ratio < 0.99:
        return "Contango: UVIX has negative roll pressure"
    if ratio <= 1.01:
        return "Flat Curve: volatility risk is rising"
    return "Backwardation: front-end volatility stress is priced in"


def _signal_tone_class(ratio: float | None) -> str:
    if ratio is None:
        return "tone-caution"
    if ratio < 0.95:
        return "tone-bad"
    if ratio < 0.99:
        return "tone-warn"
    if ratio <= 1.01:
        return "tone-caution"
    return "tone-good"


def _build_structure_explanation(
    term_structure: VxTermStructure, vx1: VxContract, vx2: VxContract
) -> tuple[str, str]:
    ratio = _last_value(term_structure.ratio_df, "ratio")
    if ratio is None or vx1.price is None or vx2.price is None:
        return (
            "VIX futures term structure is currently unavailable.",
            "当前 VIX 期货期限结构暂不可用。",
        )
    contango_pct = term_structure.contango_pct if term_structure.contango_pct is not None else 0.0
    backwardation_pct = (
        term_structure.backwardation_pct if term_structure.backwardation_pct is not None else 0.0
    )
    if 0.99 <= ratio <= 1.01:
        return (
            "VX1 and VX2 are close to flat. The market may be transitioning from "
            "calm contango toward higher near-term volatility risk.",
            "近月和次月 VIX 期货接近平坦，说明市场可能正在从平静结构转向更高的短期波动风险。",
        )
    if ratio < 0.99:
        return (
            f"VX1 is {contango_pct:.2f}% cheaper than VX2. This means the VIX futures "
            "curve is in contango. UVIX faces negative roll pressure unless volatility "
            "expands quickly.",
            f"近月 VIX 期货比次月便宜约 {contango_pct:.2f}%，说明曲线处于 Contango。"
            "UVIX 会受到负展期压力，除非波动率快速扩张。",
        )
    return (
        f"VX1 is {backwardation_pct:.2f}% more expensive than VX2. This means the front "
        "end of the VIX futures curve is stressed. UVIX has a more favourable short-term "
        "structure, but reversal risk is high.",
        f"近月 VIX 期货比次月贵约 {backwardation_pct:.2f}%，说明短端波动率压力较高。"
        "UVIX 短期结构更友好，但恐慌回落风险也更高。",
    )


def _build_roll_pressure_summary(
    term_structure: VxTermStructure, vx1: VxContract, vx2: VxContract
) -> dict[str, str]:
    ratio = _last_value(term_structure.ratio_df, "ratio")
    contango = _format_number(term_structure.contango_pct, 2)
    backwardation = _format_number(term_structure.backwardation_pct, 2)
    if ratio is None:
        roll_pressure = "Neutral"
        structure = "Unavailable"
    elif ratio < 0.95:
        roll_pressure = "Negative"
        structure = "Unfavourable"
    elif ratio < 0.99:
        roll_pressure = "Negative"
        structure = "Mildly unfavourable"
    elif ratio <= 1.01:
        roll_pressure = "Neutral"
        structure = "Neutral / watch"
    else:
        roll_pressure = "Positive"
        structure = "Favourable short-term"
    return {
        "Contango": f"{contango}%",
        "Backwardation": f"{backwardation}%",
        "Roll Pressure": roll_pressure,
        "UVIX Structure": structure,
    }


def _build_trading_interpretation(ratio: float | None) -> list[tuple[str, str]]:
    if ratio is None:
        return []
    if ratio < 0.95:
        return [
            (
                "Intraday spike trade: possible only if VIX/VX1 expands quickly.",
                "日内冲击交易：只有 VIX / VX1 快速扩张时才有优势。",
            ),
            ("Overnight hold: structurally unfavourable.", "隔夜持有：结构不友好。"),
            (
                "Multi-day hold: high decay risk unless the curve flattens.",
                "多日持有：除非曲线变平，否则损耗风险高。",
            ),
        ]
    if ratio < 0.99:
        return [
            ("Intraday spike trade: possible.", "日内交易：可以，但需要波动率继续扩张。"),
            ("Overnight hold: still has roll-cost drag.", "隔夜持有：仍有展期拖累。"),
            (
                "Multi-day hold: requires curve flattening or rising VIX.",
                "多日持有：需要曲线变平或 VIX 继续上升。",
            ),
        ]
    if ratio <= 1.01:
        return [
            ("Intraday trade: volatility risk is active.", "日内交易：波动率风险已经活跃。"),
            ("Overnight hold: less roll drag than contango.", "隔夜持有：展期拖累比 Contango 小。"),
            (
                "Multi-day hold: watch for backwardation or IV crush.",
                "多日持有：关注是否进入 Backwardation 或事件后 IV crush。",
            ),
        ]
    return [
        ("Intraday trade: structure supports UVIX.", "日内交易：结构支持 UVIX。"),
        (
            "Overnight hold: more favourable but reversal risk is high.",
            "隔夜持有：结构更友好，但反转风险高。",
        ),
        ("Multi-day hold: only if stress continues.", "多日持有：只有风险压力持续时才适合。"),
    ]


def _update_and_load_vx_ratio_history(
    term_structure: VxTermStructure, today: date
) -> tuple[str | None, pd.DataFrame]:
    """Record today's observed VX1/VX2 ratio, then return the best available history.

    Prefers the current contract pair's daily history; falls back to the observed
    rolling history. Returns ("pair" | "observed" | None, history_df).
    """
    ratio = _last_value(term_structure.ratio_df, "ratio")
    vx1 = term_structure.vx1
    vx2 = term_structure.vx2

    if ratio is not None and vx1.price is not None and vx2.price is not None:
        write_observed_daily(
            {
                "date": today.isoformat(),
                "timestamp": make_snapshot_timestamp(),
                "vx1_symbol": vx1.symbol,
                "vx1_expiry": vx1.expiry,
                "vx1_price": vx1.price,
                "vx2_symbol": vx2.symbol,
                "vx2_expiry": vx2.expiry,
                "vx2_price": vx2.price,
                "ratio": ratio,
                "contango_pct": term_structure.contango_pct,
                "backwardation_pct": term_structure.backwardation_pct,
                "status": term_structure.status,
            }
        )
        pair_rows = build_contract_pair_daily_rows(
            vx1_symbol=vx1.symbol,
            vx1_expiry=vx1.expiry,
            vx2_symbol=vx2.symbol,
            vx2_expiry=vx2.expiry,
            vx1_prices=pd.DataFrame({"date": [today], "close": [vx1.price]}),
            vx2_prices=pd.DataFrame({"date": [today], "close": [vx2.price]}),
        )
        write_contract_pair_daily(pair_rows)

        pair_df = read_contract_pair_daily(
            vx1.symbol, vx2.symbol, lookback_days=PAIR_LOOKBACK_DAYS, today=today
        )
        if len(pair_df) >= 2:
            return "pair", pair_df

    observed_df = read_observed_daily(lookback_days=OBSERVED_LOOKBACK_DAYS, today=today)
    if len(observed_df) >= 2:
        return "observed", observed_df
    return None, observed_df


def _render_vx_ratio_history(kind: str | None, history_df: pd.DataFrame) -> None:
    st.markdown("### Observed VX1 / VX2 Ratio History")
    if kind == "pair":
        title = "Current VX1 / VX2 Contract Pair Ratio History"
        caption = "该线图展示当前 VX1/VX2 这组具体合约 pair 的日级别 ratio history。"
    elif kind == "observed":
        title = "Observed Rolling VX1 / VX2 Ratio History"
        caption = "该线图展示 dashboard 实际观察并保存的日级别 rolling VX1/VX2 ratio。"
    else:
        st.info(
            "Not enough VX1/VX2 ratio history yet. "
            "Run the dashboard again later to build this line chart."
        )
        return
    st.plotly_chart(
        _build_observed_vx_ratio_history_line_chart(history_df, title=title),
        use_container_width=True,
    )
    st.caption(caption)


def _build_observed_vx_ratio_history_line_chart(history_df: pd.DataFrame, *, title: str):
    import plotly.graph_objects as go

    customdata = history_df[["vx1_symbol", "vx1_price", "vx2_symbol", "vx2_price", "status"]].to_numpy()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=history_df["date"],
            y=history_df["ratio"],
            mode="lines+markers",
            name="VX1 / VX2",
            line={"color": "#f59e0b", "width": 2},
            marker={"size": 7, "color": "#f59e0b", "line": {"color": "rgba(255,255,255,0.75)", "width": 1}},
            customdata=customdata,
            hovertemplate=(
                "Date: %{x|%Y-%m-%d}<br>"
                "Ratio: %{y:.3f}<br>"
                "VX1: %{customdata[0]} %{customdata[1]:.2f}<br>"
                "VX2: %{customdata[2]} %{customdata[3]:.2f}<br>"
                "Status: %{customdata[4]}<extra></extra>"
            ),
        )
    )

    reference_lines = [
        (0.95, "Strong Contango", "rgba(148,163,184,0.6)"),
        (0.99, "Contango / Flat", "rgba(148,163,184,0.6)"),
        (1.00, "Flat", "rgba(255,255,255,0.45)"),
        (1.01, "Backwardation", "rgba(239,68,68,0.65)"),
    ]
    for level, label, color in reference_lines:
        fig.add_hline(
            y=level,
            line_dash="solid" if level == 1.00 else "dot",
            line_color=color,
            annotation_text=label,
            annotation_position="right",
        )

    _apply_chart_layout(fig, title, y_title="VX1 / VX2 Ratio", rangeslider=False)
    fig.update_xaxes(title="Date")
    return fig


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


def _vx_contract_from_row(row: pd.Series, slot: str, source: str) -> VxContract:
    price = _safe_float(row.get("last"))
    if price is None:
        price = _safe_float(row.get("close"))
    return VxContract(
        slot=slot,
        symbol=str(row.get("symbol") or slot),
        expiry=_format_expiry(row.get("expiry")),
        price=price,
        source=source,
    )


def _unavailable_vx_contract(slot: str, *, warning: str) -> VxContract:
    return VxContract(
        slot=slot,
        symbol="unavailable",
        expiry="unavailable",
        price=None,
        source="unavailable",
        warning=warning,
    )


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
    return f"{contract.symbol} {_format_number(contract.price)}"


def _format_vx_caption(contract: VxContract) -> str:
    if contract.symbol == "unavailable":
        return "no contract-level data"
    return f"Expiry: {contract.expiry}"


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
    .vix-subtitle {
        color: #9fb3c8;
        font-size: 0.95rem;
        font-weight: 600;
        letter-spacing: 0.3px;
        margin: -0.2rem 0 1.1rem 0;
    }
    .vix-summary-grid [data-testid="stMetric"] {
        min-height: 118px;
    }
    .signal-panel {
        border: 1px solid rgba(148, 163, 184, 0.22);
        background: rgba(13, 20, 32, 0.82);
        border-radius: 12px;
        padding: 1.1rem 1.25rem;
        margin: 0.4rem 0 1.4rem 0;
    }
    .signal-title {
        font-size: 1.12rem;
        font-weight: 800;
        line-height: 1.35;
        padding-left: 0.7rem;
        border-left: 4px solid #5aa7ff;
        margin-bottom: 0.8rem;
    }
    .signal-title.tone-bad { color: #ff6b6b; border-left-color: #ef4444; }
    .signal-title.tone-warn { color: #fbbf24; border-left-color: #f59e0b; }
    .signal-title.tone-caution { color: #5aa7ff; border-left-color: #5aa7ff; }
    .signal-title.tone-good { color: #38d5b5; border-left-color: #38d5b5; }
    .signal-explain { margin-bottom: 1rem; }
    .signal-explain .en { color: #e5edf6; line-height: 1.55; }
    .signal-explain .zh {
        color: #9fb3c8;
        line-height: 1.6;
        font-size: 0.92rem;
        margin-top: 0.3rem;
    }
    .rollgrid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 0.6rem;
        margin-bottom: 1.1rem;
    }
    .roll-card {
        border: 1px solid rgba(148, 163, 184, 0.18);
        background: rgba(5, 7, 13, 0.55);
        border-radius: 8px;
        padding: 0.6rem 0.7rem;
    }
    .roll-label {
        color: #9fb3c8;
        font-size: 0.74rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.4px;
    }
    .roll-value {
        color: #ffffff;
        font-size: 1.05rem;
        font-weight: 800;
        margin-top: 0.25rem;
    }
    .trade-block {
        border-top: 1px solid rgba(148, 163, 184, 0.16);
        padding-top: 0.85rem;
    }
    .trade-head {
        color: #d7e2ee;
        font-weight: 800;
        font-size: 0.9rem;
        letter-spacing: 0.3px;
        margin-bottom: 0.55rem;
    }
    .trade-row {
        display: flex;
        flex-direction: column;
        padding: 0.4rem 0;
        border-bottom: 1px dashed rgba(148, 163, 184, 0.12);
    }
    .trade-row:last-child { border-bottom: 0; }
    .trade-en { color: #e5edf6; font-weight: 600; }
    .trade-zh { color: #9fb3c8; font-size: 0.88rem; margin-top: 0.15rem; }
    @media (max-width: 900px) {
        .rollgrid { grid-template-columns: repeat(2, 1fr); }
    }
    </style>
    """
