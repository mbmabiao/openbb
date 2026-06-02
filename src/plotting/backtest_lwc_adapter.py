from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from backtesting.schema import BacktestResult
from plotting.chart_builder import to_lwc_time


OVERLAY_COLORS = (
    "#f59e0b",
    "#5aa7ff",
    "#a78bfa",
    "#f472b6",
    "#22d3ee",
    "#eab308",
    "#fb7185",
    "#38d5b5",
)
EXTREME_ABS_LIMIT = 1e18


def build_backtest_lwc_series(result: BacktestResult) -> list[dict]:
    df = _normalize_ohlcv(result.signals)
    if df.empty:
        return []

    candle_data = _build_candles(df)
    series = [
        {
            "type": "Candlestick",
            "data": candle_data,
            "options": {
                "upColor": "#fb7185",
                "downColor": "#38d5b5",
                "borderUpColor": "#fb7185",
                "borderDownColor": "#38d5b5",
                "wickUpColor": "#fb7185",
                "wickDownColor": "#38d5b5",
                "priceLineVisible": True,
            },
            "pattern_event_markers": _build_trade_markers(result.trades),
        },
        {
            "type": "Histogram",
            "data": _build_volume(df),
            "options": {
                "priceFormat": {"type": "volume"},
                "priceScaleId": "volume",
            },
            "priceScale": {
                "scaleMargins": {
                    "top": 0.82,
                    "bottom": 0.0,
                }
            },
        },
    ]

    for index, column in enumerate(column for column in df.columns if column.startswith("plot_")):
        line_data = _build_safe_overlay(df, column)
        if not any("value" in point for point in line_data):
            continue

        color = OVERLAY_COLORS[index % len(OVERLAY_COLORS)]
        label = column.replace("plot_", "").replace("_", " ").strip() or column
        series.append(
            {
                "type": "Line",
                "data": line_data,
                "overlay_label": {
                    "text": label,
                    "color": color,
                    "labelOnChart": False,
                    "showInLegend": True,
                },
                "options": {
                    "lineWidth": 2,
                    "priceLineVisible": False,
                    "lastValueVisible": False,
                    "color": color,
                    "lineStyle": 0,
                },
            }
        )

    return series


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    required = ["date", "open", "high", "low", "close"]
    if df is None or df.empty or not set(required).issubset(df.columns):
        return pd.DataFrame()

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    if "volume" not in out.columns:
        out["volume"] = 0.0

    out = out.replace([np.inf, -np.inf], np.nan)
    out = out.dropna(subset=required).sort_values("date", kind="stable").reset_index(drop=True)
    return out


def _build_candles(df: pd.DataFrame) -> list[dict]:
    prev_close = df["close"].shift(1).replace(0, np.nan)
    change_pct = (df["close"] - prev_close) / prev_close

    candles: list[dict] = []
    for idx, row in df.iterrows():
        candles.append(
            {
                "time": to_lwc_time(row["date"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "change_pct": float(change_pct.iloc[idx]) if pd.notna(change_pct.iloc[idx]) else None,
            }
        )
    return candles


def _build_volume(df: pd.DataFrame) -> list[dict]:
    return [
        {
            "time": to_lwc_time(row["date"]),
            "value": float(row["volume"]) if pd.notna(row["volume"]) else 0.0,
            "color": "rgba(251, 113, 133, 0.62)"
            if float(row["close"]) >= float(row["open"])
            else "rgba(56, 213, 181, 0.62)",
        }
        for _, row in df.iterrows()
    ]


def _build_safe_overlay(df: pd.DataFrame, column: str) -> list[dict]:
    values = pd.to_numeric(df[column], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")

    finite_lows = low[np.isfinite(low)]
    finite_highs = high[np.isfinite(high)]
    finite_closes = close[np.isfinite(close)]
    if finite_lows.empty or finite_highs.empty:
        return []

    price_low = float(finite_lows.min())
    price_high = float(finite_highs.max())
    price_range = max(price_high - price_low, 1e-9)
    price_center = float(finite_closes.median()) if not finite_closes.empty else (price_low + price_high) / 2.0
    padding = max(price_range * 5.0, abs(price_center) * 0.5, 1e-9)
    lower_bound = price_low - padding
    upper_bound = price_high + padding

    line_data: list[dict] = []
    for time_value, value in zip(df["date"], values, strict=False):
        point_time = to_lwc_time(time_value)
        if _is_safe_overlay_value(value, lower_bound=lower_bound, upper_bound=upper_bound):
            line_data.append({"time": point_time, "value": float(value)})
        else:
            line_data.append({"time": point_time})
    return line_data


def _is_safe_overlay_value(value: Any, *, lower_bound: float, upper_bound: float) -> bool:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    if not np.isfinite(numeric):
        return False
    if abs(numeric) >= EXTREME_ABS_LIMIT:
        return False
    return lower_bound <= numeric <= upper_bound


def _build_trade_markers(trades: list) -> list[dict]:
    markers: list[dict] = []
    stack_counts: dict[tuple[str, str], int] = {}
    for trade in trades:
        trade_type = str(getattr(trade, "type", "")).upper()
        is_long = trade_type == "LONG"
        markers.extend(
            [
                _trade_marker(
                    time_value=getattr(trade, "entry_time", None),
                    position="belowBar" if is_long else "aboveBar",
                    color="#fb7185" if is_long else "#38d5b5",
                    text="开多" if is_long else "开空",
                    stack_counts=stack_counts,
                ),
                _trade_marker(
                    time_value=getattr(trade, "exit_time", None),
                    position="aboveBar" if is_long else "belowBar",
                    color="#38d5b5" if is_long else "#fb7185",
                    text="平多" if is_long else "平空",
                    stack_counts=stack_counts,
                ),
            ]
        )
    return [marker for marker in markers if marker["time"]]


def _trade_marker(
    *,
    time_value: Any,
    position: str,
    color: str,
    text: str,
    stack_counts: dict[tuple[str, str], int],
) -> dict:
    marker_time = to_lwc_time(time_value)
    stack_key = (str(marker_time), position)
    stack_index = stack_counts.get(stack_key, 0)
    stack_counts[stack_key] = stack_index + 1
    return {
        "time": marker_time,
        "position": position,
        "color": color,
        "shape": "arrowDown" if position == "aboveBar" else "arrowUp",
        "text": text,
        "stackIndex": stack_index,
    }
