from __future__ import annotations

import pandas as pd
import numpy as np

from plotting.chart_payload import ChartPayload, _candles_from_frame, _volume_from_frame, payload_time


BUY_COLOR = "#fb7185"
SELL_COLOR = "#38d5b5"
OVERLAY_COLORS = ("#fbbf24", "#60a5fa", "#c084fc", "#f97316", "#22d3ee", "#a3e635")


def build_backtest_chart_payload(result) -> ChartPayload:
    signals = result.signals.copy()
    return {
        "candles": _candles_from_frame(signals),
        "volume": _volume_from_frame(signals),
        "overlays": _build_strategy_overlays(signals),
        "markers": _build_trade_markers(result.trades),
        "zones": [],
        "trades": _build_trade_payload(result.trades),
        "equity_curve": [
            {"time": payload_time(point.time), "equity": float(point.equity)}
            for point in result.equity_curve
        ],
    }


def _build_strategy_overlays(signals: pd.DataFrame) -> list[dict]:
    overlays: list[dict] = []
    for color_index, column in enumerate([column for column in signals.columns if column.startswith("plot_")]):
        segments = split_overlay_into_segments(signals, column)
        if not segments:
            continue
        label = _format_overlay_label(column)
        color = OVERLAY_COLORS[color_index % len(OVERLAY_COLORS)]
        for segment_index, segment in enumerate(segments):
            overlays.append(
                {
                    "id": f"{column}__segment_{segment_index + 1}",
                    "source_column": column,
                    "name": label,
                    "color": color,
                    "show_legend": segment_index == 0,
                    "data": segment,
                }
            )
    return overlays


def split_overlay_into_segments(signals: pd.DataFrame, column: str) -> list[list[dict]]:
    if column not in signals.columns:
        return []

    segments: list[list[dict]] = []
    current_segment: list[dict] = []
    frame = signals.loc[:, ["date", column]].copy()

    for _, row in frame.iterrows():
        value = pd.to_numeric(pd.Series([row[column]]), errors="coerce").iloc[0]
        if pd.notna(row["date"]) and pd.notna(value) and np.isfinite(float(value)):
            current_segment.append({"time": payload_time(row["date"]), "value": float(value)})
            continue
        if current_segment:
            segments.append(current_segment)
            current_segment = []

    if current_segment:
        segments.append(current_segment)

    return segments


def _format_overlay_label(column: str) -> str:
    raw = column.removeprefix("plot_")
    return " ".join(part.capitalize() for part in raw.split("_") if part)


def _build_trade_markers(trades: list) -> list[dict]:
    markers: list[dict] = []
    for trade in trades:
        if trade.type == "LONG":
            markers.append(_marker(trade.entry_time, trade.entry_price, "开多", "buy", trade.entry_reason))
            markers.append(_marker(trade.exit_time, trade.exit_price, "平多", "sell", trade.exit_reason, trade.pnl))
        elif trade.type == "SHORT":
            markers.append(_marker(trade.entry_time, trade.entry_price, "开空", "sell", trade.entry_reason))
            markers.append(_marker(trade.exit_time, trade.exit_price, "平空", "buy", trade.exit_reason, trade.pnl))
    return markers


def _marker(time_value, price: float, text: str, action: str, reason: str, pnl: float | None = None) -> dict:
    is_buy = action == "buy"
    return {
        "time": payload_time(time_value),
        "price": float(price),
        "text": text,
        "side": "buy" if is_buy else "sell",
        "action": action,
        "reason": reason or "",
        "pnl": float(pnl) if pnl is not None else None,
        "position": "aboveBar" if is_buy else "belowBar",
        "shape": "arrowDown" if is_buy else "arrowUp",
        "color": BUY_COLOR if is_buy else SELL_COLOR,
        "size": 2,
    }


def _build_trade_payload(trades: list) -> list[dict]:
    return [
        {
            "index": trade.index,
            "type": trade.type,
            "entry_time": payload_time(trade.entry_time),
            "exit_time": payload_time(trade.exit_time),
            "entry_price": float(trade.entry_price),
            "exit_price": float(trade.exit_price),
            "entry_reason": trade.entry_reason,
            "exit_reason": trade.exit_reason,
            "pnl": float(trade.pnl),
        }
        for trade in trades
    ]
