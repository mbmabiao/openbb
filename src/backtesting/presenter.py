from __future__ import annotations

from typing import Any

import pandas as pd

from backtesting.schema import Trade


def format_trade_table(trades: list[Trade]) -> list[dict]:
    """Return frontend-friendly trade history rows without mutating raw trades."""
    return [
        {
            "序号": trade.index,
            "方向": _format_trade_type(trade.type),
            "平仓原因": _clean_reason(trade.exit_reason),
            "开仓时间": _format_time(trade.entry_time),
            "平仓时间": _format_time(trade.exit_time),
            "开仓价": _format_price(trade.entry_price),
            "平仓价": _format_price(trade.exit_price),
            "盈亏": _format_currency(trade.pnl),
            "余额": _format_currency(trade.balance),
        }
        for trade in trades
    ]


def _format_time(value: Any) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return ""
    return timestamp.strftime("%m/%d/%Y, %H:%M")


def _format_price(value: float) -> str:
    return f"{float(value):.4f}"


def _format_currency(value: float) -> str:
    amount = float(value)
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.2f}"


def _format_trade_type(value: str) -> str:
    normalized = str(value or "").upper()
    if normalized == "LONG":
        return "多头"
    if normalized == "SHORT":
        return "空头"
    return normalized or "-"


def _clean_reason(value: str) -> str:
    return str(value or "").strip() or "-"
