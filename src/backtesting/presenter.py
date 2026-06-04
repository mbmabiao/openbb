from __future__ import annotations

from typing import Any

import pandas as pd

from backtesting.schema import Trade


EXIT_REASON_LABELS = {
    "signal": "Signal",
    "end of backtest": "End of backtest",
    "atr stop": "ATR stop",
    "atr trailing stop": "ATR trailing stop",
    "atr take profit": "ATR take profit",
    "gap filled": "Gap filled",
    "regular session open exit": "Regular session open exit",
    "momentum/trend reversal": "Momentum/trend reversal",
    "opposite bearish macd divergence": "Opposite bearish MACD divergence",
    "opposite bullish macd divergence": "Opposite bullish MACD divergence",
}


def format_trade_table(trades: list[Trade]) -> list[dict]:
    """Return frontend-friendly trade history rows without mutating raw trades."""
    return [
        {
            "No.": trade.index,
            "Side": _format_trade_type(trade.type),
            "Exit Reason": _clean_reason(trade.exit_reason),
            "Entry Time": _format_time(trade.entry_time),
            "Exit Time": _format_time(trade.exit_time),
            "Entry Price": _format_price(trade.entry_price),
            "Exit Price": _format_price(trade.exit_price),
            "P/L": _format_currency(trade.pnl),
            "P/L %": _format_pct(_safe_ratio(trade.pnl, trade.position_notional)),
            "Balance": _format_currency(trade.balance),
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


def _format_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:.2f}%"


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    denominator = float(denominator)
    if denominator == 0:
        return None
    return float(numerator) / denominator


def _format_trade_type(value: str) -> str:
    normalized = str(value or "").upper()
    return normalized or "-"


def _clean_reason(value: str) -> str:
    reason = str(value or "").strip()
    if not reason:
        return "-"
    return EXIT_REASON_LABELS.get(reason.lower(), reason)
