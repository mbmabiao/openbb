from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class BacktestConfig:
    initial_capital: float = 10_000.0
    slippage: float = 0.0005
    commission_pct: float = 0.0005
    position_size_pct: float = 1.0
    allow_short: bool = True
    allow_long: bool = True
    exit_before_entry: bool = True
    price_col: str = "close"
    primary_timeframe: str = "1d"
    start_date: str | None = None
    end_date: str | None = None
    price_provider: str | None = None


@dataclass
class Position:
    side: str
    entry_index: int
    entry_time: Any
    entry_price: float
    quantity: float
    position_notional: float
    position_size_pct: float
    size_source: str
    entry_reason: str = ""
    entry_commission: float = 0.0


@dataclass
class Trade:
    index: int
    type: str
    exit_reason: str
    entry_time: Any
    exit_time: Any
    entry_price: float
    exit_price: float
    pnl: float
    balance: float
    bars_held: int
    position_notional: float
    position_size_pct: float
    size_source: str
    entry_reason: str = ""


@dataclass
class EquityPoint:
    time: Any
    equity: float
    cash: float
    position_value: float


@dataclass
class BacktestResult:
    symbol: str
    strategy_name: str
    config: BacktestConfig
    candles: pd.DataFrame
    signals: pd.DataFrame
    trades: list[Trade] = field(default_factory=list)
    equity_curve: list[EquityPoint] = field(default_factory=list)
    metrics: dict[str, float | int | None] = field(default_factory=dict)

