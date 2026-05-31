from __future__ import annotations

import math
from dataclasses import replace
from typing import Any

import numpy as np
import pandas as pd

from backtesting.metrics import calculate_metrics
from backtesting.schema import BacktestConfig, BacktestResult, EquityPoint, Position, Trade
from strategies.base import BaseStrategy, StrategyContext


SIGNAL_COLUMNS = ("open_long", "close_long", "open_short", "close_short")


class BacktestEngine:
    def run(
        self,
        context: StrategyContext,
        strategy: BaseStrategy,
        config: BacktestConfig,
    ) -> BacktestResult:
        context = strategy.prepare(context)
        primary = context.data.get(config.primary_timeframe)
        if primary is None:
            primary = context.data.get(context.primary_timeframe)
        if primary is None or primary.empty:
            raise ValueError(f"Primary timeframe '{config.primary_timeframe}' has no data.")

        signals = strategy.generate_signals(context)
        signals = _prepare_signals(signals, price_col=config.price_col)
        primary_columns = [column for column in ["date", "open", "high", "low", "close", "volume"] if column in signals.columns]
        primary = signals.loc[:, primary_columns].copy()

        cash = float(config.initial_capital)
        position: Position | None = None
        trades: list[Trade] = []
        equity_curve: list[EquityPoint] = []

        for row_number, row in signals.reset_index(drop=True).iterrows():
            price = _finite_float(row[config.price_col])
            if price is None or price <= 0:
                continue

            current_equity = _mark_to_market(cash, position, price)
            if config.exit_before_entry:
                cash, position = self._maybe_exit(row, row_number, price, cash, position, trades, config)
                current_equity = _mark_to_market(cash, position, price)
                cash, position = self._maybe_enter(row, row_number, price, cash, position, current_equity, config)
            else:
                cash, position = self._maybe_enter(row, row_number, price, cash, position, current_equity, config)
                cash, position = self._maybe_exit(row, row_number, price, cash, position, trades, config)

            position_value = _position_value(position, price)
            equity_curve.append(
                EquityPoint(
                    time=row["date"],
                    equity=float(cash + position_value),
                    cash=float(cash),
                    position_value=float(position_value),
                )
            )

        if position is not None and not signals.empty:
            last_row = signals.iloc[-1]
            last_price = _finite_float(last_row[config.price_col])
            if last_price is not None:
                cash, position = self._exit_position(
                    row=last_row,
                    row_number=len(signals) - 1,
                    price=last_price,
                    cash=cash,
                    position=position,
                    trades=trades,
                    config=config,
                    fallback_reason="End of backtest",
                )
                equity_curve.append(
                    EquityPoint(
                        time=last_row["date"],
                        equity=float(cash),
                        cash=float(cash),
                        position_value=0.0,
                    )
                )

        result = BacktestResult(
            symbol=context.symbol,
            strategy_name=strategy.name,
            config=replace(config, primary_timeframe=context.primary_timeframe),
            candles=primary,
            signals=signals,
            trades=trades,
            equity_curve=equity_curve,
        )
        result.metrics = calculate_metrics(
            initial_capital=config.initial_capital,
            equity_curve=equity_curve,
            trades=trades,
        )
        return result

    def _maybe_enter(
        self,
        row: pd.Series,
        row_number: int,
        price: float,
        cash: float,
        position: Position | None,
        current_equity: float,
        config: BacktestConfig,
    ) -> tuple[float, Position | None]:
        if position is not None:
            return cash, position

        if bool(row["open_long"]) and config.allow_long:
            return self._enter_position("long", row, row_number, price, cash, current_equity, config)
        if bool(row["open_short"]) and config.allow_short:
            return self._enter_position("short", row, row_number, price, cash, current_equity, config)
        return cash, position

    def _maybe_exit(
        self,
        row: pd.Series,
        row_number: int,
        price: float,
        cash: float,
        position: Position | None,
        trades: list[Trade],
        config: BacktestConfig,
    ) -> tuple[float, Position | None]:
        if position is None:
            return cash, None
        if position.side == "long" and bool(row["close_long"]):
            return self._exit_position(row, row_number, price, cash, position, trades, config)
        if position.side == "short" and bool(row["close_short"]):
            return self._exit_position(row, row_number, price, cash, position, trades, config)
        return cash, position

    def _enter_position(
        self,
        side: str,
        row: pd.Series,
        row_number: int,
        price: float,
        cash: float,
        current_equity: float,
        config: BacktestConfig,
    ) -> tuple[float, Position | None]:
        notional, size_pct, size_source = _resolve_position_size(row, current_equity, config)
        if notional <= 0:
            return cash, None

        entry_price = price * (1 + config.slippage) if side == "long" else price * (1 - config.slippage)
        quantity = notional / entry_price
        commission = notional * config.commission_pct
        if side == "long":
            cash -= notional + commission
        else:
            cash += notional - commission

        return cash, Position(
            side=side,
            entry_index=row_number,
            entry_time=row["date"],
            entry_price=float(entry_price),
            quantity=float(quantity),
            position_notional=float(notional),
            position_size_pct=float(size_pct),
            size_source=size_source,
            entry_reason=str(row.get("entry_reason") or ""),
            entry_commission=float(commission),
        )

    def _exit_position(
        self,
        row: pd.Series,
        row_number: int,
        price: float,
        cash: float,
        position: Position,
        trades: list[Trade],
        config: BacktestConfig,
        fallback_reason: str = "Signal",
    ) -> tuple[float, None]:
        exit_price = price * (1 - config.slippage) if position.side == "long" else price * (1 + config.slippage)
        exit_notional = position.quantity * exit_price
        exit_commission = exit_notional * config.commission_pct

        if position.side == "long":
            cash += exit_notional - exit_commission
            gross_pnl = (exit_price - position.entry_price) * position.quantity
        else:
            cash -= exit_notional + exit_commission
            gross_pnl = (position.entry_price - exit_price) * position.quantity

        pnl = gross_pnl - position.entry_commission - exit_commission
        trades.append(
            Trade(
                index=len(trades) + 1,
                type=position.side.upper(),
                exit_reason=str(row.get("exit_reason") or fallback_reason),
                entry_time=position.entry_time,
                exit_time=row["date"],
                entry_price=float(position.entry_price),
                exit_price=float(exit_price),
                pnl=float(pnl),
                balance=float(cash),
                bars_held=max(row_number - position.entry_index, 0),
                position_notional=float(position.position_notional),
                position_size_pct=float(position.position_size_pct),
                size_source=position.size_source,
                entry_reason=position.entry_reason,
            )
        )
        return cash, None


def _prepare_signals(signals: pd.DataFrame, price_col: str) -> pd.DataFrame:
    if signals is None or signals.empty:
        raise ValueError("Strategy returned no signals.")
    out = signals.copy().reset_index(drop=True)
    required = {"date", "open", "high", "low", "close", price_col}
    missing = required - set(out.columns)
    if missing:
        raise ValueError(f"Signal dataframe is missing required columns: {sorted(missing)}")
    for column in SIGNAL_COLUMNS:
        if column not in out.columns:
            out[column] = False
        out[column] = out[column].fillna(False).astype(bool)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out.dropna(subset=["date", price_col]).reset_index(drop=True)


def _resolve_position_size(row: pd.Series, equity: float, config: BacktestConfig) -> tuple[float, float, str]:
    fallback_pct = _clamp_pct(config.position_size_pct)
    for column, source in (
        ("position_notional", "strategy_position_notional"),
        ("position_size_pct", "strategy_position_size_pct"),
        ("target_weight", "strategy_target_weight"),
    ):
        value = _finite_float(row.get(column))
        if value is None:
            continue
        if column == "position_notional":
            notional = min(max(value, 0.0), max(equity, 0.0))
            return notional, notional / equity if equity > 0 else 0.0, source
        pct = _clamp_pct(value)
        return min(equity * pct, equity), pct, source

    return min(equity * fallback_pct, equity), fallback_pct, "config"


def _mark_to_market(cash: float, position: Position | None, price: float) -> float:
    return float(cash + _position_value(position, price))


def _position_value(position: Position | None, price: float) -> float:
    if position is None:
        return 0.0
    value = position.quantity * price
    return float(value if position.side == "long" else -value)


def _finite_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _clamp_pct(value: float) -> float:
    if not np.isfinite(value):
        return 0.0
    return min(max(float(value), 0.0), 1.0)
