from __future__ import annotations

from dataclasses import fields

from backtesting.data_context import build_strategy_context
from backtesting.engine import BacktestEngine
from backtesting.presenter import format_trade_table
from backtesting.schema import BacktestConfig, BacktestResult
from strategies.base import StrategyContext
from strategies.registry import get_strategy


def run_backtest(
    symbol: str,
    strategy_name: str,
    strategy_config: dict,
    backtest_config: dict,
) -> BacktestResult:
    strategy = get_strategy(strategy_name, strategy_config)
    config = _build_config(backtest_config)
    context = build_strategy_context(
        symbol=symbol,
        primary_timeframe=config.primary_timeframe,
        required_timeframes=list(strategy.required_timeframes),
        start_date=config.start_date,
        end_date=config.end_date,
        strategy_config=strategy.config,
        provider=config.price_provider,
    )
    return BacktestEngine().run(context, strategy, config)


def run_backtest_from_context(
    context: StrategyContext,
    strategy_name: str,
    strategy_config: dict,
    backtest_config: dict,
) -> BacktestResult:
    strategy = get_strategy(strategy_name, strategy_config)
    config = _build_config(backtest_config)
    return BacktestEngine().run(context, strategy, config)


def _build_config(raw: dict) -> BacktestConfig:
    allowed = {field.name for field in fields(BacktestConfig)}
    return BacktestConfig(**{key: value for key, value in raw.items() if key in allowed})


def build_chart_payload(result: BacktestResult) -> dict:
    candles = [
        {
            "time": str(row.date),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in result.candles.itertuples(index=False)
    ]
    entries = [
        {
            "time": str(trade.entry_time),
            "type": trade.type,
            "price": trade.entry_price,
            "reason": trade.entry_reason,
        }
        for trade in result.trades
    ]
    exits = [
        {
            "time": str(trade.exit_time),
            "type": trade.type,
            "price": trade.exit_price,
            "reason": trade.exit_reason,
            "pnl": trade.pnl,
        }
        for trade in result.trades
    ]
    overlays = {
        column: [
            {"time": str(row["date"]), "value": float(row[column])}
            for _, row in result.signals.loc[:, ["date", column]].dropna().iterrows()
        ]
        for column in result.signals.columns
        if column.startswith("plot_")
    }
    equity_curve = [
        {"time": str(point.time), "equity": float(point.equity)}
        for point in result.equity_curve
    ]
    trades = [trade.__dict__.copy() for trade in result.trades]
    return {
        "candles": candles,
        "entries": entries,
        "exits": exits,
        "overlays": overlays,
        "equity_curve": equity_curve,
        "metrics": result.metrics,
        "trades": trades,
        "trade_table": format_trade_table(result.trades),
    }
