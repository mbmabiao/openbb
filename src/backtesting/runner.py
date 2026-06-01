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
    strategy_config = _sync_strategy_direction_config(strategy_config, backtest_config)
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
    strategy_config = _sync_strategy_direction_config(strategy_config, backtest_config)
    strategy = get_strategy(strategy_name, strategy_config)
    config = _build_config(backtest_config)
    return BacktestEngine().run(context, strategy, config)


def _sync_strategy_direction_config(strategy_config: dict, backtest_config: dict) -> dict:
    synced = dict(strategy_config or {})
    allow_long = bool(backtest_config.get("allow_long", True))
    allow_short = bool(backtest_config.get("allow_short", True))
    synced["allow_long"] = allow_long
    synced["allow_short"] = allow_short
    synced["trade_direction"] = _trade_direction_from_flags(allow_long=allow_long, allow_short=allow_short)
    return synced


def _trade_direction_from_flags(*, allow_long: bool, allow_short: bool) -> str:
    if allow_long and allow_short:
        return "both"
    if allow_long:
        return "long"
    if allow_short:
        return "short"
    return "none"


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
