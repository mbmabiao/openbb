from __future__ import annotations

from dataclasses import fields, replace

from backtesting.data_context import build_strategy_context, is_intraday_timeframe
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
    config = _build_config(backtest_config)
    strategy_config = _sync_strategy_direction_config(strategy_config, backtest_config)
    strategy = get_strategy(strategy_name, strategy_config)
    primary_timeframe = resolve_primary_timeframe(
        strategy,
        config,
        user_provided="primary_timeframe" in backtest_config and bool(backtest_config.get("primary_timeframe")),
    )
    config = replace(config, primary_timeframe=primary_timeframe)
    extended_hours = resolve_extended_hours(strategy, config)
    timeframe_requirements = resolve_timeframe_requirements(strategy, config)
    context = build_strategy_context(
        symbol=symbol,
        primary_timeframe=primary_timeframe,
        required_timeframes=list(strategy.required_timeframes),
        start_date=config.start_date,
        end_date=config.end_date,
        strategy_config=strategy.config,
        provider=config.price_provider,
        extended_hours=extended_hours,
        data_requirements=timeframe_requirements,
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
    resolve_extended_hours(strategy, config)
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


def resolve_primary_timeframe(strategy, config: BacktestConfig, user_provided: bool = True) -> str:
    data_requirements = getattr(strategy, "data_requirements", {}) or {}
    if not user_provided:
        required_primary = data_requirements.get("primary_timeframe")
        if required_primary:
            return str(required_primary)
        preferred = getattr(strategy, "preferred_primary_timeframe", None)
        if preferred:
            return str(preferred)
    return config.primary_timeframe or getattr(strategy, "preferred_primary_timeframe", None) or "1d"


def resolve_extended_hours(strategy, config: BacktestConfig) -> bool:
    if config.extended_hours is not None:
        if getattr(strategy, "requires_extended_hours", False) and config.extended_hours is False:
            raise ValueError(
                f"Strategy '{strategy.name}' requires extended-hours data, "
                "but backtest_config.extended_hours is False."
            )
        return bool(config.extended_hours)

    if getattr(strategy, "requires_extended_hours", False):
        return True

    return False


def resolve_timeframe_requirements(strategy, config: BacktestConfig) -> dict:
    data_requirements = dict(getattr(strategy, "data_requirements", {}) or {})
    raw_timeframes = dict(data_requirements.get("timeframes", {}) or {})
    resolved_timeframes: dict[str, dict] = {}
    fallback_extended_hours = resolve_extended_hours(strategy, config)

    for timeframe in _unique_timeframes([config.primary_timeframe, *list(getattr(strategy, "required_timeframes", ["1d"]))]):
        raw_requirement = raw_timeframes.get(timeframe) or raw_timeframes.get(str(timeframe).lower()) or {}
        requirement = dict(raw_requirement) if isinstance(raw_requirement, dict) else {}
        if "extended_hours" not in requirement:
            requirement["extended_hours"] = bool(fallback_extended_hours) if is_intraday_timeframe(timeframe) else False
        resolved_timeframes[timeframe] = requirement

    return {
        **data_requirements,
        "primary_timeframe": data_requirements.get("primary_timeframe", config.primary_timeframe),
        "timeframes": resolved_timeframes,
    }


def _build_config(raw: dict) -> BacktestConfig:
    allowed = {field.name for field in fields(BacktestConfig)}
    return BacktestConfig(**{key: value for key, value in raw.items() if key in allowed})


def _unique_timeframes(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if normalized and normalized not in seen:
            output.append(normalized)
            seen.add(normalized)
    return output


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
