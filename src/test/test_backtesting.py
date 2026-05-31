from __future__ import annotations

import pandas as pd

from backtesting.engine import BacktestEngine
from backtesting.runner import run_backtest_from_context
from backtesting.schema import BacktestConfig
from strategies.base import BaseStrategy, StrategyContext
from strategies.registry import list_strategies


class LongShortSyntheticStrategy(BaseStrategy):
    name = "long_short_synthetic"
    required_timeframes = ["1d"]
    default_config = {"size": 0.5}
    config_schema = {"size": {"type": "float", "default": 0.5}}

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data["1d"].copy()
        df["open_long"] = False
        df["close_long"] = False
        df["open_short"] = False
        df["close_short"] = False
        df.loc[1, "open_long"] = True
        df.loc[3, "close_long"] = True
        df.loc[4, "open_short"] = True
        df.loc[6, "close_short"] = True
        df["position_size_pct"] = self.config.get("size", 0.5)
        return df


class MissingSignalColumnsStrategy(BaseStrategy):
    name = "missing_signal_columns"
    required_timeframes = ["1d"]

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        return context.data["1d"].copy()


class ConfigSizingStrategy(BaseStrategy):
    name = "config_sizing"
    required_timeframes = ["1d"]

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data["1d"].copy()
        df["open_long"] = False
        df["close_long"] = False
        df.loc[1, "open_long"] = True
        df.loc[3, "close_long"] = True
        return df


def test_strategy_discovery_metadata_includes_config_schema() -> None:
    strategies = list_strategies()
    names = {item["name"] for item in strategies}
    assert "supertrend_atr_trailing" in names
    supertrend = next(item for item in strategies if item["name"] == "supertrend_atr_trailing")
    assert supertrend["config_schema"]["atr_period"]["type"] == "int"
    assert supertrend["default_config"]["direction"] == "both"


def test_engine_executes_long_short_and_records_equity_curve() -> None:
    context = _context(_prices())
    result = BacktestEngine().run(
        context,
        LongShortSyntheticStrategy({"size": 0.5}),
        BacktestConfig(initial_capital=10_000, slippage=0, commission_pct=0, position_size_pct=1.0),
    )
    assert len(result.trades) == 2
    assert result.trades[0].type == "LONG"
    assert result.trades[1].type == "SHORT"
    assert result.trades[0].size_source == "strategy_position_size_pct"
    assert len(result.equity_curve) >= len(context.data["1d"])
    assert "total_return" in result.metrics


def test_engine_applies_slippage_commission_and_config_sizing() -> None:
    context = _context(_prices())
    result = BacktestEngine().run(
        context,
        ConfigSizingStrategy(),
        BacktestConfig(initial_capital=10_000, slippage=0.01, commission_pct=0.01, position_size_pct=0.25),
    )
    first = result.trades[0]
    assert first.entry_price > context.data["1d"].loc[1, "close"]
    assert first.exit_price < context.data["1d"].loc[3, "close"]
    assert first.position_notional == 2_500
    assert first.size_source == "config"


def test_missing_signal_columns_are_filled_false() -> None:
    result = BacktestEngine().run(
        _context(_prices()),
        MissingSignalColumnsStrategy(),
        BacktestConfig(initial_capital=10_000),
    )
    assert result.trades == []
    assert set(["open_long", "close_long", "open_short", "close_short"]).issubset(result.signals.columns)


def test_runner_from_context_runs_supertrend_end_to_end() -> None:
    result = run_backtest_from_context(
        context=_context(_trend_prices()),
        strategy_name="supertrend_atr_trailing",
        strategy_config={
            "atr_period": 3,
            "supertrend_multiplier": 1.0,
            "atr_exit_mult": 1.0,
            "exit_on_opposite_signal": True,
            "direction": "both",
        },
        backtest_config={"initial_capital": 10_000, "primary_timeframe": "1d"},
    )
    assert "plot_supertrend" in result.signals.columns
    assert result.metrics["trade_count"] >= 0


def test_multi_timeframe_context_can_hold_15m_and_1d() -> None:
    context = StrategyContext(
        symbol="TEST",
        primary_timeframe="15m",
        data={"15m": _intraday_prices(), "1d": _daily_trend_prices()},
        config={},
    )
    assert set(context.data) == {"15m", "1d"}
    assert context.data["15m"].iloc[0]["date"] < context.data["1d"].iloc[-1]["date"]


def test_multi_timeframe_strategy_runs_from_context() -> None:
    result = run_backtest_from_context(
        context=StrategyContext(
            symbol="TEST",
            primary_timeframe="15m",
            data={"15m": _intraday_prices(20), "1d": _daily_trend_prices()},
            config={},
        ),
        strategy_name="multi_timeframe_trend_filter",
        strategy_config={
            "daily_fast_ma": 3,
            "daily_slow_ma": 5,
            "entry_momentum_bars": 2,
            "exit_momentum_bars": 2,
            "direction": "both",
        },
        backtest_config={"initial_capital": 10_000, "primary_timeframe": "15m"},
    )
    assert "plot_daily_fast_ma" in result.signals.columns
    assert len(result.equity_curve) > 0


def _context(df: pd.DataFrame) -> StrategyContext:
    return StrategyContext(symbol="TEST", primary_timeframe="1d", data={"1d": df}, config={})


def _prices() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=8, freq="D")
    close = [100, 102, 105, 108, 107, 104, 101, 100]
    return pd.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": [value + 1 for value in close],
            "low": [value - 1 for value in close],
            "close": close,
            "volume": 1000,
        }
    )


def _trend_prices() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=40, freq="D")
    close = [100 + idx * 1.5 for idx in range(20)] + [130 - idx * 1.8 for idx in range(20)]
    return pd.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": [value + 2 for value in close],
            "low": [value - 2 for value in close],
            "close": close,
            "volume": 1000,
        }
    )


def _intraday_prices(periods: int = 8) -> pd.DataFrame:
    dates = pd.date_range("2025-03-25 09:30:00", periods=periods, freq="15min")
    close = [140 + idx for idx in range(periods)]
    return pd.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": [value + 0.5 for value in close],
            "low": [value - 0.5 for value in close],
            "close": close,
            "volume": 100,
        }
    )


def _daily_trend_prices() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=90, freq="D")
    close = [100 + idx * 0.5 for idx in range(90)]
    return pd.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": [value + 1 for value in close],
            "low": [value - 1 for value in close],
            "close": close,
            "volume": 1000,
        }
    )
