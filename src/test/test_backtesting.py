from __future__ import annotations

import pandas as pd

import backtesting.data_context as data_context_module
from backtesting.engine import BacktestEngine
from backtesting.data_context import is_intraday_timeframe
from backtesting.metrics import calculate_metrics
from backtesting.presenter import format_trade_table
from backtesting.runner import (
    _trade_direction_from_flags,
    resolve_extended_hours,
    resolve_timeframe_requirements,
    run_backtest_from_context,
)
from backtesting.schema import BacktestConfig, EquityPoint
from plotting.backtest_chart_adapter import build_backtest_chart_payload
from strategies.base import BaseStrategy, StrategyContext
from strategies.registry import get_strategy, list_strategies


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


class PlotOverlayTradeStrategy(BaseStrategy):
    name = "plot_overlay_trade"
    required_timeframes = ["1d"]

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
        df["entry_reason"] = "entry"
        df["exit_reason"] = "exit"
        df["plot_test_line"] = [pd.NA, 101, 102, pd.NA, pd.NA, 104, 103, pd.NA]
        return df


class InvalidLongToShortReversalStrategy(BaseStrategy):
    name = "invalid_long_to_short_reversal"
    required_timeframes = ["1d"]

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data["1d"].copy()
        df["open_long"] = False
        df["close_long"] = False
        df["open_short"] = False
        df["close_short"] = False
        df.loc[1, "open_long"] = True
        df.loc[3, "open_short"] = True
        return df


class InvalidShortToLongReversalStrategy(BaseStrategy):
    name = "invalid_short_to_long_reversal"
    required_timeframes = ["1d"]

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data["1d"].copy()
        df["open_long"] = False
        df["close_long"] = False
        df["open_short"] = False
        df["close_short"] = False
        df.loc[1, "open_short"] = True
        df.loc[3, "open_long"] = True
        return df


def test_strategy_discovery_metadata_includes_config_schema() -> None:
    strategies = list_strategies()
    names = {item["name"] for item in strategies}
    assert "supertrend_atr_trailing" in names
    supertrend = next(item for item in strategies if item["name"] == "supertrend_atr_trailing")
    assert supertrend["config_schema"]["atr_period"]["type"] == "int"
    assert supertrend["default_config"]["direction"] == "both"


def test_strategy_discovery_includes_extended_hours_metadata() -> None:
    strategies = list_strategies()
    premarket = next(item for item in strategies if item["name"] == "premarket_gap_mean_reversion")
    assert premarket["preferred_primary_timeframe"] == "5m"
    assert premarket["requires_extended_hours"] is True
    assert premarket["supports_extended_hours"] is True
    assert premarket["data_requirements"]["timeframes"]["5m"]["extended_hours"] is True
    assert premarket["data_requirements"]["timeframes"]["1d"]["extended_hours"] is False


def test_extended_hours_resolution_for_required_strategy() -> None:
    strategy = get_strategy("premarket_gap_mean_reversion", {})
    assert resolve_extended_hours(strategy, BacktestConfig(extended_hours=None)) is True
    try:
        resolve_extended_hours(strategy, BacktestConfig(extended_hours=False))
    except ValueError as exc:
        assert "requires extended-hours data" in str(exc)
    else:
        raise AssertionError("Expected required extended-hours strategy to reject disabled extended-hours.")


def test_detailed_timeframe_requirements_preserve_per_timeframe_extended_hours() -> None:
    strategy = get_strategy("premarket_gap_mean_reversion", {})
    requirements = resolve_timeframe_requirements(strategy, BacktestConfig(primary_timeframe="5m", extended_hours=None))
    assert requirements["timeframes"]["5m"]["extended_hours"] is True
    assert requirements["timeframes"]["1d"]["extended_hours"] is False


def test_timeframe_requirements_primary_timeframe_uses_resolved_config_value() -> None:
    strategy = get_strategy("premarket_gap_mean_reversion", {})
    requirements = resolve_timeframe_requirements(strategy, BacktestConfig(primary_timeframe="15m", extended_hours=None))
    assert requirements["primary_timeframe"] == "15m"


def test_load_timeframe_frame_passes_extended_hours_to_market_loader() -> None:
    calls: list[dict] = []
    original_fetch = data_context_module.fetch_price_history

    def fake_fetch_price_history(**kwargs):
        calls.append(kwargs)
        return _prices()

    data_context_module.fetch_price_history = fake_fetch_price_history
    try:
        frame = data_context_module._load_timeframe_frame(
            symbol="MSFT",
            timeframe="5m",
            start_date="2025-01-01",
            end_date="2025-01-02",
            provider="yfinance",
            extended_hours=True,
        )
    finally:
        data_context_module.fetch_price_history = original_fetch

    assert not frame.empty
    assert calls[0]["extended_hours_value"] is True
    assert calls[0]["interval_value"] == "5m"


def test_is_intraday_timeframe_handles_common_strings() -> None:
    for value in [
        "1",
        "5",
        "15",
        "30",
        "60",
        "1m",
        "5m",
        "15m",
        "1min",
        "5min",
        "15min",
        "1minute",
        "5minutes",
        "1h",
        "2h",
        "4h",
        "1hr",
        "2hrs",
        "1hour",
        "2hours",
        "hourly",
        "intraday",
    ]:
        assert is_intraday_timeframe(value) is True
    for value in ["1d", "d", "day", "daily", "1w", "week", "monthly", "1y", "year", "yearly"]:
        assert is_intraday_timeframe(value) is False


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


def test_backtest_chart_payload_builds_candles_overlays_and_trade_markers() -> None:
    result = BacktestEngine().run(
        _context(_plot_prices()),
        PlotOverlayTradeStrategy(),
        BacktestConfig(initial_capital=10_000, slippage=0, commission_pct=0, position_size_pct=0.5),
    )
    payload = build_backtest_chart_payload(result)
    assert len(payload["candles"]) == len(result.signals)
    assert payload["candles"][0]["open"] == 100.0
    assert payload["overlays"][0]["id"] == "plot_test_line"
    assert any("value" not in point for point in payload["overlays"][0]["data"])
    marker_texts = [marker["text"] for marker in payload["markers"]]
    assert marker_texts == ["开多", "平多", "开空", "平空"]
    assert payload["markers"][0]["price"] == result.trades[0].entry_price
    assert payload["markers"][0]["side"] == "buy"
    assert payload["markers"][1]["side"] == "sell"


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


def test_format_trade_table_returns_frontend_friendly_rows() -> None:
    context = _context(_prices())
    result = BacktestEngine().run(
        context,
        ConfigSizingStrategy(),
        BacktestConfig(initial_capital=10_000, slippage=0.01, commission_pct=0.01, position_size_pct=0.25),
    )
    rows = format_trade_table(result.trades)
    assert list(rows[0]) == [
        "序号",
        "方向",
        "平仓原因",
        "开仓时间",
        "平仓时间",
        "开仓价",
        "平仓价",
        "盈亏",
        "余额",
    ]
    assert rows[0]["方向"] == "多头"
    assert rows[0]["开仓价"].count(".") == 1
    assert rows[0]["盈亏"].startswith("$") or rows[0]["盈亏"].startswith("-$")
    assert "Size source" not in rows[0]


def test_sharpe_ratio_annualises_by_primary_timeframe() -> None:
    equity_curve = [
        EquityPoint(time=pd.Timestamp("2025-01-01 09:30") + pd.Timedelta(minutes=15 * idx), equity=value, cash=value, position_value=0)
        for idx, value in enumerate([10_000, 10_100, 10_050, 10_250, 10_200])
    ]
    daily = calculate_metrics(
        initial_capital=10_000,
        equity_curve=equity_curve,
        trades=[],
        primary_timeframe="1d",
    )
    fifteen_min = calculate_metrics(
        initial_capital=10_000,
        equity_curve=equity_curve,
        trades=[],
        primary_timeframe="15m",
    )
    assert fifteen_min["sharpe_ratio"] is not None
    assert daily["sharpe_ratio"] is not None
    assert fifteen_min["sharpe_ratio"] > daily["sharpe_ratio"]


def test_missing_signal_columns_are_filled_false() -> None:
    result = BacktestEngine().run(
        _context(_prices()),
        MissingSignalColumnsStrategy(),
        BacktestConfig(initial_capital=10_000),
    )
    assert result.trades == []
    assert set(["open_long", "close_long", "open_short", "close_short"]).issubset(result.signals.columns)


def test_engine_rejects_open_short_before_closing_long() -> None:
    try:
        BacktestEngine().run(
            _context(_prices()),
            InvalidLongToShortReversalStrategy(),
            BacktestConfig(initial_capital=10_000),
        )
    except ValueError as exc:
        assert "open_short=True without close_long=True" in str(exc)
    else:
        raise AssertionError("Expected invalid long-to-short reversal to raise ValueError.")


def test_engine_rejects_open_long_before_closing_short() -> None:
    try:
        BacktestEngine().run(
            _context(_prices()),
            InvalidShortToLongReversalStrategy(),
            BacktestConfig(initial_capital=10_000),
        )
    except ValueError as exc:
        assert "open_long=True without close_short=True" in str(exc)
    else:
        raise AssertionError("Expected invalid short-to-long reversal to raise ValueError.")


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


def test_runner_syncs_engine_direction_limits_into_supertrend_config() -> None:
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
        backtest_config={
            "initial_capital": 10_000,
            "primary_timeframe": "1d",
            "allow_long": True,
            "allow_short": False,
        },
    )
    assert not result.signals["open_short"].any()
    assert all(trade.type != "SHORT" for trade in result.trades)


def test_trade_direction_mapping_from_engine_flags() -> None:
    assert _trade_direction_from_flags(allow_long=True, allow_short=True) == "both"
    assert _trade_direction_from_flags(allow_long=True, allow_short=False) == "long"
    assert _trade_direction_from_flags(allow_long=False, allow_short=True) == "short"
    assert _trade_direction_from_flags(allow_long=False, allow_short=False) == "none"


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


def _plot_prices() -> pd.DataFrame:
    return _prices().assign(change_pct=lambda frame: frame["close"].pct_change())


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
