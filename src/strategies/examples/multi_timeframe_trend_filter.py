from __future__ import annotations

import pandas as pd

from backtesting.data_context import align_higher_timeframe_to_primary
from strategies.base import BaseStrategy, StrategyContext


class MultiTimeframeTrendFilterStrategy(BaseStrategy):
    name = "multi_timeframe_trend_filter"
    display_name = "Multi-Timeframe Trend Filter"
    description = "Uses a daily moving-average trend filter with lower-timeframe momentum entries."
    required_timeframes = ["15m", "1d"]

    default_config = {
        "daily_fast_ma": 20,
        "daily_slow_ma": 50,
        "entry_momentum_bars": 3,
        "exit_momentum_bars": 2,
        "direction": "both",
    }

    config_schema = {
        "daily_fast_ma": {"type": "int", "label": "Daily Fast MA", "default": 20, "min": 2, "max": 200, "step": 1},
        "daily_slow_ma": {"type": "int", "label": "Daily Slow MA", "default": 50, "min": 3, "max": 300, "step": 1},
        "entry_momentum_bars": {
            "type": "int",
            "label": "Entry Momentum Bars",
            "default": 3,
            "min": 1,
            "max": 20,
            "step": 1,
        },
        "exit_momentum_bars": {
            "type": "int",
            "label": "Exit Momentum Bars",
            "default": 2,
            "min": 1,
            "max": 20,
            "step": 1,
        },
        "direction": {
            "type": "select",
            "label": "Direction",
            "default": "both",
            "options": ["long", "short", "both"],
            "required": True,
        },
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        cfg = {**self.default_config, **context.config, **self.config}
        primary = context.data[context.primary_timeframe].copy().reset_index(drop=True)
        daily = context.data.get("1d", pd.DataFrame()).copy()
        if daily.empty:
            raise ValueError("Multi-timeframe strategy requires 1d data.")

        daily["daily_fast_ma"] = daily["close"].rolling(int(cfg["daily_fast_ma"]), min_periods=1).mean()
        daily["daily_slow_ma"] = daily["close"].rolling(int(cfg["daily_slow_ma"]), min_periods=1).mean()
        daily["daily_trend"] = (daily["daily_fast_ma"] > daily["daily_slow_ma"]).astype(int).replace({0: -1})

        aligned = align_higher_timeframe_to_primary(
            primary,
            daily,
            ["daily_fast_ma", "daily_slow_ma", "daily_trend"],
            higher_timeframe="1d",
        )
        df = pd.concat([primary, aligned], axis=1)
        trade_direction = str(cfg.get("trade_direction", cfg.get("direction", "both"))).lower()
        entry_bars = int(cfg["entry_momentum_bars"])
        exit_bars = int(cfg["exit_momentum_bars"])

        up_momentum = df["close"].gt(df["close"].shift(1)).rolling(entry_bars, min_periods=entry_bars).sum() == entry_bars
        down_momentum = df["close"].lt(df["close"].shift(1)).rolling(entry_bars, min_periods=entry_bars).sum() == entry_bars
        up_exit = df["close"].gt(df["close"].shift(1)).rolling(exit_bars, min_periods=exit_bars).sum() == exit_bars
        down_exit = df["close"].lt(df["close"].shift(1)).rolling(exit_bars, min_periods=exit_bars).sum() == exit_bars

        df["open_long"] = (df["daily_trend"] == 1) & up_momentum & (trade_direction in {"long", "both"})
        df["open_short"] = (df["daily_trend"] == -1) & down_momentum & (trade_direction in {"short", "both"})
        df["close_long"] = down_exit | (df["daily_trend"] == -1)
        df["close_short"] = up_exit | (df["daily_trend"] == 1)
        df["entry_reason"] = "MTF trend + momentum"
        df["exit_reason"] = "Momentum/trend reversal"
        df["plot_daily_fast_ma"] = df["daily_fast_ma"]
        df["plot_daily_slow_ma"] = df["daily_slow_ma"]
        return df
