from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.base import BaseStrategy, StrategyContext


class SuperTrendATRTrailingStrategy(BaseStrategy):
    name = "supertrend_atr_trailing"
    display_name = "SuperTrend + ATR Trailing Exit"
    description = "SuperTrend entries with ATR-based dynamic exits."
    required_timeframes = ["1d"]

    default_config = {
        "atr_period": 10,
        "supertrend_multiplier": 3.0,
        "atr_exit_mult": 1.5,
        "exit_on_opposite_signal": True,
        "direction": "both",
    }

    config_schema = {
        "atr_period": {
            "type": "int",
            "label": "ATR Period",
            "default": 10,
            "min": 1,
            "max": 100,
            "step": 1,
            "help": "ATR Wilder smoothing period.",
            "required": True,
        },
        "supertrend_multiplier": {
            "type": "float",
            "label": "SuperTrend Multiplier",
            "default": 3.0,
            "min": 0.5,
            "max": 10.0,
            "step": 0.1,
            "help": "ATR multiplier used to build SuperTrend bands.",
            "required": True,
        },
        "atr_exit_mult": {
            "type": "float",
            "label": "ATR Exit Multiplier",
            "default": 1.5,
            "min": 0.5,
            "max": 10.0,
            "step": 0.1,
            "help": "ATR multiplier used for trailing exits.",
            "required": True,
        },
        "exit_on_opposite_signal": {
            "type": "bool",
            "label": "Exit on Opposite Signal",
            "default": True,
            "help": "Close the current position when the opposite SuperTrend signal appears.",
        },
        "direction": {
            "type": "select",
            "label": "Direction",
            "default": "both",
            "options": ["long", "short", "both"],
            "help": "Allowed trading direction.",
            "required": True,
        },
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data[context.primary_timeframe].copy().reset_index(drop=True)
        cfg = {**self.default_config, **context.config, **self.config}
        atr_period = int(cfg["atr_period"])
        multiplier = float(cfg["supertrend_multiplier"])
        atr_exit_mult = float(cfg["atr_exit_mult"])
        exit_on_opposite = bool(cfg["exit_on_opposite_signal"])
        direction = str(cfg["direction"])

        df["atr"] = _atr(df, atr_period)
        hl2 = (df["high"] + df["low"]) / 2.0
        basic_upper = hl2 + multiplier * df["atr"]
        basic_lower = hl2 - multiplier * df["atr"]

        final_upper = basic_upper.copy()
        final_lower = basic_lower.copy()
        supertrend = pd.Series(np.nan, index=df.index, dtype="float64")
        trend = pd.Series(1, index=df.index, dtype="int64")

        for idx in range(1, len(df)):
            final_upper.iloc[idx] = (
                basic_upper.iloc[idx]
                if basic_upper.iloc[idx] < final_upper.iloc[idx - 1]
                or df["close"].iloc[idx - 1] > final_upper.iloc[idx - 1]
                else final_upper.iloc[idx - 1]
            )
            final_lower.iloc[idx] = (
                basic_lower.iloc[idx]
                if basic_lower.iloc[idx] > final_lower.iloc[idx - 1]
                or df["close"].iloc[idx - 1] < final_lower.iloc[idx - 1]
                else final_lower.iloc[idx - 1]
            )

            if trend.iloc[idx - 1] == -1 and df["close"].iloc[idx] > final_upper.iloc[idx]:
                trend.iloc[idx] = 1
            elif trend.iloc[idx - 1] == 1 and df["close"].iloc[idx] < final_lower.iloc[idx]:
                trend.iloc[idx] = -1
            else:
                trend.iloc[idx] = trend.iloc[idx - 1]
            supertrend.iloc[idx] = final_lower.iloc[idx] if trend.iloc[idx] == 1 else final_upper.iloc[idx]

        df["supertrend_direction"] = trend
        df["supertrend"] = supertrend.bfill()
        df["plot_supertrend"] = df["supertrend"]

        long_flip = (trend == 1) & (trend.shift(1) == -1)
        short_flip = (trend == -1) & (trend.shift(1) == 1)
        allow_long = direction in {"long", "both"}
        allow_short = direction in {"short", "both"}

        df["open_long"] = False
        df["close_long"] = False
        df["open_short"] = False
        df["close_short"] = False
        df["entry_reason"] = ""
        df["exit_reason"] = ""
        df["plot_atr_stop"] = np.nan

        position: str | None = None
        highest_high = np.nan
        lowest_low = np.nan

        for idx, row in df.iterrows():
            atr_value = row["atr"]
            if not np.isfinite(atr_value):
                continue

            if position == "long":
                highest_high = max(highest_high, row["high"]) if np.isfinite(highest_high) else row["high"]
                stop = highest_high - atr_exit_mult * atr_value
                df.at[idx, "plot_atr_stop"] = stop
                should_exit = row["close"] < stop or (exit_on_opposite and bool(short_flip.iloc[idx]))
                if should_exit:
                    df.at[idx, "close_long"] = True
                    df.at[idx, "exit_reason"] = "ATR trailing stop" if row["close"] < stop else "Opposite SuperTrend"
                    position = None

            elif position == "short":
                lowest_low = min(lowest_low, row["low"]) if np.isfinite(lowest_low) else row["low"]
                stop = lowest_low + atr_exit_mult * atr_value
                df.at[idx, "plot_atr_stop"] = stop
                should_exit = row["close"] > stop or (exit_on_opposite and bool(long_flip.iloc[idx]))
                if should_exit:
                    df.at[idx, "close_short"] = True
                    df.at[idx, "exit_reason"] = "ATR trailing stop" if row["close"] > stop else "Opposite SuperTrend"
                    position = None

            if position is None and bool(long_flip.iloc[idx]) and allow_long:
                df.at[idx, "open_long"] = True
                df.at[idx, "entry_reason"] = "SuperTrend bullish flip"
                position = "long"
                highest_high = row["high"]
            elif position is None and bool(short_flip.iloc[idx]) and allow_short:
                df.at[idx, "open_short"] = True
                df.at[idx, "entry_reason"] = "SuperTrend bearish flip"
                position = "short"
                lowest_low = row["low"]

        return df


def _atr(df: pd.DataFrame, period: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / max(period, 1), adjust=False, min_periods=1).mean()

