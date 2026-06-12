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
        "atr_period": 15,
        "supertrend_multiplier": 3.0,
        "atr_exit_mult": 2.0,
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
        trade_direction = str(cfg.get("trade_direction", cfg.get("direction", "both"))).lower()
        engine_allow_long = bool(cfg.get("allow_long", True))
        engine_allow_short = bool(cfg.get("allow_short", True))

        df["atr"] = _atr(df, atr_period)

        hl2 = (df["high"] + df["low"]) / 2.0
        basic_upper = hl2 + multiplier * df["atr"]
        basic_lower = hl2 - multiplier * df["atr"]

        n = len(df)

        final_upper = pd.Series(np.nan, index=df.index, dtype="float64")
        final_lower = pd.Series(np.nan, index=df.index, dtype="float64")
        supertrend = pd.Series(np.nan, index=df.index, dtype="float64")
        trend = pd.Series(0, index=df.index, dtype="int64")

        # Wait until ATR is stable enough before generating SuperTrend values.
        start_idx = int(atr_period)

        for idx in range(n):
            if idx < start_idx:
                continue

            if not np.isfinite(basic_upper.iloc[idx]) or not np.isfinite(basic_lower.iloc[idx]):
                continue

            if idx == start_idx or trend.iloc[idx - 1] == 0:
                final_upper.iloc[idx] = basic_upper.iloc[idx]
                final_lower.iloc[idx] = basic_lower.iloc[idx]
                trend.iloc[idx] = 1 if df["close"].iloc[idx] >= hl2.iloc[idx] else -1
                supertrend.iloc[idx] = final_lower.iloc[idx] if trend.iloc[idx] == 1 else final_upper.iloc[idx]
                continue

            prev_final_upper = final_upper.iloc[idx - 1]
            prev_final_lower = final_lower.iloc[idx - 1]
            prev_close = df["close"].iloc[idx - 1]

            if not np.isfinite(prev_final_upper) or not np.isfinite(prev_final_lower):
                final_upper.iloc[idx] = basic_upper.iloc[idx]
                final_lower.iloc[idx] = basic_lower.iloc[idx]
                trend.iloc[idx] = trend.iloc[idx - 1]
                supertrend.iloc[idx] = final_lower.iloc[idx] if trend.iloc[idx] == 1 else final_upper.iloc[idx]
                continue

            # Final upper band may only tighten downward unless price has already broken above it.
            if basic_upper.iloc[idx] < prev_final_upper or prev_close > prev_final_upper:
                final_upper.iloc[idx] = basic_upper.iloc[idx]
            else:
                final_upper.iloc[idx] = prev_final_upper

            # Final lower band may only tighten upward unless price has already broken below it.
            if basic_lower.iloc[idx] > prev_final_lower or prev_close < prev_final_lower:
                final_lower.iloc[idx] = basic_lower.iloc[idx]
            else:
                final_lower.iloc[idx] = prev_final_lower

            # Use the previous final band to avoid using the current bar's adjusted band as the trigger.
            close_now = df["close"].iloc[idx]
            if close_now > prev_final_upper:
                trend.iloc[idx] = 1
            elif close_now < prev_final_lower:
                trend.iloc[idx] = -1
            else:
                trend.iloc[idx] = trend.iloc[idx - 1]

            supertrend.iloc[idx] = final_lower.iloc[idx] if trend.iloc[idx] == 1 else final_upper.iloc[idx]

        df["supertrend_direction"] = trend

        # Important:
        # Do NOT bfill. Backfilling leaks future SuperTrend values into earlier bars
        # and can create long misleading diagonal lines on the chart.
        df["supertrend"] = supertrend
        df["plot_supertrend"] = supertrend

        long_flip = (trend == 1) & (trend.shift(1) == -1)
        short_flip = (trend == -1) & (trend.shift(1) == 1)

        allow_long = trade_direction in {"long", "both"} and engine_allow_long
        allow_short = trade_direction in {"short", "both"} and engine_allow_short

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
                    df.at[idx, "exit_reason"] = (
                        "ATR trailing stop"
                        if row["close"] < stop
                        else "Opposite SuperTrend"
                    )
                    position = None
                    highest_high = np.nan

            elif position == "short":
                lowest_low = min(lowest_low, row["low"]) if np.isfinite(lowest_low) else row["low"]
                stop = lowest_low + atr_exit_mult * atr_value
                df.at[idx, "plot_atr_stop"] = stop

                should_exit = row["close"] > stop or (exit_on_opposite and bool(long_flip.iloc[idx]))
                if should_exit:
                    df.at[idx, "close_short"] = True
                    df.at[idx, "exit_reason"] = (
                        "ATR trailing stop"
                        if row["close"] > stop
                        else "Opposite SuperTrend"
                    )
                    position = None
                    lowest_low = np.nan

            if position is None and bool(long_flip.iloc[idx]) and allow_long:
                df.at[idx, "open_long"] = True
                df.at[idx, "entry_reason"] = "SuperTrend bullish flip"
                position = "long"
                highest_high = row["high"]
                lowest_low = np.nan

            elif position is None and bool(short_flip.iloc[idx]) and allow_short:
                df.at[idx, "open_short"] = True
                df.at[idx, "entry_reason"] = "SuperTrend bearish flip"
                position = "short"
                lowest_low = row["low"]
                highest_high = np.nan

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

    safe_period = max(int(period), 1)

    # Use min_periods=safe_period so ATR has a proper warm-up period.
    return true_range.ewm(
        alpha=1 / safe_period,
        adjust=False,
        min_periods=safe_period,
    ).mean()
