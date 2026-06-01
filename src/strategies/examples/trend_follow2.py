from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.base import BaseStrategy, StrategyContext


class SuperTrendATRTrailingStrategy(BaseStrategy):
    name = "supertrend_atr_trailing2"
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
        direction_mode = str(cfg["direction"]).lower()

        allow_long = direction_mode in {"long", "both"}
        allow_short = direction_mode in {"short", "both"}

        _validate_ohlcv(df)

        df["atr"] = _atr(df, atr_period)

        hl2 = (df["high"] + df["low"]) / 2.0
        basic_upper = hl2 + multiplier * df["atr"]
        basic_lower = hl2 - multiplier * df["atr"]

        n = len(df)

        final_upper = pd.Series(np.nan, index=df.index, dtype="float64")
        final_lower = pd.Series(np.nan, index=df.index, dtype="float64")
        supertrend = pd.Series(np.nan, index=df.index, dtype="float64")
        trend = pd.Series(0, index=df.index, dtype="int64")

        for idx in range(n):
            upper_value = basic_upper.iloc[idx]
            lower_value = basic_lower.iloc[idx]

            # Warm-up period: ATR is not valid yet, so do not emit trend or plot values.
            if not np.isfinite(upper_value) or not np.isfinite(lower_value):
                continue

            # First valid SuperTrend bar.
            if idx == 0 or trend.iloc[idx - 1] == 0:
                final_upper.iloc[idx] = upper_value
                final_lower.iloc[idx] = lower_value
                trend.iloc[idx] = 1 if df["close"].iloc[idx] >= hl2.iloc[idx] else -1
                supertrend.iloc[idx] = (
                    final_lower.iloc[idx] if trend.iloc[idx] == 1 else final_upper.iloc[idx]
                )
                continue

            prev_final_upper = final_upper.iloc[idx - 1]
            prev_final_lower = final_lower.iloc[idx - 1]
            prev_close = df["close"].iloc[idx - 1]

            # If previous final bands are somehow invalid, restart from this bar.
            if not np.isfinite(prev_final_upper) or not np.isfinite(prev_final_lower):
                final_upper.iloc[idx] = upper_value
                final_lower.iloc[idx] = lower_value
                trend.iloc[idx] = 1 if df["close"].iloc[idx] >= hl2.iloc[idx] else -1
                supertrend.iloc[idx] = (
                    final_lower.iloc[idx] if trend.iloc[idx] == 1 else final_upper.iloc[idx]
                )
                continue

            # Final upper band may only tighten downward unless previous close broke above it.
            if upper_value < prev_final_upper or prev_close > prev_final_upper:
                final_upper.iloc[idx] = upper_value
            else:
                final_upper.iloc[idx] = prev_final_upper

            # Final lower band may only tighten upward unless previous close broke below it.
            if lower_value > prev_final_lower or prev_close < prev_final_lower:
                final_lower.iloc[idx] = lower_value
            else:
                final_lower.iloc[idx] = prev_final_lower

            # Important:
            # Trend flip is tested against the PREVIOUS final band, not the current updated band.
            # This avoids unstable same-bar band/flip interactions.
            if trend.iloc[idx - 1] == -1 and df["close"].iloc[idx] > prev_final_upper:
                trend.iloc[idx] = 1
            elif trend.iloc[idx - 1] == 1 and df["close"].iloc[idx] < prev_final_lower:
                trend.iloc[idx] = -1
            else:
                trend.iloc[idx] = trend.iloc[idx - 1]

            supertrend.iloc[idx] = (
                final_lower.iloc[idx] if trend.iloc[idx] == 1 else final_upper.iloc[idx]
            )

        df["supertrend_direction"] = trend
        df["supertrend"] = supertrend

        # Plot columns:
        # Do NOT bfill/ffill SuperTrend. Keep warm-up and missing values as NaN.
        # This prevents the chart from drawing artificial long diagonal lines.
        df["plot_supertrend"] = supertrend
        df["plot_supertrend_long"] = np.where(trend == 1, supertrend, np.nan)
        df["plot_supertrend_short"] = np.where(trend == -1, supertrend, np.nan)

        long_flip = (trend == 1) & (trend.shift(1) == -1)
        short_flip = (trend == -1) & (trend.shift(1) == 1)

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

            # 1. Manage active long position.
            if position == "long":
                highest_high = (
                    max(highest_high, row["high"])
                    if np.isfinite(highest_high)
                    else row["high"]
                )
                stop = highest_high - atr_exit_mult * atr_value
                df.at[idx, "plot_atr_stop"] = stop

                atr_stop_hit = row["close"] < stop
                opposite_signal = exit_on_opposite and bool(short_flip.iloc[idx])

                if atr_stop_hit or opposite_signal:
                    df.at[idx, "close_long"] = True
                    df.at[idx, "exit_reason"] = (
                        "ATR trailing stop" if atr_stop_hit else "Opposite SuperTrend"
                    )
                    position = None
                    highest_high = np.nan

            # 2. Manage active short position.
            elif position == "short":
                lowest_low = (
                    min(lowest_low, row["low"])
                    if np.isfinite(lowest_low)
                    else row["low"]
                )
                stop = lowest_low + atr_exit_mult * atr_value
                df.at[idx, "plot_atr_stop"] = stop

                atr_stop_hit = row["close"] > stop
                opposite_signal = exit_on_opposite and bool(long_flip.iloc[idx])

                if atr_stop_hit or opposite_signal:
                    df.at[idx, "close_short"] = True
                    df.at[idx, "exit_reason"] = (
                        "ATR trailing stop" if atr_stop_hit else "Opposite SuperTrend"
                    )
                    position = None
                    lowest_low = np.nan

            # 3. Enter new position after exits.
            # This allows close-and-reverse on the same bar when the engine runs exit_before_entry=True.
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
    period = max(int(period), 1)

    prev_close = df["close"].shift(1)

    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return true_range.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period,
    ).mean()


def _validate_ohlcv(df: pd.DataFrame) -> None:
    required = {"date", "open", "high", "low", "close"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"SuperTrend strategy requires OHLCV columns: {sorted(missing)}")

    if df.empty:
        raise ValueError("SuperTrend strategy received an empty dataframe.")