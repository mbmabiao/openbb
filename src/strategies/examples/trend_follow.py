from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.base import BaseStrategy, StrategyContext


class QuantDingerSuperTrendStrategy(BaseStrategy):
    name = "quantdinger_supertrend"
    display_name = "QuantDinger SuperTrend Trend-Following"
    description = (
        "Classic SuperTrend strategy using ATR-channel direction flips. "
        "It opens long on bullish flips, opens short on bearish flips, "
        "and closes the opposite side on the same bar."
    )
    required_timeframes = ["1d"]

    default_config = {
        "atr_period": 10,
        "multiplier": 3.0,
        "direction": "both",
        "position_size_pct": None,
    }

    config_schema = {
        "atr_period": {
            "type": "int",
            "label": "ATR Period",
            "default": 10,
            "min": 1,
            "max": 100,
            "step": 1,
            "help": "Wilder ATR smoothing period.",
            "required": True,
        },
        "multiplier": {
            "type": "float",
            "label": "ATR Multiplier",
            "default": 3.0,
            "min": 0.5,
            "max": 10.0,
            "step": 0.1,
            "help": "ATR band multiplier used to build the SuperTrend channel.",
            "required": True,
        },
        "direction": {
            "type": "select",
            "label": "Trade Direction",
            "default": "both",
            "options": ["long", "short", "both"],
            "help": "Allowed trading direction.",
            "required": True,
        },
        "position_size_pct": {
            "type": "float",
            "label": "Strategy Position Size",
            "default": 0.0,
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
            "help": (
                "Optional strategy-level position size. "
                "Set to 0 to let the backtest environment setting control sizing."
            ),
            "required": False,
        },
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data[context.primary_timeframe].copy().reset_index(drop=True)
        cfg = {**self.default_config, **context.config, **self.config}

        atr_period = int(cfg.get("atr_period", 10))
        multiplier = float(cfg.get("multiplier", 3.0))
        trade_direction = str(cfg.get("direction", "both")).lower()
        strategy_position_size_pct = cfg.get("position_size_pct")

        allow_long = trade_direction in {"long", "both"}
        allow_short = trade_direction in {"short", "both"}

        _validate_ohlcv(df)

        high = df["high"]
        low = df["low"]
        close = df["close"]
        prev_close = close.shift(1)

        # --- 1) True Range
        tr = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)

        # --- 2) Wilder-style ATR
        atr = tr.ewm(
            alpha=1.0 / max(atr_period, 1),
            adjust=False,
            min_periods=atr_period,
        ).mean()

        # --- 3) Basic SuperTrend bands
        hl2 = (high + low) / 2.0
        upper_basic = hl2 + multiplier * atr
        lower_basic = hl2 - multiplier * atr

        n = len(df)
        ub = upper_basic.to_numpy()
        lb = lower_basic.to_numpy()
        cl = close.to_numpy()

        final_upper = np.full(n, np.nan)
        final_lower = np.full(n, np.nan)
        direction = np.zeros(n, dtype=np.int8)  # 1=long trend, -1=short trend, 0=warmup
        supertrend = np.full(n, np.nan)

        start_idx = int(atr_period)

        for i in range(n):
            if i < start_idx or np.isnan(ub[i]) or np.isnan(lb[i]):
                continue

            if i == start_idx or direction[i - 1] == 0:
                final_upper[i] = ub[i]
                final_lower[i] = lb[i]
                direction[i] = 1 if cl[i] >= (ub[i] + lb[i]) / 2.0 else -1
                supertrend[i] = final_lower[i] if direction[i] == 1 else final_upper[i]
                continue

            # Final upper band may only tighten downward unless price already broke above it.
            if (ub[i] < final_upper[i - 1]) or (cl[i - 1] > final_upper[i - 1]):
                final_upper[i] = ub[i]
            else:
                final_upper[i] = final_upper[i - 1]

            # Final lower band may only tighten upward unless price already broke below it.
            if (lb[i] > final_lower[i - 1]) or (cl[i - 1] < final_lower[i - 1]):
                final_lower[i] = lb[i]
            else:
                final_lower[i] = final_lower[i - 1]

            # Direction uses the previous final band to avoid look-ahead bias.
            if cl[i] > final_upper[i - 1]:
                direction[i] = 1
            elif cl[i] < final_lower[i - 1]:
                direction[i] = -1
            else:
                direction[i] = direction[i - 1]

            supertrend[i] = final_lower[i] if direction[i] == 1 else final_upper[i]

        prev_direction = np.concatenate([[0], direction[:-1]])
        flip_long = (direction == 1) & (prev_direction == -1)
        flip_short = (direction == -1) & (prev_direction == 1)

        open_long = flip_long & allow_long
        open_short = flip_short & allow_short

        # R2 flip mode:
        #   bullish flip = close short + open long
        #   bearish flip = close long + open short
        close_long = flip_short
        close_short = flip_long

        # If direction is long-only, bearish flips should close long but not open short.
        # If direction is short-only, bullish flips should close short but not open long.
        if not allow_long:
            open_long = np.zeros(n, dtype=bool)
        if not allow_short:
            open_short = np.zeros(n, dtype=bool)

        df["atr"] = atr
        df["supertrend_direction"] = direction
        df["supertrend"] = supertrend
        df["plot_supertrend"] = pd.Series(supertrend).where(~np.isnan(supertrend), np.nan)

        df["open_long"] = pd.Series(open_long, index=df.index).fillna(False).astype(bool)
        df["open_short"] = pd.Series(open_short, index=df.index).fillna(False).astype(bool)
        df["close_long"] = pd.Series(close_long, index=df.index).fillna(False).astype(bool)
        df["close_short"] = pd.Series(close_short, index=df.index).fillna(False).astype(bool)

        df["entry_reason"] = ""
        df.loc[df["open_long"], "entry_reason"] = "SuperTrend bullish flip"
        df.loc[df["open_short"], "entry_reason"] = "SuperTrend bearish flip"

        df["exit_reason"] = ""
        df.loc[df["close_long"], "exit_reason"] = "SuperTrend bearish flip"
        df.loc[df["close_short"], "exit_reason"] = "SuperTrend bullish flip"

        # Optional strategy-controlled sizing.
        # If set to 0 or None, the engine will fall back to global backtest config.
        size_pct = _optional_positive_float(strategy_position_size_pct)
        if size_pct is not None and size_pct > 0:
            df["position_size_pct"] = min(size_pct, 1.0)

        return df


def _validate_ohlcv(df: pd.DataFrame) -> None:
    required = {"date", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required OHLCV columns: {sorted(missing)}")


def _optional_positive_float(value) -> float | None:
    if value is None:
        return None
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(output) or output <= 0:
        return None
    return output