from __future__ import annotations

import pandas as pd

from strategies.base import BaseStrategy, StrategyContext


class PremarketGapMeanReversionStrategy(BaseStrategy):
    name = "premarket_gap_mean_reversion"
    display_name = "Premarket Gap Mean Reversion"
    description = "Mean-reversion strategy using premarket 5-minute candles and previous daily close/ATR."

    required_timeframes = ["5m", "1d"]
    preferred_primary_timeframe = "5m"
    requires_extended_hours = True
    supports_extended_hours = True

    data_requirements = {
        "primary_timeframe": "5m",
        "timeframes": {
            "5m": {
                "extended_hours": True,
                "role": "execution",
            },
            "1d": {
                "extended_hours": False,
                "role": "daily_context",
            },
        },
    }

    default_config = {
        "gap_threshold_pct": 1.0,
    }
    config_schema = {
        "gap_threshold_pct": {
            "type": "float",
            "label": "Gap Threshold (%)",
            "default": 1.0,
            "min": 0.1,
            "max": 10.0,
            "step": 0.1,
            "help": "Minimum premarket gap versus previous daily close.",
        }
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data[context.primary_timeframe].copy().reset_index(drop=True)
        df["open_long"] = False
        df["close_long"] = False
        df["open_short"] = False
        df["close_short"] = False
        df["entry_reason"] = ""
        df["exit_reason"] = ""
        return df
