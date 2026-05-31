from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class StrategyContext:
    symbol: str
    primary_timeframe: str
    data: dict[str, pd.DataFrame]
    config: dict


class BaseStrategy:
    name: str = "base"
    display_name: str = "Base Strategy"
    description: str = ""
    required_timeframes: list[str] = ["1d"]
    default_config: dict = {}
    config_schema: dict = {}

    def __init__(self, config: dict | None = None):
        self.config = {**self.default_config, **(config or {})}

    def prepare(self, context: StrategyContext) -> StrategyContext:
        return context

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        raise NotImplementedError

