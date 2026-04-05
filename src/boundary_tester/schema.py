from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


PRICE_REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
ZONE_REQUIRED_COLUMNS = [
    "zone_id",
    "ticker",
    "valid_from",
    "zone_class",
    "side",
    "lower",
    "upper",
    "center",
    "timeframe",
    "source_reason",
]

BREAKOUT_EVENT_TYPES = {"breakout_up", "breakout_down"}
DEFENSE_EVENT_TYPES = {"test", "probe"}


@dataclass(slots=True)
class Zone:
    zone_id: str
    ticker: str
    zone_class: str
    side: str
    lower: float
    upper: float
    center: float
    timeframe: str
    source_reason: str
    valid_from: pd.Timestamp
    valid_to: pd.Timestamp | None = None
    confluence_count: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)
