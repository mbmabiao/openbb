from __future__ import annotations

from dataclasses import dataclass


PAGE_TITLE = "Institutional Support/Resistance Dashboard"
APP_TITLE = "Equity Data Dashboard"
TAB_NAMES = (
    "Historical Price",
    "Strategy Backtest",
    "Income",
    "Balance Sheet",
    "Cash Flow",
    "Ratios",
    "News",
)
HISTORY_RANGE_OPTIONS = ("1Y", "3Y", "5Y", "10Y", "Max")


@dataclass(frozen=True)
class SidebarDefaults:
    symbol: str = "NVDA"
    history_range: str = "5Y"
    news_limit: int = 10
    long_vp_lookback_days: int = 63
    long_vp_bins: int = 48
    zone_expand_bp: int = 50
    show_ema20_line: bool = True
    show_ema50_line: bool = True
    show_atr_bands: bool = True
    atr_multiplier: float = 2.0
    exclude_last_unclosed_bar: bool = False
    show_live_last_bar_on_chart: bool = True
    initial_visible_bars: int = 200

@dataclass(frozen=True)
class ChartDefaults:
    height: int = 700
    right_offset: int = 5
    bar_spacing: int = 12
    min_bar_spacing: int = 4
