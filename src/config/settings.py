from __future__ import annotations

from dataclasses import dataclass


PAGE_TITLE = "Institutional Support/Resistance Dashboard"
APP_TITLE = "Equity Data Dashboard"
TAB_NAMES = (
    "Historical Price",
    "Income",
    "Balance Sheet",
    "Cash Flow",
    "Ratios",
    "News",
)
HISTORY_RANGE_OPTIONS = ("1Y", "3Y", "5Y", "10Y", "Max")


@dataclass(frozen=True)
class SidebarDefaults:
    symbol: str = "000300.SS"
    history_range: str = "5Y"
    news_limit: int = 10
    short_vp_lookback_days: int = 21
    short_vp_bins: int = 48
    long_vp_lookback_days: int = 63
    long_vp_bins: int = 48
    zone_expand_bp: int = 50
    hv_node_quantile_pct: int = 75
    merge_pct_bp: int = 60
    max_resistance_zones: int = 3
    max_support_zones: int = 3
    show_avwap_lines: bool = True
    show_all_candidate_zones: bool = True
    show_atr_bands: bool = True
    atr_multiplier: float = 2.0
    reaction_lookahead: int = 5
    reaction_threshold_bp: int = 150
    min_touch_gap: int = 3
    exclude_last_unclosed_bar: bool = False
    show_live_last_bar_on_chart: bool = True
    initial_visible_bars: int = 200

    @property
    def vp_lookback_days(self) -> int:
        return self.short_vp_lookback_days

    @property
    def vp_bins(self) -> int:
        return self.short_vp_bins

    @property
    def weekly_vp_lookback(self) -> int:
        return self.long_vp_lookback_days

    @property
    def weekly_vp_bins(self) -> int:
        return self.long_vp_bins


@dataclass(frozen=True)
class ChartDefaults:
    height: int = 700
    right_offset: int = 5
    bar_spacing: int = 12
    min_bar_spacing: int = 4
