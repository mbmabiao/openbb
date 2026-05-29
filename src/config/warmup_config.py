from __future__ import annotations

from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml


DEFAULT_WARMUP_CONFIG_PATH = Path(__file__).with_name("warmup.yaml")


@dataclass(frozen=True, slots=True)
class ZoneGenerationThresholdConfig:
    atr_period: int = 20
    weekly_swing_max_points_per_side: int = 3
    swing_left_bars: int = 2
    swing_right_bars: int = 2
    swing_volume_lookback_bars: int = 20
    swing_volume_quantile: float = 0.75
    strength_lookback_weeks: int = 52


@dataclass(frozen=True, slots=True)
class LifecycleThresholdConfig:
    default_lookback_years: int = 2
    weekly_swing_expiration_days: int = 182
    event_zone_ttl_bars: dict[str, int] = field(default_factory=lambda: {"1w": 26, "w": 26})


@dataclass(frozen=True, slots=True)
class PatternEventThresholdConfig:
    lookback_bars: int = 20
    high_volume_percentile: float = 0.80
    low_price_movement_percentile: float = 0.20
    long_wick_ratio: float = 0.40
    long_wick_atr_multiple: float = 0.30
    atr_period: int = 20


@dataclass(frozen=True, slots=True)
class MacdDivergenceConfig:
    enabled: bool = True
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9
    swing_left_bars: int = 2
    swing_right_bars: int = 2
    min_bar_distance: int = 1


@dataclass(frozen=True, slots=True)
class BreakoutThresholdConfig:
    breakout_confirm_buffer_atr: float = 0.10
    failure_buffer_atr: float = 0.10
    strong_follow_through_atr: float = 1.00
    weak_follow_through_atr: float = 0.30
    follow_through_window_bars: int = 5
    fast_failure_window_bars: int = 3
    failure_window_bars: int = 10
    retest_window_bars: int = 3


@dataclass(frozen=True, slots=True)
class WarmupThresholdConfig:
    zone_generation: ZoneGenerationThresholdConfig = ZoneGenerationThresholdConfig()
    lifecycle: LifecycleThresholdConfig = LifecycleThresholdConfig()
    pattern_events: PatternEventThresholdConfig = PatternEventThresholdConfig()
    macd_divergence: MacdDivergenceConfig = MacdDivergenceConfig()
    breakout: BreakoutThresholdConfig = BreakoutThresholdConfig()


def load_warmup_config(config_path: str | Path | None = None) -> WarmupThresholdConfig:
    path = Path(config_path) if config_path else DEFAULT_WARMUP_CONFIG_PATH
    if not path.exists():
        return WarmupThresholdConfig()
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Warmup config must be a YAML mapping: {path}")
    return WarmupThresholdConfig(
        zone_generation=_section(ZoneGenerationThresholdConfig, raw.get("zone_generation")),
        lifecycle=_section(LifecycleThresholdConfig, raw.get("lifecycle")),
        pattern_events=_section(PatternEventThresholdConfig, raw.get("pattern_events")),
        macd_divergence=_section(MacdDivergenceConfig, raw.get("macd_divergence")),
        breakout=_section(BreakoutThresholdConfig, raw.get("breakout")),
    )


def _section(section_type, values: Any):
    if values is None:
        return section_type()
    if not isinstance(values, dict):
        raise ValueError(f"{section_type.__name__} must be a YAML mapping")
    allowed = {field.name for field in fields(section_type)}
    unknown = sorted(set(values) - allowed)
    if unknown:
        raise ValueError(f"Unknown {section_type.__name__} keys: {', '.join(unknown)}")
    return section_type(**values)
