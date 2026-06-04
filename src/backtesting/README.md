"""
Backtesting Strategy Development Protocol
=========================================

This module is a developer-facing protocol reference for creating strategies in
this OpenBB-based backtesting project.

Purpose
-------
A strategy must inherit from ``BaseStrategy`` and implement
``generate_signals(context)``. The strategy returns a primary-timeframe OHLCV
DataFrame. The backtesting engine reads that DataFrame bar by bar and executes
standard action columns.

Current execution model
-----------------------
- Single symbol.
- Single account.
- One active position at a time.
- Long and short are supported.
- No pyramiding.
- No simultaneous long and short.
- Reversal must be explicit: close existing position and open the opposite side.
- Default execution price is ``row[backtest_config.price_col]``, usually close.
- Optional intrabar trigger price columns can be used for stop, limit, breakout,
  and gap-style execution modelling.

This file is intentionally safe to import. It does not import project strategy
classes directly, so it can be used as a protocol helper, documentation source,
or test reference.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd


# =============================================================================
# Core protocol constants
# =============================================================================

REQUIRED_OHLCV_COLUMNS: tuple[str, ...] = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

REQUIRED_PRICE_COLUMNS: tuple[str, ...] = (
    "date",
    "open",
    "high",
    "low",
    "close",
)

ACTION_COLUMNS: tuple[str, ...] = (
    "open_long",
    "close_long",
    "open_short",
    "close_short",
)

ACTION_PRICE_COLUMNS: tuple[str, ...] = (
    "open_long_price",
    "close_long_price",
    "open_short_price",
    "close_short_price",
)

BUY_SIDE_TRIGGER_PRICE_COLUMNS: tuple[str, ...] = (
    "open_long_price",
    "close_short_price",
)

SELL_SIDE_TRIGGER_PRICE_COLUMNS: tuple[str, ...] = (
    "open_short_price",
    "close_long_price",
)

OPTIONAL_DISPLAY_COLUMNS: tuple[str, ...] = (
    "entry_reason",
    "exit_reason",
    "signal_score",
    "strategy_state",
    "session_type",
)

SIZING_COLUMNS: tuple[str, ...] = (
    "position_notional",
    "position_size_pct",
    "target_weight",
)

SIZING_PRIORITY: tuple[str, ...] = (
    "position_notional",
    "position_size_pct",
    "target_weight",
    "backtest_config.position_size_pct",
)

SUPPORTED_CONFIG_SCHEMA_TYPES: tuple[str, ...] = (
    "int",
    "float",
    "bool",
    "str",
    "select",
)

INTRADAY_TIMEFRAME_SUFFIXES: tuple[str, ...] = (
    "m",
    "min",
    "mins",
    "minute",
    "minutes",
    "h",
    "hr",
    "hrs",
    "hour",
    "hours",
)


# =============================================================================
# Human-readable protocol sections
# =============================================================================

PROTOCOL_SUMMARY = """
Backtesting Strategy Development Protocol

1. Strategy structure
   - Inherit from BaseStrategy.
   - Define name, display_name, description, required_timeframes,
     preferred_primary_timeframe, default_config, and config_schema.
   - Implement generate_signals(context) -> pd.DataFrame.

2. StrategyContext
   - context.symbol: selected symbol.
   - context.primary_timeframe: execution timeframe.
   - context.data: dict[str, pd.DataFrame], keyed by timeframe.
   - context.config: runtime strategy config.

3. Returned DataFrame
   - Must be based on the primary execution timeframe.
   - Must keep date, open, high, low, close, volume when available.
   - Must include or allow the engine to fill:
     open_long, close_long, open_short, close_short.

4. Execution model
   - One active position at a time.
   - No pyramiding.
   - No automatic reversal.
   - If reversing, emit close_long + open_short or close_short + open_long
     on the same bar and rely on exit_before_entry=True.

5. Price model
   - Default execution uses row[price_col], usually close.
   - Optional action-specific trigger prices:
     open_long_price, close_long_price, open_short_price, close_short_price.
   - Positive finite trigger prices must satisfy low <= price <= high.
   - Out-of-range positive trigger prices cause that action to be skipped.
   - Missing, NaN, inf, or <=0 trigger prices fall back to default price_col.

6. Slippage
   - Long entry: price * (1 + slippage).
   - Long exit: price * (1 - slippage).
   - Short entry: price * (1 - slippage).
   - Short exit: price * (1 + slippage).

7. Sizing
   - Strategy sizing priority:
     position_notional > position_size_pct > target_weight > config default.
   - In MVP, target_weight is treated as single-position equity percentage.
   - No leverage unless explicitly added later.

8. Extended hours
   - Use requires_extended_hours=True when the strategy cannot run without
     premarket or after-hours data.
   - Use supports_extended_hours=True when the strategy can optionally use it.
   - Use data_requirements to define per-timeframe extended-hours needs.

9. Multi-timeframe data
   - required_timeframes should include all needed timeframes.
   - preferred_primary_timeframe should identify the execution timeframe.
   - Higher timeframe features must be aligned to primary timeframe without
     lookahead bias.
   - Completed daily candles should only be visible to later intraday bars.

10. Lookahead bias
   - Do not generate a current-bar close execution signal from information that
     is only known after the current bar closes, unless this is intentionally a
     research-only approximation.
   - Conservative approach: shift indicator and signal inputs by one bar.
   - Intrabar trigger-price modelling only checks OHLC containment; it does not
     know the true tick path inside the candle.

11. Warmup
   - The engine does not handle warmup automatically.
   - Strategies must set all action columns to False during unstable indicator
     periods.

12. Display and debug
   - entry_reason and exit_reason are shown in trade output.
   - plot_* columns are rendered as chart overlays.
   - Use NaN for missing plot_* values, not 0.

13. Intraday metrics warning
   - Intraday annualised_return and Sharpe can be mechanically inflated.
   - Always inspect actual duration, bar count, trade count, total return,
     max drawdown, and per-trade behaviour.
""".strip()

STRATEGY_AUTHOR_CHECKLIST: tuple[str, ...] = (
    "Inherits from BaseStrategy.",
    "Defines a unique name.",
    "Defines display_name and description.",
    "Defines all required_timeframes.",
    "Defines preferred_primary_timeframe when the execution timeframe matters.",
    "Defines requires_extended_hours / supports_extended_hours correctly.",
    "Defines data_requirements for per-timeframe extended-hours needs.",
    "Exposes all user parameters through default_config.",
    "Exposes UI metadata through config_schema.",
    "generate_signals returns the primary-timeframe DataFrame.",
    "Returned DataFrame preserves date/open/high/low/close/volume.",
    "Initialises open_long, close_long, open_short, close_short.",
    "Handles missing / invalid OHLCV values explicitly.",
    "Avoids current-close signal with current-close execution unless intentional.",
    "Shifts indicators or signal inputs by one bar where needed.",
    "Handles indicator warmup by disabling all action columns.",
    "Respects allow_long / allow_short / trade_direction.",
    "Does not assume pyramiding or automatic reversal.",
    "Uses *_price columns only for intentional intrabar trigger modelling.",
    "Ensures trigger prices are finite and inside low/high when intended.",
    "Avoids outputting multiple sizing columns unless priority is intentional.",
    "Uses NaN for disconnected plot_* overlay values.",
    "For multi-timeframe strategies, uses completed higher-timeframe data only.",
    "Can run through run_backtest_from_context in a unit test.",
    "Trade log, chart markers, and equity curve agree with strategy intent.",
)


# =============================================================================
# Validation helpers
# =============================================================================

@dataclass(frozen=True)
class ProtocolCheckResult:
    """Result from a lightweight strategy-output protocol check."""

    ok: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    def raise_for_errors(self) -> None:
        """Raise ValueError if the protocol check found hard errors."""
        if self.errors:
            raise ValueError("Strategy protocol errors: " + "; ".join(self.errors))


def initialise_signal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of ``df`` with all required action columns present.

    Missing action columns are added as False. Existing action columns are filled
    with False and cast to bool.
    """
    out = df.copy()
    for col in ACTION_COLUMNS:
        if col not in out.columns:
            out[col] = False
        out[col] = out[col].fillna(False).astype(bool)
    return out


def initialise_optional_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of ``df`` with common optional display/debug columns present.
    """
    out = df.copy()
    for col in ("entry_reason", "exit_reason", "strategy_state", "session_type"):
        if col not in out.columns:
            out[col] = ""
    if "signal_score" not in out.columns:
        out["signal_score"] = np.nan
    return out


def disable_actions_during_warmup(df: pd.DataFrame, warmup_bars: int) -> pd.DataFrame:
    """
    Disable all action columns during the indicator warmup period.

    ``warmup_bars`` is interpreted as a positional row count. For example, if
    warmup_bars=50, rows 0..50 are disabled.
    """
    out = initialise_signal_columns(df)
    warmup = max(int(warmup_bars), 0)
    if not out.empty:
        out.loc[:warmup, list(ACTION_COLUMNS)] = False
    return out


def check_strategy_output(
    df: pd.DataFrame,
    *,
    price_col: str = "close",
    require_volume: bool = False,
) -> ProtocolCheckResult:
    """
    Lightly validate a strategy output DataFrame against the project protocol.

    This helper is not a replacement for the backtesting engine's internal
    checks. It is intended for strategy unit tests and development diagnostics.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if df is None or not isinstance(df, pd.DataFrame):
        return ProtocolCheckResult(
            ok=False,
            errors=("generate_signals must return a pandas DataFrame.",),
            warnings=(),
        )

    if df.empty:
        errors.append("Strategy output DataFrame is empty.")

    required = set(REQUIRED_PRICE_COLUMNS) | {price_col}
    if require_volume:
        required.add("volume")

    missing_required = sorted(required - set(df.columns))
    if missing_required:
        errors.append(f"Missing required columns: {missing_required}")

    missing_actions = sorted(set(ACTION_COLUMNS) - set(df.columns))
    if missing_actions:
        warnings.append(
            "Missing action columns will be filled False by the engine: "
            f"{missing_actions}"
        )

    for col in ACTION_COLUMNS:
        if col in df.columns and df[col].isna().any():
            warnings.append(f"Action column {col!r} contains NaN; fill with False.")

    if "date" in df.columns:
        dates = pd.to_datetime(df["date"], errors="coerce")
        if dates.isna().any():
            errors.append("date column contains invalid timestamps.")
        if not dates.dropna().is_monotonic_increasing:
            warnings.append("date column is not monotonic increasing.")

    for col in ["open", "high", "low", "close", price_col]:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.isna().any():
                errors.append(f"{col!r} contains non-numeric or NaN values.")

    if {"low", "high"}.issubset(df.columns):
        low = pd.to_numeric(df["low"], errors="coerce")
        high = pd.to_numeric(df["high"], errors="coerce")
        bad_range = (low > high).fillna(False)
        if bad_range.any():
            errors.append("Some rows have low > high.")

    for col in ACTION_PRICE_COLUMNS:
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce")
        finite_positive = values.replace([np.inf, -np.inf], np.nan).notna() & (values > 0)
        if finite_positive.any() and {"low", "high"}.issubset(df.columns):
            low = pd.to_numeric(df["low"], errors="coerce")
            high = pd.to_numeric(df["high"], errors="coerce")
            outside = finite_positive & ~((low <= values) & (values <= high))
            if outside.any():
                warnings.append(
                    f"{col!r} has finite positive prices outside low/high; "
                    "the engine will skip those actions."
                )

    multiple_sizing = [col for col in SIZING_COLUMNS if col in df.columns]
    if len(multiple_sizing) > 1:
        warnings.append(
            "Multiple sizing columns are present. Engine priority is: "
            + " > ".join(SIZING_PRIORITY)
        )

    plot_cols = [col for col in df.columns if col.startswith("plot_")]
    for col in plot_cols:
        if col in df.columns:
            values = pd.to_numeric(df[col], errors="coerce")
            if (values == 0).mean() > 0.8:
                warnings.append(
                    f"{col!r} is mostly zero. Use NaN for disconnected overlay values "
                    "unless zero is truly meaningful."
                )

    return ProtocolCheckResult(ok=not errors, errors=tuple(errors), warnings=tuple(warnings))


def check_config_schema(config_schema: Mapping[str, Mapping[str, Any]]) -> ProtocolCheckResult:
    """Validate the shape of a strategy ``config_schema`` dictionary."""
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(config_schema, Mapping):
        return ProtocolCheckResult(
            ok=False,
            errors=("config_schema must be a dictionary-like mapping.",),
            warnings=(),
        )

    for field_name, spec in config_schema.items():
        if not isinstance(spec, Mapping):
            errors.append(f"config_schema[{field_name!r}] must be a mapping.")
            continue
        field_type = str(spec.get("type", "str")).lower()
        if field_type not in SUPPORTED_CONFIG_SCHEMA_TYPES:
            errors.append(
                f"Unsupported type for {field_name!r}: {field_type!r}. "
                f"Supported: {SUPPORTED_CONFIG_SCHEMA_TYPES}"
            )
        if field_type == "select" and not spec.get("options"):
            errors.append(f"Select field {field_name!r} must define options.")
        if "default" not in spec:
            warnings.append(f"Field {field_name!r} has no default value.")
        if "label" not in spec:
            warnings.append(f"Field {field_name!r} has no label; UI may use raw name.")

    return ProtocolCheckResult(ok=not errors, errors=tuple(errors), warnings=tuple(warnings))


def normalise_trade_direction(config: Mapping[str, Any]) -> str:
    """
    Resolve trade direction from runtime strategy config.

    The runner may inject ``trade_direction`` based on UI allow_long/allow_short
    flags. If it is absent, this falls back to a strategy-level ``direction``.
    """
    direction = str(config.get("trade_direction", config.get("direction", "both"))).lower()
    if direction not in {"long", "short", "both", "none"}:
        return "both"
    return direction


def direction_allows_long(config: Mapping[str, Any]) -> bool:
    """Return True if the merged config allows long entries."""
    return normalise_trade_direction(config) in {"long", "both"}


def direction_allows_short(config: Mapping[str, Any]) -> bool:
    """Return True if the merged config allows short entries."""
    return normalise_trade_direction(config) in {"short", "both"}


def is_intraday_timeframe(timeframe: str) -> bool:
    """Lightweight intraday timeframe detector for protocol utilities."""
    normalized = str(timeframe or "").strip().lower().replace(" ", "")
    if not normalized:
        return False

    daily_or_higher = {
        "d",
        "1d",
        "day",
        "daily",
        "w",
        "1w",
        "week",
        "weekly",
        "mo",
        "1mo",
        "month",
        "monthly",
        "q",
        "1q",
        "quarter",
        "quarterly",
        "y",
        "1y",
        "year",
        "yearly",
    }
    if normalized in daily_or_higher:
        return False

    if normalized in {"hourly", "intraday"}:
        return True

    if normalized.isdigit():
        return int(normalized) > 0

    return any(normalized.endswith(suffix) for suffix in INTRADAY_TIMEFRAME_SUFFIXES)


def assert_no_illegal_reversal(df: pd.DataFrame) -> None:
    """
    Raise if a strategy emits an obvious illegal reversal in adjacent state scan.

    This is a lightweight approximation of engine behaviour for development
    checks. The engine remains the source of truth.
    """
    out = initialise_signal_columns(df)
    position: str | None = None

    for idx, row in out.reset_index(drop=True).iterrows():
        if position == "long" and bool(row["open_short"]) and not bool(row["close_long"]):
            raise ValueError(
                f"Illegal long-to-short reversal at row {idx}: "
                "open_short=True without close_long=True."
            )
        if position == "short" and bool(row["open_long"]) and not bool(row["close_short"]):
            raise ValueError(
                f"Illegal short-to-long reversal at row {idx}: "
                "open_long=True without close_short=True."
            )

        # Exit before entry approximation.
        if position == "long" and bool(row["close_long"]):
            position = None
        elif position == "short" and bool(row["close_short"]):
            position = None

        if position is None and bool(row["open_long"]):
            position = "long"
        elif position is None and bool(row["open_short"]):
            position = "short"


def format_checklist(items: Iterable[str] = STRATEGY_AUTHOR_CHECKLIST) -> str:
    """Return the strategy author checklist as markdown."""
    return "\n".join(f"- [ ] {item}" for item in items)


# =============================================================================
# Copy-paste strategy template
# =============================================================================

STRATEGY_TEMPLATE = r'''
from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.base import BaseStrategy, StrategyContext


class MyStrategy(BaseStrategy):
    name = "my_strategy"
    display_name = "My Strategy"
    description = "Example strategy following the project protocol."

    required_timeframes = ["5m"]
    preferred_primary_timeframe = "5m"

    requires_extended_hours = False
    supports_extended_hours = False

    default_config = {
        "fast_ma": 10,
        "slow_ma": 30,
        "direction": "both",
    }

    config_schema = {
        "fast_ma": {
            "type": "int",
            "label": "Fast MA",
            "default": 10,
            "min": 2,
            "max": 200,
            "step": 1,
            "required": True,
        },
        "slow_ma": {
            "type": "int",
            "label": "Slow MA",
            "default": 30,
            "min": 3,
            "max": 300,
            "step": 1,
            "required": True,
        },
        "direction": {
            "type": "select",
            "label": "Direction",
            "default": "both",
            "options": ["long", "short", "both"],
            "required": True,
        },
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        cfg = {**self.default_config, **context.config, **self.config}

        df = context.data[context.primary_timeframe].copy().reset_index(drop=True)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["date", "open", "high", "low", "close"]).copy()

        df["open_long"] = False
        df["close_long"] = False
        df["open_short"] = False
        df["close_short"] = False
        df["entry_reason"] = ""
        df["exit_reason"] = ""

        fast = int(cfg["fast_ma"])
        slow = int(cfg["slow_ma"])
        trade_direction = str(cfg.get("trade_direction", cfg.get("direction", "both"))).lower()

        df["fast_ma"] = df["close"].rolling(fast, min_periods=fast).mean()
        df["slow_ma"] = df["close"].rolling(slow, min_periods=slow).mean()

        # Conservative anti-lookahead style: use completed previous bar state.
        prev_fast = df["fast_ma"].shift(1)
        prev_slow = df["slow_ma"].shift(1)
        prev_close = df["close"].shift(1)

        long_signal = (prev_fast > prev_slow) & (prev_close > prev_fast)
        short_signal = (prev_fast < prev_slow) & (prev_close < prev_fast)

        allow_long = trade_direction in {"long", "both"}
        allow_short = trade_direction in {"short", "both"}

        df["open_long"] = long_signal & allow_long
        df["open_short"] = short_signal & allow_short
        df["close_long"] = short_signal
        df["close_short"] = long_signal

        df.loc[df["open_long"], "entry_reason"] = "Fast MA above slow MA"
        df.loc[df["open_short"], "entry_reason"] = "Fast MA below slow MA"
        df.loc[df["close_long"] | df["close_short"], "exit_reason"] = "Opposite signal"

        df["plot_fast_ma"] = df["fast_ma"]
        df["plot_slow_ma"] = df["slow_ma"]

        warmup = max(fast, slow)
        action_cols = ["open_long", "close_long", "open_short", "close_short"]
        df.loc[:warmup, action_cols] = False

        return df
'''.strip()


# =============================================================================
# Optional CLI helper
# =============================================================================

if __name__ == "__main__":
    print(PROTOCOL_SUMMARY)
    print("\nStrategy Author Checklist\n-------------------------")
    print(format_checklist())
    print("\nStrategy Template\n-----------------")
    print(STRATEGY_TEMPLATE)
