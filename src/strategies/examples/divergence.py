from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.base import BaseStrategy, StrategyContext
from zone_lifecycle.divergence_events import detect_macd_divergence_events_for_latest_bar


class MacdDivergenceMAFilterStrategy(BaseStrategy):
    name = "macd_divergence_ma_filter"
    display_name = "MACD Divergence + MA Filter"
    description = "Trades MACD bullish/bearish divergence events with optional MA filter and ATR-based exits."
    required_timeframes = ["1d"]

    default_config = {
        "enable_ma_filter": True,
        "ma_period": 50,
        "allow_long_entries": True,
        "allow_short_entries": True,
        "exit_on_opposite_divergence": True,
        "min_divergence_strength": 0.0,
        "atr_period": 10,
        "atr_exit_mult": 1.5,
        "take_profit_atr_mult": 3.0,
    }

    config_schema = {
        "enable_ma_filter": {
            "type": "bool",
            "label": "Enable MA Filter",
            "default": True,
            "help": "If enabled, long entries require close > MA and short entries require close < MA.",
        },
        "ma_period": {
            "type": "int",
            "label": "MA Period",
            "default": 50,
            "min": 2,
            "max": 300,
            "step": 1,
            "help": "Moving average period used as entry filter.",
            "required": True,
        },
        "allow_long_entries": {
            "type": "bool",
            "label": "Allow Long Entries",
            "default": True,
            "help": "Allow bullish divergence to open long positions.",
        },
        "allow_short_entries": {
            "type": "bool",
            "label": "Allow Short Entries",
            "default": True,
            "help": "Allow bearish divergence to open short positions.",
        },
        "exit_on_opposite_divergence": {
            "type": "bool",
            "label": "Exit on Opposite Divergence",
            "default": True,
            "help": "Close current position when opposite MACD divergence appears.",
        },
        "min_divergence_strength": {
            "type": "float",
            "label": "Min Divergence Strength",
            "default": 0.0,
            "min": 0.0,
            "max": 1.0,
            "step": 0.05,
            "help": "Minimum divergence strength score required to trade.",
        },
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
        "atr_exit_mult": {
            "type": "float",
            "label": "ATR Stop Multiplier",
            "default": 1.5,
            "min": 0.5,
            "max": 10.0,
            "step": 0.1,
            "help": "ATR multiplier used for trailing stop exits.",
            "required": True,
        },
        "take_profit_atr_mult": {
            "type": "float",
            "label": "ATR Take Profit Multiplier",
            "default": 3.0,
            "min": 0.5,
            "max": 20.0,
            "step": 0.1,
            "help": "ATR multiplier used for fixed take-profit distance from entry.",
            "required": True,
        },
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data[context.primary_timeframe].copy().reset_index(drop=True)
        cfg = {**self.default_config, **context.config, **self.config}

        enable_ma_filter = bool(cfg.get("enable_ma_filter", True))
        ma_period = max(int(cfg.get("ma_period", 50)), 1)

        allow_long_entries = bool(cfg.get("allow_long_entries", True)) and bool(cfg.get("allow_long", True))
        allow_short_entries = bool(cfg.get("allow_short_entries", True)) and bool(cfg.get("allow_short", True))

        trade_direction = str(cfg.get("trade_direction", "both")).lower()
        if trade_direction == "long":
            allow_short_entries = False
        elif trade_direction == "short":
            allow_long_entries = False
        elif trade_direction == "none":
            allow_long_entries = False
            allow_short_entries = False

        exit_on_opposite = bool(cfg.get("exit_on_opposite_divergence", True))
        min_strength = float(cfg.get("min_divergence_strength", 0.0))

        atr_period = max(int(cfg.get("atr_period", 10)), 1)
        atr_exit_mult = float(cfg.get("atr_exit_mult", 1.5))
        take_profit_atr_mult = float(cfg.get("take_profit_atr_mult", 3.0))

        df["ma"] = df["close"].rolling(ma_period, min_periods=ma_period).mean()
        df["atr"] = _atr(df, atr_period)

        df["plot_ma"] = df["ma"]
        df["plot_atr_stop"] = np.nan
        df["plot_take_profit"] = np.nan

        df["open_long"] = False
        df["close_long"] = False
        df["open_short"] = False
        df["close_short"] = False

        df["entry_reason"] = ""
        df["exit_reason"] = ""
        df["event_type"] = ""
        df["event_direction"] = ""
        df["event_strength"] = np.nan

        position: str | None = None
        entry_price = np.nan
        atr_at_entry = np.nan
        highest_high = np.nan
        lowest_low = np.nan

        for idx, row in df.iterrows():
            close = _to_float(row["close"])
            high = _to_float(row["high"])
            low = _to_float(row["low"])
            atr_value = _to_float(row["atr"])
            ma_value = _to_float(row["ma"])

            if not np.isfinite(close) or not np.isfinite(high) or not np.isfinite(low):
                continue

            history = _history_for_detector(df.iloc[: idx + 1])
            events = detect_macd_divergence_events_for_latest_bar(
                history,
                symbol=context.symbol,
                timeframe=context.primary_timeframe,
            )

            bullish_events = []
            bearish_events = []

            for event in events:
                strength = _to_float(getattr(event, "strength_score", 0.0))
                if not np.isfinite(strength) or strength < min_strength:
                    continue

                direction = str(getattr(event, "direction", "")).lower()
                event_type = str(getattr(event, "event_type", ""))

                if direction == "bullish" or event_type == "macd_bullish_divergence":
                    bullish_events.append(event)
                elif direction == "bearish" or event_type == "macd_bearish_divergence":
                    bearish_events.append(event)

            bullish_event = _best_event(bullish_events)
            bearish_event = _best_event(bearish_events)

            if bullish_event is not None:
                df.at[idx, "event_type"] = str(getattr(bullish_event, "event_type", ""))
                df.at[idx, "event_direction"] = "bullish"
                df.at[idx, "event_strength"] = float(getattr(bullish_event, "strength_score", np.nan))

            if bearish_event is not None:
                df.at[idx, "event_type"] = str(getattr(bearish_event, "event_type", ""))
                df.at[idx, "event_direction"] = "bearish"
                df.at[idx, "event_strength"] = float(getattr(bearish_event, "strength_score", np.nan))

            if position == "long":
                if np.isfinite(atr_value):
                    highest_high = max(highest_high, high) if np.isfinite(highest_high) else high
                    atr_stop = highest_high - atr_exit_mult * atr_value
                    take_profit = entry_price + take_profit_atr_mult * atr_at_entry

                    df.at[idx, "plot_atr_stop"] = atr_stop
                    df.at[idx, "plot_take_profit"] = take_profit

                    if close < atr_stop:
                        df.at[idx, "close_long"] = True
                        df.at[idx, "exit_reason"] = "ATR trailing stop"
                        position = None
                        entry_price = np.nan
                        atr_at_entry = np.nan
                        highest_high = np.nan

                    elif np.isfinite(take_profit) and close > take_profit:
                        df.at[idx, "close_long"] = True
                        df.at[idx, "exit_reason"] = "ATR take profit"
                        position = None
                        entry_price = np.nan
                        atr_at_entry = np.nan
                        highest_high = np.nan

                if position == "long" and exit_on_opposite and bearish_event is not None:
                    df.at[idx, "close_long"] = True
                    df.at[idx, "exit_reason"] = "Opposite bearish MACD divergence"
                    position = None
                    entry_price = np.nan
                    atr_at_entry = np.nan
                    highest_high = np.nan

            elif position == "short":
                if np.isfinite(atr_value):
                    lowest_low = min(lowest_low, low) if np.isfinite(lowest_low) else low
                    atr_stop = lowest_low + atr_exit_mult * atr_value
                    take_profit = entry_price - take_profit_atr_mult * atr_at_entry

                    df.at[idx, "plot_atr_stop"] = atr_stop
                    df.at[idx, "plot_take_profit"] = take_profit

                    if close > atr_stop:
                        df.at[idx, "close_short"] = True
                        df.at[idx, "exit_reason"] = "ATR trailing stop"
                        position = None
                        entry_price = np.nan
                        atr_at_entry = np.nan
                        lowest_low = np.nan

                    elif np.isfinite(take_profit) and close < take_profit:
                        df.at[idx, "close_short"] = True
                        df.at[idx, "exit_reason"] = "ATR take profit"
                        position = None
                        entry_price = np.nan
                        atr_at_entry = np.nan
                        lowest_low = np.nan

                if position == "short" and exit_on_opposite and bullish_event is not None:
                    df.at[idx, "close_short"] = True
                    df.at[idx, "exit_reason"] = "Opposite bullish MACD divergence"
                    position = None
                    entry_price = np.nan
                    atr_at_entry = np.nan
                    lowest_low = np.nan

            if position is None and np.isfinite(atr_value):
                long_ma_ok = True
                short_ma_ok = True

                if enable_ma_filter:
                    long_ma_ok = np.isfinite(ma_value) and close > ma_value
                    short_ma_ok = np.isfinite(ma_value) and close < ma_value

                if bullish_event is not None and allow_long_entries and long_ma_ok:
                    df.at[idx, "open_long"] = True
                    df.at[idx, "entry_reason"] = "Bullish MACD divergence"
                    df.at[idx, "event_type"] = str(getattr(bullish_event, "event_type", ""))
                    df.at[idx, "event_direction"] = "bullish"
                    df.at[idx, "event_strength"] = float(getattr(bullish_event, "strength_score", np.nan))

                    position = "long"
                    entry_price = close
                    atr_at_entry = atr_value
                    highest_high = high
                    lowest_low = np.nan

                    df.at[idx, "plot_atr_stop"] = highest_high - atr_exit_mult * atr_value
                    df.at[idx, "plot_take_profit"] = entry_price + take_profit_atr_mult * atr_at_entry

                elif bearish_event is not None and allow_short_entries and short_ma_ok:
                    df.at[idx, "open_short"] = True
                    df.at[idx, "entry_reason"] = "Bearish MACD divergence"
                    df.at[idx, "event_type"] = str(getattr(bearish_event, "event_type", ""))
                    df.at[idx, "event_direction"] = "bearish"
                    df.at[idx, "event_strength"] = float(getattr(bearish_event, "strength_score", np.nan))

                    position = "short"
                    entry_price = close
                    atr_at_entry = atr_value
                    lowest_low = low
                    highest_high = np.nan

                    df.at[idx, "plot_atr_stop"] = lowest_low + atr_exit_mult * atr_value
                    df.at[idx, "plot_take_profit"] = entry_price - take_profit_atr_mult * atr_at_entry

        return df


def _history_for_detector(frame: pd.DataFrame) -> pd.DataFrame:
    history = frame.copy()

    if "timestamp" not in history.columns:
        if "date" in history.columns:
            history["timestamp"] = pd.to_datetime(history["date"], errors="coerce")
        elif isinstance(history.index, pd.DatetimeIndex):
            history = history.reset_index().rename(columns={history.index.name or "index": "timestamp"})
        else:
            history["timestamp"] = pd.NaT

    history["timestamp"] = pd.to_datetime(history["timestamp"], errors="coerce").dt.tz_localize(None)

    required_columns = ["timestamp", "open", "high", "low", "close", "volume"]
    for column in required_columns:
        if column not in history.columns:
            history[column] = 0.0 if column == "volume" else np.nan

    return (
        history.loc[:, required_columns]
        .dropna(subset=["timestamp", "high", "low", "close"])
        .reset_index(drop=True)
    )


def _best_event(events: list) -> object | None:
    if not events:
        return None

    return max(
        events,
        key=lambda event: _to_float(getattr(event, "strength_score", 0.0))
        if np.isfinite(_to_float(getattr(event, "strength_score", 0.0)))
        else 0.0,
    )


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
    return true_range.ewm(
        alpha=1 / safe_period,
        adjust=False,
        min_periods=safe_period,
    ).mean()


def _to_float(value) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return np.nan
    return out if np.isfinite(out) else np.nan