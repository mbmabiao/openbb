from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.base import BaseStrategy, StrategyContext


class PremarketGapMeanReversionStrategy(BaseStrategy):
    name = "premarket_gap_mean_reversion"
    display_name = "Premarket Gap Mean Reversion"
    description = (
        "Mean-reversion strategy using premarket 5-minute candles and "
        "previous daily close / previous daily ATR context. Entries and exits "
        "use intrabar trigger prices when available."
    )

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
        "gap_atr_mult": 0.5,
        "atr_period": 14,
        "stop_atr_mult": 0.5,
        "exit_after_regular_open": True,
    }

    config_schema = {
        "gap_atr_mult": {
            "type": "float",
            "label": "Gap ATR Multiplier",
            "default": 0.5,
            "min": 0.1,
            "max": 5.0,
            "step": 0.1,
            "help": "Minimum premarket gap size measured as a multiple of previous daily ATR.",
            "required": True,
        },
        "atr_period": {
            "type": "int",
            "label": "Daily ATR Period",
            "default": 14,
            "min": 2,
            "max": 100,
            "step": 1,
            "help": "ATR period calculated from daily candles.",
            "required": True,
        },
        "stop_atr_mult": {
            "type": "float",
            "label": "Stop ATR Multiplier",
            "default": 0.5,
            "min": 0.1,
            "max": 5.0,
            "step": 0.1,
            "help": "Stop distance based on previous daily ATR.",
            "required": True,
        },
        "exit_after_regular_open": {
            "type": "bool",
            "label": "Exit After Regular Open",
            "default": True,
            "help": "Close any open premarket position once the regular session starts.",
        },
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        cfg = {**self.default_config, **context.config, **self.config}

        gap_atr_mult = float(cfg.get("gap_atr_mult", 0.5))
        atr_period = int(cfg.get("atr_period", 14))
        stop_atr_mult = float(cfg.get("stop_atr_mult", 0.5))
        exit_after_regular_open = bool(cfg.get("exit_after_regular_open", True))

        trade_direction = str(cfg.get("trade_direction", "both")).lower()
        allow_long = trade_direction in {"long", "both"} and bool(cfg.get("allow_long", True))
        allow_short = trade_direction in {"short", "both"} and bool(cfg.get("allow_short", True))

        df = context.data[context.primary_timeframe].copy().reset_index(drop=True)
        daily = context.data.get("1d", pd.DataFrame()).copy().reset_index(drop=True)

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        daily["date"] = pd.to_datetime(daily["date"], errors="coerce")

        for column in ["open", "high", "low", "close", "volume"]:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")
            if column in daily.columns:
                daily[column] = pd.to_numeric(daily[column], errors="coerce")

        df = df.dropna(subset=["date", "open", "high", "low", "close"]).copy()
        daily = daily.dropna(subset=["date", "high", "low", "close"]).copy()

        _initialise_signal_columns(df)

        if daily.empty:
            return df

        daily_context = _prepare_daily_context(daily, atr_period=atr_period)
        df = _attach_daily_context(df, daily_context)

        position: str | None = None
        traded_session_dates: set[pd.Timestamp] = set()
        stop_price = np.nan

        for idx, row in df.iterrows():
            timestamp = pd.Timestamp(row["date"])
            session_date = pd.Timestamp(timestamp.date())
            session_type = _session_type(timestamp)

            df.at[idx, "session_type"] = session_type

            prev_close = _to_float(row.get("prev_daily_close"))
            daily_atr = _to_float(row.get("prev_daily_atr"))
            open_price = _to_float(row["open"])
            high = _to_float(row["high"])
            low = _to_float(row["low"])
            close = _to_float(row["close"])

            if not np.isfinite(prev_close) or prev_close <= 0:
                continue
            if not all(np.isfinite(value) for value in [open_price, high, low, close]):
                continue

            has_atr = np.isfinite(daily_atr) and daily_atr > 0

            long_trigger = prev_close - gap_atr_mult * daily_atr if has_atr else np.nan
            short_trigger = prev_close + gap_atr_mult * daily_atr if has_atr else np.nan

            gap_abs = close - prev_close
            gap_pct = gap_abs / prev_close
            gap_atr = gap_abs / daily_atr if has_atr else np.nan

            df.at[idx, "gap_abs"] = gap_abs
            df.at[idx, "gap_pct"] = gap_pct
            df.at[idx, "gap_atr"] = gap_atr
            df.at[idx, "plot_prev_close"] = prev_close
            df.at[idx, "plot_gap_target"] = prev_close

            if np.isfinite(long_trigger):
                df.at[idx, "plot_long_trigger"] = long_trigger
            if np.isfinite(short_trigger):
                df.at[idx, "plot_short_trigger"] = short_trigger

            # 1) Manage open position first.
            if position == "long":
                if np.isfinite(stop_price):
                    df.at[idx, "plot_atr_stop"] = stop_price

                stopped = _bar_contains_price(low, high, stop_price)
                gap_filled = _bar_contains_price(low, high, prev_close)
                regular_open_exit = (
                    exit_after_regular_open
                    and session_type == "regular"
                    and _bar_contains_price(low, high, open_price)
                )

                if stopped:
                    df.at[idx, "close_long"] = True
                    df.at[idx, "close_long_price"] = stop_price
                    df.at[idx, "exit_reason"] = "ATR stop"
                    position = None
                    stop_price = np.nan
                    continue

                if gap_filled:
                    df.at[idx, "close_long"] = True
                    df.at[idx, "close_long_price"] = prev_close
                    df.at[idx, "exit_reason"] = "Gap filled"
                    position = None
                    stop_price = np.nan
                    continue

                if regular_open_exit:
                    df.at[idx, "close_long"] = True
                    df.at[idx, "close_long_price"] = open_price
                    df.at[idx, "exit_reason"] = "Regular session open exit"
                    position = None
                    stop_price = np.nan
                    continue

            elif position == "short":
                if np.isfinite(stop_price):
                    df.at[idx, "plot_atr_stop"] = stop_price

                stopped = _bar_contains_price(low, high, stop_price)
                gap_filled = _bar_contains_price(low, high, prev_close)
                regular_open_exit = (
                    exit_after_regular_open
                    and session_type == "regular"
                    and _bar_contains_price(low, high, open_price)
                )

                if stopped:
                    df.at[idx, "close_short"] = True
                    df.at[idx, "close_short_price"] = stop_price
                    df.at[idx, "exit_reason"] = "ATR stop"
                    position = None
                    stop_price = np.nan
                    continue

                if gap_filled:
                    df.at[idx, "close_short"] = True
                    df.at[idx, "close_short_price"] = prev_close
                    df.at[idx, "exit_reason"] = "Gap filled"
                    position = None
                    stop_price = np.nan
                    continue

                if regular_open_exit:
                    df.at[idx, "close_short"] = True
                    df.at[idx, "close_short_price"] = open_price
                    df.at[idx, "exit_reason"] = "Regular session open exit"
                    position = None
                    stop_price = np.nan
                    continue

            # 2) Entry: entire premarket only. No regular/after-hours entries.
            # Max one trade per session date.
            if position is not None:
                continue
            if session_type != "premarket":
                continue
            if session_date in traded_session_dates:
                continue
            if not has_atr:
                continue

            long_triggered = allow_long and _bar_contains_price(low, high, long_trigger)
            short_triggered = allow_short and _bar_contains_price(low, high, short_trigger)

            if long_triggered:
                df.at[idx, "open_long"] = True
                df.at[idx, "open_long_price"] = long_trigger
                df.at[idx, "entry_reason"] = f"Premarket gap down touched {gap_atr_mult:.2f} ATR trigger"

                position = "long"
                traded_session_dates.add(session_date)

                stop_price = long_trigger - stop_atr_mult * daily_atr
                df.at[idx, "plot_atr_stop"] = stop_price

            elif short_triggered:
                df.at[idx, "open_short"] = True
                df.at[idx, "open_short_price"] = short_trigger
                df.at[idx, "entry_reason"] = f"Premarket gap up touched {gap_atr_mult:.2f} ATR trigger"

                position = "short"
                traded_session_dates.add(session_date)

                stop_price = short_trigger + stop_atr_mult * daily_atr
                df.at[idx, "plot_atr_stop"] = stop_price

        return df


def _initialise_signal_columns(df: pd.DataFrame) -> None:
    df["open_long"] = False
    df["close_long"] = False
    df["open_short"] = False
    df["close_short"] = False

    # Optional intrabar execution price columns.
    df["open_long_price"] = np.nan
    df["close_long_price"] = np.nan
    df["open_short_price"] = np.nan
    df["close_short_price"] = np.nan

    df["entry_reason"] = ""
    df["exit_reason"] = ""
    df["session_type"] = ""

    # Debug / analysis columns.
    df["gap_abs"] = np.nan
    df["gap_pct"] = np.nan
    df["gap_atr"] = np.nan

    # Plot columns.
    df["plot_prev_close"] = np.nan
    df["plot_gap_target"] = np.nan
    df["plot_long_trigger"] = np.nan
    df["plot_short_trigger"] = np.nan
    df["plot_atr_stop"] = np.nan


def _prepare_daily_context(daily: pd.DataFrame, atr_period: int) -> pd.DataFrame:
    daily = daily.sort_values("date", kind="stable").reset_index(drop=True).copy()
    daily["session_date"] = daily["date"].dt.normalize()

    prev_close = daily["close"].shift(1)

    true_range = pd.concat(
        [
            daily["high"] - daily["low"],
            (daily["high"] - prev_close).abs(),
            (daily["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    safe_period = max(int(atr_period), 1)

    daily["daily_atr"] = true_range.ewm(
        alpha=1.0 / safe_period,
        adjust=False,
        min_periods=safe_period,
    ).mean()

    # For an intraday session on day D, use the previous completed daily candle.
    daily["prev_daily_close"] = daily["close"].shift(1)
    daily["prev_daily_atr"] = daily["daily_atr"].shift(1)

    return daily.loc[:, ["session_date", "prev_daily_close", "prev_daily_atr"]]


def _attach_daily_context(intraday: pd.DataFrame, daily_context: pd.DataFrame) -> pd.DataFrame:
    out = intraday.copy()
    out["session_date"] = out["date"].dt.normalize()

    context = daily_context.dropna(subset=["session_date"]).sort_values("session_date", kind="stable").copy()
    out = out.merge(context, on="session_date", how="left")

    missing = out["prev_daily_close"].isna()
    if missing.any() and not context.empty:
        fallback_source = (
            out.loc[missing, ["date", "session_date"]]
            .sort_values("session_date", kind="stable")
            .copy()
        )

        fallback = pd.merge_asof(
            fallback_source,
            context,
            on="session_date",
            direction="backward",
        )

        out.loc[missing, "prev_daily_close"] = fallback["prev_daily_close"].to_numpy()
        out.loc[missing, "prev_daily_atr"] = fallback["prev_daily_atr"].to_numpy()

    return out


def _session_type(timestamp: pd.Timestamp) -> str:
    t = pd.Timestamp(timestamp).time()

    # US equities rough session split.
    # Premarket: 04:00 - 09:30
    # Regular:   09:30 - 16:00
    # After:     16:00 - 20:00
    if pd.Timestamp("04:00").time() <= t < pd.Timestamp("09:30").time():
        return "premarket"
    if pd.Timestamp("09:30").time() <= t < pd.Timestamp("16:00").time():
        return "regular"
    if pd.Timestamp("16:00").time() <= t < pd.Timestamp("20:00").time():
        return "afterhours"
    return "closed"


def _bar_contains_price(low: float, high: float, price: float) -> bool:
    if not all(np.isfinite(value) for value in [low, high, price]):
        return False
    lower = min(low, high)
    upper = max(low, high)
    return lower <= price <= upper


def _to_float(value) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return np.nan
    return out if np.isfinite(out) else np.nan