from __future__ import annotations

import pandas as pd


def zone_strength_from_center(
    *,
    price_history: pd.DataFrame,
    center: float,
    band_pct: float,
    date_column: str = "date",
    lookback_weeks: int = 52,
) -> dict[str, float]:
    pct = max(float(band_pct), 0.0)
    lower = float(center) * (1.0 - pct)
    upper = float(center) * (1.0 + pct)
    return zone_strength_from_interval(
        price_history=price_history,
        price_low=lower,
        price_high=upper,
        date_column=date_column,
        lookback_weeks=lookback_weeks,
    )


def zone_strength_from_interval(
    *,
    price_history: pd.DataFrame,
    price_low: float,
    price_high: float,
    date_column: str = "date",
    lookback_weeks: int = 52,
) -> dict[str, float]:
    recent = _recent_frame(price_history, date_column=date_column, lookback_weeks=lookback_weeks)
    zone_volume = _interval_volume(df=recent, price_low=price_low, price_high=price_high)
    return {
        "zone_volume": zone_volume,
        "zone_strength_pct": _strength_pct_from_volume(
            zone_volume=zone_volume,
            total_volume=_total_volume(recent),
        ),
    }


def _interval_volume(df: pd.DataFrame, price_low: float, price_high: float) -> float:
    if df.empty or "volume" not in df.columns or "high" not in df.columns or "low" not in df.columns:
        return 0.0

    lower, upper = sorted((float(price_low), float(price_high)))
    if upper <= 0:
        return 0.0

    high = pd.to_numeric(df.get("high"), errors="coerce")
    low = pd.to_numeric(df.get("low"), errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    touched = (low <= upper) & (high >= lower)
    return float(volume.where(touched, 0.0).sum())


def _recent_frame(df: pd.DataFrame, *, date_column: str = "date", lookback_weeks: int = 52) -> pd.DataFrame:
    if df.empty or date_column not in df.columns:
        return df
    dates = pd.to_datetime(df[date_column], errors="coerce")
    if dates.dropna().empty:
        return df
    cutoff = dates.max() - pd.Timedelta(weeks=max(int(lookback_weeks), 0))
    return df.loc[dates >= cutoff].copy()


def _total_volume(df: pd.DataFrame) -> float:
    if df.empty or "volume" not in df.columns:
        return 0.0
    return float(pd.to_numeric(df["volume"], errors="coerce").fillna(0.0).sum())


def _strength_pct_from_volume(*, zone_volume: float, total_volume: float) -> float:
    if total_volume <= 0:
        return 0.0
    return float(zone_volume) / float(total_volume) * 100.0
