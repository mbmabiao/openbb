from __future__ import annotations

import json

import pandas as pd

from .config import BoundaryTesterConfig
from .schema import PRICE_REQUIRED_COLUMNS, ZONE_REQUIRED_COLUMNS


def _rename_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).strip() for col in out.columns]
    rename_map: dict[str, str] = {}

    for col in out.columns:
        lower = str(col).lower().strip()
        if lower in {"timestamp", "datetime", "date", "time"}:
            rename_map[col] = "timestamp"
        elif lower in {"open", "adj_open"}:
            rename_map[col] = "open"
        elif lower in {"high", "adj_high"}:
            rename_map[col] = "high"
        elif lower in {"low", "adj_low"}:
            rename_map[col] = "low"
        elif lower in {"close", "adj_close", "price"}:
            rename_map[col] = "close"
        elif lower in {"volume", "vol"}:
            rename_map[col] = "volume"
        elif lower == "ticker":
            rename_map[col] = "ticker"

    return out.rename(columns=rename_map)


def prepare_price_frame(price_df: pd.DataFrame, config: BoundaryTesterConfig) -> pd.DataFrame:
    out = _rename_price_columns(price_df)
    missing = [col for col in PRICE_REQUIRED_COLUMNS if col not in out.columns]
    if missing:
        raise ValueError(f"Price data missing required columns: {missing}")

    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    try:
        if getattr(out["timestamp"].dt, "tz", None) is not None:
            out["timestamp"] = out["timestamp"].dt.tz_localize(None)
    except (AttributeError, TypeError):
        pass

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()

    out = out.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"]).copy()
    out = out.sort_values(["ticker", "timestamp"] if "ticker" in out.columns else ["timestamp"], kind="stable")
    out = out.reset_index(drop=True)

    if config.use_atr_filter:
        out["atr"] = compute_atr(out, window=config.atr_window)

    return out


def prepare_zone_frame(zone_df: pd.DataFrame) -> pd.DataFrame:
    out = zone_df.copy()
    out.columns = [str(col).strip() for col in out.columns]
    missing = [col for col in ZONE_REQUIRED_COLUMNS if col not in out.columns]
    if missing:
        raise ValueError(f"Zone data missing required columns: {missing}")

    out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
    out["valid_from"] = pd.to_datetime(out["valid_from"], errors="coerce")
    out["valid_to"] = pd.to_datetime(out["valid_to"], errors="coerce") if "valid_to" in out.columns else pd.NaT

    for col in ["lower", "upper", "center"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "confluence_count" not in out.columns:
        out["confluence_count"] = 1
    out["confluence_count"] = pd.to_numeric(out["confluence_count"], errors="coerce").fillna(1).astype(int)
    if "structural_zone_key" not in out.columns:
        out["structural_zone_key"] = out["zone_id"].astype(str)

    if "metadata" not in out.columns:
        out["metadata"] = [{} for _ in range(len(out))]
    out["metadata"] = out["metadata"].apply(_normalize_metadata)

    out = out.dropna(subset=["zone_id", "ticker", "valid_from", "lower", "upper", "center"]).copy()
    out = out.sort_values(["ticker", "valid_from", "zone_id"], kind="stable").reset_index(drop=True)
    return out


def _normalize_metadata(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        try:
            parsed = json.loads(stripped)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except json.JSONDecodeError:
            return {"raw": stripped}
    if pd.isna(value):
        return {}
    return {"value": value}


def compute_atr(price_df: pd.DataFrame, window: int = 14) -> pd.Series:
    if "ticker" in price_df.columns:
        grouped = []
        for _, group in price_df.groupby("ticker", sort=False):
            grouped.append(_compute_group_atr(group, window))
        return pd.concat(grouped).sort_index()
    return _compute_group_atr(price_df, window)


def _compute_group_atr(group: pd.DataFrame, window: int) -> pd.Series:
    prev_close = group["close"].shift(1)
    true_range = pd.concat(
        [
            (group["high"] - group["low"]).abs(),
            (group["high"] - prev_close).abs(),
            (group["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(window=window, min_periods=1).mean()
