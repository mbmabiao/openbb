from __future__ import annotations

from typing import Any, TypedDict

import pandas as pd


class ChartPayload(TypedDict, total=False):
    candles: list[dict]
    volume: list[dict]
    overlays: list[dict]
    markers: list[dict]
    zones: list[dict]
    trades: list[dict]
    equity_curve: list[dict]


def build_market_chart_payload(
    df: pd.DataFrame,
    *,
    overlays: list[dict] | None = None,
    markers: list[dict] | None = None,
    zones: list[dict] | None = None,
) -> ChartPayload:
    return {
        "candles": _candles_from_frame(df),
        "volume": _volume_from_frame(df),
        "overlays": overlays or [],
        "markers": markers or [],
        "zones": zones or [],
        "trades": [],
        "equity_curve": [],
    }


def build_backtest_chart_payload(result) -> ChartPayload:
    from plotting.backtest_chart_adapter import build_backtest_chart_payload as _build

    return _build(result)


def payload_time(value: Any) -> int | str:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return ""
    if timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0:
        return timestamp.strftime("%Y-%m-%d")
    return int(timestamp.timestamp())


def _candles_from_frame(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    rows: list[dict] = []
    for _, row in df.iterrows():
        if any(pd.isna(row.get(column)) for column in ["date", "open", "high", "low", "close"]):
            continue
        rows.append(
            {
                "time": payload_time(row["date"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "change_pct": float(row["change_pct"]) if pd.notna(row.get("change_pct")) else None,
            }
        )
    return rows


def _volume_from_frame(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty or "volume" not in df.columns:
        return []
    rows: list[dict] = []
    for _, row in df.iterrows():
        if pd.isna(row.get("date")):
            continue
        close = float(row["close"]) if pd.notna(row.get("close")) else 0.0
        open_ = float(row["open"]) if pd.notna(row.get("open")) else close
        rows.append(
            {
                "time": payload_time(row["date"]),
                "value": float(row["volume"]) if pd.notna(row.get("volume")) else 0.0,
                "color": "rgba(251, 113, 133, 0.8)" if close >= open_ else "rgba(56, 213, 181, 0.8)",
            }
        )
    return rows

