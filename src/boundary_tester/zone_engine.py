from __future__ import annotations

import pandas as pd


def to_dataframe(result):
    if result is None:
        return None
    if hasattr(result, "to_dataframe"):
        return result.to_dataframe()
    if hasattr(result, "to_df"):
        return result.to_df()
    if isinstance(result, pd.DataFrame):
        return result
    try:
        return pd.DataFrame(result)
    except Exception:
        return None


def normalise_ohlcv_columns(df: pd.DataFrame, date_col_name: str = "date") -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if isinstance(out.index, pd.DatetimeIndex):
        index_name = out.index.name if out.index.name is not None else date_col_name
        if index_name in out.columns:
            index_name = f"__index_{date_col_name}__"
        out = out.reset_index(names=index_name)
    else:
        out = out.reset_index(drop=False)
        if date_col_name in out.columns and "index" in out.columns:
            out = out.drop(columns=["index"])

    rename_map = {}
    for col in out.columns:
        lower = str(col).lower().strip()
        if lower in ("date", "datetime", "timestamp", "time", f"__index_{date_col_name}__"):
            rename_map[col] = date_col_name
        elif lower in ("open", "adj_open"):
            rename_map[col] = "open"
        elif lower in ("high", "adj_high"):
            rename_map[col] = "high"
        elif lower in ("low", "adj_low"):
            rename_map[col] = "low"
        elif lower in ("close", "adj_close", "price"):
            rename_map[col] = "close"
        elif lower in ("volume", "vol"):
            rename_map[col] = "volume"

    out = out.rename(columns=rename_map)

    if date_col_name not in out.columns and len(out.columns) > 0:
        first_col = out.columns[0]
        trial = pd.to_datetime(out[first_col], errors="coerce")
        if trial.notna().sum() > 0:
            out[date_col_name] = trial

    if date_col_name in out.columns:
        out[date_col_name] = pd.to_datetime(out[date_col_name], errors="coerce")
        try:
            if getattr(out[date_col_name].dt, "tz", None) is not None:
                out[date_col_name] = out[date_col_name].dt.tz_localize(None)
        except (AttributeError, TypeError):
            pass

    out = out.reset_index(drop=True)
    if date_col_name in out.columns:
        out = out.dropna(subset=[date_col_name])
        out = out.sort_values(by=date_col_name, kind="stable").reset_index(drop=True)

    return out


def build_zone_rows_from_snapshot(
    ticker: str,
    valid_from: pd.Timestamp,
    valid_to: pd.Timestamp | None,
    selected_zones: list[dict],
) -> list[dict]:
    rows = []
    for index, zone in enumerate(selected_zones, start=1):
        source_types = set(zone.get("source_types", set()) or set())
        timeframes = set(zone.get("timeframes", set()) or set())
        rows.append(
            {
                "zone_id": f"{ticker}::{pd.Timestamp(valid_from).strftime('%Y%m%d')}::{zone['side']}::{index}",
                "structural_zone_key": _build_structural_zone_key(ticker=ticker, zone=zone),
                "ticker": ticker,
                "valid_from": pd.Timestamp(valid_from),
                "valid_to": valid_to,
                "zone_class": _zone_class_from_source_types(source_types),
                "side": zone["side"],
                "lower": float(zone["lower"]),
                "upper": float(zone["upper"]),
                "center": float(zone["center"]),
                "timeframe": zone.get("timeframe_sources") or ",".join(sorted(timeframes)),
                "source_reason": zone.get("source_types_label") or zone.get("source_label", ""),
                "confluence_count": max(len(source_types), 1),
                "metadata": {
                    "institutional_score": float(zone.get("institutional_score", 0.0)),
                    "structural_score": float(zone.get("structural_score", 0.0)),
                    "touch_count": int(zone.get("touch_count", 0)),
                    "reaction_score": float(zone.get("reaction_score", 0.0)),
                    "source_label": zone.get("source_label", ""),
                    "source_types": sorted(source_types),
                    "timeframes": sorted(timeframes),
                },
            }
        )
    return rows


def merge_snapshot_zones_into_structural_zones(zone_df: pd.DataFrame) -> pd.DataFrame:
    if zone_df.empty or "structural_zone_key" not in zone_df.columns:
        return zone_df.copy()

    grouped_rows: list[dict] = []
    for structural_zone_key, group in zone_df.groupby("structural_zone_key", sort=False):
        row0 = group.iloc[0]
        grouped_rows.append(
            {
                "structural_zone_key": structural_zone_key,
                "ticker": row0["ticker"],
                "side": row0["side"],
                "zone_class": row0["zone_class"],
                "timeframe": ",".join(sorted(set(",".join(group["timeframe"].astype(str)).split(",")))),
                "source_reason": ",".join(sorted(set(group["source_reason"].astype(str)))),
                "lower": float(group["lower"].min()),
                "upper": float(group["upper"].max()),
                "center": float(group["center"].mean()),
                "snapshot_count": int(len(group)),
                "valid_from": pd.to_datetime(group["valid_from"]).min(),
                "valid_to": pd.to_datetime(group["valid_to"]).max(),
            }
        )
    return pd.DataFrame(grouped_rows)


def _build_structural_zone_key(ticker: str, zone: dict) -> str:
    center = float(zone["center"])
    width = max(float(zone["upper"]) - float(zone["lower"]), 0.0)
    center_bucket = round(center * 200.0) / 200.0
    width_bucket = round(width * 400.0) / 400.0
    source_types = ",".join(sorted(set(zone.get("source_types", set()) or set())))
    timeframes = ",".join(sorted(set(zone.get("timeframes", set()) or set())))
    return f"{ticker}::{zone['side']}::{center_bucket:.4f}::{width_bucket:.4f}::{source_types}::{timeframes}"


def _zone_class_from_source_types(source_types: set[str]) -> str:
    if not source_types:
        return "composite"
    if len(source_types) > 1:
        return "composite"
    source = next(iter(source_types))
    if source.startswith("vp_"):
        return "inventory"
    if source.startswith("avwap_"):
        return "cost"
    return "structural"
