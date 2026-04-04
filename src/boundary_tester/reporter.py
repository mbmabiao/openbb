from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from .config import BoundaryTesterConfig
from .schema import BREAKOUT_EVENT_TYPES


def build_summary_table(labeled_events_df: pd.DataFrame) -> pd.DataFrame:
    breakout_df = (
        labeled_events_df[labeled_events_df["event_type"].isin(BREAKOUT_EVENT_TYPES)].copy()
        if not labeled_events_df.empty else pd.DataFrame()
    )
    if breakout_df.empty:
        return pd.DataFrame(
            [
                {
                    "total_events": 0,
                    "success_rate": 0.0,
                    "failure_rate": 0.0,
                    "unresolved_rate": 0.0,
                    "false_breakout_rate": 0.0,
                    "avg_follow_through": np.nan,
                    "avg_mfe": np.nan,
                    "avg_mae": np.nan,
                    "by_zone_class": "{}",
                    "by_first_test_flag": "{}",
                    "by_timeframe": "{}",
                    "by_confluence_bucket": "{}",
                }
            ]
        )

    working = breakout_df.copy()
    working["confluence_bucket"] = working["confluence_count"].apply(_bucket_confluence)
    working["first_test_bucket"] = working["is_first_test"].map({True: "first_test", False: "repeated_test"})

    summary = {
        "total_events": int(len(working)),
        "success_rate": float(working["success_flag"].mean()),
        "failure_rate": float(working["failure_flag"].mean()),
        "unresolved_rate": float(working["unresolved_flag"].mean()),
        "false_breakout_rate": float(working["failure_flag"].mean()),
        "avg_follow_through": float(working["follow_through_pct"].mean()),
        "avg_mfe": float(working["max_favorable_excursion"].mean()),
        "avg_mae": float(working["max_adverse_excursion"].mean()),
        "by_zone_class": _group_summary_json(working, "zone_class"),
        "by_first_test_flag": _group_summary_json(working, "first_test_bucket"),
        "by_timeframe": _group_summary_json(working, "zone_timeframe"),
        "by_confluence_bucket": _group_summary_json(working, "confluence_bucket"),
    }

    return pd.DataFrame([summary])


def write_report(
    output_dir: str | Path,
    events_df: pd.DataFrame,
    labeled_events_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    config: BoundaryTesterConfig,
) -> Path:
    output_path = Path(output_dir)
    report_path = output_path / "report.md"
    breakout_df = (
        labeled_events_df[labeled_events_df["event_type"].isin(BREAKOUT_EVENT_TYPES)].copy()
        if not labeled_events_df.empty else pd.DataFrame()
    )
    summary_row = summary_df.iloc[0].to_dict() if not summary_df.empty else {}

    lines = [
        "# Boundary Tester Report",
        "",
        "## 数据概览",
        f"- 总事件数: {len(events_df)}",
        f"- Breakout 事件数: {len(breakout_df)}",
        f"- 覆盖 ticker 数: {events_df['ticker'].nunique() if not events_df.empty else 0}",
        "",
        "## 总体统计",
        f"- Success Rate: {_fmt_pct(summary_row.get('success_rate'))}",
        f"- Failure Rate: {_fmt_pct(summary_row.get('failure_rate'))}",
        f"- Unresolved Rate: {_fmt_pct(summary_row.get('unresolved_rate'))}",
        f"- False Breakout Rate: {_fmt_pct(summary_row.get('false_breakout_rate'))}",
        f"- Avg Follow Through: {_fmt_pct(summary_row.get('avg_follow_through'))}",
        f"- Avg MFE: {_fmt_pct(summary_row.get('avg_mfe'))}",
        f"- Avg MAE: {_fmt_pct(summary_row.get('avg_mae'))}",
        "",
        "## 分组统计",
        "",
        "### By Zone Class",
        _json_block(summary_row.get("by_zone_class")),
        "",
        "### By First Test Flag",
        _json_block(summary_row.get("by_first_test_flag")),
        "",
        "### By Timeframe",
        _json_block(summary_row.get("by_timeframe")),
        "",
        "### By Confluence Bucket",
        _json_block(summary_row.get("by_confluence_bucket")),
        "",
        "## 主要发现",
        f"- Breakout 样本以 `{_dominant_bucket(breakout_df, 'zone_class')}` 为主。",
        f"- 首测与重复测试中表现更优的组别: `{_best_bucket(summary_row.get('by_first_test_flag'))}`。",
        f"- 时间框架表现最优组: `{_best_bucket(summary_row.get('by_timeframe'))}`。",
        "",
        "## 配置快照",
        "```json",
        json.dumps(config.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        "```",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def _group_summary_json(df: pd.DataFrame, column: str) -> str:
    payload: dict[str, dict[str, float | int]] = {}
    for key, group in df.groupby(column, dropna=False):
        payload[str(key)] = {
            "count": int(len(group)),
            "success_rate": float(group["success_flag"].mean()),
            "failure_rate": float(group["failure_flag"].mean()),
            "unresolved_rate": float(group["unresolved_flag"].mean()),
            "avg_follow_through": float(group["follow_through_pct"].mean()),
        }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _bucket_confluence(value: int) -> str:
    if value <= 1:
        return "single-source"
    if value == 2:
        return "double-source"
    return "3-source+"


def _fmt_pct(value) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return "N/A"


def _json_block(raw) -> str:
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        parsed = raw
    return f"```json\n{json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)}\n```"


def _dominant_bucket(df: pd.DataFrame, column: str) -> str:
    if df.empty or column not in df.columns:
        return "N/A"
    counts = df[column].value_counts(dropna=False)
    return str(counts.index[0]) if not counts.empty else "N/A"


def _best_bucket(raw_json: str | None) -> str:
    if not raw_json:
        return "N/A"
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return "N/A"
    if not payload:
        return "N/A"
    best_key = max(payload, key=lambda key: payload[key].get("success_rate", float("-inf")))
    return best_key
