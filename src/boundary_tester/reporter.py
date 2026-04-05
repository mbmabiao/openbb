from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from .config import BoundaryTesterConfig
from .schema import BREAKOUT_EVENT_TYPES, DEFENSE_EVENT_TYPES


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
                    "failed_follow_through_rate": 0.0,
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
        "false_breakout_rate": float(working["false_breakout_flag"].mean()),
        "failed_follow_through_rate": float(working["failed_follow_through_flag"].mean()),
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
        "## Data Overview / 数据概览",
        f"- Total events: {len(events_df)}",
        f"- Breakout events: {len(breakout_df)}",
        f"- Covered tickers: {events_df['ticker'].nunique() if not events_df.empty else 0}",
        "",
        "## Aggregate Metrics / 总体统计",
        f"- Success Rate: {_fmt_pct(summary_row.get('success_rate'))}",
        f"- Failure Rate: {_fmt_pct(summary_row.get('failure_rate'))}",
        f"- Unresolved Rate: {_fmt_pct(summary_row.get('unresolved_rate'))}",
        f"- False Breakout Rate: {_fmt_pct(summary_row.get('false_breakout_rate'))}",
        f"- Failed Follow Through Rate: {_fmt_pct(summary_row.get('failed_follow_through_rate'))}",
        f"- Avg Follow Through: {_fmt_pct(summary_row.get('avg_follow_through'))}",
        f"- Avg MFE: {_fmt_pct(summary_row.get('avg_mfe'))}",
        f"- Avg MAE: {_fmt_pct(summary_row.get('avg_mae'))}",
        "",
        "## Segment Summaries / 分组统计",
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
        "## Key Findings / 主要发现",
        f"- Dominant breakout zone class: `{_dominant_bucket(breakout_df, 'zone_class')}`.",
        f"- Best first-test bucket by success rate: `{_best_bucket(summary_row.get('by_first_test_flag'))}`.",
        f"- Best timeframe bucket by success rate: `{_best_bucket(summary_row.get('by_timeframe'))}`.",
        "",
        "## Config Snapshot / 配置快照",
        "```json",
        json.dumps(config.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        "```",
    ]

    return _safe_write_report(report_path, "\n".join(lines))


def _group_summary_json(df: pd.DataFrame, column: str) -> str:
    payload: dict[str, dict[str, float | int]] = {}
    for key, group in df.groupby(column, dropna=False):
        payload[str(key)] = {
            "count": int(len(group)),
            "success_rate": float(group["success_flag"].mean()),
            "failure_rate": float(group["failure_flag"].mean()),
            "unresolved_rate": float(group["unresolved_flag"].mean()),
            "false_breakout_rate": float(group["false_breakout_flag"].mean()),
            "failed_follow_through_rate": float(group["failed_follow_through_flag"].mean()),
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


def _safe_write_report(path: Path, content: str) -> Path:
    try:
        path.write_text(content, encoding="utf-8")
        return path
    except PermissionError:
        fallback_path = _build_locked_file_fallback_path(path)
        fallback_path.write_text(content, encoding="utf-8")
        return fallback_path


def _build_locked_file_fallback_path(path: Path) -> Path:
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    return path.with_name(f"{path.stem}.{timestamp}{path.suffix}")


def build_zone_breakout_summary_table(labeled_events_df: pd.DataFrame) -> pd.DataFrame:
    breakout_df = (
        labeled_events_df[labeled_events_df["event_type"].isin(BREAKOUT_EVENT_TYPES)].copy()
        if not labeled_events_df.empty else pd.DataFrame()
    )
    if breakout_df.empty:
        return pd.DataFrame([_empty_breakout_summary()])

    working = breakout_df.copy()
    working["confluence_bucket"] = working["confluence_count"].apply(_bucket_confluence)
    working["first_meaningful_test_bucket"] = working["is_first_meaningful_test"].map({True: "first_touch", False: "repeated_touch"})
    working["touch_count_bucket"] = working["meaningful_touch_count_before_event"].apply(_bucket_touch_count_v2)
    working["zone_width_bucket"] = working["zone_width_pct"].apply(_bucket_zone_width_v2)
    working["breakout_quality_bucket"] = working["breakout_quality_score"].apply(_bucket_breakout_quality_v2)

    structural_zone_count = int(working["structural_zone_key"].nunique()) if "structural_zone_key" in working.columns else 0
    return pd.DataFrame(
        [
            {
                "total_events": int(len(working)),
                "structural_zone_count": structural_zone_count,
                "event_per_structural_zone": float(len(working) / max(structural_zone_count, 1)),
                "success_rate": float(working["success_flag"].mean()),
                "failure_rate": float(working["failure_flag"].mean()),
                "unresolved_rate": float(working["unresolved_flag"].mean()),
                "false_breakout_rate": float(working["false_breakout_flag"].mean()),
                "failed_follow_through_rate": float(working["failed_follow_through_flag"].mean()),
                "hold_rate_after_breakout": float(working["hold_outside_flag"].mean()),
                "avg_follow_through": float(working["follow_through_pct"].mean()),
                "avg_mfe": float(working["max_favorable_excursion"].mean()),
                "avg_mae": float(working["max_adverse_excursion"].mean()),
                "median_follow_through": float(working["follow_through_pct"].median()),
                "median_mfe": float(working["max_favorable_excursion"].median()),
                "median_mae": float(working["max_adverse_excursion"].median()),
                "by_zone_class": _group_summary_json_v2(working, "zone_class", "breakout"),
                "by_first_meaningful_test": _group_summary_json_v2(working, "first_meaningful_test_bucket", "breakout"),
                "by_timeframe": _group_summary_json_v2(working, "zone_timeframe", "breakout"),
                "by_confluence_bucket": _group_summary_json_v2(working, "confluence_bucket", "breakout"),
                "by_touch_count": _group_summary_json_v2(working, "touch_count_bucket", "breakout"),
                "by_zone_width_bucket": _group_summary_json_v2(working, "zone_width_bucket", "breakout"),
                "by_breakout_quality_bucket": _group_summary_json_v2(working, "breakout_quality_bucket", "breakout"),
                "by_failure_subtype": _group_summary_json_v2(working, "failure_subtype", "breakout"),
            }
        ]
    )


def build_zone_defense_summary_table(labeled_events_df: pd.DataFrame) -> pd.DataFrame:
    defense_df = (
        labeled_events_df[labeled_events_df["event_type"].isin(DEFENSE_EVENT_TYPES)].copy()
        if not labeled_events_df.empty else pd.DataFrame()
    )
    if defense_df.empty:
        return pd.DataFrame([_empty_defense_summary()])

    working = defense_df.copy()
    working["confluence_bucket"] = working["confluence_count"].apply(_bucket_confluence)
    working["first_meaningful_test_bucket"] = working["is_first_meaningful_test"].map({True: "first_touch", False: "repeated_touch"})
    working["touch_count_bucket"] = working["meaningful_touch_count_before_event"].apply(_bucket_touch_count_v2)

    return pd.DataFrame(
        [
            {
                "total_events": int(len(working)),
                "hold_rate": float(working["hold_flag"].mean()),
                "failed_hold_rate": float(working["failed_hold_flag"].mean()),
                "unresolved_rate": float(working["defense_unresolved_flag"].mean()),
                "avg_reversal_strength": float(working["reversal_strength_pct"].mean()),
                "avg_zone_defense_score": float(working["zone_defense_score"].mean()),
                "median_zone_defense_score": float(working["zone_defense_score"].median()),
                "by_zone_class": _group_summary_json_v2(working, "zone_class", "defense"),
                "by_first_meaningful_test": _group_summary_json_v2(working, "first_meaningful_test_bucket", "defense"),
                "by_timeframe": _group_summary_json_v2(working, "zone_timeframe", "defense"),
                "by_confluence_bucket": _group_summary_json_v2(working, "confluence_bucket", "defense"),
                "by_touch_count": _group_summary_json_v2(working, "touch_count_bucket", "defense"),
            }
        ]
    )


def write_research_report(
    output_dir: str | Path,
    raw_interactions_df: pd.DataFrame,
    breakout_labeled_events_df: pd.DataFrame,
    defense_labeled_events_df: pd.DataFrame,
    breakout_summary_df: pd.DataFrame,
    defense_summary_df: pd.DataFrame,
    structural_zones_df: pd.DataFrame,
    config: BoundaryTesterConfig,
) -> Path:
    output_path = Path(output_dir)
    report_path = output_path / "report.md"
    breakout_row = breakout_summary_df.iloc[0].to_dict() if not breakout_summary_df.empty else {}
    defense_row = defense_summary_df.iloc[0].to_dict() if not defense_summary_df.empty else {}

    lines = [
        "# Boundary Tester Report",
        "",
        "## Data Overview",
        f"- Raw interactions: {len(raw_interactions_df)}",
        f"- Breakout labeled events: {len(breakout_labeled_events_df)}",
        f"- Defense labeled events: {len(defense_labeled_events_df)}",
        f"- Structural zones: {structural_zones_df['structural_zone_key'].nunique() if not structural_zones_df.empty and 'structural_zone_key' in structural_zones_df.columns else 0}",
        "",
        "## Zone Defense Summary",
        f"- Hold Rate: {_fmt_pct(defense_row.get('hold_rate'))}",
        f"- Failed Hold Rate: {_fmt_pct(defense_row.get('failed_hold_rate'))}",
        f"- Unresolved Rate: {_fmt_pct(defense_row.get('unresolved_rate'))}",
        f"- Avg Reversal Strength: {_fmt_pct(defense_row.get('avg_reversal_strength'))}",
        "",
        "### Defense By Touch Count",
        _json_block(defense_row.get("by_touch_count")),
        "",
        "## Breakout Continuation Summary",
        f"- Success Rate: {_fmt_pct(breakout_row.get('success_rate'))}",
        f"- Failure Rate: {_fmt_pct(breakout_row.get('failure_rate'))}",
        f"- False Breakout Rate: {_fmt_pct(breakout_row.get('false_breakout_rate'))}",
        f"- Failed Follow Through Rate: {_fmt_pct(breakout_row.get('failed_follow_through_rate'))}",
        f"- Hold Rate After Breakout: {_fmt_pct(breakout_row.get('hold_rate_after_breakout'))}",
        f"- Event Per Structural Zone: {_fmt_num_v2(breakout_row.get('event_per_structural_zone'))}",
        "",
        "### Breakout By Failure Subtype",
        _json_block(breakout_row.get("by_failure_subtype")),
        "",
        "### Breakout By Touch Count",
        _json_block(breakout_row.get("by_touch_count")),
        "",
        "## Config Snapshot",
        "```json",
        json.dumps(config.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
        "```",
    ]
    return _safe_write_report(report_path, "\n".join(lines))


def _group_summary_json_v2(df: pd.DataFrame, column: str, summary_type: str) -> str:
    payload: dict[str, dict[str, float | int]] = {}
    for key, group in df.groupby(column, dropna=False):
        if summary_type == "defense":
            payload[str(key)] = {
                "count": int(len(group)),
                "hold_rate": float(group["hold_flag"].mean()),
                "failed_hold_rate": float(group["failed_hold_flag"].mean()),
                "unresolved_rate": float(group["defense_unresolved_flag"].mean()),
                "avg_reversal_strength": float(group["reversal_strength_pct"].mean()),
                "avg_zone_defense_score": float(group["zone_defense_score"].mean()),
            }
        else:
            payload[str(key)] = {
                "count": int(len(group)),
                "success_rate": float(group["success_flag"].mean()),
                "failure_rate": float(group["failure_flag"].mean()),
                "unresolved_rate": float(group["unresolved_flag"].mean()),
                "false_breakout_rate": float(group["false_breakout_flag"].mean()),
                "failed_follow_through_rate": float(group["failed_follow_through_flag"].mean()),
                "avg_follow_through": float(group["follow_through_pct"].mean()),
                "median_follow_through": float(group["follow_through_pct"].median()),
            }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _empty_breakout_summary() -> dict:
    return {
        "total_events": 0,
        "structural_zone_count": 0,
        "event_per_structural_zone": 0.0,
        "success_rate": 0.0,
        "failure_rate": 0.0,
        "unresolved_rate": 0.0,
        "false_breakout_rate": 0.0,
        "failed_follow_through_rate": 0.0,
        "hold_rate_after_breakout": 0.0,
        "avg_follow_through": np.nan,
        "avg_mfe": np.nan,
        "avg_mae": np.nan,
        "median_follow_through": np.nan,
        "median_mfe": np.nan,
        "median_mae": np.nan,
        "by_zone_class": "{}",
        "by_first_meaningful_test": "{}",
        "by_timeframe": "{}",
        "by_confluence_bucket": "{}",
        "by_touch_count": "{}",
        "by_zone_width_bucket": "{}",
        "by_breakout_quality_bucket": "{}",
        "by_failure_subtype": "{}",
    }


def _empty_defense_summary() -> dict:
    return {
        "total_events": 0,
        "hold_rate": 0.0,
        "failed_hold_rate": 0.0,
        "unresolved_rate": 0.0,
        "avg_reversal_strength": np.nan,
        "avg_zone_defense_score": np.nan,
        "median_zone_defense_score": np.nan,
        "by_zone_class": "{}",
        "by_first_meaningful_test": "{}",
        "by_timeframe": "{}",
        "by_confluence_bucket": "{}",
        "by_touch_count": "{}",
    }


def _bucket_touch_count_v2(value: int) -> str:
    num = int(value)
    if num <= 0:
        return "first_touch"
    if num == 1:
        return "second_touch"
    if num == 2:
        return "third_touch"
    return "4plus_touch"


def _bucket_zone_width_v2(value: float) -> str:
    if pd.isna(value):
        return "unknown"
    if value < 0.01:
        return "narrow"
    if value < 0.03:
        return "medium"
    return "wide"


def _bucket_breakout_quality_v2(value: float) -> str:
    if pd.isna(value):
        return "unknown"
    if value < 1.5:
        return "low"
    if value < 3.5:
        return "medium"
    return "high"


def _fmt_num_v2(value) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "N/A"
