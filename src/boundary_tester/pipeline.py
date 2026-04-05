from __future__ import annotations

from pathlib import Path
import json

import pandas as pd

from .config import BoundaryTesterConfig
from .defense_labeler import label_zone_defense_events
from .event_detector import detect_boundary_interactions
from .labeler import label_breakout_events
from .reporter import (
    build_zone_breakout_summary_table,
    build_zone_defense_summary_table,
    write_research_report,
)
from .validator import prepare_price_frame, prepare_zone_frame
from .zone_engine import merge_snapshot_zones_into_structural_zones


def run_boundary_tester(
    price_df: pd.DataFrame,
    zone_df: pd.DataFrame,
    config: BoundaryTesterConfig | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, pd.DataFrame | Path]:
    config = config or BoundaryTesterConfig()
    prepared_prices = prepare_price_frame(price_df, config)
    prepared_zones = prepare_zone_frame(zone_df)
    structural_zones_df = merge_snapshot_zones_into_structural_zones(prepared_zones)

    raw_interactions_df, events_df = detect_boundary_interactions(prepared_prices, prepared_zones, config)
    breakout_labeled_events_df = label_breakout_events(events_df, prepared_prices, prepared_zones, config)
    defense_labeled_events_df = label_zone_defense_events(events_df, prepared_prices, prepared_zones, config)
    breakout_summary_df = build_zone_breakout_summary_table(breakout_labeled_events_df)
    defense_summary_df = build_zone_defense_summary_table(defense_labeled_events_df)
    research_slices = {
        "breakout_by_failure_subtype": json.loads(breakout_summary_df.iloc[0]["by_failure_subtype"]) if not breakout_summary_df.empty else {},
        "breakout_by_touch_count": json.loads(breakout_summary_df.iloc[0]["by_touch_count"]) if not breakout_summary_df.empty else {},
        "defense_by_touch_count": json.loads(defense_summary_df.iloc[0]["by_touch_count"]) if not defense_summary_df.empty else {},
    }

    result: dict[str, pd.DataFrame | Path] = {
        "prices": prepared_prices,
        "zones": prepared_zones,
        "structural_zones": structural_zones_df,
        "raw_interactions": raw_interactions_df,
        "events": events_df,
        "breakout_labeled_events": breakout_labeled_events_df,
        "defense_labeled_events": defense_labeled_events_df,
        "labeled_events": breakout_labeled_events_df,
        "zone_breakout_summary": breakout_summary_df,
        "zone_defense_summary": defense_summary_df,
        "summary": breakout_summary_df,
        "research_slices": research_slices,
    }

    if output_dir is not None:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        raw_interactions_path = output_path / "raw_interactions.csv"
        events_path = output_path / "events.csv"
        breakout_labeled_path = output_path / "breakout_labeled_events.csv"
        defense_labeled_path = output_path / "defense_labeled_events.csv"
        breakout_summary_path = output_path / "zone_breakout_summary.csv"
        defense_summary_path = output_path / "zone_defense_summary.csv"
        structural_zones_path = output_path / "structural_zones.csv"
        research_slices_path = output_path / "research_slices.json"

        raw_interactions_path = _safe_write_csv(raw_interactions_df, raw_interactions_path)
        events_path = _safe_write_csv(events_df, events_path)
        breakout_labeled_path = _safe_write_csv(breakout_labeled_events_df, breakout_labeled_path)
        defense_labeled_path = _safe_write_csv(defense_labeled_events_df, defense_labeled_path)
        breakout_summary_path = _safe_write_csv(breakout_summary_df, breakout_summary_path)
        defense_summary_path = _safe_write_csv(defense_summary_df, defense_summary_path)
        structural_zones_path = _safe_write_csv(structural_zones_df, structural_zones_path)
        research_slices_path = _safe_write_text(
            research_slices_path,
            json.dumps(research_slices, ensure_ascii=False, indent=2, sort_keys=True),
        )
        report_path = write_research_report(
            output_path,
            raw_interactions_df,
            breakout_labeled_events_df,
            defense_labeled_events_df,
            breakout_summary_df,
            defense_summary_df,
            structural_zones_df,
            config,
        )

        result.update(
            {
                "raw_interactions_path": raw_interactions_path,
                "events_path": events_path,
                "breakout_labeled_events_path": breakout_labeled_path,
                "defense_labeled_events_path": defense_labeled_path,
                "labeled_events_path": breakout_labeled_path,
                "zone_breakout_summary_path": breakout_summary_path,
                "zone_defense_summary_path": defense_summary_path,
                "summary_path": breakout_summary_path,
                "structural_zones_path": structural_zones_path,
                "research_slices_path": research_slices_path,
                "report_path": report_path,
            }
        )

    return result


def _safe_write_csv(df: pd.DataFrame, path: Path) -> Path:
    try:
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path
    except PermissionError:
        fallback_path = _build_locked_file_fallback_path(path)
        df.to_csv(fallback_path, index=False, encoding="utf-8-sig")
        return fallback_path


def _build_locked_file_fallback_path(path: Path) -> Path:
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    stem = path.stem
    suffix = path.suffix or ".csv"
    return path.with_name(f"{stem}.{timestamp}{suffix}")


def _safe_write_text(path: Path, content: str) -> Path:
    try:
        path.write_text(content, encoding="utf-8")
        return path
    except PermissionError:
        fallback_path = _build_locked_file_fallback_path(path)
        fallback_path.write_text(content, encoding="utf-8")
        return fallback_path
