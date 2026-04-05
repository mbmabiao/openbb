from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import BoundaryTesterConfig
from .event_detector import detect_boundary_events
from .labeler import label_breakout_events
from .reporter import build_summary_table, write_report
from .validator import prepare_price_frame, prepare_zone_frame


def run_boundary_tester(
    price_df: pd.DataFrame,
    zone_df: pd.DataFrame,
    config: BoundaryTesterConfig | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, pd.DataFrame | Path]:
    config = config or BoundaryTesterConfig()
    prepared_prices = prepare_price_frame(price_df, config)
    prepared_zones = prepare_zone_frame(zone_df)

    events_df = detect_boundary_events(prepared_prices, prepared_zones, config)
    labeled_events_df = label_breakout_events(events_df, prepared_prices, prepared_zones, config)
    summary_df = build_summary_table(labeled_events_df)

    result: dict[str, pd.DataFrame | Path] = {
        "prices": prepared_prices,
        "zones": prepared_zones,
        "events": events_df,
        "labeled_events": labeled_events_df,
        "summary": summary_df,
    }

    if output_dir is not None:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        events_path = output_path / "events.csv"
        labeled_path = output_path / "labeled_events.csv"
        summary_path = output_path / "summary.csv"

        events_path = _safe_write_csv(events_df, events_path)
        labeled_path = _safe_write_csv(labeled_events_df, labeled_path)
        summary_path = _safe_write_csv(summary_df, summary_path)
        report_path = write_report(output_path, events_df, labeled_events_df, summary_df, config)

        result.update(
            {
                "events_path": events_path,
                "labeled_events_path": labeled_path,
                "summary_path": summary_path,
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
