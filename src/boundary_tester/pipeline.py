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

        events_df.to_csv(events_path, index=False, encoding="utf-8-sig")
        labeled_events_df.to_csv(labeled_path, index=False, encoding="utf-8-sig")
        summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
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
