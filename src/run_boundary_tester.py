from __future__ import annotations

import argparse
from pathlib import Path

from boundary_tester import run_auto_boundary_tester


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the automated Boundary Tester pipeline from YAML.")
    parser.add_argument("--config", required=True, help="Path to YAML config.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_path}")
    if not config_path.is_file():
        raise ValueError(f"Config path is not a file: {config_path}")

    result = run_auto_boundary_tester(config_path)

    print("Boundary Tester completed.")
    print(f"Raw interactions: {len(result['raw_interactions'])}")
    print(f"Events: {len(result['events'])}")
    print(f"Breakout labeled events: {len(result['breakout_labeled_events'])}")
    print(f"Defense labeled events: {len(result['defense_labeled_events'])}")
    print(f"Generated zones: {len(result['generated_zones'])}")
    print("Breakout summary:")
    print(result["summary"].to_string(index=False))
    print("Defense summary:")
    print(result["zone_defense_summary"].to_string(index=False))
    print(f"Output directory: {Path(result['report_path']).resolve().parent}")
    print(f"Generated zones file: {Path(result['generated_zones_path']).resolve()}")
    print(f"Validation prices file: {Path(result['validation_prices_path']).resolve()}")
    print(f"Structural zones file: {Path(result['structural_zones_path']).resolve()}")
    print(f"Breakout labeled file: {Path(result['breakout_labeled_events_path']).resolve()}")
    print(f"Defense labeled file: {Path(result['defense_labeled_events_path']).resolve()}")
    print(f"Research slices file: {Path(result['research_slices_path']).resolve()}")
    print(f"Config snapshot: {Path(result['config_snapshot_path']).resolve()}")


if __name__ == "__main__":
    main()
