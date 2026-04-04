from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from boundary_tester import BoundaryTesterConfig, run_boundary_tester


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Boundary Tester pipeline.")
    parser.add_argument("--prices", required=True, help="Path to prices CSV.")
    parser.add_argument("--zones", required=True, help="Path to zones CSV.")
    parser.add_argument("--output", required=True, help="Output directory.")
    parser.add_argument("--config", help="Optional JSON config file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = BoundaryTesterConfig.from_json_file(args.config) if args.config else BoundaryTesterConfig()

    prices_df = pd.read_csv(args.prices)
    zones_df = pd.read_csv(args.zones)

    result = run_boundary_tester(
        price_df=prices_df,
        zone_df=zones_df,
        config=config,
        output_dir=args.output,
    )

    print("Boundary Tester completed.")
    print(f"Events: {len(result['events'])}")
    print(f"Labeled events: {len(result['labeled_events'])}")
    print("Summary:")
    print(result["summary"].to_string(index=False))
    print(f"Output directory: {Path(args.output).resolve()}")
    print("Config snapshot:")
    print(json.dumps(config.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
