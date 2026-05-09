from __future__ import annotations

import argparse
from datetime import date

from engines.zone_generation import ZoneGenerationConfig
from zone_lifecycle.offline_snapshots import build_zone_snapshots_offline


def main() -> None:
    parser = argparse.ArgumentParser(description="Build zone lifecycle daily snapshots offline.")
    parser.add_argument("--symbol", required=True, help="Ticker symbol, for example AAPL.")
    parser.add_argument("--start-date", required=True, help="Snapshot start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Snapshot end date, YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--provider", default=None, help="OpenBB provider name.")
    parser.add_argument("--database-url", default=None, help="SQLAlchemy database URL. Defaults to outputs/zone_lifecycle.sqlite.")
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=None,
        help="Warmup lookback years before start date. Omit to start exactly from --start-date.",
    )
    parser.add_argument("--no-force", action="store_true", help="Process incrementally instead of rebuilding the range.")
    parser.add_argument("--reset", action="store_true", help="Delete existing lifecycle data for the symbol before rebuilding.")
    parser.add_argument("--short-vp-lookback-days", type=int, default=21)
    parser.add_argument("--short-vp-bins", type=int, default=48)
    parser.add_argument("--long-vp-lookback-days", type=int, default=63)
    parser.add_argument("--long-vp-bins", type=int, default=48)
    parser.add_argument("--vp-lookback-days", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--vp-bins", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--weekly-vp-lookback", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--weekly-vp-bins", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--zone-expand-bp", type=int, default=50)
    parser.add_argument("--hv-node-quantile", type=float, default=0.80)
    parser.add_argument("--merge-bp", type=int, default=60)
    parser.add_argument("--max-resistance-zones", type=int, default=4)
    parser.add_argument("--max-support-zones", type=int, default=4)
    parser.add_argument("--reaction-lookahead", type=int, default=5)
    parser.add_argument("--reaction-threshold-bp", type=int, default=100)
    parser.add_argument("--min-touch-gap", type=int, default=3)
    args = parser.parse_args()
    end_date = args.end_date or str(date.today())

    result = build_zone_snapshots_offline(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=end_date,
        provider=args.provider,
        database_url=args.database_url,
        lookback_years=0 if args.lookback_years is None else args.lookback_years,
        force=not args.no_force,
        reset=args.reset,
        config=ZoneGenerationConfig(
            short_vp_lookback_days=args.vp_lookback_days or args.short_vp_lookback_days,
            short_vp_bins=args.vp_bins or args.short_vp_bins,
            long_vp_lookback_days=args.weekly_vp_lookback or args.long_vp_lookback_days,
            long_vp_bins=args.weekly_vp_bins or args.long_vp_bins,
            zone_expand_pct=args.zone_expand_bp / 10000.0,
            hv_node_quantile=args.hv_node_quantile,
            merge_pct=args.merge_bp / 10000.0,
            max_resistance_zones=args.max_resistance_zones,
            max_support_zones=args.max_support_zones,
            reaction_lookahead=args.reaction_lookahead,
            reaction_return_threshold=args.reaction_threshold_bp / 10000.0,
            min_touch_gap=args.min_touch_gap,
        ),
    )
    lifecycle = result.lifecycle
    print(
        "Built zone snapshots "
        f"symbol={result.symbol} "
        f"range={result.start_date.date()}..{result.end_date.date()} "
        f"processed_bars={lifecycle.processed_bars} "
        f"upserted_zones={lifecycle.upserted_zones} "
        f"snapshots={lifecycle.snapshots} "
        f"zone_bar_updates={lifecycle.zone_bar_updates} "
        f"breakout_updates={lifecycle.breakout_updates}"
    )


if __name__ == "__main__":
    main()
