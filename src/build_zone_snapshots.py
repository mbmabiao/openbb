from __future__ import annotations

import argparse
from datetime import date, datetime

import pandas as pd

from config.warmup_config import load_warmup_config
from engines.zone_generation import ZoneGenerationConfig
from zone_lifecycle.offline_snapshots import build_zone_snapshots_offline


def _date_arg(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("expected date in YYYY-MM-DD format") from exc


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _resolve_snapshot_range(args: argparse.Namespace, parser: argparse.ArgumentParser) -> tuple[str, str]:
    today = pd.Timestamp(date.today()).normalize()
    end_ts = pd.Timestamp(args.end_date).normalize() if args.end_date else today

    if args.lookback_years is not None:
        if args.end_date is not None:
            parser.error("--end-date can only be used with --start-date, not --lookback-years")
        start_ts = today - pd.DateOffset(years=args.lookback_years)
        return str(start_ts.date()), str(today.date())

    start_ts = pd.Timestamp(args.start_date).normalize()
    if end_ts < start_ts:
        parser.error("--end-date must be on or after --start-date")
    return str(start_ts.date()), str(end_ts.date())


def main() -> None:
    parser = argparse.ArgumentParser(description="Build zone lifecycle daily snapshots offline.")
    parser.add_argument("--symbol", required=True, help="Ticker symbol, for example AAPL.")
    range_group = parser.add_mutually_exclusive_group(required=True)
    range_group.add_argument(
        "--start-date",
        type=_date_arg,
        help="Snapshot start date, YYYY-MM-DD. Defaults end date to today unless --end-date is provided.",
    )
    range_group.add_argument(
        "--lookback-years",
        type=_positive_int,
        help="Build snapshots from today back this many years. Cannot be combined with --start-date or --end-date.",
    )
    parser.add_argument(
        "--end-date",
        type=_date_arg,
        default=None,
        help="Snapshot end date, YYYY-MM-DD. Only valid with --start-date; defaults to today.",
    )
    parser.add_argument("--provider", default=None, help="OpenBB provider name.")
    parser.add_argument("--database-url", default=None, help="SQLAlchemy database URL. Defaults to outputs/zone_lifecycle.sqlite.")
    parser.add_argument("--warmup-config-path", default=None, help="Warmup threshold YAML path. Defaults to src/config/warmup.yaml.")
    parser.add_argument("--no-force", action="store_true", help="Process incrementally instead of rebuilding the range.")
    parser.add_argument("--reset", action="store_true", help="Delete existing lifecycle data for the symbol before rebuilding.")
    parser.add_argument("--long-vp-lookback-days", type=int, default=63)
    parser.add_argument("--long-vp-bins", type=int, default=48)
    parser.add_argument("--zone-expand-bp", type=int, default=50)
    parser.add_argument("--max-resistance-zones", type=int, default=4)
    parser.add_argument("--max-support-zones", type=int, default=4)
    args = parser.parse_args()
    start_date, end_date = _resolve_snapshot_range(args, parser)
    warmup_config = load_warmup_config(args.warmup_config_path)

    result = build_zone_snapshots_offline(
        symbol=args.symbol,
        start_date=start_date,
        end_date=end_date,
        provider=args.provider,
        database_url=args.database_url,
        lookback_years=0,
        force=not args.no_force,
        reset=args.reset,
        warmup_config=warmup_config,
        config=ZoneGenerationConfig(
            long_vp_lookback_days=args.long_vp_lookback_days,
            long_vp_bins=args.long_vp_bins,
            zone_expand_pct=args.zone_expand_bp / 10000.0,
            max_resistance_zones=args.max_resistance_zones,
            max_support_zones=args.max_support_zones,
            thresholds=warmup_config.zone_generation,
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
        f"observations={lifecycle.observations} "
        f"pattern_events={lifecycle.pattern_events} "
        f"divergence_events={lifecycle.divergence_events} "
        f"zone_bar_updates={lifecycle.zone_bar_updates} "
        f"breakout_updates={lifecycle.breakout_updates}"
    )


if __name__ == "__main__":
    main()
