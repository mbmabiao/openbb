from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from openbb import obb

from .config import BoundaryTesterConfig
from .pipeline import run_boundary_tester
from .zone_engine import (
    ZoneEngineConfig,
    build_avwap_features,
    build_composite_interval_volume_profile_zones,
    build_zone_rows_from_snapshot,
    create_candidate_zones_from_avwap,
    create_candidate_zones_from_vp,
    filter_frame_by_dates,
    get_recent_trading_dates,
    get_recent_trading_dates_for_weekly_window,
    merge_close_zones,
    normalise_ohlcv_columns,
    rank_zones_for_side,
    resample_to_weekly,
    to_dataframe,
)


@dataclass(slots=True)
class AutoBoundaryRunnerConfig:
    tickers: list[str]
    start_date: str
    end_date: str
    output_dir: str
    price_provider: str | None = None
    hourly_interval: str = "1h"
    daily_interval: str = "1d"
    adjustment: str = "splits_only"
    extended_hours: bool = False
    zone_engine: ZoneEngineConfig = field(default_factory=ZoneEngineConfig)
    boundary_tester: BoundaryTesterConfig = field(default_factory=BoundaryTesterConfig)

    @classmethod
    def from_yaml_file(cls, path: str | Path) -> "AutoBoundaryRunnerConfig":
        config_path = Path(path).resolve()
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        universe = payload.get("universe", {})
        data = payload.get("data", {})
        output = payload.get("output", {})

        tickers = [str(t).strip().upper() for t in universe.get("tickers", []) if str(t).strip()]
        if not tickers:
            raise ValueError("YAML config must contain at least one ticker under universe.tickers.")

        start_date = str(data.get("start_date", "")).strip()
        end_date = str(data.get("end_date", "")).strip()
        if not start_date or not end_date:
            raise ValueError("YAML config must provide data.start_date and data.end_date.")

        output_dir = str(output.get("dir", "")).strip()
        if not output_dir:
            raise ValueError("YAML config must provide output.dir.")
        output_dir_path = Path(output_dir)
        if not output_dir_path.is_absolute():
            output_dir_path = (config_path.parent / output_dir_path).resolve()

        return cls(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            output_dir=str(output_dir_path),
            price_provider=(str(data.get("price_provider")).strip() or None) if data.get("price_provider") is not None else None,
            hourly_interval=str(data.get("hourly_interval", "1h")).strip(),
            daily_interval=str(data.get("daily_interval", "1d")).strip(),
            adjustment=str(data.get("adjustment", "splits_only")).strip(),
            extended_hours=bool(data.get("extended_hours", False)),
            zone_engine=ZoneEngineConfig.from_dict(payload.get("zone_engine")),
            boundary_tester=BoundaryTesterConfig.from_dict(payload.get("boundary_tester")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "tickers": self.tickers,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "output_dir": self.output_dir,
            "price_provider": self.price_provider,
            "hourly_interval": self.hourly_interval,
            "daily_interval": self.daily_interval,
            "adjustment": self.adjustment,
            "extended_hours": self.extended_hours,
            "zone_engine": asdict(self.zone_engine),
            "boundary_tester": self.boundary_tester.to_dict(),
        }


def run_auto_boundary_tester(config_path: str | Path) -> dict[str, Any]:
    config = AutoBoundaryRunnerConfig.from_yaml_file(config_path)
    validate_provider_constraints(config)
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_prices: list[pd.DataFrame] = []
    all_zones: list[pd.DataFrame] = []

    for ticker in config.tickers:
        daily_prices = fetch_price_frame(
            symbol=ticker,
            start_date=config.start_date,
            end_date=config.end_date,
            provider=config.price_provider,
            interval=config.daily_interval,
            adjustment=config.adjustment,
            extended_hours=config.extended_hours,
        )
        hourly_prices = fetch_price_frame(
            symbol=ticker,
            start_date=config.start_date,
            end_date=config.end_date,
            provider=config.price_provider,
            interval=config.hourly_interval,
            adjustment=config.adjustment,
            extended_hours=config.extended_hours,
        )
        validate_source_coverage_for_ticker(
            ticker=ticker,
            daily_df=daily_prices,
            hourly_df=hourly_prices,
            zone_config=config.zone_engine,
            hourly_interval=config.hourly_interval,
            daily_interval=config.daily_interval,
            provider=config.price_provider,
        )

        generated_zones = generate_historical_zones_for_ticker(
            ticker=ticker,
            daily_df=daily_prices,
            hourly_df=hourly_prices,
            config=config.zone_engine,
        )

        all_prices.append(daily_prices.assign(ticker=ticker))
        all_zones.append(generated_zones)

    prices_df = pd.concat(all_prices, ignore_index=True)
    prices_df = prices_df.rename(columns={"date": "timestamp"})
    zones_df = pd.concat(all_zones, ignore_index=True)

    zones_path = output_dir / "generated_zones.csv"
    prices_path = output_dir / "validation_prices.csv"
    zones_path = _safe_write_csv(zones_df, zones_path)
    prices_path = _safe_write_csv(prices_df, prices_path)

    pipeline_result = run_boundary_tester(
        price_df=prices_df,
        zone_df=zones_df,
        config=config.boundary_tester,
        output_dir=output_dir,
    )

    config_snapshot_path = output_dir / "run_config.json"
    config_snapshot_path = _safe_write_text(
        config_snapshot_path,
        json.dumps(config.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
    )

    pipeline_result.update(
        {
            "generated_zones": zones_df,
            "generated_zones_path": zones_path,
            "validation_prices_path": prices_path,
            "config_snapshot_path": config_snapshot_path,
        }
    )
    return pipeline_result


def validate_provider_constraints(config: AutoBoundaryRunnerConfig) -> None:
    start_date = pd.Timestamp(config.start_date).normalize()
    end_date = pd.Timestamp(config.end_date).normalize()
    if end_date < start_date:
        raise ValueError(
            f"Invalid config date range: start_date {start_date.date()} is after end_date {end_date.date()}."
        )


def _is_intraday_interval(interval: str) -> bool:
    normalized = interval.strip().lower()
    return normalized not in {"1d", "1w", "1wk", "1mo", "1mth", "1month", "1q", "1y"}


def _safe_write_csv(df: pd.DataFrame, path: Path) -> Path:
    try:
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path
    except PermissionError:
        fallback_path = _build_locked_file_fallback_path(path)
        df.to_csv(fallback_path, index=False, encoding="utf-8-sig")
        return fallback_path


def _safe_write_text(path: Path, content: str) -> Path:
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


def fetch_price_frame(
    symbol: str,
    start_date: str,
    end_date: str,
    provider: str | None,
    interval: str,
    adjustment: str,
    extended_hours: bool,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    provider_label = (provider or "").strip().lower()

    if _should_chunk_intraday_requests(provider_label, interval, start_ts, end_ts):
        return _fetch_chunked_intraday_price_frame(
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
            provider=provider,
            interval=interval,
            adjustment=adjustment,
            extended_hours=extended_hours,
        )

    return _fetch_single_price_frame(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        interval=interval,
        adjustment=adjustment,
        extended_hours=extended_hours,
    )


def _fetch_single_price_frame(
    symbol: str,
    start_date: str,
    end_date: str,
    provider: str | None,
    interval: str,
    adjustment: str,
    extended_hours: bool,
) -> pd.DataFrame:
    kwargs: dict[str, Any] = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "interval": interval,
        "adjustment": adjustment,
        "extended_hours": extended_hours,
    }
    if provider:
        kwargs["provider"] = provider

    result = obb.equity.price.historical(**kwargs)
    df = to_dataframe(result)
    if df is None or df.empty:
        raise ValueError(f"No price data returned for {symbol} at interval {interval}.")

    out = normalise_ohlcv_columns(df, date_col_name="date")
    required_cols = {"date", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(set(out.columns)):
        raise ValueError(f"Price data for {symbol} at interval {interval} is missing OHLCV columns.")

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["date", "open", "high", "low", "close", "volume"]).copy()
    out = out.sort_values("date", kind="stable").reset_index(drop=True)
    if out.empty:
        raise ValueError(f"Price data for {symbol} at interval {interval} became empty after cleaning.")
    return out


def _fetch_chunked_intraday_price_frame(
    symbol: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    provider: str | None,
    interval: str,
    adjustment: str,
    extended_hours: bool,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    chunk_windows = _build_intraday_request_windows(start_ts, end_ts, max_days_per_request=60)

    for chunk_start, chunk_end in chunk_windows:
        frame = _fetch_single_price_frame(
            symbol=symbol,
            start_date=str(chunk_start.date()),
            end_date=str(chunk_end.date()),
            provider=provider,
            interval=interval,
            adjustment=adjustment,
            extended_hours=extended_hours,
        )
        frames.append(frame)

    if not frames:
        raise ValueError(f"No chunked price data could be fetched for {symbol} at interval {interval}.")

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["date"], keep="last").sort_values("date", kind="stable").reset_index(drop=True)
    if out.empty:
        raise ValueError(f"Chunked price data for {symbol} at interval {interval} became empty after deduplication.")
    return out


def _build_intraday_request_windows(
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    max_days_per_request: int,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if end_ts < start_ts:
        return []

    max_days_per_request = max(int(max_days_per_request), 1)
    chunk_span = pd.Timedelta(days=max_days_per_request - 1)
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    current_start = start_ts

    while current_start <= end_ts:
        current_end = min(current_start + chunk_span, end_ts)
        windows.append((current_start, current_end))
        if current_end >= end_ts:
            break
        current_start = current_end

    return windows


def _should_chunk_intraday_requests(
    provider: str,
    interval: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> bool:
    if provider not in {"yfinance", "yahoo_finance", "yahoo"}:
        return False
    if not _is_intraday_interval(interval):
        return False
    return (end_ts - start_ts).days + 1 > 60


def validate_source_coverage_for_ticker(
    ticker: str,
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    zone_config: ZoneEngineConfig,
    hourly_interval: str,
    daily_interval: str,
    provider: str | None,
) -> None:
    snapshot_indices = build_snapshot_indices(
        total_bars=len(daily_df),
        min_history_bars=zone_config.min_history_bars,
        refresh_every=zone_config.zone_refresh_every_n_bars,
    )
    if not snapshot_indices:
        raise ValueError(f"Not enough daily history to generate zones for {ticker}.")

    first_snapshot_idx = snapshot_indices[0]
    first_snapshot_df = daily_df.iloc[:first_snapshot_idx].copy().reset_index(drop=True)
    if first_snapshot_df.empty:
        raise ValueError(f"Unable to build the first validation snapshot for {ticker}.")

    required_hourly_dates = get_recent_trading_dates(first_snapshot_df, zone_config.daily_vp_lookback_days)
    required_weekly_dates = get_recent_trading_dates_for_weekly_window(first_snapshot_df, zone_config.weekly_vp_lookback_weeks)

    if not required_hourly_dates:
        raise ValueError(f"No required {hourly_interval} dates could be derived for {ticker}.")
    if not required_weekly_dates:
        raise ValueError(f"No required {daily_interval} dates could be derived for {ticker}.")

    available_hourly_dates = set(pd.to_datetime(hourly_df["date"]).dt.normalize().tolist())
    missing_hourly_dates = [d for d in required_hourly_dates if d not in available_hourly_dates]
    if missing_hourly_dates:
        first_missing = pd.Timestamp(missing_hourly_dates[0]).date()
        last_missing = pd.Timestamp(missing_hourly_dates[-1]).date()
        available_start = pd.Timestamp(hourly_df["date"].min()).date()
        available_end = pd.Timestamp(hourly_df["date"].max()).date()
        provider_label = provider or "default provider"
        raise ValueError(
            f"{ticker} {hourly_interval} data coverage is insufficient for strict daily VP generation. "
            f"Required trading dates start at {required_hourly_dates[0].date()} but available {hourly_interval} data "
            f"from {provider_label} only covers {available_start} to {available_end}. "
            f"Missing window: {first_missing} to {last_missing}. "
            "Please shorten the research range, reduce min_history / VP lookback, or switch to a provider that serves deeper intraday history."
        )

    available_daily_dates = set(pd.to_datetime(daily_df["date"]).dt.normalize().tolist())
    missing_weekly_dates = [d for d in required_weekly_dates if d not in available_daily_dates]
    if missing_weekly_dates:
        first_missing = pd.Timestamp(missing_weekly_dates[0]).date()
        last_missing = pd.Timestamp(missing_weekly_dates[-1]).date()
        available_start = pd.Timestamp(daily_df["date"].min()).date()
        available_end = pd.Timestamp(daily_df["date"].max()).date()
        provider_label = provider or "default provider"
        raise ValueError(
            f"{ticker} {daily_interval} data coverage is insufficient for strict higher-timeframe VP generation. "
            f"Required trading dates start at {required_weekly_dates[0].date()} but available {daily_interval} data "
            f"from {provider_label} only covers {available_start} to {available_end}. "
            f"Missing window: {first_missing} to {last_missing}."
        )


def generate_historical_zones_for_ticker(
    ticker: str,
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    config: ZoneEngineConfig,
) -> pd.DataFrame:
    if daily_df.empty:
        raise ValueError(f"No daily validation data available for {ticker}.")
    if hourly_df.empty:
        raise ValueError(f"No hourly background data available for {ticker}.")

    snapshot_indices = build_snapshot_indices(len(daily_df), config.min_history_bars, config.zone_refresh_every_n_bars)
    if not snapshot_indices:
        raise ValueError(f"Not enough daily history to generate zones for {ticker}.")

    snapshot_rows: list[tuple[pd.Timestamp, list[dict]]] = []
    for idx in snapshot_indices:
        calc_df = daily_df.iloc[:idx].copy().reset_index(drop=True)
        if calc_df.empty:
            continue

        current_price = float(calc_df["close"].iloc[-1])
        daily_calc_with_features, daily_anchor_meta = build_avwap_features(calc_df, timeframe="D")

        recent_daily_dates = get_recent_trading_dates(calc_df, config.daily_vp_lookback_days)
        daily_vp_source = filter_frame_by_dates(hourly_df, recent_daily_dates)
        if daily_vp_source.empty:
            raise ValueError(f"No {ticker} 1h data available for daily VP window ending {daily_df.iloc[idx]['date']}.")

        daily_vp_zones_raw, vp_df_daily = build_composite_interval_volume_profile_zones(
            interval_df=daily_vp_source,
            bins=config.daily_vp_bins,
            zone_expand=config.zone_expand_pct,
            hv_quantile=config.hv_node_quantile,
            timeframe="D",
            source_label="VP (D, 1h composite)",
            source_mode="1h_composite",
        )
        if vp_df_daily.empty:
            raise ValueError(f"Daily VP profile build failed for {ticker} snapshot {daily_df.iloc[idx]['date']}.")

        daily_vp_zones = create_candidate_zones_from_vp(daily_calc_with_features, daily_vp_zones_raw)
        daily_avwap_zones = create_candidate_zones_from_avwap(
            df=daily_calc_with_features,
            anchor_meta=daily_anchor_meta,
            zone_expand_pct=config.zone_expand_pct,
        )

        weekly_calc_df = resample_to_weekly(calc_df)
        if weekly_calc_df.empty:
            continue
        weekly_calc_with_features, weekly_anchor_meta = build_avwap_features(weekly_calc_df, timeframe="W")

        recent_weekly_dates = get_recent_trading_dates_for_weekly_window(calc_df, config.weekly_vp_lookback_weeks)
        weekly_vp_source = filter_frame_by_dates(calc_df, recent_weekly_dates)
        if weekly_vp_source.empty:
            raise ValueError(f"No {ticker} 1d data available for weekly VP window ending {daily_df.iloc[idx]['date']}.")

        weekly_vp_zones_raw, vp_df_weekly = build_composite_interval_volume_profile_zones(
            interval_df=weekly_vp_source,
            bins=config.weekly_vp_bins,
            zone_expand=config.zone_expand_pct,
            hv_quantile=config.hv_node_quantile,
            timeframe="W",
            source_label="VP (W, 1d higher-timeframe composite)",
            source_mode="1d_higher_timeframe_composite",
        )
        if vp_df_weekly.empty:
            raise ValueError(f"Weekly VP profile build failed for {ticker} snapshot {daily_df.iloc[idx]['date']}.")

        weekly_vp_zones = create_candidate_zones_from_vp(weekly_calc_with_features, weekly_vp_zones_raw)
        weekly_avwap_zones = create_candidate_zones_from_avwap(
            df=weekly_calc_with_features,
            anchor_meta=weekly_anchor_meta,
            zone_expand_pct=config.zone_expand_pct,
        )

        all_candidate_zones = merge_close_zones(
            daily_vp_zones + daily_avwap_zones + weekly_vp_zones + weekly_avwap_zones,
            merge_pct=config.merge_pct,
        )

        resistance_zones = rank_zones_for_side(
            zones=all_candidate_zones,
            vp_df_daily=vp_df_daily,
            vp_df_weekly=vp_df_weekly,
            current_price=current_price,
            side="resistance",
            max_zones=config.max_resistance_zones,
            df_reaction=calc_df,
            lookahead=config.reaction_lookahead_bars,
            reaction_threshold=config.reaction_return_threshold,
            min_gap=config.min_touch_gap,
        )
        support_zones = rank_zones_for_side(
            zones=all_candidate_zones,
            vp_df_daily=vp_df_daily,
            vp_df_weekly=vp_df_weekly,
            current_price=current_price,
            side="support",
            max_zones=config.max_support_zones,
            df_reaction=calc_df,
            lookahead=config.reaction_lookahead_bars,
            reaction_threshold=config.reaction_return_threshold,
            min_gap=config.min_touch_gap,
        )

        valid_from = pd.Timestamp(daily_df.iloc[idx]["date"])
        selected_rows = build_zone_rows_from_snapshot(
            ticker=ticker,
            valid_from=valid_from,
            valid_to=None,
            selected_zones=resistance_zones + support_zones,
        )
        snapshot_rows.append((valid_from, selected_rows))

    zone_rows = materialize_validity_windows(snapshot_rows)
    if not zone_rows:
        raise ValueError(f"No zones were generated for {ticker}.")
    return pd.DataFrame(zone_rows)


def build_snapshot_indices(total_bars: int, min_history_bars: int, refresh_every: int) -> list[int]:
    refresh_every = max(refresh_every, 1)
    start_idx = max(min_history_bars, 1)
    if total_bars <= start_idx:
        return []
    indices = list(range(start_idx, total_bars, refresh_every))
    if indices[-1] != total_bars - 1:
        indices.append(total_bars - 1)
    return indices


def materialize_validity_windows(snapshot_rows: list[tuple[pd.Timestamp, list[dict]]]) -> list[dict]:
    output: list[dict] = []
    for i, (valid_from, rows) in enumerate(snapshot_rows):
        next_valid_from = snapshot_rows[i + 1][0] if i + 1 < len(snapshot_rows) else None
        valid_to = next_valid_from - pd.Timedelta(microseconds=1) if next_valid_from is not None else pd.NaT
        for row in rows:
            row_copy = row.copy()
            row_copy["valid_to"] = valid_to
            output.append(row_copy)
    return output
