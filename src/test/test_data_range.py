"""
Test OpenBB/yfinance historical data availability by interval using point-window probing.

测试 OpenBB/yfinance 在不同 interval 下最远可请求到哪一天。

核心算法：
1. 每个 interval 先测试 max_cap_days，默认 20 年前附近是否有数据。
2. 如果 20 年前有数据，直接认为至少支持 20 年。
3. 如果 20 年前没有数据，则先测试最近完整窗口。
4. 如果最近窗口可用，则在 [recent_days, max_cap_days] 之间做二分搜索。
5. 每次不是请求 target_date -> today，而是请求：
       target_date -> target_date + probe_window_days
   这样更轻量，也更贴近 replay 场景。
6. 最终输出：
   - 确定边界日期 first_actual_data_date
   - 近似可用范围，例如：5m 最远约 65 天

运行：
    python src/test/test_data_range.py --symbol AAPL --provider yfinance

可选：
    python src/test/test_data_range.py --symbol AAPL --provider yfinance --max-cap-days 7300 --probe-window-days 7
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from openbb import obb


SCRIPT_VERSION = "point-window-binary-v3"


INTERVALS = [
    "1m",
    "2m",
    "5m",
    "15m",
    "30m",
    "60m",
    "90m",
    "1h",
    "1d",
    "5d",
    "1W",
    "1M",
    "1Q",
]


DAILY_PLUS_INTERVALS = {
    "1d",
    "5d",
    "1W",
    "1M",
    "1Q",
}


# 1m 用 1 天窗口更稳，避免 Yahoo / yfinance 内部把请求扩成 period=1mo 后失败。
# 其他 intraday 默认用命令行的 --probe-window-days。
INTERVAL_PROBE_WINDOW_OVERRIDES = {
    "1m": 1,
}


def normalise_ohlcv_response(result: Any) -> pd.DataFrame:
    """
    Convert OpenBB response to DataFrame.

    将 OpenBB 返回结果转换为 DataFrame。
    """
    if result is None:
        return pd.DataFrame()

    if hasattr(result, "to_df"):
        df = result.to_df()
    elif hasattr(result, "results"):
        df = pd.DataFrame(result.results)
    else:
        df = pd.DataFrame(result)

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    elif "datetime" in df.columns:
        df["date"] = pd.to_datetime(df["datetime"], errors="coerce")
    elif df.index.name in {"date", "datetime"}:
        df = df.reset_index()
        first_col = df.columns[0]
        df["date"] = pd.to_datetime(df[first_col], errors="coerce")
    else:
        df["date"] = pd.to_datetime(df.index, errors="coerce")

    df = df.dropna(subset=["date"])
    return df


def fetch_history(
    symbol: str,
    provider: str,
    interval: str,
    start_date: date,
    end_date: date,
    adjustment: str,
    extended_hours: bool,
) -> pd.DataFrame:
    """
    Fetch historical OHLCV through OpenBB.

    通过 OpenBB 请求 OHLCV 历史数据。
    """
    kwargs: dict[str, Any] = {
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "provider": provider,
        "interval": interval,
    }

    if provider == "yfinance":
        kwargs["adjustment"] = adjustment
        kwargs["extended_hours"] = extended_hours

    result = obb.equity.price.historical(**kwargs)
    return normalise_ohlcv_response(result)


def evaluate_point_request(
    symbol: str,
    provider: str,
    interval: str,
    today: date,
    requested_days: int,
    probe_window_days: int,
    adjustment: str,
    extended_hours: bool,
) -> tuple[bool, dict[str, Any]]:
    """
    Test whether data exists around a target date.

    判断 target_date 附近是否有数据。

    这里不是请求 target_date 到 today，
    而是请求 target_date 到 target_date + probe_window_days。
    """
    target_date = today - timedelta(days=requested_days)

    probe_start = target_date
    probe_end = target_date + timedelta(days=probe_window_days)

    if probe_end > today:
        probe_end = today

    # 避免 today -> today 这种空窗口。
    if probe_end <= probe_start:
        probe_start = today - timedelta(days=probe_window_days)
        probe_end = today

    meta: dict[str, Any] = {
        "interval": interval,
        "requested_days": requested_days,
        "target_date": target_date.isoformat(),
        "probe_start": probe_start.isoformat(),
        "probe_end": probe_end.isoformat(),
        "ok": False,
        "non_empty": False,
        "rows": 0,
        "earliest_timestamp": None,
        "latest_timestamp": None,
        "first_actual_data_date": None,
        "actual_distance_days": None,
        "reason": None,
        "error_type": None,
    }

    try:
        df = fetch_history(
            symbol=symbol,
            provider=provider,
            interval=interval,
            start_date=probe_start,
            end_date=probe_end,
            adjustment=adjustment,
            extended_hours=extended_hours,
        )

        if df.empty:
            meta["reason"] = "empty"
            return False, meta

        earliest = pd.to_datetime(df["date"]).min()
        latest = pd.to_datetime(df["date"]).max()

        earliest_date = earliest.date()
        latest_date = latest.date()

        has_data_in_probe_window = earliest_date <= probe_end and latest_date >= probe_start
        actual_distance_days = (today - earliest_date).days

        meta.update(
            {
                "ok": has_data_in_probe_window,
                "non_empty": True,
                "rows": len(df),
                "earliest_timestamp": str(earliest),
                "latest_timestamp": str(latest),
                "first_actual_data_date": earliest_date.isoformat(),
                "actual_distance_days": actual_distance_days,
                "reason": "available_near_target"
                if has_data_in_probe_window
                else "data_outside_probe_window",
            }
        )

        return has_data_in_probe_window, meta

    except Exception as exc:
        meta["reason"] = str(exc)
        meta["error_type"] = type(exc).__name__
        return False, meta


def build_failure_result(
    symbol: str,
    provider: str,
    interval: str,
    cap_meta: dict[str, Any],
    attempts: list[dict[str, Any]],
    request_count: int,
) -> dict[str, Any]:
    """
    Build a standard failure result.

    构造统一失败结果。
    """
    return {
        "symbol": symbol,
        "provider": provider,
        "interval": interval,
        "final_status": "no_success",
        "oldest_available_days": None,
        "oldest_probe_start": None,
        "probe_start": None,
        "probe_end": None,
        "earliest_timestamp_seen": None,
        "latest_timestamp_seen": None,
        "first_actual_data_date": None,
        "rows": 0,
        "request_count": request_count,
        "cap_attempt": cap_meta,
        "attempts": attempts,
    }


def find_oldest_available_point(
    symbol: str,
    provider: str,
    interval: str,
    today: date,
    max_cap_days: int,
    default_probe_window_days: int,
    adjustment: str,
    extended_hours: bool,
) -> dict[str, Any]:
    """
    Find the oldest available target date for one interval.

    对单个 interval 查找最远可用日期。
    """
    probe_window_days = INTERVAL_PROBE_WINDOW_OVERRIDES.get(
        interval,
        default_probe_window_days,
    )

    cache: dict[int, tuple[bool, dict[str, Any]]] = {}

    def cached_eval(days: int) -> tuple[bool, dict[str, Any]]:
        if days not in cache:
            cache[days] = evaluate_point_request(
                symbol=symbol,
                provider=provider,
                interval=interval,
                today=today,
                requested_days=days,
                probe_window_days=probe_window_days,
                adjustment=adjustment,
                extended_hours=extended_hours,
            )
        return cache[days]

    attempts: list[dict[str, Any]] = []

    # 1. 先测试 20 年前 / max cap。
    cap_ok, cap_meta = cached_eval(max_cap_days)
    attempts.append(cap_meta)

    if cap_ok:
        return {
            "symbol": symbol,
            "provider": provider,
            "interval": interval,
            "final_status": "success",
            "oldest_available_days": max_cap_days,
            "oldest_probe_start": cap_meta["target_date"],
            "probe_start": cap_meta["probe_start"],
            "probe_end": cap_meta["probe_end"],
            "earliest_timestamp_seen": cap_meta["earliest_timestamp"],
            "latest_timestamp_seen": cap_meta["latest_timestamp"],
            "first_actual_data_date": cap_meta["first_actual_data_date"],
            "rows": cap_meta["rows"],
            "request_count": len(cache),
            "cap_attempt": cap_meta,
            "attempts": attempts,
        }

    # 2. 测试最近完整窗口，不要用 requested_days=0。
    recent_days = max(1, probe_window_days)
    print(f"  recent probe days={recent_days}")

    recent_ok, recent_meta = cached_eval(recent_days)
    attempts.append(recent_meta)

    if not recent_ok:
        return build_failure_result(
            symbol=symbol,
            provider=provider,
            interval=interval,
            cap_meta=cap_meta,
            attempts=attempts,
            request_count=len(cache),
        )

    # 3. 二分搜索。
    # low = 已知可用，high = 已知不可用。
    low = recent_days
    high = max_cap_days
    best_days = recent_days
    best_meta = recent_meta

    while low + 1 < high:
        mid = (low + high) // 2

        ok, meta = cached_eval(mid)
        attempts.append(meta)

        if ok:
            best_days = mid
            best_meta = meta
            low = mid
        else:
            high = mid

    return {
        "symbol": symbol,
        "provider": provider,
        "interval": interval,
        "final_status": "success",
        "oldest_available_days": best_days,
        "oldest_probe_start": best_meta["target_date"],
        "probe_start": best_meta["probe_start"],
        "probe_end": best_meta["probe_end"],
        "earliest_timestamp_seen": best_meta["earliest_timestamp"],
        "latest_timestamp_seen": best_meta["latest_timestamp"],
        "first_actual_data_date": best_meta["first_actual_data_date"],
        "rows": best_meta["rows"],
        "request_count": len(cache),
        "cap_attempt": cap_meta,
        "attempts": attempts,
    }


def calculate_distance_from_date(
    today: date,
    date_str: str | None,
) -> tuple[int | None, float | None, float | None]:
    """
    Calculate days, months, and years from today.

    计算距今天数、月数、年数。
    """
    if not date_str:
        return None, None, None

    target = pd.to_datetime(date_str).date()

    days = (today - target).days
    months = round(days / 30.4375, 2)
    years = round(days / 365.25, 2)

    return days, months, years


def build_summary_df(
    results: list[dict[str, Any]],
    today: date,
) -> pd.DataFrame:
    """
    Build summary DataFrame.

    构造最终汇总表。
    """
    summary_rows: list[dict[str, Any]] = []

    for item in results:
        cap = item.get("cap_attempt") or {}

        probe_days, probe_months, probe_years = calculate_distance_from_date(
            today=today,
            date_str=item.get("oldest_probe_start"),
        )

        actual_days, actual_months, actual_years = calculate_distance_from_date(
            today=today,
            date_str=item.get("first_actual_data_date"),
        )

        summary_rows.append(
            {
                "symbol": item["symbol"],
                "provider": item["provider"],
                "interval": item["interval"],
                "final_status": item["final_status"],

                # 近似边界：二分搜索找到的最远探测窗口起点。
                # 这个用于输出 “最远约 xx 天”。
                "oldest_probe_start": item["oldest_probe_start"],
                "days_from_oldest_probe": probe_days,
                "months_from_oldest_probe": probe_months,
                "years_from_oldest_probe": probe_years,

                # 确定边界：provider 实际返回的第一根 K 线日期。
                # 这个用于生产逻辑判断。
                "first_actual_data_date": item["first_actual_data_date"],
                "days_from_first_actual_data": actual_days,
                "months_from_first_actual_data": actual_months,
                "years_from_first_actual_data": actual_years,

                # 原始请求信息。
                "probe_start": item["probe_start"],
                "probe_end": item["probe_end"],
                "earliest_timestamp_seen": item["earliest_timestamp_seen"],
                "latest_timestamp_seen": item["latest_timestamp_seen"],
                "rows": item["rows"],
                "request_count": item["request_count"],

                # 20 年初始探测信息。
                "cap_requested_days": cap.get("requested_days"),
                "cap_target_date": cap.get("target_date"),
                "cap_probe_start": cap.get("probe_start"),
                "cap_probe_end": cap.get("probe_end"),
                "cap_non_empty": cap.get("non_empty"),
                "cap_earliest_timestamp": cap.get("earliest_timestamp"),
                "cap_latest_timestamp": cap.get("latest_timestamp"),
                "cap_reason": cap.get("reason"),
                "cap_error_type": cap.get("error_type"),
            }
        )

    return pd.DataFrame(summary_rows)


def print_final_summary(summary_df: pd.DataFrame) -> None:
    """
    Print deterministic final summary.

    打印确定日期边界。
    """
    print("\nFinal Summary:")
    final_columns = [
        "symbol",
        "provider",
        "interval",
        "final_status",
        "oldest_probe_start",
        "days_from_oldest_probe",
        "first_actual_data_date",
        "days_from_first_actual_data",
        "rows",
        "request_count",
    ]
    print(summary_df[final_columns].to_string(index=False))


def print_approx_range_summary(
    summary_df: pd.DataFrame,
    max_cap_days: int,
) -> None:
    """
    Print user-facing approximate range summary.

    打印用户需要的近似范围总结，例如：
        5m   最远约 65 天
        1d+  至少 20 年
    """
    print("\nApprox Range Summary:")

    intraday_rows = summary_df[
        ~summary_df["interval"].isin(DAILY_PLUS_INTERVALS)
    ].copy()

    for _, row in intraday_rows.iterrows():
        interval = row["interval"]
        status = row["final_status"]
        days = row["days_from_oldest_probe"]

        if status != "success" or pd.isna(days):
            print(f"{interval:<4} 当前测试未成功")
            continue

        print(f"{interval:<4} 最远约 {int(days)} 天")

    daily_plus_rows = summary_df[
        summary_df["interval"].isin(DAILY_PLUS_INTERVALS)
    ].copy()

    daily_plus_success = daily_plus_rows[
        (daily_plus_rows["final_status"] == "success")
        & (daily_plus_rows["days_from_oldest_probe"] >= max_cap_days)
    ]

    if not daily_plus_success.empty:
        years = round(max_cap_days / 365.25)
        print(f"{'1d+':<4} 至少 {years} 年")
    else:
        for _, row in daily_plus_rows.iterrows():
            interval = row["interval"]
            status = row["final_status"]
            days = row["days_from_oldest_probe"]

            if status != "success" or pd.isna(days):
                print(f"{interval:<4} 当前测试未成功")
                continue

            print(f"{interval:<4} 最远约 {int(days)} 天")


def print_debug_summary(summary_df: pd.DataFrame) -> None:
    """
    Print debug summary.

    打印调试信息。
    """
    print("\nDebug Summary:")
    debug_columns = [
        "symbol",
        "provider",
        "interval",
        "cap_reason",
        "cap_error_type",
        "cap_non_empty",
        "cap_earliest_timestamp",
        "cap_latest_timestamp",
    ]
    print(summary_df[debug_columns].to_string(index=False))


def save_outputs(
    results: list[dict[str, Any]],
    summary_df: pd.DataFrame,
    output_dir: Path,
    symbol: str,
) -> tuple[Path, Path]:
    """
    Save CSV and JSON outputs.

    保存 CSV 和 JSON 结果。
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = output_dir / f"intraday_point_limit_summary_{symbol}_{timestamp}.csv"
    json_path = output_dir / f"intraday_point_limit_details_{symbol}_{timestamp}.json"

    summary_df.to_csv(csv_path, index=False)

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    return csv_path, json_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--provider", default="yfinance")
    parser.add_argument(
        "--adjustment",
        default="splits_and_dividends",
        choices=["splits_only", "splits_and_dividends"],
    )
    parser.add_argument("--extended-hours", action="store_true")
    parser.add_argument("--output-dir", default="reports/data_limit_tests")
    parser.add_argument("--max-cap-days", type=int, default=365 * 20)
    parser.add_argument("--probe-window-days", type=int, default=7)

    args = parser.parse_args()

    today = datetime.now().date()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Running {SCRIPT_VERSION}")
    print(f"Today: {today}")
    print(f"Symbol: {args.symbol}")
    print(f"Provider: {args.provider}")
    print(f"Max cap days: {args.max_cap_days}")
    print(f"Default probe window days: {args.probe_window_days}")

    results: list[dict[str, Any]] = []

    for interval in INTERVALS:
        probe_window_days = INTERVAL_PROBE_WINDOW_OVERRIDES.get(
            interval,
            args.probe_window_days,
        )

        print(
            f"\nTesting interval={interval} | "
            f"cap={args.max_cap_days} days | "
            f"probe_window={probe_window_days} days ..."
        )

        result = find_oldest_available_point(
            symbol=args.symbol,
            provider=args.provider,
            interval=interval,
            today=today,
            max_cap_days=args.max_cap_days,
            default_probe_window_days=args.probe_window_days,
            adjustment=args.adjustment,
            extended_hours=args.extended_hours,
        )

        results.append(result)

        probe_days, probe_months, probe_years = calculate_distance_from_date(
            today=today,
            date_str=result["oldest_probe_start"],
        )

        actual_days, actual_months, actual_years = calculate_distance_from_date(
            today=today,
            date_str=result["first_actual_data_date"],
        )

        print(
            f"  status={result['final_status']} | "
            f"oldest_probe_start={result['oldest_probe_start']} | "
            f"probe_days={probe_days} | "
            f"first_actual_data_date={result['first_actual_data_date']} | "
            f"actual_days={actual_days} | "
            f"rows={result['rows']} | "
            f"requests={result['request_count']}"
        )

    summary_df = build_summary_df(results=results, today=today)

    csv_path, json_path = save_outputs(
        results=results,
        summary_df=summary_df,
        output_dir=output_dir,
        symbol=args.symbol,
    )

    print("\nDone.")
    print(f"Summary CSV: {csv_path}")
    print(f"Details JSON: {json_path}")

    print_final_summary(summary_df)
    print_approx_range_summary(
        summary_df=summary_df,
        max_cap_days=args.max_cap_days,
    )
    print_debug_summary(summary_df)


if __name__ == "__main__":
    main()