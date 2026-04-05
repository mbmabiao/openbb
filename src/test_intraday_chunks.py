from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any

import pandas as pd
from openbb import obb


@dataclass
class ChunkResult:
    chunk_start: pd.Timestamp
    chunk_end: pd.Timestamp
    status: str
    rows: int
    first_bar: str | None
    last_bar: str | None
    error: str | None = None
    columns: list[str] | None = None


def split_date_range_into_chunks(
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    max_span_days: int,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if end_ts < start_ts:
        return []

    max_span_days = max(int(max_span_days), 1)
    span = pd.Timedelta(days=max_span_days - 1)

    output: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    current_start = start_ts.normalize()
    final_end = end_ts.normalize()

    while current_start <= final_end:
        current_end = min(current_start + span, final_end)
        output.append((current_start, current_end))
        if current_end >= final_end:
            break
        current_start = current_end + pd.Timedelta(days=1)

    return output


def to_dataframe(result: Any) -> pd.DataFrame | None:
    if result is None:
        return None

    if hasattr(result, "to_dataframe"):
        try:
            return result.to_dataframe()
        except Exception:
            pass

    if hasattr(result, "to_df"):
        try:
            return result.to_df()
        except Exception:
            pass

    if isinstance(result, pd.DataFrame):
        return result

    try:
        return pd.DataFrame(result)
    except Exception:
        return None


def normalise_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # 关键修正：如果时间在 DatetimeIndex，先把 index 拉出来
    if isinstance(out.index, pd.DatetimeIndex):
        index_name = out.index.name if out.index.name is not None else "date"
        if index_name in out.columns:
            index_name = "__index_date__"
        out = out.reset_index(names=index_name)
    else:
        out = out.reset_index(drop=False)
        if "date" in out.columns and "index" in out.columns:
            out = out.drop(columns=["index"])

    rename_map = {}
    for col in out.columns:
        lower = str(col).lower().strip()
        if lower in ("date", "datetime", "timestamp", "time", "__index_date__"):
            rename_map[col] = "date"
        elif lower in ("open", "adj_open"):
            rename_map[col] = "open"
        elif lower in ("high", "adj_high"):
            rename_map[col] = "high"
        elif lower in ("low", "adj_low"):
            rename_map[col] = "low"
        elif lower in ("close", "adj_close", "price"):
            rename_map[col] = "close"
        elif lower in ("volume", "vol"):
            rename_map[col] = "volume"

    out = out.rename(columns=rename_map)

    # 兜底：如果还没有 date，尝试把第一列识别成时间
    if "date" not in out.columns and len(out.columns) > 0:
        first_col = out.columns[0]
        trial = pd.to_datetime(out[first_col], errors="coerce")
        if trial.notna().sum() > 0:
            out["date"] = trial

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        try:
            if getattr(out["date"].dt, "tz", None) is not None:
                out["date"] = out["date"].dt.tz_localize(None)
        except (AttributeError, TypeError):
            pass

    out = out.reset_index(drop=True)
    out.index.name = None

    if "date" in out.columns:
        out = out.dropna(subset=["date"])
        out = out.sort_values(by="date", kind="stable").reset_index(drop=True)

    return out


def fetch_single_price_frame(
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
    raw_df = to_dataframe(result)
    if raw_df is None or raw_df.empty:
        raise ValueError("No price data returned.")

    out = normalise_ohlcv_columns(raw_df)
    required_cols = {"date", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(set(out.columns)):
        raise ValueError(
            f"Returned data missing required OHLCV columns. "
            f"Columns={list(out.columns)}"
        )

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["date", "open", "high", "low", "close", "volume"]).copy()
    out = out.sort_values("date", kind="stable").reset_index(drop=True)

    if out.empty:
        raise ValueError("Data became empty after cleaning.")

    return out


def test_chunks(
    symbol: str,
    start_date: str,
    end_date: str,
    interval: str,
    provider: str | None,
    adjustment: str,
    extended_hours: bool,
    max_span_days: int,
) -> list[ChunkResult]:
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()

    chunks = split_date_range_into_chunks(start_ts, end_ts, max_span_days=max_span_days)
    print(f"\n总共 {len(chunks)} 个 chunk")
    print(f"symbol={symbol}, interval={interval}, provider={provider}, range={start_ts.date()} -> {end_ts.date()}")
    print(f"max_span_days={max_span_days}\n")

    results: list[ChunkResult] = []

    for idx, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        query_end = chunk_end + pd.Timedelta(days=1)
        print(f"[{idx}/{len(chunks)}] 测试 chunk: {chunk_start.date()} -> {chunk_end.date()} ... ", end="")

        try:
            df = fetch_single_price_frame(
                symbol=symbol,
                start_date=str(chunk_start.date()),
                end_date=str(query_end.date()),
                provider=provider,
                interval=interval,
                adjustment=adjustment,
                extended_hours=extended_hours,
            )

            first_bar = str(df["date"].iloc[0])
            last_bar = str(df["date"].iloc[-1])
            rows = len(df)

            print(f"OK rows={rows} first={first_bar} last={last_bar}")
            results.append(
                ChunkResult(
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    status="ok",
                    rows=rows,
                    first_bar=first_bar,
                    last_bar=last_bar,
                    columns=list(df.columns),
                )
            )
        except Exception as exc:
            print(f"FAIL {type(exc).__name__}: {exc}")
            results.append(
                ChunkResult(
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    status="fail",
                    rows=0,
                    first_bar=None,
                    last_bar=None,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )

    return results


def print_summary(results: list[ChunkResult]) -> None:
    ok_results = [r for r in results if r.status == "ok"]
    fail_results = [r for r in results if r.status != "ok"]

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"成功 chunk 数: {len(ok_results)}")
    print(f"失败 chunk 数: {len(fail_results)}")

    if ok_results:
        total_rows = sum(r.rows for r in ok_results)
        print(f"成功 chunk 总行数: {total_rows}")

        print("\n成功窗口示例:")
        for r in ok_results[:5]:
            print(
                f"- {r.chunk_start.date()} -> {r.chunk_end.date()} | "
                f"rows={r.rows} | first={r.first_bar} | last={r.last_bar}"
            )

    if fail_results:
        print("\n失败窗口明细:")
        for r in fail_results:
            print(f"- {r.chunk_start.date()} -> {r.chunk_end.date()} | {r.error}")

    print("=" * 80)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="测试 OpenBB / provider 的 per-chunk 拉数能力")
    parser.add_argument("--symbol", required=True, help="例如 AAPL")
    parser.add_argument("--start-date", required=True, help="例如 2023-02-21")
    parser.add_argument("--end-date", required=True, help="例如 2023-06-30")
    parser.add_argument("--interval", default="1h", help="例如 1h / 5m / 15m / 1d")
    parser.add_argument("--provider", default="yfinance", help="例如 yfinance")
    parser.add_argument("--adjustment", default="splits_only", help="例如 splits_only")
    parser.add_argument("--max-span-days", type=int, default=59, help="每个 chunk 最大天数")
    parser.add_argument("--extended-hours", action="store_true", help="是否启用 extended hours")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    results = test_chunks(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        interval=args.interval,
        provider=args.provider,
        adjustment=args.adjustment,
        extended_hours=args.extended_hours,
        max_span_days=args.max_span_days,
    )
    print_summary(results)


if __name__ == "__main__":
    main()