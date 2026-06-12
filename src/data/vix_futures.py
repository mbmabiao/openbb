from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from io import StringIO
import json
from pathlib import Path
import re
from typing import Any
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VIX_FUTURES_CONTRACTS_CSV = PROJECT_ROOT / "vix_futures_contracts.csv"
VIX_FUTURES_UNAVAILABLE_MESSAGE = "VIX futures term structure unavailable: no contract-level VX futures data."

VIX_FUTURES_COLUMNS = [
    "symbol",
    "expiry",
    "last",
    "open",
    "high",
    "low",
    "close",
    "source",
    "timestamp",
]

MONTH_CODE_TO_NUMBER = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}
NUMBER_TO_MONTH_CODE = {value: key for key, value in MONTH_CODE_TO_NUMBER.items()}


@dataclass(frozen=True)
class VixFuturesContractsResult:
    contracts: pd.DataFrame
    source: str
    warning: str | None
    csv_path: str


def load_vix_futures_contracts(
    *,
    today: date | None = None,
    manual_csv_path: Path | str = DEFAULT_VIX_FUTURES_CONTRACTS_CSV,
) -> VixFuturesContractsResult:
    today = today or date.today()
    csv_path = Path(manual_csv_path)
    warnings: list[str] = []

    for source_name, fetcher in (
        ("cboe", fetch_vix_futures_contracts_cboe),
        ("tradingview", fetch_vix_futures_contracts_tradingview_fallback),
    ):
        try:
            contracts = normalise_vix_futures_contracts(fetcher(), source=source_name)
            contracts = _filter_unexpired(contracts, today=today)
            if not contracts.empty:
                return VixFuturesContractsResult(
                    contracts=contracts,
                    source=source_name,
                    warning=None,
                    csv_path=str(csv_path),
                )
        except Exception as error:
            warnings.append(f"{source_name}: {error}")

    try:
        manual = load_manual_vix_futures_contracts_csv(csv_path)
        manual = _filter_unexpired(manual, today=today)
        if not manual.empty:
            warning = "; ".join(warnings) if warnings else None
            return VixFuturesContractsResult(
                contracts=manual,
                source="manual_csv",
                warning=warning,
                csv_path=str(csv_path),
            )
    except Exception as error:
        warnings.append(f"manual_csv: {error}")

    warning = VIX_FUTURES_UNAVAILABLE_MESSAGE
    if warnings:
        warning = f"{warning} {'; '.join(warnings)}"
    return VixFuturesContractsResult(
        contracts=empty_vix_futures_contracts_frame(),
        source="unavailable",
        warning=warning,
        csv_path=str(csv_path),
    )


def fetch_vix_futures_contracts_cboe() -> pd.DataFrame:
    """Fetch contract-level VIX futures from Cboe delayed quote pages."""
    urls = (
        "https://www.cboe.com/en/tradable-products/vix/vix-futures/",
        "https://www.cboe.com/delayed_quotes/vix/quote_table/",
    )
    errors: list[str] = []
    for url in urls:
        try:
            html = _http_get(url)
            tables = pd.read_html(StringIO(html))
            for table in tables:
                candidate = _flatten_columns(table)
                if _looks_like_vix_futures_table(candidate):
                    candidate = candidate.copy()
                    candidate["source"] = "cboe"
                    return candidate
        except Exception as error:
            errors.append(f"{url}: {error}")
    raise RuntimeError("; ".join(errors) or "Cboe VIX futures table not found.")


def fetch_vix_futures_contracts_tradingview_fallback() -> pd.DataFrame:
    """Best-effort TradingView fallback for contract-level futures metadata."""
    url = "https://symbol-search.tradingview.com/symbol_search/?text=VX&exchange=CFE&type=futures"
    payload = _http_get(url)
    records = json.loads(payload)
    if not isinstance(records, list) or not records:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol") or item.get("ticker") or item.get("name")
        if not symbol or not str(symbol).upper().startswith("VX"):
            continue
        rows.append(
            {
                "symbol": symbol,
                "expiry": item.get("expiration") or item.get("expiry") or item.get("expiration_date"),
                "last": item.get("last") or item.get("close") or item.get("price"),
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item.get("close") or item.get("last") or item.get("price"),
                "source": "tradingview",
                "timestamp": item.get("update_time") or item.get("timestamp"),
            }
        )
    return pd.DataFrame(rows)


def load_manual_vix_futures_contracts_csv(
    path: Path | str = DEFAULT_VIX_FUTURES_CONTRACTS_CSV,
) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return empty_vix_futures_contracts_frame()
    return normalise_vix_futures_contracts(pd.read_csv(csv_path), source="manual")


def normalise_vix_futures_contracts(df: pd.DataFrame | None, *, source: str | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return empty_vix_futures_contracts_frame()

    raw = _flatten_columns(df.copy())
    lower_to_column = {str(column).strip().lower(): column for column in raw.columns}
    symbol_col = _first_column(lower_to_column, ("symbol", "contract", "ticker", "name"))
    expiry_col = _first_column(lower_to_column, ("expiry", "expiration", "expiration date", "expiration_date", "maturity"))
    open_col = _first_column(lower_to_column, ("open", "open price", "open_price"))
    high_col = _first_column(lower_to_column, ("high", "high price", "high_price"))
    low_col = _first_column(lower_to_column, ("low", "low price", "low_price"))
    source_col = _first_column(lower_to_column, ("source", "provider"))
    timestamp_col = _first_column(lower_to_column, ("timestamp", "time", "updated", "last update", "last_update"))

    if symbol_col is None and expiry_col is None:
        return empty_vix_futures_contracts_frame()

    out = pd.DataFrame(index=raw.index)
    out["expiry"] = _parse_expiry_series(raw[expiry_col]) if expiry_col else pd.NaT
    out["symbol"] = raw[symbol_col].astype("string").str.strip() if symbol_col else pd.NA
    out["last"] = _numeric_coalesce(raw, ("last", "last price", "last_price", "settlement", "settle", "price", "close"))
    out["open"] = _numeric_column(raw, open_col)
    out["high"] = _numeric_column(raw, high_col)
    out["low"] = _numeric_column(raw, low_col)
    out["close"] = _numeric_coalesce(raw, ("close", "last", "last price", "last_price", "settlement", "settle", "price"))
    out["source"] = raw[source_col] if source_col else (source or "unknown")
    out["timestamp"] = raw[timestamp_col] if timestamp_col else pd.Timestamp.utcnow().isoformat()

    inferred_expiry = [
        _infer_expiry_from_symbol(symbol, fallback_year=None)
        for symbol in out["symbol"].astype("string")
    ]
    out["expiry"] = out["expiry"].fillna(pd.Series(inferred_expiry, index=out.index))
    out["last"] = out["last"].fillna(out["close"])
    out["close"] = out["close"].fillna(out["last"])
    for column in ["open", "high", "low"]:
        out[column] = out[column].fillna(out["close"])

    out = out.dropna(subset=["symbol", "expiry"]).copy()
    out = out.loc[out["last"].notna() | out["close"].notna()].copy()
    if out.empty:
        return empty_vix_futures_contracts_frame()

    out["symbol"] = [
        _normalise_contract_symbol(symbol, expiry)
        for symbol, expiry in zip(out["symbol"], out["expiry"], strict=False)
    ]
    out["expiry"] = pd.to_datetime(out["expiry"], errors="coerce").dt.normalize()
    out = out.dropna(subset=["expiry"]).copy()
    out = out.sort_values(["expiry", "symbol"], kind="stable").drop_duplicates("symbol", keep="first")
    return out[VIX_FUTURES_COLUMNS].reset_index(drop=True)


def select_vx1_vx2(df: pd.DataFrame, today: date | None = None) -> tuple[pd.Series | None, pd.Series | None]:
    contracts = _filter_unexpired(normalise_vix_futures_contracts(df), today=today or date.today())
    contracts = contracts.loc[contracts["symbol"].map(_is_standard_monthly_vx_symbol)].copy()
    if len(contracts) < 2:
        return None, None
    return contracts.iloc[0], contracts.iloc[1]


def calculate_vx_term_structure(vx1: pd.Series | dict[str, Any] | None, vx2: pd.Series | dict[str, Any] | None) -> dict[str, float | str | None]:
    if vx1 is None or vx2 is None:
        return _empty_term_structure()

    vx1_price = _row_price(vx1)
    vx2_price = _row_price(vx2)
    if vx1_price is None or vx2_price is None or vx1_price <= 0 or vx2_price <= 0:
        return _empty_term_structure()

    ratio = vx1_price / vx2_price
    contango_pct = (vx2_price / vx1_price - 1) * 100
    backwardation_pct = (vx1_price / vx2_price - 1) * 100
    if ratio < 0.99:
        status = "Contango"
    elif ratio > 1.01:
        status = "Backwardation"
    else:
        status = "Flat"
    return {
        "vx1_price": vx1_price,
        "vx2_price": vx2_price,
        "vx1_vx2_ratio": ratio,
        "contango_pct": contango_pct,
        "backwardation_pct": backwardation_pct,
        "status": status,
    }


def fetch_current_vx_contract_daily_history(
    symbol: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Best-effort daily close history for a single VX futures contract.

    Returns a ``DataFrame`` with ``date`` and ``close`` columns, sorted ascending.
    When no per-contract history source is reachable, an empty frame is returned so
    callers can fall back to a single live snapshot.
    """
    expiry = _infer_expiry_from_symbol(symbol)
    history = _fetch_vx_contract_history_openbb(symbol, expiry, start_date, end_date)
    if history is not None and not history.empty:
        return history
    return pd.DataFrame(columns=["date", "close"])


def _fetch_vx_contract_history_openbb(
    symbol: str,
    expiry: pd.Timestamp | Any,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    try:
        from openbb import obb
    except Exception:
        return pd.DataFrame(columns=["date", "close"])

    attempts: list[dict[str, Any]] = []
    if expiry is not None and not pd.isna(expiry):
        expiration = pd.Timestamp(expiry).strftime("%Y-%m")
        attempts.append({"symbol": "VX", "expiration": expiration})
    attempts.append({"symbol": symbol})

    for kwargs in attempts:
        for provider in ("yfinance",):
            try:
                result = obb.derivatives.futures.historical(
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    provider=provider,
                    **kwargs,
                )
                cleaned = _to_contract_daily_close(_result_to_frame(result))
                if not cleaned.empty:
                    return cleaned
            except Exception:
                continue
    return pd.DataFrame(columns=["date", "close"])


def _result_to_frame(result: Any) -> pd.DataFrame | None:
    if result is None:
        return None
    if hasattr(result, "to_dataframe"):
        return result.to_dataframe()
    if hasattr(result, "to_df"):
        return result.to_df()
    if hasattr(result, "results"):
        rows = getattr(result, "results")
        records = []
        try:
            iterable = rows if isinstance(rows, list) else list(rows)
        except TypeError:
            iterable = [rows]
        for item in iterable:
            if hasattr(item, "model_dump"):
                records.append(item.model_dump())
            elif hasattr(item, "dict"):
                records.append(item.dict())
            else:
                records.append(item)
        return pd.DataFrame(records)
    if isinstance(result, pd.DataFrame):
        return result
    try:
        return pd.DataFrame(result)
    except Exception:
        return None


def _to_contract_daily_close(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "close"])

    frame = df.copy()
    if isinstance(frame.index, pd.DatetimeIndex):
        frame = frame.reset_index()
    frame = _flatten_columns(frame)
    lower_to_column = {str(column).strip().lower(): column for column in frame.columns}

    date_col = _first_column(lower_to_column, ("date", "datetime", "time", "timestamp", "index"))
    close_col = _first_column(
        lower_to_column,
        ("close", "settle", "settlement", "settlement_price", "last", "price", "adj close", "adj_close"),
    )
    if date_col is None or close_col is None:
        return pd.DataFrame(columns=["date", "close"])

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(frame[date_col], errors="coerce").dt.normalize(),
            "close": pd.to_numeric(frame[close_col], errors="coerce"),
        }
    )
    out = out.dropna(subset=["date", "close"])
    out = out.loc[out["close"] > 0]
    if out.empty:
        return pd.DataFrame(columns=["date", "close"])
    out = out.sort_values("date", kind="stable").drop_duplicates("date", keep="last")
    return out.reset_index(drop=True)


def empty_vix_futures_contracts_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=VIX_FUTURES_COLUMNS)


def _http_get(url: str, *, timeout: int = 12) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; OpenBB VIX Dashboard)",
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def _looks_like_vix_futures_table(df: pd.DataFrame) -> bool:
    columns = {str(column).strip().lower() for column in df.columns}
    has_expiry = bool(columns & {"expiration", "expiration date", "expiry", "expiration_date"})
    has_price = bool(columns & {"last", "last price", "last_price", "settlement", "settle", "price"})
    has_symbol = bool(columns & {"symbol", "contract", "ticker"})
    if not (has_expiry and has_price and has_symbol):
        return False
    symbol_col = _first_column({str(column).strip().lower(): column for column in df.columns}, ("symbol", "contract", "ticker"))
    if symbol_col is None:
        return False
    symbols = df[symbol_col].astype("string").str.upper()
    return bool(symbols.str.contains(r"\bVX|^VX", regex=True, na=False).any())


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(str(part).strip() for part in column if str(part).strip() and "Unnamed" not in str(part))
            for column in df.columns
        ]
    else:
        df.columns = [str(column).strip() for column in df.columns]
    return df


def _first_column(lower_to_column: dict[str, str], aliases: tuple[str, ...]) -> str | None:
    for alias in aliases:
        column = lower_to_column.get(alias.lower())
        if column is not None:
            return column
    return None


def _numeric_column(df: pd.DataFrame, column: str | None) -> pd.Series:
    if column is None:
        return pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    text = df[column].astype("string").str.replace(",", "", regex=False).str.extract(r"([-+]?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(text, errors="coerce")


def _numeric_coalesce(df: pd.DataFrame, aliases: tuple[str, ...]) -> pd.Series:
    lower_to_column = {str(column).strip().lower(): column for column in df.columns}
    values = pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    for alias in aliases:
        column = lower_to_column.get(alias.lower())
        if column is None:
            continue
        values = values.fillna(_numeric_column(df, column))
    return values


def _parse_expiry_series(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, errors="coerce", format="mixed").dt.normalize()
    except TypeError:
        return pd.to_datetime(series, errors="coerce").dt.normalize()


def _filter_unexpired(df: pd.DataFrame, *, today: date) -> pd.DataFrame:
    if df is None or df.empty:
        return empty_vix_futures_contracts_frame()
    out = normalise_vix_futures_contracts(df)
    today_ts = pd.Timestamp(today).normalize()
    out = out.loc[pd.to_datetime(out["expiry"], errors="coerce") >= today_ts].copy()
    return out.sort_values(["expiry", "symbol"], kind="stable").reset_index(drop=True)


def _normalise_contract_symbol(symbol: Any, expiry: Any) -> str:
    raw = str(symbol).strip().upper()
    expiry_ts = pd.to_datetime(expiry, errors="coerce")
    if pd.isna(expiry_ts):
        return raw.replace("/", "")
    year = int(pd.Timestamp(expiry_ts).year)
    month_code = NUMBER_TO_MONTH_CODE.get(int(pd.Timestamp(expiry_ts).month), "")

    slash_match = re.fullmatch(r"(VX\d*)/([FGHJKMNQUVXZ])(\d)", raw)
    if slash_match:
        prefix, code, _ = slash_match.groups()
        return f"{prefix}{code}{year}"

    compact_match = re.fullmatch(r"(VX\d*)([FGHJKMNQUVXZ])(\d)$", raw)
    if compact_match:
        prefix, code, _ = compact_match.groups()
        return f"{prefix}{code}{year}"

    if raw in {"VX", "VIX"} and month_code:
        return f"VX{month_code}{year}"
    return raw.replace("/", "")


def _is_standard_monthly_vx_symbol(symbol: Any) -> bool:
    return bool(re.fullmatch(r"VX[FGHJKMNQUVXZ]\d{4}", str(symbol).strip().upper()))


def _infer_expiry_from_symbol(symbol: Any, fallback_year: int | None = None) -> pd.Timestamp | pd.NaT:
    text = str(symbol).strip().upper()
    match = re.search(r"([FGHJKMNQUVXZ])(\d{1,4})$", text)
    if not match:
        return pd.NaT
    month_code, year_text = match.groups()
    month = MONTH_CODE_TO_NUMBER.get(month_code)
    if month is None:
        return pd.NaT
    if len(year_text) == 1:
        year = (fallback_year or date.today().year)
        decade = year - year % 10
        year = decade + int(year_text)
        if year < date.today().year - 1:
            year += 10
    elif len(year_text) == 2:
        year = 2000 + int(year_text)
    else:
        year = int(year_text)
    return pd.Timestamp(year=year, month=month, day=1)


def _row_price(row: pd.Series | dict[str, Any]) -> float | None:
    for key in ("last", "close", "settlement", "price"):
        try:
            value = row[key]
        except Exception:
            continue
        try:
            if value is not None and pd.notna(value):
                return float(value)
        except Exception:
            continue
    return None


def _empty_term_structure() -> dict[str, float | str | None]:
    return {
        "vx1_price": None,
        "vx2_price": None,
        "vx1_vx2_ratio": None,
        "contango_pct": None,
        "backwardation_pct": None,
        "status": "Term structure unavailable",
    }
