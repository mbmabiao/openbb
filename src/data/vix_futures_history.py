from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from data.vix_futures import NUMBER_TO_MONTH_CODE


RATIO_HISTORY_COLUMNS = [
    "date",
    "vx1_symbol",
    "vx1_expiry",
    "vx1_price",
    "vx2_symbol",
    "vx2_expiry",
    "vx2_price",
    "ratio",
    "contango_pct",
    "backwardation_pct",
    "status",
]


def load_cboe_vix_term_structure() -> tuple[pd.DataFrame, str | None]:
    """Load the full CBOE VIX futures term structure via vix_utils.

    Returns ``(term_structure, warning)``. On failure the frame is empty and the
    warning explains why -- the exception is never silently swallowed.
    """
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            import vix_utils

            term_structure = vix_utils.load_vix_term_structure()
    except Exception as error:  # noqa: BLE001 - surface any failure to the caller
        return pd.DataFrame(), f"Failed to load CBOE VIX term structure via vix_utils: {error}"

    if term_structure is None or len(term_structure) == 0:
        return pd.DataFrame(), "vix_utils returned no VIX futures term structure data."
    return term_structure, None


def build_vx1_vx2_ratio_from_term_structure(term_structure: pd.DataFrame) -> pd.DataFrame:
    """Derive a daily VX1/VX2 ratio history from the vix_utils term structure.

    The term structure is in skinny/record format: one row per (Trade Date,
    contract). Monthly contracts have ``Weekly == False`` and ``Tenor_Monthly``
    gives the monthly rank (1 = front month / VX1, 2 = second month / VX2).
    """
    if term_structure is None or term_structure.empty:
        return _empty_ratio_frame()

    ts = term_structure.copy()
    if "Weekly" in ts.columns:
        ts = ts.loc[ts["Weekly"] == False].copy()  # noqa: E712 - pandas boolean mask

    required = {"Trade Date", "Tenor_Monthly", "Expiry"}
    if not required.issubset(ts.columns):
        return _empty_ratio_frame()

    ts["date"] = pd.to_datetime(ts["Trade Date"], errors="coerce").dt.normalize()
    ts["expiry"] = pd.to_datetime(ts["Expiry"], errors="coerce").dt.normalize()
    ts["price"] = _settlement_price(ts)
    ts["symbol"] = _contract_symbols(ts)
    ts["tenor"] = pd.to_numeric(ts["Tenor_Monthly"], errors="coerce")

    legs = ts.dropna(subset=["date", "price", "tenor"])
    legs = legs.loc[(legs["price"] > 0) & legs["tenor"].isin([1, 2])]
    if legs.empty:
        return _empty_ratio_frame()

    columns = ["date", "symbol", "expiry", "price"]
    vx1 = (
        legs.loc[legs["tenor"] == 1, columns]
        .sort_values("date", kind="stable")
        .drop_duplicates("date", keep="first")
        .rename(columns={"symbol": "vx1_symbol", "expiry": "vx1_expiry", "price": "vx1_price"})
    )
    vx2 = (
        legs.loc[legs["tenor"] == 2, columns]
        .sort_values("date", kind="stable")
        .drop_duplicates("date", keep="first")
        .rename(columns={"symbol": "vx2_symbol", "expiry": "vx2_expiry", "price": "vx2_price"})
    )

    merged = vx1.merge(vx2, on="date", how="inner")
    merged = merged.loc[(merged["vx1_price"] > 0) & (merged["vx2_price"] > 0)].copy()
    if merged.empty:
        return _empty_ratio_frame()

    merged["ratio"] = merged["vx1_price"] / merged["vx2_price"]
    merged["contango_pct"] = (merged["vx2_price"] / merged["vx1_price"] - 1) * 100
    merged["backwardation_pct"] = (merged["vx1_price"] / merged["vx2_price"] - 1) * 100
    merged["status"] = np.where(
        merged["ratio"] < 0.99,
        "Contango",
        np.where(merged["ratio"] > 1.01, "Backwardation", "Flat"),
    )

    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    merged["vx1_expiry"] = merged["vx1_expiry"].dt.strftime("%Y-%m-%d")
    merged["vx2_expiry"] = merged["vx2_expiry"].dt.strftime("%Y-%m-%d")
    return merged[RATIO_HISTORY_COLUMNS].sort_values("date", kind="stable").reset_index(drop=True)


def _settlement_price(ts: pd.DataFrame) -> pd.Series:
    settle = (
        pd.to_numeric(ts["Settle"], errors="coerce")
        if "Settle" in ts.columns
        else pd.Series(np.nan, index=ts.index)
    )
    close = (
        pd.to_numeric(ts["Close"], errors="coerce")
        if "Close" in ts.columns
        else pd.Series(np.nan, index=ts.index)
    )
    return settle.where(settle > 0, close)


def _contract_symbols(ts: pd.DataFrame) -> list[str]:
    expiry = pd.to_datetime(ts["Expiry"], errors="coerce")
    years = ts["Year"] if "Year" in ts.columns else expiry.dt.year
    months = ts["MonthOfYear"] if "MonthOfYear" in ts.columns else expiry.dt.month
    symbols: list[str] = []
    for year, month in zip(years, months, strict=False):
        symbols.append(_contract_symbol(year, month))
    return symbols


def _contract_symbol(year: object, month: object) -> str:
    try:
        month_code = NUMBER_TO_MONTH_CODE.get(int(month), "")
        return f"VX{month_code}{int(year)}"
    except (TypeError, ValueError):
        return "VX"


def _empty_ratio_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=RATIO_HISTORY_COLUMNS)
