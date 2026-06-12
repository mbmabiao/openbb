from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


DB_PATH = Path(__file__).with_name("vx_ratio_history.sqlite")

PAIR_LOOKBACK_DAYS = 60
OBSERVED_LOOKBACK_DAYS = 180

OBSERVED_COLUMNS = [
    "date",
    "timestamp",
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
PAIR_COLUMNS = [
    "date",
    "vx1_symbol",
    "vx2_symbol",
    "vx1_expiry",
    "vx2_expiry",
    "vx1_price",
    "vx2_price",
    "ratio",
    "contango_pct",
    "backwardation_pct",
    "status",
    "updated_at",
]

_CREATE_OBSERVED_SQL = """
CREATE TABLE IF NOT EXISTS vx_ratio_observed_daily (
    date TEXT PRIMARY KEY,
    timestamp TEXT,
    vx1_symbol TEXT,
    vx1_expiry TEXT,
    vx1_price REAL,
    vx2_symbol TEXT,
    vx2_expiry TEXT,
    vx2_price REAL,
    ratio REAL,
    contango_pct REAL,
    backwardation_pct REAL,
    status TEXT
)
"""
_CREATE_PAIR_SQL = """
CREATE TABLE IF NOT EXISTS vx_ratio_contract_pair_daily (
    date TEXT,
    vx1_symbol TEXT,
    vx2_symbol TEXT,
    vx1_expiry TEXT,
    vx2_expiry TEXT,
    vx1_price REAL,
    vx2_price REAL,
    ratio REAL,
    contango_pct REAL,
    backwardation_pct REAL,
    status TEXT,
    updated_at TEXT,
    PRIMARY KEY (date, vx1_symbol, vx2_symbol)
)
"""


def _connect(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.execute(_CREATE_OBSERVED_SQL)
    connection.execute(_CREATE_PAIR_SQL)
    return connection


def make_snapshot_timestamp(now: datetime | None = None) -> str:
    return (now or datetime.now()).isoformat(timespec="seconds")


def ratio_fields(vx1_price: float, vx2_price: float) -> dict[str, float | str]:
    """Compute ratio / contango / backwardation / status for a VX1, VX2 price pair."""
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
        "ratio": ratio,
        "contango_pct": contango_pct,
        "backwardation_pct": backwardation_pct,
        "status": status,
    }


def write_observed_daily(snapshot: dict[str, Any], *, db_path: Path | str = DB_PATH) -> None:
    """Upsert today's observed VX1/VX2 snapshot. Re-running the same day overwrites the row."""
    values = [snapshot.get(column) for column in OBSERVED_COLUMNS]
    placeholders = ", ".join(["?"] * len(OBSERVED_COLUMNS))
    columns = ", ".join(OBSERVED_COLUMNS)
    with closing(_connect(db_path)) as connection:
        connection.execute(
            f"INSERT OR REPLACE INTO vx_ratio_observed_daily ({columns}) VALUES ({placeholders})",
            values,
        )
        connection.commit()


def write_contract_pair_daily(rows: list[dict[str, Any]], *, db_path: Path | str = DB_PATH) -> None:
    """Upsert daily ratio rows for a specific VX1/VX2 contract pair (keyed by date+symbols)."""
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(PAIR_COLUMNS))
    columns = ", ".join(PAIR_COLUMNS)
    payload = [[row.get(column) for column in PAIR_COLUMNS] for row in rows]
    with closing(_connect(db_path)) as connection:
        connection.executemany(
            f"INSERT OR REPLACE INTO vx_ratio_contract_pair_daily ({columns}) VALUES ({placeholders})",
            payload,
        )
        connection.commit()


def build_contract_pair_daily_rows(
    *,
    vx1_symbol: str,
    vx1_expiry: str,
    vx2_symbol: str,
    vx2_expiry: str,
    vx1_prices: pd.DataFrame,
    vx2_prices: pd.DataFrame,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Merge two contracts' daily close prices by date and compute per-day ratio rows.

    Only dates where both contracts have a positive price are kept. With a single
    current observation this yields one row; a richer price-history source yields
    one row per overlapping trading day.
    """
    if vx1_prices is None or vx2_prices is None or vx1_prices.empty or vx2_prices.empty:
        return []

    left = vx1_prices[["date", "close"]].rename(columns={"close": "vx1_price"}).copy()
    right = vx2_prices[["date", "close"]].rename(columns={"close": "vx2_price"}).copy()
    left["date"] = pd.to_datetime(left["date"], errors="coerce")
    right["date"] = pd.to_datetime(right["date"], errors="coerce")
    merged = left.merge(right, on="date", how="inner").dropna(subset=["date", "vx1_price", "vx2_price"])
    merged = merged.loc[(merged["vx1_price"] > 0) & (merged["vx2_price"] > 0)]
    if merged.empty:
        return []

    updated_at = make_snapshot_timestamp(now)
    rows: list[dict[str, Any]] = []
    for record in merged.itertuples(index=False):
        vx1_price = float(record.vx1_price)
        vx2_price = float(record.vx2_price)
        fields = ratio_fields(vx1_price, vx2_price)
        rows.append(
            {
                "date": pd.Timestamp(record.date).date().isoformat(),
                "vx1_symbol": vx1_symbol,
                "vx2_symbol": vx2_symbol,
                "vx1_expiry": vx1_expiry,
                "vx2_expiry": vx2_expiry,
                "vx1_price": vx1_price,
                "vx2_price": vx2_price,
                "ratio": fields["ratio"],
                "contango_pct": fields["contango_pct"],
                "backwardation_pct": fields["backwardation_pct"],
                "status": fields["status"],
                "updated_at": updated_at,
            }
        )
    return rows


def read_observed_daily(
    *,
    lookback_days: int = OBSERVED_LOOKBACK_DAYS,
    today: date | None = None,
    db_path: Path | str = DB_PATH,
) -> pd.DataFrame:
    cutoff = (today or date.today()) - timedelta(days=lookback_days)
    with closing(_connect(db_path)) as connection:
        history = pd.read_sql_query(
            "SELECT * FROM vx_ratio_observed_daily WHERE date >= ? ORDER BY date ASC",
            connection,
            params=(cutoff.isoformat(),),
        )
    return _clean_history(history, OBSERVED_COLUMNS)


def read_contract_pair_daily(
    vx1_symbol: str,
    vx2_symbol: str,
    *,
    lookback_days: int = PAIR_LOOKBACK_DAYS,
    today: date | None = None,
    db_path: Path | str = DB_PATH,
) -> pd.DataFrame:
    cutoff = (today or date.today()) - timedelta(days=lookback_days)
    with closing(_connect(db_path)) as connection:
        history = pd.read_sql_query(
            "SELECT * FROM vx_ratio_contract_pair_daily "
            "WHERE vx1_symbol = ? AND vx2_symbol = ? AND date >= ? ORDER BY date ASC",
            connection,
            params=(vx1_symbol, vx2_symbol, cutoff.isoformat()),
        )
    return _clean_history(history, PAIR_COLUMNS)


def _clean_history(history: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=columns)
    history["date"] = pd.to_datetime(history["date"], errors="coerce")
    for column in ("vx1_price", "vx2_price", "ratio", "contango_pct", "backwardation_pct"):
        if column in history.columns:
            history[column] = pd.to_numeric(history[column], errors="coerce")
    return history.dropna(subset=["date", "ratio"]).reset_index(drop=True)
