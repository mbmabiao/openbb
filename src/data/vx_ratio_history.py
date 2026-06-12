from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VX_RATIO_DB = PROJECT_ROOT / "vix_dashboard.db"

SNAPSHOT_COLUMNS = [
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

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vx_ratio_snapshots (
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


def _connect(db_path: Path | str = DEFAULT_VX_RATIO_DB) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.execute(_CREATE_TABLE_SQL)
    return connection


def has_vx_ratio_snapshot(snapshot_date: date | str, *, db_path: Path | str = DEFAULT_VX_RATIO_DB) -> bool:
    """Return True if a snapshot already exists for the given date."""
    key = snapshot_date.isoformat() if isinstance(snapshot_date, date) else str(snapshot_date)
    with _connect(db_path) as connection:
        cursor = connection.execute("SELECT 1 FROM vx_ratio_snapshots WHERE date = ?", (key,))
        return cursor.fetchone() is not None


def write_vx_ratio_snapshot(snapshot: dict[str, Any], *, db_path: Path | str = DEFAULT_VX_RATIO_DB) -> None:
    """Insert today's snapshot. Existing rows for the same date are left untouched."""
    values = [snapshot.get(column) for column in SNAPSHOT_COLUMNS]
    placeholders = ", ".join(["?"] * len(SNAPSHOT_COLUMNS))
    columns = ", ".join(SNAPSHOT_COLUMNS)
    with _connect(db_path) as connection:
        connection.execute(
            f"INSERT OR IGNORE INTO vx_ratio_snapshots ({columns}) VALUES ({placeholders})",
            values,
        )
        connection.commit()


def read_vx_ratio_history(*, db_path: Path | str = DEFAULT_VX_RATIO_DB) -> pd.DataFrame:
    """Read every recorded VX1/VX2 ratio snapshot, ordered by date ascending."""
    with _connect(db_path) as connection:
        history = pd.read_sql_query(
            "SELECT * FROM vx_ratio_snapshots ORDER BY date ASC",
            connection,
        )
    if history.empty:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)
    history["date"] = pd.to_datetime(history["date"], errors="coerce")
    for column in ("vx1_price", "vx2_price", "ratio", "contango_pct", "backwardation_pct"):
        history[column] = pd.to_numeric(history[column], errors="coerce")
    return history.dropna(subset=["date", "ratio"]).reset_index(drop=True)


def make_snapshot_timestamp(now: datetime | None = None) -> str:
    return (now or datetime.now()).isoformat(timespec="seconds")
