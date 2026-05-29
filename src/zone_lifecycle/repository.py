from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from .models import Base


DEFAULT_DB_PATH = Path("outputs") / "zone_lifecycle.sqlite"


def create_session_factory(database_url: str | None = None) -> sessionmaker[Session]:
    engine = create_zone_engine(database_url)
    init_db(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def create_zone_engine(database_url: str | None = None) -> Engine:
    url = database_url or f"sqlite:///{DEFAULT_DB_PATH.as_posix()}"
    if url.startswith("sqlite:///"):
        db_path = Path(url.removeprefix("sqlite:///"))
        if db_path != Path(":memory:"):
            db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(url, future=True)


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)
    _ensure_schema_columns(engine)


def _ensure_schema_columns(engine: Engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "zones" in table_names:
        _add_column_if_missing(engine, "zones", "zone_strength_pct", "FLOAT NOT NULL DEFAULT 0.0")
    if "zone_daily_snapshots" in table_names:
        _add_column_if_missing(engine, "zone_daily_snapshots", "zone_strength_pct", "FLOAT NOT NULL DEFAULT 0.0")


def _add_column_if_missing(engine: Engine, table_name: str, column_name: str, ddl: str) -> None:
    columns = {column["name"] for column in inspect(engine).get_columns(table_name)}
    if column_name in columns:
        return
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}"))
