from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine
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
