from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session, sessionmaker

from .adapters import upsert_dashboard_zone
from .repository import create_session_factory
from .service import ZoneSnapshotInput, record_zone_snapshot


@dataclass(frozen=True, slots=True)
class DashboardZonePersistenceResult:
    zone_count: int
    snapshot_count: int


def persist_dashboard_zones(
    *,
    symbol: str,
    replay_date,
    current_price: float,
    atr_value: float | None,
    support_zones: list[dict],
    resistance_zones: list[dict],
    database_url: str | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> DashboardZonePersistenceResult:
    zones = [*support_zones, *resistance_zones]
    if not zones:
        return DashboardZonePersistenceResult(zone_count=0, snapshot_count=0)

    session_factory = session_factory or create_session_factory(database_url)
    snapshot_ts = pd.Timestamp(replay_date).to_pydatetime().replace(tzinfo=None)
    normalized_atr = _normalize_atr(atr_value)

    with session_factory() as session:
        persisted_zones = []
        for zone in zones:
            for component in zone.get("source_components") or []:
                if component.get("zone_id") and component.get("zone_id") != zone.get("zone_id"):
                    upsert_dashboard_zone(
                        session,
                        symbol=symbol,
                        zone=component,
                        observed_ts=snapshot_ts,
                    )
            persisted_zones.append(
                upsert_dashboard_zone(
                    session,
                    symbol=symbol,
                    zone=zone,
                    observed_ts=snapshot_ts,
                )
            )
        snapshots = [
            record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=zone.zone_id,
                    snapshot_ts=snapshot_ts,
                    current_price=current_price,
                    atr=normalized_atr,
                ),
            )
            for zone in persisted_zones
        ]
        session.commit()
        return DashboardZonePersistenceResult(
            zone_count=len(persisted_zones),
            snapshot_count=len(snapshots),
        )


def persist_dashboard_zones_safely(**kwargs) -> DashboardZonePersistenceResult | None:
    try:
        return persist_dashboard_zones(**kwargs)
    except Exception:
        return None


def sqlite_database_url(path: str | Path) -> str:
    return f"sqlite:///{Path(path).as_posix()}"


def _normalize_atr(value: float | None) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    if not np.isfinite(numeric) or numeric <= 0:
        return None
    return numeric
