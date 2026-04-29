from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from .constants import ACTIVE_ZONE_STATUSES, ZONE_STATUS_RANK, ZoneRole
from .models import Zone, ZoneDailySnapshot


@dataclass(frozen=True, slots=True)
class ReplayZoneSnapshotResult:
    support_zones: list[dict]
    resistance_zones: list[dict]
    all_zones: list[dict]


def load_replay_zone_snapshots(
    session: Session,
    *,
    symbol: str,
    replay_date,
    max_distance_atr: float = 3.0,
    max_support_zones: int | None = None,
    max_resistance_zones: int | None = None,
) -> ReplayZoneSnapshotResult:
    replay_ts = pd.Timestamp(replay_date).normalize().to_pydatetime()
    rows = session.execute(
        select(ZoneDailySnapshot, Zone)
        .join(Zone, Zone.zone_id == ZoneDailySnapshot.zone_id)
        .where(ZoneDailySnapshot.symbol == str(symbol).strip().upper())
        .where(ZoneDailySnapshot.snapshot_ts == replay_ts)
    ).all()

    zones = [
        _snapshot_to_dashboard_zone(snapshot=snapshot, zone=zone)
        for snapshot, zone in rows
        if _within_distance(snapshot, max_distance_atr)
    ]
    zones.sort(key=_sort_key)

    active_zones = [zone for zone in zones if zone.get("zone_status") in ACTIVE_ZONE_STATUSES]
    resistance = [zone for zone in active_zones if zone.get("side") == ZoneRole.RESISTANCE]
    support = [zone for zone in active_zones if zone.get("side") == ZoneRole.SUPPORT]
    if max_resistance_zones is not None:
        resistance = resistance[: int(max_resistance_zones)]
    if max_support_zones is not None:
        support = support[: int(max_support_zones)]

    return ReplayZoneSnapshotResult(
        support_zones=_assign_display_labels(support, "S"),
        resistance_zones=_assign_display_labels(resistance, "R"),
        all_zones=zones,
    )


def _snapshot_to_dashboard_zone(*, snapshot: ZoneDailySnapshot, zone: Zone) -> dict:
    source_types = set(zone.source or [])
    source_types_label = zone.metadata_json.get("source_types_label") if zone.metadata_json else ""
    if not source_types_label:
        source_types_label = ",".join(sorted(source.upper() for source in source_types))

    distance_atr = snapshot.distance_atr
    return {
        "zone_id": zone.zone_id,
        "zone_kind": zone.zone_kind,
        "type": zone.metadata_json.get("dashboard_type", zone.zone_kind) if zone.metadata_json else zone.zone_kind,
        "side": snapshot.current_role,
        "lower": float(snapshot.price_low),
        "upper": float(snapshot.price_high),
        "center": float(snapshot.price_center),
        "current_price": float(snapshot.current_price),
        "distance_to_price": float(snapshot.distance_to_price),
        "distance_atr": float(distance_atr) if distance_atr is not None else math.inf,
        "distance_pct": float(snapshot.distance_to_price) / max(float(snapshot.current_price), 1e-9),
        "zone_status": snapshot.zone_status,
        "current_role": snapshot.current_role,
        "source_types": source_types,
        "source_types_label": source_types_label,
        "timeframe_sources": zone.timeframe,
        "timeframes": set(str(zone.timeframe).split(",")),
        "confluence_count": len(zone.merged_from_zone_ids or []) or len(source_types) or 1,
        "vp_volume": 0.0,
        "anchor_count": 0,
        "avwap_strength": 0.0,
        "touch_count": zone.touch_count,
        "close_inside_count": zone.close_inside_count,
        "break_count": zone.break_count,
        "false_break_count": zone.false_break_count,
        "confirmed_breakout_count": zone.confirmed_breakout_count,
        "failed_breakout_count": zone.failed_breakout_count,
        "retest_num": zone.retest_num,
        "institutional_score": 0.0,
    }


def _within_distance(snapshot: ZoneDailySnapshot, max_distance_atr: float) -> bool:
    if snapshot.distance_atr is None:
        return True
    return float(snapshot.distance_atr) <= float(max_distance_atr)


def _sort_key(zone: dict) -> tuple[float, int, str]:
    return (
        float(zone.get("distance_atr", math.inf)),
        ZONE_STATUS_RANK.get(str(zone.get("zone_status")), 99),
        str(zone.get("zone_id", "")),
    )


def _assign_display_labels(zones: list[dict], prefix: str) -> list[dict]:
    output: list[dict] = []
    for index, zone in enumerate(zones, start=1):
        zone_copy = zone.copy()
        zone_copy["display_label"] = f"{prefix}{index}"
        output.append(zone_copy)
    return output
