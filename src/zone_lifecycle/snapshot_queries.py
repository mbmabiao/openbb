from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from config.warmup_config import WarmupThresholdConfig, load_warmup_config
from .constants import ACTIVE_ZONE_STATUSES, DEPRECATED_ZONE_SOURCES, ZONE_STATUS_RANK, ZoneKind, ZoneRole
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
    warmup_config: WarmupThresholdConfig | None = None,
) -> ReplayZoneSnapshotResult:
    warmup_config = warmup_config or load_warmup_config()
    replay_ts = pd.Timestamp(replay_date).normalize().to_pydatetime()
    rows = session.execute(
        select(ZoneDailySnapshot, Zone)
        .join(Zone, Zone.zone_id == ZoneDailySnapshot.zone_id)
        .where(ZoneDailySnapshot.symbol == str(symbol).strip().upper())
        .where(ZoneDailySnapshot.snapshot_ts == replay_ts)
        .where(ZoneDailySnapshot.zone_status.in_(ACTIVE_ZONE_STATUSES))
    ).all()

    zones = [
        _snapshot_to_dashboard_zone(snapshot=snapshot, zone=zone)
        for snapshot, zone in rows
        if not _has_deprecated_source(zone)
        and not _is_expired_at_snapshot(
            snapshot,
            zone,
            expiration_days=warmup_config.lifecycle.weekly_swing_expiration_days,
        )
    ]
    zones.sort(key=_sort_key)

    zones = [_with_price_relative_role(zone) for zone in zones]
    resistance = [zone for zone in zones if zone.get("side") == ZoneRole.RESISTANCE]
    support = [zone for zone in zones if zone.get("side") == ZoneRole.SUPPORT]
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
        "zone_strength_pct": float(snapshot.zone_strength_pct or 0.0),
        "source_types": source_types,
        "source_types_label": source_types_label,
        "timeframe_sources": zone.timeframe,
        "timeframes": set(str(zone.timeframe).split(",")),
        "center_volume": 0.0,
        "touch_count": zone.touch_count,
        "close_inside_count": zone.close_inside_count,
        "break_count": zone.break_count,
        "confirmed_breakout_count": zone.confirmed_breakout_count,
        "retest_num": zone.retest_num,
    }


def _with_price_relative_role(zone: dict) -> dict:
    zone_copy = zone.copy()
    side = ZoneRole.RESISTANCE if float(zone_copy["center"]) >= float(zone_copy["current_price"]) else ZoneRole.SUPPORT
    zone_copy["side"] = side
    zone_copy["current_role"] = side
    return zone_copy


def _has_deprecated_source(zone: Zone) -> bool:
    if zone.zone_kind == ZoneKind.COMPOSITE:
        return True
    sources = {str(source).strip().lower() for source in zone.source or []}
    if "swing_w" not in sources:
        return True
    return bool(sources & DEPRECATED_ZONE_SOURCES) or any(
        source.startswith("avwap_") or source.startswith("vp_")
        for source in sources
    )


def _is_expired_at_snapshot(snapshot: ZoneDailySnapshot, zone: Zone, *, expiration_days: int) -> bool:
    sources = {str(source).strip().lower() for source in zone.source or []}
    if "swing_w" not in sources or zone.origin_bar is None:
        return False
    age = pd.Timestamp(snapshot.snapshot_ts) - pd.Timestamp(zone.origin_bar)
    return age >= pd.Timedelta(days=max(int(expiration_days), 0))


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
