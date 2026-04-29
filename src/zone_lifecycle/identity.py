from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from typing import Iterable

import pandas as pd

from .constants import ZoneKind


@dataclass(frozen=True, slots=True)
class ZoneIdentityInput:
    symbol: str
    timeframe: str
    zone_kind: str
    source: tuple[str, ...]
    price_low: float | None = None
    price_high: float | None = None
    origin_bar: datetime | pd.Timestamp | str | None = None
    origin_event_id: str | None = None
    vp_window_type: str | None = None
    merged_from_zone_ids: tuple[str, ...] | None = None


def generate_zone_id(identity: ZoneIdentityInput) -> str:
    zone_kind = identity.zone_kind.lower().strip()
    if zone_kind == ZoneKind.EVENT:
        payload = {
            "symbol": _normalize_symbol(identity.symbol),
            "timeframe": _normalize_timeframe(identity.timeframe),
            "zone_kind": ZoneKind.EVENT,
            "source": _normalize_string_list(identity.source),
            "origin_bar": _normalize_timestamp(identity.origin_bar),
            "origin_event_id": identity.origin_event_id or "",
            "price_low": _round_price(identity.price_low),
            "price_high": _round_price(identity.price_high),
        }
    elif zone_kind == ZoneKind.AVWAP:
        payload = {
            "symbol": _normalize_symbol(identity.symbol),
            "timeframe": _normalize_timeframe(identity.timeframe),
            "zone_kind": ZoneKind.AVWAP,
            "source": _normalize_string_list(identity.source),
            "anchor_start": _normalize_timestamp(identity.origin_bar),
            "anchor_id": identity.origin_event_id or "",
        }
    elif zone_kind == ZoneKind.VP:
        payload = {
            "symbol": _normalize_symbol(identity.symbol),
            "timeframe": _normalize_timeframe(identity.timeframe),
            "zone_kind": ZoneKind.VP,
            "vp_window_type": identity.vp_window_type or "",
            "source": _normalize_string_list(identity.source),
        }
    elif zone_kind == ZoneKind.COMPOSITE:
        payload = {
            "symbol": _normalize_symbol(identity.symbol),
            "timeframe": _normalize_timeframe(identity.timeframe),
            "zone_kind": ZoneKind.COMPOSITE,
            "merged_from_zone_ids": _normalize_string_list(identity.merged_from_zone_ids or ()),
        }
    else:
        raise ValueError(f"Unsupported zone_kind: {identity.zone_kind}")

    return _hash_payload(payload)


def infer_zone_kind(source: Iterable[str], merged_from_zone_ids: Iterable[str] | None = None) -> str:
    merged_from = tuple(merged_from_zone_ids or ())
    if len(merged_from) > 1:
        return ZoneKind.COMPOSITE

    normalized_sources = _normalize_string_list(source)
    if len(normalized_sources) > 1:
        return ZoneKind.COMPOSITE
    if normalized_sources and normalized_sources[0].startswith("avwap"):
        return ZoneKind.AVWAP
    if normalized_sources and normalized_sources[0].startswith("vp"):
        return ZoneKind.VP
    return ZoneKind.EVENT


def _hash_payload(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"zone_{digest}"


def _normalize_string_list(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip().lower() for value in values if str(value).strip()}))


def _normalize_symbol(value: str) -> str:
    return str(value).strip().upper()


def _normalize_timeframe(value: str) -> str:
    return str(value).strip().lower()


def _normalize_timestamp(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def _round_price(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 4)
