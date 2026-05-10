from __future__ import annotations

import datetime as dt
from pathlib import Path
import sys
import unittest

import pandas as pd
from sqlalchemy import func, select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from zone_lifecycle.breakout_state_machine import process_zone_bar
from zone_lifecycle.constants import BreakoutEventStatus, ZoneKind, ZoneRole, ZoneStatus
from zone_lifecycle.dashboard_persistence import persist_dashboard_zones
from zone_lifecycle.lifecycle import BarInput, apply_composite_lifecycle, expire_event_zones, update_zone_interaction_counts
from zone_lifecycle.models import BreakoutEvent, SymbolLifecycleState, Zone, ZoneDailySnapshot
from zone_lifecycle.offline_snapshots import reset_symbol_lifecycle_data
from zone_lifecycle.repository import create_session_factory
from zone_lifecycle.service import ZoneSnapshotInput, record_zone_snapshot, upsert_zone
from zone_lifecycle.snapshot_queries import load_replay_zone_snapshots
from zone_lifecycle.warmup import ensure_symbol_lifecycle_ready
from engines.zone_generation import (
    ZoneGenerationConfig,
    generate_zones_for_replay,
    make_preloaded_interval_history_loader,
    make_preloaded_zone_provider,
    make_replay_zone_provider,
)
from engines.validation_engine import rank_zones_for_side
from features.boundaries import create_candidate_zones_from_avwap, merge_close_zones
from features.volume_profile import build_avwap_features, find_anchor_points


class ZoneLifecyclePhaseOneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.Session = create_session_factory("sqlite:///:memory:")

    def test_event_zone_upsert_preserves_identity_and_lifecycle_state(self) -> None:
        with self.Session() as session:
            first = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role="resistance",
                origin_bar=dt.datetime(2026, 1, 5),
                origin_event_id="swing-high-2026-01-05",
                origin_event_type="swing_high",
                observed_ts=dt.datetime(2026, 1, 6),
            )
            first.status = ZoneStatus.FLIPPED
            first.touch_count = 3
            first_zone_id = first.zone_id
            first_created_ts = first.created_ts

            second = upsert_zone(
                session,
                symbol="aapl",
                timeframe="1D",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role="support",
                origin_bar=dt.datetime(2026, 1, 5),
                origin_event_id="swing-high-2026-01-05",
                origin_event_type="swing_high",
                observed_ts=dt.datetime(2026, 1, 7),
            )

            self.assertEqual(second.zone_id, first_zone_id)
            self.assertEqual(second.status, ZoneStatus.FLIPPED)
            self.assertEqual(second.touch_count, 3)
            self.assertEqual(second.created_ts, first_created_ts)
            self.assertEqual(second.updated_ts, dt.datetime(2026, 1, 7))
            self.assertEqual(second.price_low, 100.0)
            self.assertEqual(second.price_high, 102.0)
            self.assertEqual(second.current_role, "support")

            zone_count = session.scalar(select(func.count()).select_from(Zone))
            self.assertEqual(zone_count, 1)

    def test_new_event_origin_creates_new_zone(self) -> None:
        with self.Session() as session:
            first = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role="resistance",
                origin_bar=dt.datetime(2026, 1, 5),
                origin_event_id="event-1",
            )
            second = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role="resistance",
                origin_bar=dt.datetime(2026, 1, 6),
                origin_event_id="event-2",
            )

            self.assertNotEqual(first.zone_id, second.zone_id)
            zone_count = session.scalar(select(func.count()).select_from(Zone))
            self.assertEqual(zone_count, 2)

    def test_vp_zone_identity_distinguishes_high_volume_nodes(self) -> None:
        with self.Session() as session:
            first = upsert_zone(
                session,
                symbol="MSFT",
                timeframe="short",
                zone_kind=ZoneKind.VP,
                source=["vp_short"],
                price_low=300.0,
                price_high=304.0,
                current_role="support",
                vp_window_type="short_21d",
            )
            second = upsert_zone(
                session,
                symbol="MSFT",
                timeframe="short",
                zone_kind=ZoneKind.VP,
                source=["vp_short"],
                price_low=312.0,
                price_high=316.0,
                current_role="support",
                vp_window_type="short_21d",
            )

            self.assertNotEqual(first.zone_id, second.zone_id)
            zone_count = session.scalar(select(func.count()).select_from(Zone))
            self.assertEqual(zone_count, 2)

    def test_vp_zone_identity_uses_structure_key_not_daily_bounds(self) -> None:
        with self.Session() as session:
            first = upsert_zone(
                session,
                symbol="MSFT",
                timeframe="short",
                zone_kind=ZoneKind.VP,
                source=["vp_short"],
                price_low=300.0,
                price_high=304.0,
                current_role="support",
                vp_window_type="short_21d",
                vp_structure_key="short_21d:bucket_1144",
            )
            second = upsert_zone(
                session,
                symbol="MSFT",
                timeframe="short",
                zone_kind=ZoneKind.VP,
                source=["vp_short"],
                price_low=301.0,
                price_high=305.0,
                current_role="support",
                vp_window_type="short_21d",
                vp_structure_key="short_21d:bucket_1144",
            )

            self.assertEqual(second.zone_id, first.zone_id)
            self.assertEqual(second.price_low, 301.0)
            self.assertEqual(second.price_high, 305.0)
            zone_count = session.scalar(select(func.count()).select_from(Zone))
            self.assertEqual(zone_count, 1)

    def test_avwap_zone_keeps_anchor_identity_while_price_window_drifts(self) -> None:
        anchor_meta = {
            "avwap_short_rolling_21_high": {
                "anchor_name": "rolling_21_high",
                "anchor_family": "rolling",
                "start_date": dt.datetime(2026, 1, 5),
                "timeframe": "short",
            }
        }
        first_df = pd.DataFrame(
            [
                {"date": dt.datetime(2026, 1, 5), "close": 100.0, "avwap_short_rolling_21_high": 101.0},
                {"date": dt.datetime(2026, 1, 6), "close": 102.0, "avwap_short_rolling_21_high": 102.5},
            ]
        )
        second_df = pd.DataFrame(
            [
                {"date": dt.datetime(2026, 1, 5), "close": 100.0, "avwap_short_rolling_21_high": 101.0},
                {"date": dt.datetime(2026, 1, 7), "close": 104.0, "avwap_short_rolling_21_high": 103.5},
            ]
        )

        first = create_candidate_zones_from_avwap(
            first_df,
            anchor_meta=anchor_meta,
            zone_expand_pct=0.01,
            symbol="AAPL",
        )[0]
        second = create_candidate_zones_from_avwap(
            second_df,
            anchor_meta=anchor_meta,
            zone_expand_pct=0.01,
            symbol="AAPL",
        )[0]

        self.assertEqual(first["zone_kind"], ZoneKind.AVWAP)
        self.assertEqual(second["zone_kind"], ZoneKind.AVWAP)
        self.assertEqual(first["zone_id"], second["zone_id"])
        self.assertNotEqual(first["lower"], second["lower"])
        self.assertNotEqual(first["upper"], second["upper"])

    def test_avwap_identity_uses_anchor_date_not_intraday_time_or_price(self) -> None:
        with self.Session() as session:
            first = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="short",
                zone_kind=ZoneKind.AVWAP,
                source=["avwap_short_rolling"],
                price_low=100.0,
                price_high=102.0,
                current_role="support",
                origin_bar=dt.datetime(2026, 1, 5, 10, 30),
                origin_event_id="rolling_21_high",
            )
            second = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="short",
                zone_kind=ZoneKind.AVWAP,
                source=["avwap_short_rolling"],
                price_low=104.0,
                price_high=106.0,
                current_role="support",
                origin_bar=dt.datetime(2026, 1, 5, 15, 45),
                origin_event_id="rolling_21_high",
            )

            self.assertEqual(second.zone_id, first.zone_id)
            self.assertEqual(second.price_low, 104.0)
            self.assertEqual(second.price_high, 106.0)
            zone_count = session.scalar(select(func.count()).select_from(Zone))
            self.assertEqual(zone_count, 1)

    def test_reset_symbol_lifecycle_data_deletes_only_requested_symbol(self) -> None:
        with self.Session() as session:
            aapl_zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="short",
                zone_kind=ZoneKind.AVWAP,
                source=["avwap_short_rolling"],
                price_low=100.0,
                price_high=102.0,
                current_role="support",
                origin_bar=dt.datetime(2026, 1, 5),
                origin_event_id="rolling_21_high",
            )
            msft_zone = upsert_zone(
                session,
                symbol="MSFT",
                timeframe="short",
                zone_kind=ZoneKind.AVWAP,
                source=["avwap_short_rolling"],
                price_low=200.0,
                price_high=202.0,
                current_role="support",
                origin_bar=dt.datetime(2026, 1, 5),
                origin_event_id="rolling_21_high",
            )
            record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=aapl_zone.zone_id,
                    snapshot_ts=dt.datetime(2026, 1, 6),
                    current_price=101.0,
                    atr=2.0,
                ),
            )
            record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=msft_zone.zone_id,
                    snapshot_ts=dt.datetime(2026, 1, 6),
                    current_price=201.0,
                    atr=2.0,
                ),
            )
            session.add(
                SymbolLifecycleState(
                    state_id="state_aapl_1d",
                    symbol="AAPL",
                    timeframe="1d",
                    warmup_start_ts=dt.datetime(2025, 1, 1),
                    last_processed_ts=dt.datetime(2026, 1, 6),
                    lookback_years=1,
                    created_ts=dt.datetime(2026, 1, 6),
                    updated_ts=dt.datetime(2026, 1, 6),
                    metadata_json={},
                )
            )
            session.add(
                SymbolLifecycleState(
                    state_id="state_msft_1d",
                    symbol="MSFT",
                    timeframe="1d",
                    warmup_start_ts=dt.datetime(2025, 1, 1),
                    last_processed_ts=dt.datetime(2026, 1, 6),
                    lookback_years=1,
                    created_ts=dt.datetime(2026, 1, 6),
                    updated_ts=dt.datetime(2026, 1, 6),
                    metadata_json={},
                )
            )
            session.flush()

            reset_symbol_lifecycle_data(session, "aapl")
            session.flush()

            self.assertEqual(session.scalar(select(func.count()).select_from(Zone).where(Zone.symbol == "AAPL")), 0)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ZoneDailySnapshot).where(ZoneDailySnapshot.symbol == "AAPL")),
                0,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(SymbolLifecycleState).where(SymbolLifecycleState.symbol == "AAPL")
                ),
                0,
            )
            self.assertEqual(session.scalar(select(func.count()).select_from(Zone).where(Zone.symbol == "MSFT")), 1)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(ZoneDailySnapshot).where(ZoneDailySnapshot.symbol == "MSFT")),
                1,
            )

    def test_snapshot_records_distance_and_updates_same_day(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="TSLA",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_low"],
                price_low=200.0,
                price_high=210.0,
                current_role="support",
                origin_bar=dt.datetime(2026, 2, 1),
                origin_event_id="low-2026-02-01",
            )
            snapshot = record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=zone.zone_id,
                    snapshot_ts=dt.datetime(2026, 2, 10),
                    current_price=220.0,
                    atr=5.0,
                ),
            )
            self.assertEqual(snapshot.distance_to_price, 10.0)
            self.assertEqual(snapshot.distance_atr, 2.0)

            updated = record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=zone.zone_id,
                    snapshot_ts=dt.datetime(2026, 2, 10),
                    current_price=205.0,
                    atr=5.0,
                ),
            )
            self.assertEqual(updated.snapshot_id, snapshot.snapshot_id)
            self.assertEqual(updated.distance_to_price, 0.0)
            snapshot_count = session.scalar(select(func.count()).select_from(ZoneDailySnapshot))
            self.assertEqual(snapshot_count, 1)

    def test_replay_zone_snapshots_are_read_from_database(self) -> None:
        with self.Session() as session:
            support = upsert_zone(
                session,
                symbol="TSLA",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_low"],
                price_low=200.0,
                price_high=210.0,
                current_role="support",
                origin_bar=dt.datetime(2026, 2, 1),
                origin_event_id="support-2026-02-01",
            )
            expired = upsert_zone(
                session,
                symbol="TSLA",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=260.0,
                price_high=270.0,
                current_role="resistance",
                origin_bar=dt.datetime(2026, 2, 1),
                origin_event_id="expired-2026-02-01",
            )
            expired.status = ZoneStatus.EXPIRED
            record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=support.zone_id,
                    snapshot_ts=dt.datetime(2026, 2, 10),
                    current_price=215.0,
                    atr=5.0,
                ),
            )
            record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=expired.zone_id,
                    snapshot_ts=dt.datetime(2026, 2, 10),
                    current_price=215.0,
                    atr=5.0,
                ),
            )

            result = load_replay_zone_snapshots(
                session,
                symbol="tsla",
                replay_date=dt.datetime(2026, 2, 10),
                max_support_zones=3,
                max_resistance_zones=3,
            )

        self.assertEqual(len(result.support_zones), 1)
        self.assertEqual(result.support_zones[0]["zone_id"], support.zone_id)
        self.assertEqual(result.support_zones[0]["display_label"], "S1")
        self.assertEqual(result.resistance_zones, [])
        self.assertEqual([zone["zone_id"] for zone in result.all_zones], [support.zone_id])

    def test_dashboard_shadow_persistence_is_idempotent(self) -> None:
        support_zone = self._build_composite_dashboard_zone()

        first = persist_dashboard_zones(
            symbol="AAPL",
            replay_date=dt.datetime(2026, 3, 1),
            current_price=105.0,
            atr_value=2.0,
            support_zones=[support_zone],
            resistance_zones=[],
            session_factory=self.Session,
        )
        second = persist_dashboard_zones(
            symbol="AAPL",
            replay_date=dt.datetime(2026, 3, 1),
            current_price=99.0,
            atr_value=2.0,
            support_zones=[support_zone],
            resistance_zones=[],
            session_factory=self.Session,
        )

        self.assertEqual(first.zone_count, 1)
        self.assertEqual(first.snapshot_count, 1)
        self.assertEqual(second.zone_count, 1)
        self.assertEqual(second.snapshot_count, 1)

        with self.Session() as session:
            zone_count = session.scalar(select(func.count()).select_from(Zone))
            snapshot_count = session.scalar(select(func.count()).select_from(ZoneDailySnapshot))
            snapshot = session.scalars(select(ZoneDailySnapshot)).one()

        self.assertEqual(zone_count, 3)
        self.assertEqual(snapshot_count, 1)
        self.assertEqual(snapshot.current_price, 99.0)
        self.assertEqual(snapshot.distance_to_price, 0.0)

    def test_merge_tracks_component_zone_ids_for_composite_identity(self) -> None:
        merged = self._build_composite_dashboard_zone()

        self.assertEqual(merged["zone_kind"], ZoneKind.COMPOSITE)
        self.assertIn("zone_id", merged)
        self.assertEqual(len(merged["merged_from_zone_ids"]), 2)
        self.assertEqual(len(merged["source_components"]), 2)
        self.assertNotEqual(merged["zone_id"], merged["merged_from_zone_ids"][0])

        reversed_merge = merge_close_zones(
            list(reversed(self._source_component_zones())),
            merge_pct=0.10,
            symbol="AAPL",
        )[0]
        self.assertEqual(reversed_merge["zone_id"], merged["zone_id"])

    def test_existing_composite_id_survives_new_component_merge(self) -> None:
        base_composite = self._build_composite_dashboard_zone()
        new_component = {
            "zone_id": "zone_component_event",
            "zone_kind": ZoneKind.EVENT,
            "type": "event_support_short",
            "side": "support",
            "lower": 100.0,
            "upper": 102.0,
            "center": 101.0,
            "vp_volume": 0.0,
            "anchor_count": 1,
            "avwap_strength": 0.0,
            "timeframes": {"short"},
            "source_types": {"gap_up"},
            "source_label": "Gap up",
            "source_zone_ids": ["zone_component_event"],
            "primary_timeframe": "short",
        }

        expanded = merge_close_zones(
            [base_composite, new_component],
            merge_pct=0.10,
            symbol="AAPL",
        )[0]

        self.assertEqual(expanded["zone_id"], base_composite["zone_id"])
        self.assertEqual(
            sorted(expanded["merged_from_zone_ids"]),
            ["zone_component_avwap", "zone_component_event", "zone_component_vp"],
        )

    def test_composite_upsert_reuses_existing_id_by_price_iou_across_role_flip(self) -> None:
        with self.Session() as session:
            initial = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="short",
                zone_kind=ZoneKind.COMPOSITE,
                source=["avwap_short_rolling", "vp_short"],
                price_low=100.0,
                price_high=110.0,
                current_role=ZoneRole.RESISTANCE,
                merged_from_zone_ids=["source-a", "source-b"],
            )
            initial_id = initial.zone_id

            flipped = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="short,w",
                zone_kind=ZoneKind.COMPOSITE,
                source=["avwap_short_rolling", "gap_up", "vp_short"],
                price_low=101.0,
                price_high=110.0,
                current_role=ZoneRole.SUPPORT,
                merged_from_zone_ids=["source-a", "source-b", "source-c"],
            )
            zone_count = session.scalar(select(func.count()).select_from(Zone))

        self.assertEqual(flipped.zone_id, initial_id)
        self.assertEqual(flipped.current_role, ZoneRole.SUPPORT)
        self.assertEqual(flipped.timeframe, "short,w")
        self.assertEqual(zone_count, 1)

    def test_composite_upsert_prefers_iou_match_over_incoming_zone_id(self) -> None:
        with self.Session() as session:
            existing = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="short",
                zone_kind=ZoneKind.COMPOSITE,
                source=["avwap_short_rolling", "vp_short"],
                price_low=100.0,
                price_high=110.0,
                current_role=ZoneRole.RESISTANCE,
                merged_from_zone_ids=["source-a", "source-b"],
            )

            updated = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="short",
                zone_kind=ZoneKind.COMPOSITE,
                zone_id="zone_intraday_merge_candidate",
                source=["gap_up", "vp_short"],
                price_low=101.0,
                price_high=110.0,
                current_role=ZoneRole.SUPPORT,
                merged_from_zone_ids=["source-b", "source-c"],
            )
            zone_count = session.scalar(select(func.count()).select_from(Zone))

        self.assertEqual(updated.zone_id, existing.zone_id)
        self.assertNotEqual(updated.zone_id, "zone_intraday_merge_candidate")
        self.assertEqual(updated.current_role, ZoneRole.SUPPORT)
        self.assertEqual(zone_count, 1)

    def test_dashboard_shadow_persistence_writes_components_and_composite(self) -> None:
        composite = self._build_composite_dashboard_zone()

        persist_dashboard_zones(
            symbol="AAPL",
            replay_date=dt.datetime(2026, 3, 2),
            current_price=105.0,
            atr_value=2.0,
            support_zones=[composite],
            resistance_zones=[],
            session_factory=self.Session,
        )

        with self.Session() as session:
            zones = session.scalars(select(Zone)).all()
            snapshots = session.scalars(select(ZoneDailySnapshot)).all()

        self.assertEqual(len(zones), 3)
        self.assertEqual(len(snapshots), 1)
        persisted_composite = next(zone for zone in zones if zone.zone_kind == ZoneKind.COMPOSITE)
        self.assertEqual(
            sorted(persisted_composite.merged_from_zone_ids),
            sorted(composite["merged_from_zone_ids"]),
        )

    def test_event_ttl_expires_event_but_not_vp_or_invalidated(self) -> None:
        with self.Session() as session:
            event_zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="event-expire",
            )
            vp_zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.VP,
                source=["vp_hvn"],
                price_low=90.0,
                price_high=95.0,
                current_role=ZoneRole.SUPPORT,
                vp_window_type="daily_63d",
            )
            invalidated_zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_low"],
                price_low=80.0,
                price_high=82.0,
                current_role=ZoneRole.SUPPORT,
                origin_bar=dt.datetime(2026, 1, 2),
                origin_event_id="already-invalid",
            )
            invalidated_zone.status = ZoneStatus.INVALIDATED

            count = expire_event_zones(
                session,
                current_ts=dt.datetime(2026, 4, 1),
                bars_since_created_by_zone_id={
                    event_zone.zone_id: 63,
                    vp_zone.zone_id: 63,
                    invalidated_zone.zone_id: 63,
                },
            )

            self.assertEqual(count, 1)
            self.assertEqual(event_zone.status, ZoneStatus.EXPIRED)
            self.assertEqual(vp_zone.status, ZoneStatus.ACTIVE)
            self.assertEqual(invalidated_zone.status, ZoneStatus.INVALIDATED)

    def test_composite_lifecycle_follows_sources(self) -> None:
        with self.Session() as session:
            source_a = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="source-a",
            )
            source_b = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_low"],
                price_low=101.0,
                price_high=103.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 2),
                origin_event_id="source-b",
            )
            composite = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.COMPOSITE,
                source=["swing_high", "swing_low"],
                price_low=100.0,
                price_high=103.0,
                current_role=ZoneRole.RESISTANCE,
                merged_from_zone_ids=[source_a.zone_id, source_b.zone_id],
            )
            source_a.status = ZoneStatus.EXPIRED
            source_b.status = ZoneStatus.EXPIRED

            changed = apply_composite_lifecycle(session, current_ts=dt.datetime(2026, 4, 1))

            self.assertEqual(changed, 1)
            self.assertEqual(composite.status, ZoneStatus.EXPIRED)

    def test_zero_lookback_starts_warmup_at_first_available_bar(self) -> None:
        prices = pd.DataFrame(
            [
                {"date": dt.datetime(2026, 1, 10), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
                {"date": dt.datetime(2026, 1, 11), "open": 101.0, "high": 102.0, "low": 100.0, "close": 101.0},
            ]
        )

        with self.Session() as session:
            result = ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=lambda history, bar: [],
                lookback_years=0,
                as_of_date=dt.datetime(2026, 1, 11),
                snapshot_start_date=dt.datetime(2026, 1, 10),
                snapshot_end_date=dt.datetime(2026, 1, 11),
                force=True,
            )
            state = session.scalars(select(SymbolLifecycleState)).one()

        self.assertEqual(result.warmup_start_ts, dt.datetime(2026, 1, 10))
        self.assertEqual(result.processed_bars, 2)
        self.assertEqual(state.lookback_years, 0)

    def test_interaction_counts_follow_bar_rules(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="count-zone",
            )

            update_zone_interaction_counts(
                zone,
                BarInput(
                    timestamp=dt.datetime(2026, 1, 3),
                    open=101.0,
                    high=103.0,
                    low=99.0,
                    close=101.0,
                    atr=2.0,
                ),
                breakout_buffer=0.2,
            )

            self.assertEqual(zone.close_inside_count, 1)
            self.assertEqual(zone.touch_count, 1)
            self.assertEqual(zone.break_count, 1)
            self.assertEqual(zone.false_break_count, 1)

    def test_breakout_confirmed_flips_zone_and_records_event(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="breakout-zone",
            )

            event = process_zone_bar(
                session,
                zone,
                BarInput(
                    timestamp=dt.datetime(2026, 1, 4),
                    open=101.0,
                    high=104.0,
                    low=100.5,
                    close=102.5,
                    atr=2.0,
                ),
            )

            self.assertIsNotNone(event)
            self.assertEqual(event.status, BreakoutEventStatus.CONFIRMED)
            self.assertEqual(event.direction, "up")
            self.assertEqual(zone.status, ZoneStatus.FLIPPED)
            self.assertEqual(zone.current_role, ZoneRole.SUPPORT)
            self.assertEqual(zone.confirmed_breakout_count, 1)
            self.assertEqual(session.scalar(select(func.count()).select_from(BreakoutEvent)), 1)

    def test_breakout_retest_success_marks_zone_retested(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="retest-zone",
            )
            process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 4), open=101.0, high=104.0, low=100.5, close=102.5, atr=2.0),
            )
            event = process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 6), open=103.0, high=103.5, low=101.5, close=102.8, atr=2.0),
            )

            self.assertEqual(event.status, BreakoutEventStatus.RETEST_SUCCESS)
            self.assertEqual(zone.status, ZoneStatus.RETESTED)
            self.assertEqual(zone.current_role, ZoneRole.SUPPORT)

    def test_breakout_failure_invalidates_zone(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="failure-zone",
            )
            process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 4), open=101.0, high=104.0, low=100.5, close=102.5, atr=2.0),
            )
            event = process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 6), open=101.0, high=101.5, low=98.5, close=99.7, atr=2.0),
            )

            self.assertEqual(event.status, BreakoutEventStatus.FAILED_BREAKOUT)
            self.assertEqual(zone.status, ZoneStatus.INVALIDATED)
            self.assertIsNotNone(zone.invalidated_ts)

    def test_symbol_lifecycle_warmup_processes_two_year_batch_once(self) -> None:
        prices = self._warmup_prices()

        with self.Session() as session:
            first = ensure_symbol_lifecycle_ready(
                session,
                symbol="aapl",
                price_df=prices,
                zone_provider=self._warmup_zone_provider,
                as_of_date=dt.datetime(2026, 1, 5),
            )
            second = ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=self._warmup_zone_provider,
                as_of_date=dt.datetime(2026, 1, 5),
            )

            state = session.scalars(select(SymbolLifecycleState)).one()
            zone_count = session.scalar(select(func.count()).select_from(Zone))
            snapshot_count = session.scalar(select(func.count()).select_from(ZoneDailySnapshot))
            event_count = session.scalar(select(func.count()).select_from(BreakoutEvent))

        self.assertEqual(first.processed_bars, 5)
        self.assertEqual(first.upserted_zones, 1)
        self.assertGreater(first.zone_bar_updates, 0)
        self.assertGreater(event_count, 0)
        self.assertEqual(second.processed_bars, 0)
        self.assertEqual(zone_count, 1)
        self.assertEqual(snapshot_count, 5)
        self.assertEqual(state.symbol, "AAPL")
        self.assertEqual(state.timeframe, "1d")
        self.assertEqual(state.last_processed_ts, dt.datetime(2026, 1, 5))

    def test_symbol_lifecycle_warmup_incrementally_processes_new_bars(self) -> None:
        prices = self._warmup_prices()

        with self.Session() as session:
            first = ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices.iloc[:3],
                zone_provider=self._warmup_zone_provider,
            )
            second = ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=self._warmup_zone_provider,
            )

            state = session.scalars(select(SymbolLifecycleState)).one()
            snapshot_count = session.scalar(select(func.count()).select_from(ZoneDailySnapshot))

        self.assertEqual(first.processed_bars, 3)
        self.assertEqual(second.processed_bars, 2)
        self.assertEqual(snapshot_count, 5)
        self.assertEqual(state.last_processed_ts, dt.datetime(2026, 1, 5))

    def test_shared_zone_generation_returns_identity_annotated_zones(self) -> None:
        prices = self._zone_generation_prices()

        generated = generate_zones_for_replay(
            symbol="AAPL",
            provider=None,
            df_calc_daily=prices,
            config=ZoneGenerationConfig(
                vp_lookback_days=21,
                vp_bins=20,
                weekly_vp_lookback=63,
                weekly_vp_bins=10,
                zone_expand_pct=0.001,
                hv_node_quantile=0.8,
                merge_pct=0.002,
                max_resistance_zones=4,
                max_support_zones=4,
                reaction_lookahead=3,
                reaction_return_threshold=0.01,
                min_touch_gap=2,
            ),
            interval_history_loader=lambda symbol, trading_dates, provider, interval: pd.DataFrame(),
        )

        self.assertFalse(generated.df_calc_daily_with_features.empty)
        self.assertGreater(len(generated.all_candidate_zones), 0)
        self.assertTrue(
            all("zone_id" in zone and "zone_kind" in zone for zone in generated.all_candidate_zones)
        )
        self.assertEqual(generated.short_vp_context.mode, "short disabled")
        self.assertEqual(generated.short_vp_context.zones_raw, [])
        self.assertGreater(len(generated.long_vp_context.zones_raw), 0)
        self.assertIsInstance(generated.support_zones, list)
        self.assertIsInstance(generated.resistance_zones, list)

    def test_recent_vp_windows_use_5m_source_when_fully_available(self) -> None:
        start = pd.Timestamp.today().normalize() - pd.Timedelta(days=69)
        prices = pd.DataFrame(
            [
                {
                    "date": start + pd.Timedelta(days=index),
                    "open": 100.0 + index,
                    "high": 101.5 + index,
                    "low": 99.5 + index,
                    "close": 100.5 + index,
                    "volume": 1_000.0 + index,
                }
                for index in range(70)
            ]
        )

        def loader(symbol, trading_dates, provider, interval):
            del symbol, provider
            if interval != "5m":
                return pd.DataFrame()
            rows = []
            for date_value in trading_dates:
                base = 100.0 + len(rows)
                for offset in range(2):
                    rows.append(
                        {
                            "date": pd.Timestamp(date_value) + pd.Timedelta(minutes=offset * 5),
                            "open": base,
                            "high": base + 1.0,
                            "low": base - 1.0,
                            "close": base + 0.5,
                            "volume": 100.0,
                        }
                    )
            return pd.DataFrame(rows)

        generated = generate_zones_for_replay(
            symbol="AAPL",
            provider="yfinance",
            df_calc_daily=prices,
            config=ZoneGenerationConfig(
                short_vp_lookback_days=21,
                short_vp_bins=20,
                long_vp_lookback_days=63,
                long_vp_bins=20,
                zone_expand_pct=0.001,
                hv_node_quantile=0.8,
                merge_pct=0.002,
                max_resistance_zones=4,
                max_support_zones=4,
                reaction_lookahead=3,
                reaction_return_threshold=0.01,
                min_touch_gap=2,
            ),
            interval_history_loader=loader,
        )

        self.assertEqual(generated.short_vp_context.mode, "short disabled")
        self.assertEqual(generated.long_vp_context.mode, "long 63 trading days 5m")
        self.assertEqual(
            sorted({source for zone in generated.all_candidate_zones for source in zone.get("source_types", set()) if source.startswith("vp_")}),
            ["vp_long"],
        )

    def test_rolling_avwap_uses_only_long_trading_day_window(self) -> None:
        prices = pd.DataFrame(
            [
                {
                    "date": dt.datetime(2024, 1, 1) + dt.timedelta(days=index),
                    "open": 100.0 + index * 0.03 + (index % 13) * 0.2,
                    "high": 102.0 + index * 0.03 + (index % 17) * 0.25,
                    "low": 98.0 + index * 0.03 - (index % 11) * 0.2,
                    "close": 100.0 + index * 0.03 + (1.0 if index % 9 else -1.0),
                    "volume": 1_000_000 + index * 1_000,
                }
                for index in range(420)
            ]
        )

        generated = generate_zones_for_replay(
            symbol="AAPL",
            provider=None,
            df_calc_daily=prices,
            config=ZoneGenerationConfig(
                short_vp_lookback_days=21,
                short_vp_bins=20,
                long_vp_lookback_days=63,
                long_vp_bins=20,
                zone_expand_pct=0.001,
                hv_node_quantile=0.8,
                merge_pct=0.002,
                max_resistance_zones=8,
                max_support_zones=8,
                reaction_lookahead=3,
                reaction_return_threshold=0.01,
                min_touch_gap=2,
            ),
            interval_history_loader=lambda symbol, trading_dates, provider, interval: pd.DataFrame(),
        )

        rolling_sources = {
            source
            for zone in generated.all_candidate_zones
            for source in zone.get("source_types", set())
            if source.startswith("avwap_") and source.endswith("_rolling")
        }

        self.assertIn("avwap_long_rolling", rolling_sources)
        self.assertNotIn("avwap_short_rolling", rolling_sources)
        self.assertNotIn("avwap_D_rolling", rolling_sources)
        self.assertNotIn("avwap_W_rolling", rolling_sources)
        self.assertTrue(
            all(
                meta.get("anchor_family") != "rolling"
                for meta in generated.weekly_anchor_meta.values()
            )
        )
        weekly_nonrolling_search_bars = {
            meta.get("anchor_search_bars")
            for meta in generated.weekly_anchor_meta.values()
            if meta.get("anchor_family") in {"swing", "event"}
        }
        self.assertEqual(weekly_nonrolling_search_bars, {52})

    def test_weekly_avwap_defaults_to_one_year_without_rolling_windows(self) -> None:
        weekly_prices = pd.DataFrame(
            [
                {
                    "date": dt.datetime(2024, 1, 5) + dt.timedelta(days=index * 7),
                    "open": 100.0 + index * 0.2,
                    "high": 102.0 + index * 0.2 + (index % 5) * 0.3,
                    "low": 98.0 + index * 0.2 - (index % 4) * 0.25,
                    "close": 100.0 + index * 0.2 + (1.0 if index % 7 else -1.0),
                    "volume": 1_000_000 + index * 10_000,
                }
                for index in range(80)
            ]
        )

        _features, anchor_meta = build_avwap_features(weekly_prices, timeframe="W")

        self.assertTrue(anchor_meta)
        self.assertTrue(all(meta.get("anchor_family") != "rolling" for meta in anchor_meta.values()))
        self.assertEqual(
            {
                meta.get("anchor_search_bars")
                for meta in anchor_meta.values()
                if meta.get("anchor_family") in {"swing", "event"}
            },
            {52},
        )

    def test_gap_events_require_gap_to_remain_at_close(self) -> None:
        prices = pd.DataFrame(
            [
                {"date": dt.datetime(2026, 1, 1), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1000},
                {"date": dt.datetime(2026, 1, 2), "open": 110.0, "high": 111.0, "low": 99.0, "close": 99.5, "volume": 1000},
                {"date": dt.datetime(2026, 1, 3), "open": 104.0, "high": 106.0, "low": 103.0, "close": 105.0, "volume": 1000},
                {"date": dt.datetime(2026, 1, 4), "open": 95.0, "high": 106.0, "low": 94.0, "close": 106.0, "volume": 1000},
                {"date": dt.datetime(2026, 1, 5), "open": 105.0, "high": 107.0, "low": 104.0, "close": 106.5, "volume": 1000},
                {"date": dt.datetime(2026, 1, 6), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1000},
            ]
        )

        anchors = find_anchor_points(prices, timeframe="D", rolling_window_bars=(), event_search_bars=6)

        self.assertEqual(anchors["gap_up"]["index"], 2)
        self.assertNotEqual(anchors["gap_up"]["index"], 1)
        self.assertNotIn("gap_down", anchors)

    def test_gap_down_event_requires_gap_to_remain_at_close(self) -> None:
        prices = pd.DataFrame(
            [
                {"date": dt.datetime(2026, 1, 1), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1000},
                {"date": dt.datetime(2026, 1, 2), "open": 90.0, "high": 101.0, "low": 89.0, "close": 101.0, "volume": 1000},
                {"date": dt.datetime(2026, 1, 3), "open": 97.0, "high": 98.0, "low": 95.0, "close": 96.0, "volume": 1000},
                {"date": dt.datetime(2026, 1, 4), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1000},
            ]
        )

        anchors = find_anchor_points(prices, timeframe="D", rolling_window_bars=(), event_search_bars=4)

        self.assertEqual(anchors["gap_down"]["index"], 2)
        self.assertNotEqual(anchors["gap_down"]["index"], 1)
        self.assertNotIn("gap_up", anchors)

    def test_event_anchors_require_high_relative_volume(self) -> None:
        rows = []
        for index in range(70):
            rows.append(
                {
                    "date": dt.datetime(2026, 1, 1) + dt.timedelta(days=index),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1_000.0,
                }
            )
        rows[65].update({"open": 120.0, "high": 121.0, "low": 119.0, "close": 120.5, "volume": 100.0})
        rows[66].update({"open": 130.0, "high": 131.0, "low": 129.0, "close": 130.5, "volume": 2_000.0})
        rows[67].update({"open": 120.0, "high": 121.0, "low": 100.0, "close": 101.0, "volume": 100.0})
        rows[68].update({"open": 100.0, "high": 121.0, "low": 99.0, "close": 120.0, "volume": 2_000.0})
        prices = pd.DataFrame(rows)

        anchors = find_anchor_points(prices, timeframe="D", rolling_window_bars=(), event_search_bars=60)

        self.assertEqual(anchors["gap_up"]["index"], 66)
        self.assertEqual(anchors["big_up"]["index"], 68)

    def test_rolling_anchors_require_high_relative_volume(self) -> None:
        rows = []
        for index in range(70):
            rows.append(
                {
                    "date": dt.datetime(2026, 1, 1) + dt.timedelta(days=index),
                    "open": 100.0,
                    "high": 100.0 + index * 0.01,
                    "low": 95.0,
                    "close": 100.0,
                    "volume": 1_000.0,
                }
            )
        rows[69]["high"] = 150.0
        rows[69]["volume"] = 100.0
        rows[68]["low"] = 80.0
        rows[68]["volume"] = 2_000.0
        prices = pd.DataFrame(rows)

        anchors = find_anchor_points(prices, timeframe="D", rolling_window_bars=(21,), event_search_bars=1)

        self.assertNotIn("rolling_21_high", anchors)
        self.assertEqual(anchors["rolling_21_low"]["index"], 68)

    def test_swing_anchors_require_high_relative_volume(self) -> None:
        rows = []
        for index in range(80):
            rows.append(
                {
                    "date": dt.datetime(2026, 1, 1) + dt.timedelta(days=index),
                    "open": 100.0,
                    "high": 100.0,
                    "low": 95.0,
                    "close": 100.0,
                    "volume": 1_000.0,
                }
            )
        for index, high in [(27, 100.0), (28, 101.0), (29, 102.0), (30, 120.0), (31, 102.0), (32, 101.0), (33, 100.0)]:
            rows[index]["high"] = high
        rows[30]["volume"] = 100.0
        for index, high in [(47, 100.0), (48, 101.0), (49, 102.0), (50, 115.0), (51, 102.0), (52, 101.0), (53, 100.0)]:
            rows[index]["high"] = high
        rows[50]["volume"] = 2_000.0
        prices = pd.DataFrame(rows)

        anchors = find_anchor_points(prices, timeframe="D", rolling_window_bars=(), event_search_bars=1)

        self.assertEqual(anchors["recent_swing_high"]["index"], 50)
        self.assertNotIn("previous_swing_high", anchors)

    def test_zone_ranking_uses_only_center_band_volume(self) -> None:
        prices = pd.DataFrame(
            [
                {"date": dt.datetime(2026, 1, 1), "high": 100.1, "low": 99.9, "close": 100.0, "volume": 10_000.0},
                {"date": dt.datetime(2026, 1, 2), "high": 101.1, "low": 100.9, "close": 101.0, "volume": 1_000.0},
                {"date": dt.datetime(2026, 1, 3), "high": 102.1, "low": 101.9, "close": 102.0, "volume": 100.0},
            ]
        )
        zones = [
            {
                "zone_id": "low-volume-near",
                "side": "support",
                "lower": 100.8,
                "upper": 101.2,
                "center": 101.0,
                "timeframes": {"short"},
                "source_types": {"avwap_short_event"},
            },
            {
                "zone_id": "high-volume-far",
                "side": "support",
                "lower": 99.8,
                "upper": 100.2,
                "center": 100.0,
                "timeframes": {"short"},
                "source_types": {"avwap_short_event"},
            },
            {
                "zone_id": "tiny-volume",
                "side": "support",
                "lower": 101.8,
                "upper": 102.2,
                "center": 102.0,
                "timeframes": {"short"},
                "source_types": {"avwap_short_event"},
            },
        ]

        ranked = rank_zones_for_side(
            zones=zones,
            current_price=103.0,
            side="support",
            max_zones=2,
            price_history=prices,
            center_volume_pct=0.002,
        )

        self.assertEqual([zone["zone_id"] for zone in ranked], ["high-volume-far", "low-volume-near"])
        self.assertEqual([zone["center_volume"] for zone in ranked], [10_000.0, 1_000.0])
        self.assertNotIn("institutional_score", ranked[0])
        self.assertNotIn("reaction_score", ranked[0])

    def test_warmup_can_use_shared_zone_generation_provider(self) -> None:
        prices = self._zone_generation_prices()
        provider = make_replay_zone_provider(
            symbol="AAPL",
            provider=None,
            config=ZoneGenerationConfig(
                vp_lookback_days=21,
                vp_bins=20,
                weekly_vp_lookback=63,
                weekly_vp_bins=10,
                zone_expand_pct=0.001,
                hv_node_quantile=0.8,
                merge_pct=0.002,
                max_resistance_zones=4,
                max_support_zones=4,
                reaction_lookahead=3,
                reaction_return_threshold=0.01,
                min_touch_gap=2,
            ),
            interval_history_loader=lambda symbol, trading_dates, provider, interval: pd.DataFrame(),
            include_all_candidates=True,
        )

        with self.Session() as session:
            result = ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=provider,
            )
            zone_count = session.scalar(select(func.count()).select_from(Zone))
            snapshot_count = session.scalar(select(func.count()).select_from(ZoneDailySnapshot))

        self.assertEqual(result.processed_bars, len(prices))
        self.assertGreater(zone_count, 0)
        self.assertGreater(snapshot_count, 0)

    def test_warmup_snapshots_only_selected_active_zones(self) -> None:
        prices = pd.DataFrame(
            [
                {"date": dt.datetime(2026, 1, 1), "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0},
                {"date": dt.datetime(2026, 1, 2), "open": 101.0, "high": 103.0, "low": 100.0, "close": 102.0},
            ]
        )

        with self.Session() as session:
            active_zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_low"],
                price_low=95.0,
                price_high=97.0,
                current_role=ZoneRole.SUPPORT,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="active-zone",
            )
            invalidated_zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_high"],
                price_low=110.0,
                price_high=112.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="invalid-zone",
            )
            invalidated_zone.status = ZoneStatus.INVALIDATED

            ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=lambda history, bar: [
                    {
                        "type": "selected_event_support",
                        "side": ZoneRole.SUPPORT,
                        "lower": 98.0,
                        "upper": 100.0,
                        "center": 99.0,
                        "timeframes": {"D"},
                        "source_types": {"swing_low"},
                        "primary_timeframe": "D",
                        "zone_kind": ZoneKind.EVENT,
                        "origin_bar": dt.datetime(2026, 1, 2),
                        "origin_event_id": "selected-zone",
                    },
                    {
                        "type": "selected_weekly_confluence",
                        "side": ZoneRole.RESISTANCE,
                        "lower": 108.0,
                        "upper": 110.0,
                        "center": 109.0,
                        "timeframes": {"D", "W"},
                        "source_types": {"avwap_d_event", "avwap_w_event"},
                        "primary_timeframe": "W",
                        "zone_kind": ZoneKind.COMPOSITE,
                        "origin_bar": dt.datetime(2026, 1, 2),
                        "origin_event_id": "selected-weekly-confluence",
                    },
                    {
                        "type": "selected_weekly_avwap",
                        "side": ZoneRole.RESISTANCE,
                        "lower": 112.0,
                        "upper": 114.0,
                        "center": 113.0,
                        "timeframes": {"W"},
                        "source_types": {"avwap_w_event"},
                        "primary_timeframe": "W",
                        "zone_kind": ZoneKind.AVWAP,
                        "origin_bar": dt.datetime(2026, 1, 2),
                        "origin_event_id": "selected-weekly-only",
                    },
                    {
                        "type": "selected_short_vp",
                        "side": ZoneRole.SUPPORT,
                        "lower": 96.0,
                        "upper": 98.0,
                        "center": 97.0,
                        "timeframes": {"short"},
                        "source_types": {"vp_short"},
                        "primary_timeframe": "short",
                        "source_label": "VP (short 21 trading days, 5m)",
                        "vp_window_type": "short_21d",
                        "zone_kind": ZoneKind.VP,
                        "origin_event_id": "selected-vp-short",
                    }
                ],
                snapshot_start_date=dt.datetime(2026, 1, 2),
                snapshot_end_date=dt.datetime(2026, 1, 2),
                force=True,
            )

            snapshot_rows = [
                row
                for row in session.execute(
                    select(ZoneDailySnapshot.zone_id, Zone.origin_event_id)
                    .join(Zone, Zone.zone_id == ZoneDailySnapshot.zone_id)
                    .where(
                        ZoneDailySnapshot.snapshot_ts == dt.datetime(2026, 1, 2)
                    )
                ).all()
            ]

        self.assertEqual(
            sorted(row.origin_event_id for row in snapshot_rows),
            ["selected-vp-short", "selected-weekly-confluence", "selected-weekly-only", "selected-zone"],
        )
        self.assertNotIn(active_zone.zone_id, [row.zone_id for row in snapshot_rows])
        self.assertNotIn(invalidated_zone.zone_id, [row.zone_id for row in snapshot_rows])

    def test_preloaded_interval_loader_slices_local_frames(self) -> None:
        prices = self._zone_generation_prices()
        loader = make_preloaded_interval_history_loader({"5m": prices, "1d": prices})
        selected_dates = [pd.Timestamp("2025-10-03"), pd.Timestamp("2025-10-05")]

        five_min = loader("AAPL", selected_dates, "yfinance", "5m")
        daily = loader("AAPL", selected_dates, "yfinance", "1d")
        missing = loader("AAPL", selected_dates, "yfinance", "15m")

        self.assertEqual(pd.to_datetime(five_min["date"]).dt.normalize().nunique(), 2)
        self.assertEqual(pd.to_datetime(daily["date"]).dt.normalize().nunique(), 2)
        self.assertTrue(missing.empty)

    def test_preloaded_zone_provider_uses_cached_interval_frames(self) -> None:
        prices = self._zone_generation_prices()
        provider = make_preloaded_zone_provider(
            symbol="AAPL",
            provider="unused-provider",
            config=ZoneGenerationConfig(
                vp_lookback_days=21,
                vp_bins=20,
                weekly_vp_lookback=63,
                weekly_vp_bins=10,
                zone_expand_pct=0.001,
                hv_node_quantile=0.8,
                merge_pct=0.002,
                max_resistance_zones=4,
                max_support_zones=4,
                reaction_lookahead=3,
                reaction_return_threshold=0.01,
                min_touch_gap=2,
            ),
            interval_frames={"5m": prices, "1d": prices},
            include_all_candidates=True,
        )

        zones = provider(prices, None)

        self.assertGreater(len(zones), 0)
        self.assertTrue(all("zone_id" in zone for zone in zones))

    def _build_composite_dashboard_zone(self) -> dict:
        return merge_close_zones(
            self._source_component_zones(),
            merge_pct=0.10,
            symbol="AAPL",
        )[0]

    def _source_component_zones(self) -> list[dict]:
        avwap_id = "zone_component_avwap"
        vp_id = "zone_component_vp"
        return [
            {
                "zone_id": avwap_id,
                "zone_kind": ZoneKind.AVWAP,
                "type": "avwap_support_short",
                "side": "support",
                "lower": 98.0,
                "upper": 100.0,
                "center": 99.0,
                "vp_volume": 0.0,
                "anchor_count": 1,
                "avwap_strength": 1.0,
                "timeframes": {"short"},
                "source_types": {"avwap_short_rolling"},
                "source_label": "AVWAP (short, rolling)",
                "source_zone_ids": [avwap_id],
                "source_components": [
                    {
                        "zone_id": avwap_id,
                        "zone_kind": ZoneKind.AVWAP,
                        "type": "avwap_support_short",
                        "side": "support",
                        "lower": 98.0,
                        "upper": 100.0,
                        "center": 99.0,
                        "timeframes": {"short"},
                        "source_types": {"avwap_short_rolling"},
                        "source_label": "AVWAP (short, rolling)",
                        "primary_timeframe": "short",
                    }
                ],
                "primary_timeframe": "short",
            },
            {
                "zone_id": vp_id,
                "zone_kind": ZoneKind.VP,
                "type": "vp_zone_short",
                "side": "support",
                "lower": 99.0,
                "upper": 101.0,
                "center": 100.0,
                "vp_volume": 1000.0,
                "anchor_count": 0,
                "avwap_strength": 0.0,
                "timeframes": {"short"},
                "source_types": {"vp_short"},
                "source_label": "VP (short 21 trading days, 5m)",
                "vp_window_type": "short_21d",
                "source_zone_ids": [vp_id],
                "source_components": [
                    {
                        "zone_id": vp_id,
                        "zone_kind": ZoneKind.VP,
                        "type": "vp_zone_short",
                        "side": "support",
                        "lower": 99.0,
                        "upper": 101.0,
                        "center": 100.0,
                        "timeframes": {"short"},
                        "source_types": {"vp_short"},
                        "source_label": "VP (short 21 trading days, 5m)",
                        "vp_window_type": "short_21d",
                        "primary_timeframe": "short",
                    }
                ],
                "primary_timeframe": "short",
            },
        ]

    def _warmup_prices(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"date": dt.datetime(2026, 1, 1), "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "atr": 2.0},
                {"date": dt.datetime(2026, 1, 2), "open": 100.5, "high": 101.5, "low": 99.5, "close": 100.8, "atr": 2.0},
                {"date": dt.datetime(2026, 1, 3), "open": 101.0, "high": 104.0, "low": 100.5, "close": 102.5, "atr": 2.0},
                {"date": dt.datetime(2026, 1, 4), "open": 103.0, "high": 103.5, "low": 101.5, "close": 102.8, "atr": 2.0},
                {"date": dt.datetime(2026, 1, 5), "open": 102.0, "high": 103.0, "low": 99.5, "close": 100.0, "atr": 2.0},
            ]
        )

    def _warmup_zone_provider(self, history: pd.DataFrame, bar: BarInput) -> list[dict]:
        return [
            {
                "zone_kind": ZoneKind.EVENT,
                "type": "swing_high",
                "side": "resistance",
                "lower": 101.0,
                "upper": 102.0,
                "center": 101.5,
                "timeframes": {"1d"},
                "source_types": {"swing_high"},
                "source_label": "Swing high",
                "primary_timeframe": "1d",
                "origin_bar": dt.datetime(2026, 1, 1),
                "origin_event_id": "warmup-swing-high",
                "origin_event_type": "swing_high",
            }
        ]

    def _zone_generation_prices(self) -> pd.DataFrame:
        rows: list[dict] = []
        start = dt.datetime(2025, 10, 1)
        for index in range(80):
            base = 100.0 + index * 0.1
            rows.append(
                {
                    "date": start + dt.timedelta(days=index),
                    "open": base,
                    "high": base + 1.0 + (index % 5) * 0.1,
                    "low": base - 1.0 - (index % 3) * 0.1,
                    "close": base + (0.2 if index % 2 else -0.2),
                    "volume": 1_000_000 + index * 1_000,
                }
            )
        return pd.DataFrame(rows)


if __name__ == "__main__":
    unittest.main()
