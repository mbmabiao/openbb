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
from zone_lifecycle.repository import create_session_factory
from zone_lifecycle.service import ZoneSnapshotInput, record_zone_snapshot, upsert_zone
from zone_lifecycle.snapshot_queries import load_replay_zone_snapshots
from zone_lifecycle.warmup import ensure_symbol_lifecycle_ready
from engines.zone_generation import ZoneGenerationConfig, generate_zones_for_replay, make_replay_zone_provider
from features.boundaries import merge_close_zones


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

    def test_vp_zone_keeps_id_while_price_window_rolls(self) -> None:
        with self.Session() as session:
            first = upsert_zone(
                session,
                symbol="MSFT",
                timeframe="1d",
                zone_kind=ZoneKind.VP,
                source=["vp_hvn"],
                price_low=300.0,
                price_high=304.0,
                current_role="support",
                vp_window_type="daily_63d",
            )
            second = upsert_zone(
                session,
                symbol="MSFT",
                timeframe="1d",
                zone_kind=ZoneKind.VP,
                source=["vp_hvn"],
                price_low=301.0,
                price_high=305.0,
                current_role="support",
                vp_window_type="daily_63d",
            )

            self.assertEqual(first.zone_id, second.zone_id)
            self.assertEqual(second.price_low, 301.0)
            self.assertEqual(second.price_high, 305.0)
            zone_count = session.scalar(select(func.count()).select_from(Zone))
            self.assertEqual(zone_count, 1)

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
                vp_lookback_days=20,
                vp_bins=20,
                weekly_vp_lookback=20,
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
        self.assertEqual(generated.daily_vp_context.mode, "5m unavailable")
        self.assertIsInstance(generated.support_zones, list)
        self.assertIsInstance(generated.resistance_zones, list)

    def test_warmup_can_use_shared_zone_generation_provider(self) -> None:
        prices = self._zone_generation_prices()
        provider = make_replay_zone_provider(
            symbol="AAPL",
            provider=None,
            config=ZoneGenerationConfig(
                vp_lookback_days=20,
                vp_bins=20,
                weekly_vp_lookback=20,
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
                "zone_kind": ZoneKind.EVENT,
                "type": "avwap_support_D",
                "side": "support",
                "lower": 98.0,
                "upper": 100.0,
                "center": 99.0,
                "vp_volume": 0.0,
                "anchor_count": 1,
                "avwap_strength": 1.0,
                "timeframes": {"D"},
                "source_types": {"avwap_D_rolling"},
                "source_label": "AVWAP (D, rolling)",
                "source_zone_ids": [avwap_id],
                "source_components": [
                    {
                        "zone_id": avwap_id,
                        "zone_kind": ZoneKind.EVENT,
                        "type": "avwap_support_D",
                        "side": "support",
                        "lower": 98.0,
                        "upper": 100.0,
                        "center": 99.0,
                        "timeframes": {"D"},
                        "source_types": {"avwap_D_rolling"},
                        "source_label": "AVWAP (D, rolling)",
                        "primary_timeframe": "D",
                    }
                ],
                "primary_timeframe": "D",
            },
            {
                "zone_id": vp_id,
                "zone_kind": ZoneKind.VP,
                "type": "vp_zone_D",
                "side": "support",
                "lower": 99.0,
                "upper": 101.0,
                "center": 100.0,
                "vp_volume": 1000.0,
                "anchor_count": 0,
                "avwap_strength": 0.0,
                "timeframes": {"D"},
                "source_types": {"vp_D"},
                "source_label": "VP (D, 5m composite)",
                "vp_window_type": "VP (D, 5m composite)",
                "source_zone_ids": [vp_id],
                "source_components": [
                    {
                        "zone_id": vp_id,
                        "zone_kind": ZoneKind.VP,
                        "type": "vp_zone_D",
                        "side": "support",
                        "lower": 99.0,
                        "upper": 101.0,
                        "center": 100.0,
                        "timeframes": {"D"},
                        "source_types": {"vp_D"},
                        "source_label": "VP (D, 5m composite)",
                        "vp_window_type": "VP (D, 5m composite)",
                        "primary_timeframe": "D",
                    }
                ],
                "primary_timeframe": "D",
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
