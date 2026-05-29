from __future__ import annotations

import datetime as dt
from pathlib import Path
import sys
import unittest

import pandas as pd
from sqlalchemy import func, select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from zone_lifecycle.breakout_state_machine import BreakoutStateConfig, process_zone_bar
from zone_lifecycle.breakout_event_queries import load_breakout_events
from zone_lifecycle.constants import BreakoutEventStatus, ZoneKind, ZoneRole, ZoneStatus
from zone_lifecycle.divergence_event_queries import _prefer_confirmed_over_risk
from zone_lifecycle.divergence_events import _build_divergence_event
from zone_lifecycle.lifecycle import BarInput, expire_event_zones, update_zone_interaction_counts
from zone_lifecycle.models import BreakoutEvent, DivergenceEvent, MarketObservation, PatternEvent, SymbolLifecycleState, Zone, ZoneDailySnapshot
from zone_lifecycle.offline_snapshots import reset_symbol_lifecycle_data
from zone_lifecycle.pattern_events import detect_pattern_events_for_latest_bar
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
from features.volume_profile import find_recent_swing_points


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
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
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
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=200.0,
                price_high=210.0,
                current_role="support",
                origin_bar=dt.datetime(2026, 2, 1),
                origin_event_id="support-2026-02-01",
            )
            expired = upsert_zone(
                session,
                symbol="TSLA",
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
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
            )

        self.assertEqual(len(result.support_zones), 1)
        self.assertEqual(result.support_zones[0]["zone_id"], support.zone_id)
        self.assertEqual(result.support_zones[0]["display_label"], "S1")
        self.assertEqual(result.resistance_zones, [])
        self.assertEqual([zone["zone_id"] for zone in result.all_zones], [support.zone_id])

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

    def test_weekly_swing_zone_does_not_expire_by_event_ttl(self) -> None:
        with self.Session() as session:
            weekly_swing = upsert_zone(
                session,
                symbol="YINN",
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=40.0,
                price_high=41.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 2),
                origin_event_id="weekly-swing",
                origin_event_type="swing",
            )

            count = expire_event_zones(
                session,
                current_ts=dt.datetime(2026, 5, 11),
                bars_since_created_by_zone_id={weekly_swing.zone_id: 100},
            )

            self.assertEqual(count, 0)
            self.assertEqual(weekly_swing.status, ZoneStatus.ACTIVE)
            self.assertIsNone(weekly_swing.expired_ts)

    def test_weekly_swing_zone_expires_after_182_calendar_days(self) -> None:
        with self.Session() as session:
            weekly_swing = upsert_zone(
                session,
                symbol="YINN",
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=40.0,
                price_high=41.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2025, 7, 5),
                origin_event_id="weekly-swing-old",
                origin_event_type="swing",
            )

            count = expire_event_zones(
                session,
                current_ts=dt.datetime(2026, 1, 3),
                bars_since_created_by_zone_id={weekly_swing.zone_id: 1},
            )

            self.assertEqual(count, 1)
            self.assertEqual(weekly_swing.status, ZoneStatus.EXPIRED)
            self.assertEqual(weekly_swing.expired_ts, dt.datetime(2026, 1, 3))

    def test_reselected_weekly_swing_zone_is_reactivated(self) -> None:
        with self.Session() as session:
            weekly_swing = upsert_zone(
                session,
                symbol="YINN",
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=40.0,
                price_high=41.0,
                current_role=ZoneRole.RESISTANCE,
                origin_bar=dt.datetime(2026, 1, 2),
                origin_event_id="weekly-swing",
                origin_event_type="swing",
            )
            weekly_swing.status = ZoneStatus.EXPIRED
            weekly_swing.expired_ts = dt.datetime(2026, 4, 1)
            session.flush()

            updated = upsert_zone(
                session,
                symbol="YINN",
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=40.0,
                price_high=41.0,
                current_role=ZoneRole.SUPPORT,
                origin_bar=dt.datetime(2026, 1, 2),
                origin_event_id="weekly-swing",
                origin_event_type="swing",
            )

            self.assertEqual(updated.zone_id, weekly_swing.zone_id)
            self.assertEqual(updated.status, ZoneStatus.ACTIVE)
            self.assertEqual(updated.current_role, ZoneRole.SUPPORT)
            self.assertIsNone(updated.expired_ts)

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
                    close=100.8,
                    atr=2.0,
                    previous_close=100.5,
                ),
                breakout_buffer=0.2,
            )

            self.assertEqual(zone.close_inside_count, 1)
            self.assertEqual(zone.touch_count, 1)
            self.assertEqual(zone.break_count, 1)

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
                    previous_close=100.5,
                    volume=1000.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
            )

            self.assertIsNotNone(event)
            self.assertEqual(event.status, BreakoutEventStatus.CONFIRMED)
            self.assertEqual(event.direction, "up")
            self.assertEqual(zone.status, ZoneStatus.FLIPPED)
            self.assertEqual(zone.current_role, ZoneRole.SUPPORT)
            self.assertEqual(zone.confirmed_breakout_count, 1)
            self.assertEqual(session.scalar(select(func.count()).select_from(BreakoutEvent)), 1)

    def test_intraday_center_cross_without_close_cross_does_not_create_breakout_event(self) -> None:
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
                origin_event_id="no-false-breakout-zone",
            )

            event = process_zone_bar(
                session,
                zone,
                BarInput(
                    timestamp=dt.datetime(2026, 1, 4),
                    open=100.5,
                    high=102.5,
                    low=100.2,
                    close=100.8,
                    atr=2.0,
                    previous_close=100.5,
                    volume=1000.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
            )

            self.assertIsNone(event)
            self.assertEqual(session.scalar(select(func.count()).select_from(BreakoutEvent)), 0)

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
                BarInput(
                    dt.datetime(2026, 1, 4),
                    open=101.0,
                    high=104.0,
                    low=100.5,
                    close=102.5,
                    atr=2.0,
                    previous_close=100.5,
                    volume=1000.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
            )
            event = process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 6), open=103.0, high=103.5, low=100.8, close=102.8, atr=2.0, bar_index=12),
            )
            events = session.scalars(select(BreakoutEvent).order_by(BreakoutEvent.created_ts)).all()

            self.assertEqual(event.status, BreakoutEventStatus.RETEST_SUCCESS)
            self.assertEqual([row.status for row in events], [BreakoutEventStatus.CONFIRMED, BreakoutEventStatus.RETEST_SUCCESS])
            self.assertEqual(events[1].metadata_json["parent_breakout_event_id"], events[0].breakout_event_id)
            self.assertEqual(zone.status, ZoneStatus.RETESTED)
            self.assertEqual(zone.current_role, ZoneRole.SUPPORT)

    def test_breakout_retest_requires_wick_only_center_retest(self) -> None:
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
                origin_event_id="body-cross-retest-zone",
            )
            event = process_zone_bar(
                session,
                zone,
                BarInput(
                    dt.datetime(2026, 1, 4),
                    open=101.0,
                    high=104.0,
                    low=100.5,
                    close=102.5,
                    atr=2.0,
                    previous_close=100.5,
                    volume=1000.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
            )
            self.assertEqual(event.status, BreakoutEventStatus.CONFIRMED)

            event = process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 6), open=100.8, high=103.5, low=100.5, close=102.8, atr=2.0, bar_index=12),
            )

            self.assertEqual(event.status, BreakoutEventStatus.CONFIRMED)
            self.assertEqual(zone.status, ZoneStatus.FLIPPED)

    def test_breakout_retest_is_never_valid_after_three_trading_bars(self) -> None:
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
                origin_event_id="late-retest-zone",
            )
            process_zone_bar(
                session,
                zone,
                BarInput(
                    dt.datetime(2026, 1, 4),
                    open=101.0,
                    high=104.0,
                    low=100.5,
                    close=102.5,
                    atr=2.0,
                    previous_close=100.5,
                    volume=1000.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
                config=BreakoutStateConfig(retest_window_bars=10),
            )

            event = process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 10), open=103.0, high=103.5, low=100.8, close=102.8, atr=2.0, bar_index=14),
                config=BreakoutStateConfig(retest_window_bars=10),
            )

            self.assertEqual(event.status, BreakoutEventStatus.CONFIRMED)
            self.assertEqual(session.scalar(select(func.count()).select_from(BreakoutEvent)), 1)

    def test_close_back_below_center_does_not_create_new_event(self) -> None:
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
                origin_event_id="no-failure-zone",
            )
            process_zone_bar(
                session,
                zone,
                BarInput(
                    dt.datetime(2026, 1, 4),
                    open=101.0,
                    high=104.0,
                    low=100.5,
                    close=102.5,
                    atr=2.0,
                    previous_close=100.5,
                    volume=1000.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
            )
            event = process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 6), open=101.0, high=101.5, low=98.5, close=99.7, atr=2.0, bar_index=12),
            )

            self.assertEqual(event.status, BreakoutEventStatus.CONFIRMED)
            self.assertEqual(zone.status, ZoneStatus.FLIPPED)
            self.assertIsNone(zone.invalidated_ts)
            markers = load_breakout_events(
                session,
                symbol="AAPL",
                start_time=dt.datetime(2026, 1, 6),
                end_time=dt.datetime(2026, 1, 6),
            )
            self.assertEqual(markers, [])

    def test_breakout_without_volume_confirmation_is_weak_and_does_not_fail(self) -> None:
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
                origin_event_id="weak-breakout-zone",
            )

            event = process_zone_bar(
                session,
                zone,
                BarInput(
                    dt.datetime(2026, 1, 4),
                    open=101.0,
                    high=104.0,
                    low=100.5,
                    close=102.5,
                    atr=2.0,
                    previous_close=100.5,
                    volume=500.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
            )

            self.assertEqual(event.status, BreakoutEventStatus.TRUE_BREAKOUT_WEAK)
            self.assertEqual(zone.status, ZoneStatus.FLIPPED)

            event = process_zone_bar(
                session,
                zone,
                BarInput(dt.datetime(2026, 1, 6), open=101.0, high=101.5, low=98.5, close=99.7, atr=2.0, bar_index=12),
            )

            self.assertEqual(event.status, BreakoutEventStatus.TRUE_BREAKOUT_WEAK)

    def test_down_close_cross_does_not_create_breakout_event(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="1d",
                zone_kind=ZoneKind.EVENT,
                source=["swing_low"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.SUPPORT,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="down-cross-zone",
            )

            event = process_zone_bar(
                session,
                zone,
                BarInput(
                    dt.datetime(2026, 1, 4),
                    open=101.2,
                    high=101.8,
                    low=98.5,
                    close=99.6,
                    atr=2.0,
                    previous_close=101.5,
                    volume=1000.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
            )

            self.assertIsNone(event)
            self.assertEqual(session.scalar(select(func.count()).select_from(BreakoutEvent)), 0)

    def test_breakout_direction_is_inferred_from_close_cross_not_zone_role(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="w",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.SUPPORT,
                origin_bar=dt.datetime(2026, 1, 1),
                origin_event_id="role-independent-breakout-zone",
            )

            event = process_zone_bar(
                session,
                zone,
                BarInput(
                    dt.datetime(2026, 1, 4),
                    open=100.6,
                    high=103.0,
                    low=100.4,
                    close=102.2,
                    atr=2.0,
                    previous_close=100.5,
                    volume=1000.0,
                    volume_p80_20=800.0,
                    bar_index=10,
                ),
            )

            self.assertIsNotNone(event)
            self.assertEqual(event.direction, "up")
            self.assertEqual(event.status, BreakoutEventStatus.CONFIRMED)
            self.assertEqual(zone.current_role, ZoneRole.SUPPORT)

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
                long_vp_lookback_days=63,
                long_vp_bins=10,
                zone_expand_pct=0.001,
                max_resistance_zones=4,
                max_support_zones=4,
            ),
            interval_history_loader=lambda symbol, trading_dates, provider, interval: pd.DataFrame(),
        )

        self.assertFalse(generated.df_calc_daily_with_features.empty)
        self.assertGreater(len(generated.all_candidate_zones), 0)
        self.assertTrue(
            all("zone_id" in zone and "zone_kind" in zone for zone in generated.all_candidate_zones)
        )
        self.assertGreater(len(generated.long_vp_profile_df), 0)
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
                long_vp_lookback_days=63,
                long_vp_bins=20,
                zone_expand_pct=0.001,
                max_resistance_zones=4,
                max_support_zones=4,
            ),
            interval_history_loader=loader,
        )

        self.assertFalse(generated.long_vp_profile_df.empty)
        self.assertEqual(
            sorted({source for zone in generated.all_candidate_zones for source in zone.get("source_types", set()) if source.startswith("vp_")}),
            [],
        )

    def test_zone_generation_uses_weekly_swing_price_zones_only(self) -> None:
        prices = self._zone_generation_prices()

        generated = generate_zones_for_replay(
            symbol="AAPL",
            provider=None,
            df_calc_daily=prices,
            config=ZoneGenerationConfig(
                long_vp_lookback_days=63,
                long_vp_bins=20,
                zone_expand_pct=0.001,
                max_resistance_zones=8,
                max_support_zones=8,
            ),
            interval_history_loader=lambda symbol, trading_dates, provider, interval: pd.DataFrame(),
        )

        zone_sources = {
            source
            for zone in generated.all_candidate_zones
            for source in zone.get("source_types", set())
        }

        self.assertEqual(zone_sources, {"swing_W"})
        self.assertFalse(
            {
                source
                for source in zone_sources
                if source.startswith("avwap_") or source.startswith("vp_")
            }
        )
        self.assertTrue(
            all(
                meta.get("anchor_family") == "swing" and meta.get("timeframe") == "W"
                for meta in generated.daily_anchor_meta.values()
            )
        )
        self.assertFalse(
            {
                source
                for zone in generated.all_candidate_zones
                for source in zone.get("source_types", set())
                if source in {"avwap_D_swing", "avwap_D_event"}
            }
        )
    def test_recent_swing_points_require_high_relative_volume_and_two_bar_confirmation(self) -> None:
        rows = []
        for index in range(36):
            rows.append(
                {
                    "date": dt.datetime(2026, 1, 2) + dt.timedelta(days=index * 7),
                    "open": 100.0,
                    "high": 100.0,
                    "low": 95.0,
                    "close": 98.0,
                    "volume": 1_000.0,
                }
            )

        for index, high, volume in [
            (22, 101.0, 1_000.0),
            (23, 102.0, 1_000.0),
            (24, 120.0, 900.0),
            (25, 102.0, 1_000.0),
            (26, 101.0, 1_000.0),
            (29, 101.0, 1_000.0),
            (30, 102.0, 1_000.0),
            (31, 118.0, 2_000.0),
            (32, 102.0, 1_000.0),
            (33, 101.0, 1_000.0),
        ]:
            rows[index]["high"] = high
            rows[index]["volume"] = volume

        for index, low, volume in [
            (18, 94.0, 1_000.0),
            (19, 93.0, 1_000.0),
            (20, 80.0, 900.0),
            (21, 93.0, 1_000.0),
            (22, 94.0, 1_000.0),
            (27, 94.0, 1_000.0),
            (28, 93.0, 1_000.0),
            (29, 82.0, 2_000.0),
            (30, 93.0, 1_000.0),
            (31, 94.0, 2_000.0),
        ]:
            rows[index]["low"] = low
            rows[index]["volume"] = volume

        points = find_recent_swing_points(
            pd.DataFrame(rows),
            timeframe="W",
            max_points_per_side=3,
        )

        self.assertEqual(
            [(point["side"], point["index"]) for point in points],
            [("support", 29), ("resistance", 31)],
        )

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
                long_vp_lookback_days=63,
                long_vp_bins=10,
                zone_expand_pct=0.001,
                max_resistance_zones=4,
                max_support_zones=4,
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
            observation_count = session.scalar(select(func.count()).select_from(MarketObservation))

        self.assertEqual(result.processed_bars, len(prices))
        self.assertGreater(zone_count, 0)
        self.assertGreater(snapshot_count, 0)
        self.assertGreater(observation_count, 0)
        self.assertEqual(result.observations, observation_count)

    def test_warmup_records_volume_price_pattern_events(self) -> None:
        rows = []
        close = 100.0
        start = dt.datetime(2026, 1, 1)
        for index in range(22):
            open_price = close
            close = close * 1.01
            rows.append(
                {
                    "date": start + dt.timedelta(days=index),
                    "open": open_price,
                    "high": close + 0.2,
                    "low": open_price - 0.2,
                    "close": close,
                    "volume": 100.0 + index,
                }
            )
        previous_close = close
        rows.append(
            {
                "date": start + dt.timedelta(days=22),
                "open": previous_close,
                "high": previous_close + 3.0,
                "low": previous_close - 1.0,
                "close": previous_close * 1.004,
                "volume": 1_000.0,
            }
        )
        prices = pd.DataFrame(rows)

        with self.Session() as session:
            result = ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=lambda history, bar: [],
                lookback_years=0,
                snapshot_start_date=start,
                snapshot_end_date=start + dt.timedelta(days=22),
                as_of_date=start + dt.timedelta(days=22),
                force=True,
            )
            events = session.scalars(select(PatternEvent).order_by(PatternEvent.event_type)).all()

        event_types = {event.event_type for event in events}
        self.assertIn("volume_stall_up", event_types)
        self.assertIn("volume_long_upper_wick", event_types)
        self.assertEqual(result.pattern_events, len(events))
        stall = next(event for event in events if event.event_type == "volume_stall_up")
        self.assertEqual(stall.direction, "bearish")
        self.assertGreater(stall.price_change_pct, 0)
        self.assertGreaterEqual(stall.volume_percentile_20, 0.80)
        self.assertLessEqual(stall.abs_price_change_percentile_20, 0.20)
        self.assertEqual(stall.lookback_bars, 20)

    def test_long_wick_pattern_requires_meaningful_body_return(self) -> None:
        rows = []
        start = dt.datetime(2026, 1, 1)
        for index in range(22):
            close = 100.0 + index
            rows.append(
                {
                    "timestamp": start + dt.timedelta(days=index),
                    "open": close - 0.5,
                    "high": close + 0.5,
                    "low": close - 1.0,
                    "close": close,
                    "volume": 100.0 + index,
                }
            )
        previous_close = float(rows[-1]["close"])
        rows.append(
            {
                "timestamp": start + dt.timedelta(days=22),
                "open": previous_close,
                "high": previous_close + 4.0,
                "low": previous_close - 0.5,
                "close": previous_close * 1.001,
                "volume": 1_000.0,
            }
        )

        events = detect_pattern_events_for_latest_bar(
            pd.DataFrame(rows),
            symbol="AAPL",
            timeframe="1d",
        )

        self.assertNotIn("volume_long_upper_wick", {event.event_type for event in events})

    def test_long_wick_pattern_requires_atr_scaled_wick_size(self) -> None:
        rows = []
        start = dt.datetime(2026, 1, 1)
        for index in range(22):
            close = 100.0
            rows.append(
                {
                    "timestamp": start + dt.timedelta(days=index),
                    "open": close - 5.0,
                    "high": close + 5.0,
                    "low": close - 5.0,
                    "close": close,
                    "volume": 100.0 + index,
                }
            )
        previous_close = float(rows[-1]["close"])
        rows.append(
            {
                "timestamp": start + dt.timedelta(days=22),
                "open": previous_close,
                "high": previous_close + 1.0,
                "low": previous_close - 0.6,
                "close": previous_close + 0.4,
                "volume": 1_000.0,
            }
        )

        events = detect_pattern_events_for_latest_bar(
            pd.DataFrame(rows),
            symbol="AAPL",
            timeframe="1d",
        )

        self.assertNotIn("volume_long_upper_wick", {event.event_type for event in events})

    def test_warmup_records_macd_divergence_events(self) -> None:
        close_values = [
            100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
            110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
            120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
            130, 129, 128, 127, 126, 125, 124, 123, 122, 121,
            120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
            130, 131, 130, 129, 128,
        ]
        start = dt.datetime(2026, 1, 1)
        prices = pd.DataFrame(
            [
                {
                    "date": start + dt.timedelta(days=index),
                    "open": float(close),
                    "high": float(close) + 0.5,
                    "low": float(close) - 0.5,
                    "close": float(close),
                    "volume": 1000.0,
                }
                for index, close in enumerate(close_values)
            ]
        )

        with self.Session() as session:
            result = ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=lambda history, bar: [],
                lookback_years=0,
                snapshot_start_date=start,
                snapshot_end_date=start + dt.timedelta(days=len(close_values) - 1),
                as_of_date=start + dt.timedelta(days=len(close_values) - 1),
                force=True,
            )
            events = session.scalars(select(DivergenceEvent).order_by(DivergenceEvent.timestamp)).all()

        event_types = {event.event_type for event in events}
        self.assertIn("macd_bearish_divergence", event_types)
        self.assertNotIn("macd_bearish_divergence_risk", event_types)
        bearish = next(event for event in events if event.event_type == "macd_bearish_divergence")
        self.assertEqual(bearish.event_name, "顶背离")
        self.assertEqual(bearish.direction, "bearish")
        self.assertEqual(bearish.source, "macd_divergence")
        self.assertGreater(bearish.metadata_json["current_price"], bearish.metadata_json["previous_price"])
        self.assertLessEqual(bearish.metadata_json["current_dif"], bearish.metadata_json["previous_dif"])
        self.assertFalse(bearish.metadata_json["is_risk"])
        self.assertGreaterEqual(result.divergence_events, len(events))

    def test_bearish_divergence_requires_current_absolute_high_since_prior_swing(self) -> None:
        frame = pd.DataFrame(
            [
                {"timestamp": dt.datetime(2026, 1, 1), "high": 100.0, "low": 95.0, "dif": 1.0, "dea": 0.8, "histogram": 0.2},
                {"timestamp": dt.datetime(2026, 1, 2), "high": 105.0, "low": 96.0, "dif": 1.8, "dea": 1.0, "histogram": 0.8},
                {"timestamp": dt.datetime(2026, 1, 3), "high": 108.0, "low": 97.0, "dif": 2.0, "dea": 1.2, "histogram": 0.8},
                {"timestamp": dt.datetime(2026, 1, 4), "high": 107.0, "low": 98.0, "dif": 1.6, "dea": 1.1, "histogram": 0.5},
                {"timestamp": dt.datetime(2026, 1, 5), "high": 106.0, "low": 99.0, "dif": 1.5, "dea": 1.0, "histogram": 0.5},
            ]
        )

        events = _build_divergence_event(
            frame,
            symbol="AAPL",
            timeframe="1d",
            side="high",
            price_column="high",
            event_type="macd_bearish_divergence",
            event_name="顶背离",
            direction="bearish",
            previous_idx=1,
            current_idx=4,
            left_bars=1,
            right_bars=0,
            min_bar_distance=1,
        )

        self.assertEqual(events, [])

    def test_bullish_divergence_requires_current_absolute_low_since_prior_swing(self) -> None:
        frame = pd.DataFrame(
            [
                {"timestamp": dt.datetime(2026, 1, 1), "high": 110.0, "low": 100.0, "dif": -1.0, "dea": -0.8, "histogram": -0.2},
                {"timestamp": dt.datetime(2026, 1, 2), "high": 109.0, "low": 95.0, "dif": -1.8, "dea": -1.0, "histogram": -0.8},
                {"timestamp": dt.datetime(2026, 1, 3), "high": 108.0, "low": 92.0, "dif": -2.0, "dea": -1.2, "histogram": -0.8},
                {"timestamp": dt.datetime(2026, 1, 4), "high": 107.0, "low": 93.0, "dif": -1.6, "dea": -1.1, "histogram": -0.5},
                {"timestamp": dt.datetime(2026, 1, 5), "high": 106.0, "low": 94.0, "dif": -1.5, "dea": -1.0, "histogram": -0.5},
            ]
        )

        events = _build_divergence_event(
            frame,
            symbol="AAPL",
            timeframe="1d",
            side="low",
            price_column="low",
            event_type="macd_bullish_divergence",
            event_name="底背离",
            direction="bullish",
            previous_idx=1,
            current_idx=4,
            left_bars=1,
            right_bars=0,
            min_bar_distance=1,
        )

        self.assertEqual(events, [])

    def test_divergence_query_prefers_confirmed_over_risk_for_same_bar(self) -> None:
        event_time = dt.datetime(2026, 2, 1)
        events = [
            {
                "event_id": "risk",
                "event_time": event_time,
                "event_type": "macd_bullish_divergence_risk",
                "event_name": "底背离风险",
                "direction": "bullish",
                "source": "macd_divergence",
            },
            {
                "event_id": "confirmed",
                "event_time": event_time,
                "event_type": "macd_bullish_divergence",
                "event_name": "底背离",
                "direction": "bullish",
                "source": "macd_divergence",
            },
            {
                "event_id": "other-risk",
                "event_time": event_time + dt.timedelta(days=1),
                "event_type": "macd_bullish_divergence_risk",
                "event_name": "底背离风险",
                "direction": "bullish",
                "source": "macd_divergence",
            },
        ]

        filtered = _prefer_confirmed_over_risk(events)

        self.assertEqual(
            [event["event_id"] for event in filtered],
            ["confirmed", "other-risk"],
        )

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
                        "timeframes": {"W"},
                        "source_types": {"swing_w"},
                        "primary_timeframe": "W",
                        "zone_kind": ZoneKind.EVENT,
                        "origin_bar": dt.datetime(2026, 1, 2),
                        "origin_event_id": "selected-zone",
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
                        "type": "selected_deprecated_vp",
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
            ["selected-zone"],
        )
        self.assertNotIn(active_zone.zone_id, [row.zone_id for row in snapshot_rows])
        self.assertNotIn(invalidated_zone.zone_id, [row.zone_id for row in snapshot_rows])

    def test_replay_snapshot_query_filters_deprecated_zone_sources(self) -> None:
        with self.Session() as session:
            swing_zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.SUPPORT,
                origin_event_id="weekly-swing-low",
            )
            long_vp = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="long",
                zone_kind=ZoneKind.VP,
                source=["vp_long"],
                price_low=103.0,
                price_high=104.0,
                current_role=ZoneRole.RESISTANCE,
                vp_window_type="long_63d",
            )
            deprecated_vp = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="short",
                zone_kind=ZoneKind.VP,
                source=["vp_short"],
                price_low=98.0,
                price_high=99.0,
                current_role=ZoneRole.SUPPORT,
                vp_window_type="short_21d",
            )
            daily_event = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="D",
                zone_kind=ZoneKind.AVWAP,
                source=["avwap_d_event"],
                price_low=104.0,
                price_high=105.0,
                current_role=ZoneRole.RESISTANCE,
                origin_event_id="daily-event",
            )
            for zone in [swing_zone, long_vp, deprecated_vp, daily_event]:
                record_zone_snapshot(
                    session,
                    ZoneSnapshotInput(
                        zone_id=zone.zone_id,
                        snapshot_ts=dt.datetime(2026, 1, 2),
                        current_price=101.0,
                        atr=3.0,
                    ),
                )

            result = load_replay_zone_snapshots(
                session,
                symbol="AAPL",
                replay_date=dt.datetime(2026, 1, 2),
            )

        self.assertEqual([zone["zone_id"] for zone in result.all_zones], [swing_zone.zone_id])

    def test_zone_strength_is_persisted_to_zone_and_snapshot(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=100.0,
                price_high=102.0,
                current_role=ZoneRole.SUPPORT,
                origin_event_id="weekly-swing",
                zone_strength_pct=12.5,
            )
            snapshot = record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=zone.zone_id,
                    snapshot_ts=dt.datetime(2026, 1, 2),
                    current_price=101.0,
                    atr=3.0,
                ),
            )

        self.assertEqual(zone.zone_strength_pct, 12.5)
        self.assertEqual(snapshot.zone_strength_pct, 12.5)

    def test_replay_snapshot_query_returns_all_active_zones(self) -> None:
        with self.Session() as session:
            zones = []
            for index, center in enumerate([96.0, 98.0, 104.0, 106.0], start=1):
                role = ZoneRole.SUPPORT if center < 100.0 else ZoneRole.RESISTANCE
                zone = upsert_zone(
                    session,
                    symbol="AAPL",
                    timeframe="W",
                    zone_kind=ZoneKind.EVENT,
                    source=["swing_w"],
                    price_low=center - 0.5,
                    price_high=center + 0.5,
                    current_role=role,
                    origin_event_id=f"weekly-swing-{index}",
                )
                zones.append(zone)
                record_zone_snapshot(
                    session,
                    ZoneSnapshotInput(
                        zone_id=zone.zone_id,
                        snapshot_ts=dt.datetime(2026, 1, 2),
                        current_price=100.0,
                        atr=3.0,
                    ),
                )
            zones[0].status = ZoneStatus.EXPIRED
            record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=zones[0].zone_id,
                    snapshot_ts=dt.datetime(2026, 1, 2),
                    current_price=100.0,
                    atr=3.0,
                ),
            )

            result = load_replay_zone_snapshots(
                session,
                symbol="AAPL",
                replay_date=dt.datetime(2026, 1, 2),
            )

        self.assertEqual(len(result.support_zones), 1)
        self.assertEqual(len(result.resistance_zones), 2)
        self.assertNotIn(zones[0].zone_id, [zone["zone_id"] for zone in result.all_zones])

    def test_replay_snapshot_query_filters_weekly_swing_expired_at_snapshot_time(self) -> None:
        with self.Session() as session:
            zone = upsert_zone(
                session,
                symbol="AAPL",
                timeframe="W",
                zone_kind=ZoneKind.EVENT,
                source=["swing_w"],
                price_low=90.0,
                price_high=91.0,
                current_role=ZoneRole.SUPPORT,
                origin_bar=dt.datetime(2025, 6, 1),
                origin_event_id="old-weekly-swing",
            )
            record_zone_snapshot(
                session,
                ZoneSnapshotInput(
                    zone_id=zone.zone_id,
                    snapshot_ts=dt.datetime(2026, 1, 3),
                    current_price=100.0,
                    atr=3.0,
                ),
            )

            result = load_replay_zone_snapshots(
                session,
                symbol="AAPL",
                replay_date=dt.datetime(2026, 1, 3),
            )

        self.assertEqual(result.all_zones, [])

    def test_warmup_expires_old_weekly_swing_before_snapshot(self) -> None:
        prices = pd.DataFrame(
            [
                {
                    "date": dt.datetime(2026, 1, 3),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1_000.0,
                }
            ]
        )

        def old_swing_provider(history, bar):
            del history, bar
            return [
                {
                    "type": "weekly_swing_support",
                    "side": ZoneRole.SUPPORT,
                    "lower": 90.0,
                    "upper": 91.0,
                    "center": 90.5,
                    "timeframes": {"W"},
                    "source_types": {"swing_w"},
                    "primary_timeframe": "W",
                    "zone_kind": ZoneKind.EVENT,
                    "origin_bar": dt.datetime(2025, 6, 1),
                    "origin_event_id": "old-weekly-swing",
                    "origin_event_type": "swing",
                }
            ]

        with self.Session() as session:
            ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=old_swing_provider,
                snapshot_start_date=dt.datetime(2026, 1, 3),
                snapshot_end_date=dt.datetime(2026, 1, 3),
                force=True,
            )
            zone = session.scalars(select(Zone)).one()
            snapshot_count = session.scalar(select(func.count()).select_from(ZoneDailySnapshot))

        self.assertEqual(zone.status, ZoneStatus.EXPIRED)
        self.assertEqual(snapshot_count, 0)

    def test_warmup_snapshots_active_weekly_swing_even_when_not_reselected(self) -> None:
        prices = pd.DataFrame(
            [
                {
                    "date": dt.datetime(2026, 3, 19) + dt.timedelta(days=index),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1_000.0,
                }
                for index in range(2)
            ]
        )

        def first_day_only_provider(history, bar):
            del history
            if pd.Timestamp(bar.timestamp).date() != dt.date(2026, 3, 19):
                return []
            return [
                {
                    "type": "weekly_swing_support",
                    "side": ZoneRole.SUPPORT,
                    "lower": 90.0,
                    "upper": 91.0,
                    "center": 90.5,
                    "timeframes": {"W"},
                    "source_types": {"swing_w"},
                    "primary_timeframe": "W",
                    "zone_kind": ZoneKind.EVENT,
                    "origin_bar": dt.datetime(2026, 3, 6),
                    "origin_event_id": "weekly-swing-low",
                    "origin_event_type": "swing",
                }
            ]

        with self.Session() as session:
            ensure_symbol_lifecycle_ready(
                session,
                symbol="AAPL",
                price_df=prices,
                zone_provider=first_day_only_provider,
                snapshot_start_date=dt.datetime(2026, 3, 19),
                snapshot_end_date=dt.datetime(2026, 3, 20),
                force=True,
            )
            snapshots = session.scalars(
                select(ZoneDailySnapshot).order_by(ZoneDailySnapshot.snapshot_ts)
            ).all()

        self.assertEqual([snapshot.snapshot_ts for snapshot in snapshots], [
            dt.datetime(2026, 3, 19),
            dt.datetime(2026, 3, 20),
        ])

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
                long_vp_lookback_days=63,
                long_vp_bins=10,
                zone_expand_pct=0.001,
                max_resistance_zones=4,
                max_support_zones=4,
            ),
            interval_frames={"5m": prices, "1d": prices},
            include_all_candidates=True,
        )

        zones = provider(prices, None)

        self.assertGreater(len(zones), 0)
        self.assertTrue(all("zone_id" in zone for zone in zones))

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
                "timeframes": {"W"},
                "source_types": {"swing_w"},
                "source_label": "Swing high",
                "primary_timeframe": "W",
                "origin_bar": dt.datetime(2026, 1, 1),
                "origin_event_id": "warmup-swing-high",
                "origin_event_type": "swing_high",
            }
        ]

    def _zone_generation_prices(self) -> pd.DataFrame:
        rows: list[dict] = []
        start = dt.datetime(2025, 10, 1)
        weekly_levels = [100, 105, 101, 108, 102, 111, 104, 109, 101, 106, 99, 104]
        for index in range(80):
            week_index = min(index // 7, len(weekly_levels) - 1)
            base = float(weekly_levels[week_index]) + (index % 7) * 0.05
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

