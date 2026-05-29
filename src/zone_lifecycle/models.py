from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from .constants import ZoneRole, ZoneStatus


class Base(DeclarativeBase):
    pass


class Zone(Base):
    __tablename__ = "zones"

    zone_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    zone_kind: Mapped[str] = mapped_column(String(24), index=True, nullable=False)
    source: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    price_center: Mapped[float] = mapped_column(Float, nullable=False)
    price_high: Mapped[float] = mapped_column(Float, nullable=False)
    price_low: Mapped[float] = mapped_column(Float, nullable=False)
    current_role: Mapped[str] = mapped_column(String(24), index=True, default=ZoneRole.NEUTRAL, nullable=False)
    status: Mapped[str] = mapped_column(String(24), index=True, default=ZoneStatus.ACTIVE, nullable=False)
    zone_strength_pct: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    origin_bar: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    origin_event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    origin_event_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    retest_num: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    break_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    touch_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    false_break_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    close_inside_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    confirmed_breakout_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_breakout_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    invalidated_ts: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    expired_ts: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    vp_window_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    snapshots: Mapped[list["ZoneDailySnapshot"]] = relationship(
        back_populates="zone",
        cascade="all, delete-orphan",
    )
    breakout_events: Mapped[list["BreakoutEvent"]] = relationship(
        back_populates="zone",
        cascade="all, delete-orphan",
    )


class ZoneDailySnapshot(Base):
    __tablename__ = "zone_daily_snapshots"
    __table_args__ = (
        UniqueConstraint("zone_id", "snapshot_ts", name="uq_zone_daily_snapshot_zone_ts"),
    )

    snapshot_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    zone_id: Mapped[str] = mapped_column(ForeignKey("zones.zone_id"), index=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    snapshot_ts: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    current_price: Mapped[float] = mapped_column(Float, nullable=False)
    price_low: Mapped[float] = mapped_column(Float, nullable=False)
    price_high: Mapped[float] = mapped_column(Float, nullable=False)
    price_center: Mapped[float] = mapped_column(Float, nullable=False)
    distance_to_price: Mapped[float] = mapped_column(Float, nullable=False)
    distance_atr: Mapped[float | None] = mapped_column(Float, nullable=True)
    zone_status: Mapped[str] = mapped_column(String(24), index=True, nullable=False)
    current_role: Mapped[str] = mapped_column(String(24), index=True, nullable=False)
    zone_strength_pct: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

    zone: Mapped[Zone] = relationship(back_populates="snapshots")


class MarketObservation(Base):
    __tablename__ = "market_observations"
    __table_args__ = (
        UniqueConstraint("symbol", "snapshot_ts", "observation_type", "label", name="uq_market_observation_key"),
    )

    observation_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    snapshot_ts: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    observation_type: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    label: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class PatternEvent(Base):
    __tablename__ = "pattern_events"
    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", "event_time", "event_type", name="uq_pattern_event_key"),
    )

    event_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(48), index=True, nullable=False)
    direction: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    price_open: Mapped[float] = mapped_column(Float, nullable=False)
    price_high: Mapped[float] = mapped_column(Float, nullable=False)
    price_low: Mapped[float] = mapped_column(Float, nullable=False)
    price_close: Mapped[float] = mapped_column(Float, nullable=False)
    previous_close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    body_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    upper_wick_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    lower_wick_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    close_position: Mapped[float] = mapped_column(Float, nullable=False)
    price_change_pct: Mapped[float] = mapped_column(Float, nullable=False)
    intrabar_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    gap_pct: Mapped[float] = mapped_column(Float, nullable=False)
    abs_price_change_pct: Mapped[float] = mapped_column(Float, nullable=False)
    volume_percentile_20: Mapped[float] = mapped_column(Float, nullable=False)
    abs_price_change_percentile_20: Mapped[float] = mapped_column(Float, nullable=False)
    lookback_bars: Mapped[int] = mapped_column(Integer, nullable=False)
    related_zone_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class DivergenceEvent(Base):
    __tablename__ = "divergence_events"
    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", "timestamp", "event_type", name="uq_divergence_event_key"),
    )

    event_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(48), index=True, nullable=False)
    event_name: Mapped[str] = mapped_column(String(64), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    direction: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    strength_score: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class BreakoutEvent(Base):
    __tablename__ = "breakout_events"

    breakout_event_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    zone_id: Mapped[str] = mapped_column(ForeignKey("zones.zone_id"), index=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    direction: Mapped[str] = mapped_column(String(12), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    breakout_bar: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    breakout_close: Mapped[float] = mapped_column(Float, nullable=False)
    atr_at_breakout: Mapped[float] = mapped_column(Float, nullable=False)
    max_high_after_breakout: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_low_after_breakout: Mapped[float | None] = mapped_column(Float, nullable=True)
    follow_through_atr: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    zone: Mapped[Zone] = relationship(back_populates="breakout_events")


class SymbolLifecycleState(Base):
    __tablename__ = "symbol_lifecycle_state"
    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", name="uq_symbol_lifecycle_state_symbol_timeframe"),
    )

    state_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    warmup_start_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_processed_ts: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    lookback_years: Mapped[int] = mapped_column(Integer, nullable=False)
    created_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
