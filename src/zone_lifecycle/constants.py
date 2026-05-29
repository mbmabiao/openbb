from __future__ import annotations


class ZoneKind:
    EVENT = "event"
    AVWAP = "avwap"
    VP = "vp"
    COMPOSITE = "composite"


class ZoneStatus:
    ACTIVE = "active"
    BROKEN = "broken"
    FLIPPED = "flipped"
    RETESTED = "retested"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"


class BreakoutEventStatus:
    ATTEMPT = "attempt"
    CONFIRMED = "confirmed"
    TRUE_BREAKOUT_STRONG = "true_breakout_strong"
    TRUE_BREAKOUT_WEAK = "true_breakout_weak"
    RECLAIMED = "reclaimed"
    RETESTING = "retesting"
    RETEST_SUCCESS = "retest_success"


class ZoneRole:
    SUPPORT = "support"
    RESISTANCE = "resistance"
    NEUTRAL = "neutral"


ACTIVE_ZONE_STATUSES = {
    ZoneStatus.ACTIVE,
    ZoneStatus.BROKEN,
    ZoneStatus.FLIPPED,
    ZoneStatus.RETESTED,
}


DEPRECATED_ZONE_SOURCES = {
    "avwap_d_event",
    "avwap_d_swing",
    "avwap_short_rolling",
    "avwap_w_event",
    "avwap_w_swing",
    "vp_long",
    "vp_short",
}


ZONE_STATUS_RANK = {
    ZoneStatus.RETESTED: 0,
    ZoneStatus.FLIPPED: 1,
    ZoneStatus.ACTIVE: 2,
    ZoneStatus.BROKEN: 3,
}


EVENT_ZONE_TTL_BARS = {
    "5m": 300,
    "15m": 300,
    "1h": 300,
    "1d": 63,
    "d": 63,
    "1w": 26,
    "w": 26,
}


BREAKOUT_TERMINAL_STATUSES = {
    BreakoutEventStatus.TRUE_BREAKOUT_STRONG,
    BreakoutEventStatus.RETEST_SUCCESS,
}


STATUS_PRIORITY = [
    BreakoutEventStatus.RETEST_SUCCESS,
    BreakoutEventStatus.TRUE_BREAKOUT_STRONG,
    BreakoutEventStatus.TRUE_BREAKOUT_WEAK,
    BreakoutEventStatus.RECLAIMED,
    BreakoutEventStatus.RETESTING,
    BreakoutEventStatus.CONFIRMED,
    BreakoutEventStatus.ATTEMPT,
]
