from __future__ import annotations


class ZoneKind:
    EVENT = "event"
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
    FAILED_BREAKOUT = "failed_breakout"
    FALSE_BREAKOUT = "false_breakout"
    RECLAIMED = "reclaimed"
    RETESTING = "retesting"
    RETEST_SUCCESS = "retest_success"
    RETEST_FAILED = "retest_failed"


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
    BreakoutEventStatus.TRUE_BREAKOUT_WEAK,
    BreakoutEventStatus.FAILED_BREAKOUT,
    BreakoutEventStatus.FALSE_BREAKOUT,
    BreakoutEventStatus.RETEST_SUCCESS,
    BreakoutEventStatus.RETEST_FAILED,
}


STATUS_PRIORITY = [
    BreakoutEventStatus.FAILED_BREAKOUT,
    BreakoutEventStatus.RETEST_FAILED,
    BreakoutEventStatus.FALSE_BREAKOUT,
    BreakoutEventStatus.RETEST_SUCCESS,
    BreakoutEventStatus.TRUE_BREAKOUT_STRONG,
    BreakoutEventStatus.TRUE_BREAKOUT_WEAK,
    BreakoutEventStatus.RECLAIMED,
    BreakoutEventStatus.RETESTING,
    BreakoutEventStatus.CONFIRMED,
    BreakoutEventStatus.ATTEMPT,
]
