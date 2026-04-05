# Boundary Tester Report

## Data Overview
- Raw interactions: 254
- Breakout labeled events: 180
- Defense labeled events: 180
- Structural zones: 374

## Zone Defense Summary
- Hold Rate: 8.20%
- Failed Hold Rate: 91.80%
- Unresolved Rate: 0.00%
- Avg Reversal Strength: 6.18%

### Defense By Touch Count
```json
{
  "first_touch": {
    "avg_reversal_strength": 0.05965654804671635,
    "avg_zone_defense_score": 2.4964935414307936,
    "count": 85,
    "failed_hold_rate": 0.9294117647058824,
    "hold_rate": 0.07058823529411765,
    "unresolved_rate": 0.0
  },
  "second_touch": {
    "avg_reversal_strength": 0.06670216098692607,
    "avg_zone_defense_score": 2.4755747300282875,
    "count": 37,
    "failed_hold_rate": 0.8918918918918919,
    "hold_rate": 0.10810810810810811,
    "unresolved_rate": 0.0
  }
}
```

## Breakout Continuation Summary
- Success Rate: 55.00%
- Failure Rate: 40.00%
- False Breakout Rate: 35.00%
- Failed Follow Through Rate: 0.00%
- Hold Rate After Breakout: 87.50%
- Event Per Structural Zone: 1.00

### Breakout By Failure Subtype
```json
{
  "fast_false_breakout": {
    "avg_follow_through": -0.04785029143126742,
    "count": 14,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "median_follow_through": -0.05176231586226952,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  },
  "late_failure": {
    "avg_follow_through": 0.004393051384034646,
    "count": 2,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 0.0,
    "median_follow_through": 0.004393051384034646,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  },
  "none": {
    "avg_follow_through": 0.004072175080772494,
    "count": 22,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 0.0,
    "false_breakout_rate": 0.0,
    "median_follow_through": 0.011865088494033232,
    "success_rate": 1.0,
    "unresolved_rate": 0.0
  },
  "unresolved": {
    "avg_follow_through": 0.014176540921013156,
    "count": 2,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 0.0,
    "false_breakout_rate": 0.0,
    "median_follow_through": 0.014176540921013156,
    "success_rate": 0.0,
    "unresolved_rate": 1.0
  }
}
```

### Breakout By Touch Count
```json
{
  "second_touch": {
    "avg_follow_through": -0.013579426091266337,
    "count": 40,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 0.4,
    "false_breakout_rate": 0.35,
    "median_follow_through": 0.0017817017747225712,
    "success_rate": 0.55,
    "unresolved_rate": 0.05
  }
}
```

## Config Snapshot
```json
{
  "atr_multiple_success": 1.5,
  "atr_window": 14,
  "breakout_buffer_pct": 0.002,
  "defense_reversal_pct": 0.01,
  "event_emission_gap_bars": 3,
  "failed_breakout_min_consecutive_inside_bars": 2,
  "failed_breakout_reentry_depth_frac": 0.25,
  "failure_reentry_bars": 5,
  "hold_outside_bars_required": 2,
  "lookahead_bars": 20,
  "min_breakout_distance_atr": 0.0,
  "min_breakout_distance_pct": 0.0,
  "min_close_outside_zone": 2,
  "probe_buffer_pct": 0.001,
  "require_retest_success": false,
  "retest_buffer_pct": 0.002,
  "success_move_mode": "fixed_pct",
  "success_move_pct": 0.03,
  "success_move_zone_width_multiple": 1.0,
  "touch_merge_gap_bars": 3,
  "use_atr_filter": false,
  "zone_universe_mode": "selected_only"
}
```