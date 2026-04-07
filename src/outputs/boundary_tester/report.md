# Boundary Tester Report

## Data Overview
- Raw interactions: 963
- Breakout labeled events: 702
- Defense labeled events: 702
- Structural zones: 1466

## Zone Defense Summary
- Hold Rate: 8.21%
- Failed Hold Rate: 90.65%
- Unresolved Rate: 1.15%
- Avg Reversal Strength: 7.53%

### Defense By Touch Count
```json
{
  "first_touch": {
    "avg_reversal_strength": 0.07800158393357906,
    "avg_zone_defense_score": 2.68339235613189,
    "count": 354,
    "failed_hold_rate": 0.8870056497175142,
    "hold_rate": 0.096045197740113,
    "unresolved_rate": 0.01694915254237288
  },
  "second_touch": {
    "avg_reversal_strength": 0.06960239831613461,
    "avg_zone_defense_score": 1.8810712262978635,
    "count": 170,
    "failed_hold_rate": 0.9470588235294117,
    "hold_rate": 0.052941176470588235,
    "unresolved_rate": 0.0
  }
}
```

## Breakout Continuation Summary
- Success Rate: 54.81%
- Failure Rate: 41.48%
- False Breakout Rate: 37.04%
- Failed Follow Through Rate: 1.48%
- Hold Rate After Breakout: 89.63%
- Event Per Structural Zone: 1.00

### Breakout By Failure Subtype
```json
{
  "failed_follow_through": {
    "avg_follow_through": -0.06679373057157395,
    "count": 2,
    "failed_follow_through_rate": 1.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 0.0,
    "median_follow_through": -0.06679373057157395,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  },
  "fast_false_breakout": {
    "avg_follow_through": -0.04890732670438281,
    "count": 50,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "median_follow_through": -0.04159477476086082,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  },
  "late_failure": {
    "avg_follow_through": -0.13217279625086323,
    "count": 4,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 0.0,
    "median_follow_through": -0.08709337572465577,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  },
  "none": {
    "avg_follow_through": 0.03464582589843092,
    "count": 74,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 0.0,
    "false_breakout_rate": 0.0,
    "median_follow_through": 0.026497309314366585,
    "success_rate": 1.0,
    "unresolved_rate": 0.0
  },
  "unresolved": {
    "avg_follow_through": 0.018003173909950664,
    "count": 5,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 0.0,
    "false_breakout_rate": 0.0,
    "median_follow_through": 0.010580833781037143,
    "success_rate": 0.0,
    "unresolved_rate": 1.0
  }
}
```

### Breakout By Touch Count
```json
{
  "second_touch": {
    "avg_follow_through": -0.0033617629283859275,
    "count": 135,
    "failed_follow_through_rate": 0.014814814814814815,
    "failure_rate": 0.4148148148148148,
    "false_breakout_rate": 0.37037037037037035,
    "median_follow_through": -0.007674795070072814,
    "success_rate": 0.5481481481481482,
    "unresolved_rate": 0.037037037037037035
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