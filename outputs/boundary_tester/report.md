# Boundary Tester Report

## Data Overview / 数据概览
- Total events: 12
- Breakout events: 2
- Covered tickers: 3

## Aggregate Metrics / 总体统计
- Success Rate: 0.00%
- Failure Rate: 100.00%
- Unresolved Rate: 0.00%
- False Breakout Rate: 100.00%
- Failed Follow Through Rate: 0.00%
- Avg Follow Through: -2.33%
- Avg MFE: -0.24%
- Avg MAE: 5.46%

## Segment Summaries / 分组统计

### By Zone Class
```json
{
  "composite": {
    "avg_follow_through": -0.043891552416845676,
    "count": 1,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  },
  "cost": {
    "avg_follow_through": -0.0027757338262369966,
    "count": 1,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  }
}
```

### By First Test Flag
```json
{
  "repeated_test": {
    "avg_follow_through": -0.023333643121541337,
    "count": 2,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  }
}
```

### By Timeframe
```json
{
  "D": {
    "avg_follow_through": -0.0027757338262369966,
    "count": 1,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  },
  "D,W": {
    "avg_follow_through": -0.043891552416845676,
    "count": 1,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  }
}
```

### By Confluence Bucket
```json
{
  "3-source+": {
    "avg_follow_through": -0.043891552416845676,
    "count": 1,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  },
  "single-source": {
    "avg_follow_through": -0.0027757338262369966,
    "count": 1,
    "failed_follow_through_rate": 0.0,
    "failure_rate": 1.0,
    "false_breakout_rate": 1.0,
    "success_rate": 0.0,
    "unresolved_rate": 0.0
  }
}
```

## Key Findings / 主要发现
- Dominant breakout zone class: `composite`.
- Best first-test bucket by success rate: `repeated_test`.
- Best timeframe bucket by success rate: `D`.

## Config Snapshot / 配置快照
```json
{
  "atr_multiple_success": 1.5,
  "atr_window": 14,
  "breakout_buffer_pct": 0.002,
  "failed_breakout_min_consecutive_inside_bars": 2,
  "failed_breakout_reentry_depth_frac": 0.25,
  "failure_reentry_bars": 5,
  "lookahead_bars": 20,
  "max_event_gap": 3,
  "min_close_outside_zone": 2,
  "probe_buffer_pct": 0.001,
  "retest_buffer_pct": 0.002,
  "success_move_pct": 0.03,
  "use_atr_filter": false
}
```