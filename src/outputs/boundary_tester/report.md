# Boundary Tester Report

## 数据概览
- 总事件数: 183
- Breakout 事件数: 36
- 覆盖 ticker 数: 5

## 总体统计
- Success Rate: 58.33%
- Failure Rate: 41.67%
- Unresolved Rate: 0.00%
- False Breakout Rate: 41.67%
- Avg Follow Through: 4.71%
- Avg MFE: 10.60%
- Avg MAE: 3.63%

## 分组统计

### By Zone Class
```json
{
  "composite": {
    "avg_follow_through": -0.005474006488167946,
    "count": 11,
    "failure_rate": 0.5454545454545454,
    "success_rate": 0.45454545454545453,
    "unresolved_rate": 0.0
  },
  "cost": {
    "avg_follow_through": 0.07298132045134176,
    "count": 17,
    "failure_rate": 0.35294117647058826,
    "success_rate": 0.6470588235294118,
    "unresolved_rate": 0.0
  },
  "inventory": {
    "avg_follow_through": 0.06418018383923459,
    "count": 8,
    "failure_rate": 0.375,
    "success_rate": 0.625,
    "unresolved_rate": 0.0
  }
}
```

### By First Test Flag
```json
{
  "repeated_test": {
    "avg_follow_through": 0.04705305130602331,
    "count": 36,
    "failure_rate": 0.4166666666666667,
    "success_rate": 0.5833333333333334,
    "unresolved_rate": 0.0
  }
}
```

### By Timeframe
```json
{
  "D": {
    "avg_follow_through": 0.03857309625167168,
    "count": 21,
    "failure_rate": 0.47619047619047616,
    "success_rate": 0.5238095238095238,
    "unresolved_rate": 0.0
  },
  "D,W": {
    "avg_follow_through": -0.008034281627346145,
    "count": 9,
    "failure_rate": 0.4444444444444444,
    "success_rate": 0.5555555555555556,
    "unresolved_rate": 0.0
  },
  "W": {
    "avg_follow_through": 0.15936389339630824,
    "count": 6,
    "failure_rate": 0.16666666666666666,
    "success_rate": 0.8333333333333334,
    "unresolved_rate": 0.0
  }
}
```

### By Confluence Bucket
```json
{
  "3-source+": {
    "avg_follow_through": 0.048967250169319895,
    "count": 5,
    "failure_rate": 0.4,
    "success_rate": 0.6,
    "unresolved_rate": 0.0
  },
  "double-source": {
    "avg_follow_through": -0.05084172036940782,
    "count": 6,
    "failure_rate": 0.6666666666666666,
    "success_rate": 0.3333333333333333,
    "unresolved_rate": 0.0
  },
  "single-source": {
    "avg_follow_through": 0.07016495673546747,
    "count": 25,
    "failure_rate": 0.36,
    "success_rate": 0.64,
    "unresolved_rate": 0.0
  }
}
```

## 主要发现
- Breakout 样本以 `cost` 为主。
- 首测与重复测试中表现更优的组别: `repeated_test`。
- 时间框架表现最优组: `W`。

## 配置快照
```json
{
  "atr_multiple_success": 1.5,
  "atr_window": 14,
  "breakout_buffer_pct": 0.002,
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