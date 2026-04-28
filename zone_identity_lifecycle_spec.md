# Zone 身份化与生命周期技术设计文档

## 1. 总结

本设计的目标是将 zone 从一次性计算结果升级为可持久追踪的市场结构实体。

当前核心问题是：

> old support / old resistance 不应因为新的 anchor 或新的 zone 检测结果而被直接覆盖消失。

因此，系统需要为每一个 zone 建立唯一身份、状态、生命周期、来源关系和交互历史。

本设计固定采用以下原则：

1. 每个 zone 必须有唯一 `zone_id`。
2. 新 anchor 不能覆盖旧 zone，只能创建新 zone 或更新已有 zone 状态。
3. event zone 有生命周期。
4. VP zone 持续滚动追踪，不按普通 event 生命周期自动失效。
5. composite zone 必须记录由哪些 zone 合并而来。
6. zone 被突破后不能删除，只能进入 `broken` / `flipped` / `retested` / `invalidated` 等状态。
7. breakout event 必须绑定具体 `zone_id`。
8. 当前交易判断只使用有效且接近当前价格的 zone。

---

## 2. 术语定义

| 术语 | 定义 |
|---|---|
| Zone | 一个价格区间，代表潜在支撑、阻力或成交量结构 |
| Event Zone | 由明确市场事件生成的 zone，例如前高、前低、swing high、swing low、gap |
| VP Zone | 由 Volume Profile 生成的滚动追踪 zone |
| Composite Zone | 多个 zone 合并后形成的复合 zone |
| BreakoutEvent | 针对某个 zone 的突破、跌破、回踩、失败等事件记录 |
| ATR | Average True Range，平均真实波幅，用于标准化距离和 buffer |

---

## 3. Zone 类型设计

### 3.1 zone_kind 枚举

```python
class ZoneKind:
    EVENT = "event"
    VP = "vp"
    COMPOSITE = "composite"
```

### 3.2 Event Zone

Event Zone 来源于明确的离散市场事件。

允许来源：

```python
EVENT_SOURCES = [
    "high",
    "low",
    "swing_high",
    "swing_low",
    "gap_up",
    "gap_down",
]
```

Event Zone 特征：

1. 有明确 `origin_bar`。
2. 有明确 `origin_event_id`。
3. 有明确 `origin_event_type`。
4. 有生命周期。
5. 可发生突破、跌破、翻转、回踩、反抽、失效。

### 3.3 VP Zone

VP Zone 来源于 Volume Profile，即成交量分布结构。

允许来源：

```python
VP_SOURCES = [
    "vp_poc",
    "vp_vah",
    "vp_val",
    "vp_hvn",
    "vp_lvn",
]
```

VP Zone 特征：

1. 不按普通 event 生命周期自动失效。
2. 随 VP 计算窗口滚动更新价格区间。
3. 需要保留稳定的 `zone_id`。
4. `origin_bar` 固定为 `None`。
5. 必须填写 `vp_window_type`。

### 3.4 Composite Zone

Composite Zone 是由多个 zone 合并产生的复合 zone。

Composite Zone 特征：

1. 必须有独立 `zone_id`。
2. 必须记录 `merged_from_zone_ids`。
3. 可以参与 breakout 判断。
4. 生命周期由其组成 zone 的状态决定。

---

## 4. Zone 表结构

建议表名：

```text
zones
```

### 4.1 字段定义

| 字段 | 类型 | 必填 | 说明 |
|---|---:|---:|---|
| zone_id | str | 是 | zone 唯一 ID |
| symbol | str | 是 | 股票或标的，例如 AAPL、TSLA |
| timeframe | str | 是 | K 线级别，例如 5m、1h、1d、1w |
| zone_kind | str | 是 | event / vp / composite |
| source | list[str] | 是 | zone 来源，可一个或多个 |
| price_center | float | 是 | zone 中线价格 |
| price_high | float | 是 | zone 上边界 |
| price_low | float | 是 | zone 下边界 |
| current_role | str | 是 | support / resistance / neutral |
| status | str | 是 | active / broken / flipped / retested / expired / invalidated |
| origin_bar | datetime | None | 否 | zone 最原始 event 所在 K 线时间，VP zone 为 None |
| origin_event_id | str | None | 否 | 创建该 zone 的原始事件 ID |
| origin_event_type | str | None | 否 | 创建该 zone 的原始事件类型 |
| retest_num | int | 是 | 回踩 / 反抽次数 |
| break_count | int | 是 | zone 中线被 high/low 穿越次数 |
| touch_count | int | 是 | 收盘价在 zone 内且未形成 confirmed breakout 的次数 |
| false_break_count | int | 是 | 影线突破边界但收盘回到 zone 内的次数 |
| close_inside_count | int | 是 | 收盘价在 zone 内的次数 |
| confirmed_breakout_count | int | 是 | 有效收盘突破 / 跌破次数 |
| failed_breakout_count | int | 是 | confirmed 后失败的次数 |
| created_ts | datetime | 是 | zone 创建时间 |
| updated_ts | datetime | 是 | zone 最近更新时间 |
| invalidated_ts | datetime | None | 否 | 明确失效时间 |
| expired_ts | datetime | None | 否 | 生命周期结束时间 |
| vp_window_type | str | None | 否 | VP 窗口类型，仅 VP zone 使用 |
| merged_from_zone_ids | list[str] | None | 否 | composite zone 来源 zone ID 列表 |

---

## 5. Zone ID 生成规则

### 5.1 Event Zone ID

Event Zone 的 `zone_id` 必须由以下字段生成：

```python
zone_id = hash(
    symbol,
    timeframe,
    zone_kind,
    source,
    origin_bar,
    origin_event_id,
    round(price_low, 4),
    round(price_high, 4),
)
```

要求：

1. 同一原始事件不能重复创建多个不同 `zone_id`。
2. 新 anchor 不能覆盖旧 `zone_id`。
3. 如果新检测结果与旧 zone 高度重叠，应走 merge 或 update 逻辑，不允许直接删除旧 zone。

### 5.2 VP Zone ID

VP Zone 的 `zone_id` 必须由以下字段生成：

```python
zone_id = hash(
    symbol,
    timeframe,
    "vp",
    vp_window_type,
    source,
)
```

说明：

1. VP zone 没有 `origin_bar`。
2. VP zone 的价格区间可以滚动更新。
3. 同一个 `symbol + timeframe + vp_window_type + source` 应保持同一个 `zone_id`。

### 5.3 Composite Zone ID

Composite Zone 的 `zone_id` 必须由来源 zone ID 列表生成：

```python
zone_id = hash(
    symbol,
    timeframe,
    "composite",
    sorted(merged_from_zone_ids),
)
```

---

## 6. Zone 状态枚举

```python
class ZoneStatus:
    ACTIVE = "active"
    BROKEN = "broken"
    FLIPPED = "flipped"
    RETESTED = "retested"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"
```

### 6.1 active

定义：

当前有效，可参与交易判断。

进入条件：

1. 新 event zone 创建后默认为 `active`。
2. 当前 VP zone 默认为 `active`。
3. composite zone 创建后默认为 `active`。

### 6.2 broken

定义：

价格已对 zone 形成 confirmed breakout，但尚未确认失效或完成角色转换。

进入条件：

向上突破 resistance：

```python
close > price_high + breakout_buffer
```

向下跌破 support：

```python
close < price_low - breakout_buffer
```

### 6.3 flipped

定义：

zone 已完成支撑阻力角色切换。

进入条件：

1. 原 `resistance` 被向上 confirmed breakout 后，`current_role` 改为 `support`，`status` 改为 `flipped`。
2. 原 `support` 被向下 confirmed breakdown 后，`current_role` 改为 `resistance`，`status` 改为 `flipped`。

### 6.4 retested

定义：

突破后的回踩成功，或跌破后的反抽成功。

进入条件：

BreakoutEvent 状态进入 `retest_success` 后，绑定 zone 的 `status` 改为 `retested`。

### 6.5 expired

定义：

zone 达到生命周期上限，但不是价格结构明确失败。

进入条件：

1. 日线 event zone 创建后超过 63 根日 K。
2. 周线 event zone 创建后超过 26 根周 K。
3. 其他 timeframe 的 event zone 使用配置表中的生命周期。

### 6.6 invalidated

定义：

zone 被价格结构明确否定。

进入条件：

向上突破后的失败：

```python
close < price_low - failure_buffer
```

向下跌破后的失败：

```python
close > price_high + failure_buffer
```

---

## 7. Zone 生命周期规则

### 7.1 Event Zone 生命周期

Event Zone 必须按 K 线数量失效。

固定规则：

| timeframe | 生命周期 |
|---|---:|
| 1d | 63 根日 K |
| 1w | 26 根周 K |

如果 timeframe 不是 `1d` 或 `1w`，使用配置：

```python
EVENT_ZONE_TTL_BARS = {
    "5m": 300,
    "15m": 300,
    "1h": 300,
    "1d": 63,
    "1w": 26,
}
```

处理规则：

```python
if zone.zone_kind == "event" and bars_since_created >= ttl_bars:
    zone.status = "expired"
    zone.expired_ts = current_ts
```

### 7.2 VP Zone 生命周期

VP Zone 不按 event TTL 自动失效。

固定规则：

```python
if zone.zone_kind == "vp":
    skip_event_ttl_expiry = True
```

VP zone 只在以下情况下失效：

1. VP 计算窗口被系统配置废弃。
2. VP 数据源不可用，且连续不可用超过系统配置阈值。
3. 用户主动关闭该 VP 类型。

### 7.3 Composite Zone 生命周期

Composite Zone 生命周期规则：

1. 如果所有来源 zone 都是 `expired`，composite zone 进入 `expired`。
2. 如果任一核心来源 zone 是 `invalidated`，且没有 VP 来源支撑，composite zone 进入 `invalidated`。
3. 如果 composite zone 包含有效 VP zone，则保持 `active`，除非价格结构明确 invalidated。

---

## 8. Zone 交互计数规则

所有计数均在每根新 K 线收盘后更新。

### 8.1 close_inside_count

定义：

收盘价在 zone 内的次数。

规则：

```python
if price_low <= close <= price_high:
    close_inside_count += 1
```

### 8.2 touch_count

定义：

收盘价在 zone 内，且当前 bar 没有形成 confirmed breakout。

规则：

```python
is_close_inside = price_low <= close <= price_high
is_confirmed_up = close > price_high + breakout_buffer
is_confirmed_down = close < price_low - breakout_buffer

if is_close_inside and not is_confirmed_up and not is_confirmed_down:
    touch_count += 1
```

### 8.3 break_count

定义：

当前 bar 的 high / low 穿越 zone 中线。影线穿越也算。

规则：

```python
if low <= price_center <= high:
    break_count += 1
```

### 8.4 false_break_count

定义：

影线越过 zone 边界，但收盘价回到 zone 内。

向上 false break：

```python
if high > price_high + breakout_buffer and price_low <= close <= price_high:
    false_break_count += 1
```

向下 false break：

```python
if low < price_low - breakout_buffer and price_low <= close <= price_high:
    false_break_count += 1
```

### 8.5 confirmed_breakout_count

定义：

收盘价有效突破 zone 边界。

向上 confirmed breakout：

```python
if close > price_high + breakout_buffer:
    confirmed_breakout_count += 1
```

向下 confirmed breakdown：

```python
if close < price_low - breakout_buffer:
    confirmed_breakout_count += 1
```

### 8.6 failed_breakout_count

定义：

confirmed breakout 后，后续失败。

向上突破失败：

```python
if breakout_direction == "up" and close < price_low - failure_buffer:
    failed_breakout_count += 1
```

向下跌破失败：

```python
if breakout_direction == "down" and close > price_high + failure_buffer:
    failed_breakout_count += 1
```

### 8.7 retest_num

定义：

confirmed breakout 后，价格回到 zone 区间测试新角色的次数。

向上突破后的回踩：

```python
if breakout_direction == "up" and low <= price_high and close >= price_low:
    retest_num += 1
```

向下跌破后的反抽：

```python
if breakout_direction == "down" and high >= price_low and close <= price_high:
    retest_num += 1
```

---

## 9. BreakoutEvent 表结构

建议表名：

```text
breakout_events
```

### 9.1 字段定义

| 字段 | 类型 | 必填 | 说明 |
|---|---:|---:|---|
| breakout_event_id | str | 是 | breakout event 唯一 ID |
| zone_id | str | 是 | 绑定的 zone ID |
| symbol | str | 是 | 股票或标的 |
| timeframe | str | 是 | K 线级别 |
| direction | str | 是 | up / down |
| status | str | 是 | breakout event 当前状态 |
| breakout_bar | datetime | 是 | 首次 confirmed breakout 的 K 线时间 |
| breakout_close | float | 是 | confirmed breakout 的收盘价 |
| atr_at_breakout | float | 是 | breakout 时的 ATR |
| max_high_after_breakout | float | 否 | confirmed 后观察窗口内最高价 |
| min_low_after_breakout | float | 否 | confirmed 后观察窗口内最低价 |
| follow_through_atr | float | 否 | 用 ATR 标准化后的最大有利幅度 |
| created_ts | datetime | 是 | 创建时间 |
| updated_ts | datetime | 是 | 更新时间 |

---

## 10. BreakoutEvent 状态枚举

```python
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
```

---

## 11. BreakoutEvent 判定规则

### 11.1 全局参数

系统固定使用以下参数：

```python
BREAKOUT_CONFIRM_BUFFER_ATR = 0.10
FAILURE_BUFFER_ATR = 0.10
STRONG_FOLLOW_THROUGH_ATR = 1.00
WEAK_FOLLOW_THROUGH_ATR = 0.30
FOLLOW_THROUGH_WINDOW_BARS = 5
FAST_FAILURE_WINDOW_BARS = 3
FAILURE_WINDOW_BARS = 10
RETEST_WINDOW_BARS = 10
```

计算：

```python
breakout_buffer = BREAKOUT_CONFIRM_BUFFER_ATR * atr
failure_buffer = FAILURE_BUFFER_ATR * atr
```

---

### 11.2 attempt

定义：

价格已经触碰或刺破 zone 的突破方向边界，但未形成有效收盘突破。

向上：

```python
if high > price_high and close <= price_high + breakout_buffer:
    status = "attempt"
```

向下：

```python
if low < price_low and close >= price_low - breakout_buffer:
    status = "attempt"
```

---

### 11.3 confirmed

定义：

价格在突破方向一侧完成有效收盘，且突破距离超过 buffer。

向上：

```python
if close > price_high + breakout_buffer:
    status = "confirmed"
```

向下：

```python
if close < price_low - breakout_buffer:
    status = "confirmed"
```

Zone 同步更新：

向上突破：

```python
zone.status = "flipped"
zone.current_role = "support"
zone.confirmed_breakout_count += 1
```

向下跌破：

```python
zone.status = "flipped"
zone.current_role = "resistance"
zone.confirmed_breakout_count += 1
```

---

### 11.4 true_breakout_strong

定义：

confirmed 后 5 根 K 线内，最大有利幅度达到 `1.0 * ATR`，且没有被收回。

向上：

```python
if (
    max_high_after_breakout - breakout_close >= STRONG_FOLLOW_THROUGH_ATR * atr
    and no_close_below_price_high_within_window
):
    status = "true_breakout_strong"
```

向下：

```python
if (
    breakout_close - min_low_after_breakout >= STRONG_FOLLOW_THROUGH_ATR * atr
    and no_close_above_price_low_within_window
):
    status = "true_breakout_strong"
```

---

### 11.5 true_breakout_weak

定义：

confirmed 后 5 根 K 线内，最大有利幅度达到 `0.3 * ATR`，但小于 `1.0 * ATR`，且没有失败。

向上：

```python
follow_through = max_high_after_breakout - breakout_close

if (
    WEAK_FOLLOW_THROUGH_ATR * atr <= follow_through < STRONG_FOLLOW_THROUGH_ATR * atr
    and no_close_below_price_low_within_window
):
    status = "true_breakout_weak"
```

向下：

```python
follow_through = breakout_close - min_low_after_breakout

if (
    WEAK_FOLLOW_THROUGH_ATR * atr <= follow_through < STRONG_FOLLOW_THROUGH_ATR * atr
    and no_close_above_price_high_within_window
):
    status = "true_breakout_weak"
```

---

### 11.6 false_breakout

定义：

刺破或短暂突破后快速被打回，且没有形成有效延续。

情况一：盘中刺破但收盘失败。

向上：

```python
if high > price_high + breakout_buffer and close <= price_high:
    status = "false_breakout"
    zone.false_break_count += 1
```

向下：

```python
if low < price_low - breakout_buffer and close >= price_low:
    status = "false_breakout"
    zone.false_break_count += 1
```

情况二：confirmed 后 3 根 K 线内快速打回，且 follow-through 小于 `0.3 * ATR`。

向上：

```python
if (
    close <= price_high
    and bars_since_confirmed <= FAST_FAILURE_WINDOW_BARS
    and max_high_after_breakout - breakout_close < WEAK_FOLLOW_THROUGH_ATR * atr
):
    status = "false_breakout"
```

向下：

```python
if (
    close >= price_low
    and bars_since_confirmed <= FAST_FAILURE_WINDOW_BARS
    and breakout_close - min_low_after_breakout < WEAK_FOLLOW_THROUGH_ATR * atr
):
    status = "false_breakout"
```

---

### 11.7 reclaimed

定义：

confirmed 后重新收盘回到 zone 内，但没有穿过失效侧。

向上突破后 reclaimed：

```python
if price_low <= close <= price_high:
    status = "reclaimed"
```

向下跌破后 reclaimed：

```python
if price_low <= close <= price_high:
    status = "reclaimed"
```

说明：

`reclaimed` 不是 `failed_breakout`。

只有继续穿过失效侧，才升级为 `failed_breakout`。

---

### 11.8 failed_breakout

定义：

confirmed 后 10 根 K 线内，价格穿过 zone 失效侧。

向上突破失败：

```python
if (
    bars_since_confirmed <= FAILURE_WINDOW_BARS
    and close < price_low - failure_buffer
):
    status = "failed_breakout"
    zone.failed_breakout_count += 1
    zone.status = "invalidated"
    zone.invalidated_ts = current_ts
```

向下跌破失败：

```python
if (
    bars_since_confirmed <= FAILURE_WINDOW_BARS
    and close > price_high + failure_buffer
):
    status = "failed_breakout"
    zone.failed_breakout_count += 1
    zone.status = "invalidated"
    zone.invalidated_ts = current_ts
```

---

### 11.9 retesting

定义：

confirmed 后，价格回到 zone 区间附近，测试原 zone 是否完成角色转换。

向上突破后的回踩：

```python
if low <= price_high and close >= price_low:
    status = "retesting"
    zone.retest_num += 1
```

向下跌破后的反抽：

```python
if high >= price_low and close <= price_high:
    status = "retesting"
    zone.retest_num += 1
```

---

### 11.10 retest_success

定义：

confirmed 后 10 根 K 线内，价格回到 zone 测试后重新收在突破方向一侧，且没有穿过失效侧。

向上突破后的回踩成功：

```python
if (
    bars_since_confirmed <= RETEST_WINDOW_BARS
    and low <= price_high
    and close >= price_high
    and close >= price_low - failure_buffer
):
    status = "retest_success"
    zone.status = "retested"
    zone.current_role = "support"
```

向下跌破后的反抽成功：

```python
if (
    bars_since_confirmed <= RETEST_WINDOW_BARS
    and high >= price_low
    and close <= price_low
    and close <= price_high + failure_buffer
):
    status = "retest_success"
    zone.status = "retested"
    zone.current_role = "resistance"
```

---

### 11.11 retest_failed

定义：

confirmed 后回到 zone 测试，但收盘穿过 zone 失效侧。

向上突破后的回踩失败：

```python
if close < price_low - failure_buffer:
    status = "retest_failed"
    zone.status = "invalidated"
    zone.invalidated_ts = current_ts
```

向下跌破后的反抽失败：

```python
if close > price_high + failure_buffer:
    status = "retest_failed"
    zone.status = "invalidated"
    zone.invalidated_ts = current_ts
```

---

## 12. 状态优先级

当同一根 K 线同时满足多个状态条件时，按以下优先级处理：

```python
STATUS_PRIORITY = [
    "failed_breakout",
    "retest_failed",
    "false_breakout",
    "retest_success",
    "true_breakout_strong",
    "true_breakout_weak",
    "reclaimed",
    "retesting",
    "confirmed",
    "attempt",
]
```

说明：

1. 失败类状态优先级最高。
2. 成功回踩优先于普通延续。
3. confirmed 是中间状态，不是最终质量标签。
4. attempt 优先级最低。

---

## 13. Composite Zone 合并规则

### 13.1 合并条件

两个 zone 满足任一条件即可合并：

条件一：价格区间重叠。

```python
is_overlap = max(zone_a.price_low, zone_b.price_low) <= min(zone_a.price_high, zone_b.price_high)
```

条件二：中心距离足够近。

```python
abs(zone_a.price_center - zone_b.price_center) <= 0.30 * atr
```

### 13.2 合并价格区间

Composite Zone 的价格区间：

```python
composite.price_low = min(source_zone.price_low for source_zone in zones)
composite.price_high = max(source_zone.price_high for source_zone in zones)
composite.price_center = (composite.price_low + composite.price_high) / 2
```

### 13.3 合并来源

```python
composite.merged_from_zone_ids = [zone.zone_id for zone in source_zones]
composite.source = flatten([zone.source for zone in source_zones])
```

---

## 14. Zone Daily Snapshots 表结构

建议表名：

```text
zone_daily_snapshots
```

用途：

1. 记录每天 zone 与当前价格的距离。
2. 支持交易决策排序。
3. 支持回测复现。
4. 支持 debug。

### 14.1 字段定义

| 字段 | 类型 | 必填 | 说明 |
|---|---:|---:|---|
| snapshot_id | str | 是 | 快照 ID |
| zone_id | str | 是 | 关联 zone ID |
| symbol | str | 是 | 股票或标的 |
| timeframe | str | 是 | K 线级别 |
| snapshot_ts | datetime | 是 | 快照时间 |
| current_price | float | 是 | 当前价格 |
| price_low | float | 是 | 快照时 zone 下边界 |
| price_high | float | 是 | 快照时 zone 上边界 |
| price_center | float | 是 | 快照时 zone 中线 |
| distance_to_price | float | 是 | 当前价格到 zone 的绝对距离 |
| distance_atr | float | 是 | ATR 标准化距离 |
| zone_status | str | 是 | 快照时 zone 状态 |
| current_role | str | 是 | 快照时 zone 角色 |

### 14.2 distance_to_price 计算

```python
def distance_to_zone(current_price, price_low, price_high):
    if price_low <= current_price <= price_high:
        return 0.0
    if current_price < price_low:
        return price_low - current_price
    return current_price - price_high
```

### 14.3 distance_atr 计算

```python
distance_atr = distance_to_price / atr
```

---

## 15. 当前交易决策的 Zone 选择规则

交易决策不能扫描全部历史 zone。

必须先过滤，再排序。

### 15.1 候选过滤规则

保留条件：

```python
zone.symbol == current_symbol
zone.timeframe in allowed_timeframes
zone.status in ["active", "broken", "flipped", "retested"]
zone.status not in ["expired", "invalidated"]
distance_atr <= MAX_ZONE_DISTANCE_ATR
```

固定参数：

```python
MAX_ZONE_DISTANCE_ATR = 3.0
```

VP 例外：

```python
if zone.zone_kind == "vp":
    ignore_expired_check = True
```

### 15.2 排序规则

固定排序：

```python
candidate_zones.sort(
    key=lambda z: (
        z.distance_atr,
        ZONE_STATUS_RANK[z.status],
        z.created_ts,
    )
)
```

状态排序权重：

```python
ZONE_STATUS_RANK = {
    "retested": 0,
    "flipped": 1,
    "active": 2,
    "broken": 3,
}
```

说明：

1. 第一优先级是价格距离。
2. 第二优先级是状态质量。
3. 第三优先级是创建时间。
4. 不使用复杂 score 作为主排序依据。

---

## 16. 状态流转示例

### 16.1 Resistance 向上突破

```text
1. 创建 resistance event zone
2. zone.status = active
3. high > price_high，但 close <= price_high + breakout_buffer
4. BreakoutEvent.status = attempt
5. close > price_high + breakout_buffer
6. BreakoutEvent.status = confirmed
7. zone.confirmed_breakout_count += 1
8. zone.current_role = support
9. zone.status = flipped
10. 后续 low <= price_high 且 close >= price_low
11. BreakoutEvent.status = retesting
12. zone.retest_num += 1
13. 后续 close >= price_high 且未跌破 price_low - failure_buffer
14. BreakoutEvent.status = retest_success
15. zone.status = retested
```

### 16.2 Support 向下跌破

```text
1. 创建 support event zone
2. zone.status = active
3. low < price_low，但 close >= price_low - breakout_buffer
4. BreakoutEvent.status = attempt
5. close < price_low - breakout_buffer
6. BreakoutEvent.status = confirmed
7. zone.confirmed_breakout_count += 1
8. zone.current_role = resistance
9. zone.status = flipped
10. 后续 high >= price_low 且 close <= price_high
11. BreakoutEvent.status = retesting
12. zone.retest_num += 1
13. 后续 close <= price_low 且未突破 price_high + failure_buffer
14. BreakoutEvent.status = retest_success
15. zone.status = retested
```

---

## 17. 开发优先级

### 17.1 第一阶段：Zone 身份化

必须完成：

1. 新增 `zone_id`。
2. 新增 `zone_kind`。
3. 新增 `current_role`。
4. 新增 `status`。
5. 新增 `origin_bar`、`origin_event_id`、`origin_event_type`。
6. 禁止新 anchor 直接覆盖旧 zone。

### 17.2 第二阶段：Zone 生命周期

必须完成：

1. Event zone 按 timeframe 自动 expired。
2. VP zone 不走普通 event expired。
3. `expired` 与 `invalidated` 分开。
4. 突破后不删除 zone，只改状态。

### 17.3 第三阶段：BreakoutEvent 状态机

必须完成：

1. `attempt`
2. `confirmed`
3. `false_breakout`
4. `failed_breakout`
5. `reclaimed`
6. `retesting`
7. `retest_success`
8. `retest_failed`
9. `true_breakout_strong`
10. `true_breakout_weak`

### 17.4 第四阶段：Composite Zone

必须完成：

1. 支持多个 zone 合并。
2. 记录 `merged_from_zone_ids`。
3. composite zone 有独立 `zone_id`。
4. composite zone 可参与 breakout 判断。

### 17.5 第五阶段：Snapshot 与排序

必须完成：

1. 新增 `zone_daily_snapshots`。
2. 计算 `distance_to_price`。
3. 计算 `distance_atr`。
4. 当前交易决策只使用有效且接近当前价格的 zone。

---

## 18. 最终设计原则

1. Zone 是市场结构实体，不是一次性信号。
2. Event zone 有生命周期。
3. VP zone 是滚动追踪结构。
4. Composite zone 必须记录来源。
5. BreakoutEvent 是事件记录，不应覆盖 zone。
6. Zone 被突破后不删除，而是进入状态流转。
7. 当前交易判断只使用有效且接近当前价格的 zone。
8. old support / old resistance 必须通过 `zone_id`、`status` 和 `lifecycle` 被持续追踪。
