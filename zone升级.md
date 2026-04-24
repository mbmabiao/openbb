把支撑/阻力从“每根 bar 临时计算出来的线”，升级为“有 identity、有 lifecycle、有 flip 机制的结构对象”。

支撑/阻力重构需求文档
1. 文档目标
本文档用于定义现有支撑/阻力（Support / Resistance, S/R）识别与交易逻辑的重构需求。
核心目标不是继续微调现有参数，而是重构为一套具有身份延续（persistent identity）、状态迁移（state transition）、**阻力翻支撑（resistance-to-support flip）**能力的结构系统。

该文档聚焦以下内容：

现有问题定义
新系统设计目标
核心逻辑流程
数据结构设计
状态机设计
输入输出定义
与现有代码的集成方式
边界条件与异常情况
实现优先级建议
2. 背景与现有问题
2.1 现有实现特征

现有代码具备以下特征：

每根 bar 都重新调用 zone engine，从历史数据中构建 support / resistance zones。
support / resistance 的归属是根据当前价格重新切分出来的。
breakout 发生后，只记录一条 last_boundary，用于后续持仓风控。
旧阻力位并没有作为结构对象继续保留，也没有正式进入新的 support 集合。
2.2 当前问题

现有实现会导致以下问题：

问题 1：阻力位在接近突破时可能消失

因为每根 bar 都会重算 zone，原本接近突破的阻力位可能在下一根 bar：

被 merge 掉
被挤出 top N
边界发生漂移
不再被识别为 resistance
问题 2：突破后的旧阻力无法稳定翻转为支撑

系统目前只保存 last_boundary 作为风控参考线，没有把旧阻力转成新的支撑结构。

问题 3：缺少结构连续性

当前 zone 仅是“本 bar 的计算结果”，而不是“有生命周期的结构对象”。

问题 4：执行层与结构层混杂

当前逻辑中，识别层、结构层、执行层没有严格分开，导致：

策略可以下单
但无法稳定追踪 level 的结构演化
3. 重构目标
3.1 总体目标

构建一套新的 S/R 管理层，使 zone 不再只是“临时计算结果”，而是“可持续追踪的结构对象”。

3.2 核心目标

新系统必须满足：

Zone identity persistence
同一个 level 在不同 bars 之间应尽可能保持同一身份。
Adaptive but stable boundaries
zone 边界允许缓慢更新，但不能每根 bar 完全重生。
Resistance-to-support flip support
阻力突破后应进入待翻转状态，并在 retest 成功后成为正式支撑。
Separation of structure layer and execution layer
结构识别与交易执行解耦。
Breakout watch / pending breakout support
接近突破的关键阻力需要缓存，避免在临门一脚时消失。
State machine driven management
每个关键 level 需要具备明确状态，而不是仅仅用一条边界线表示。
4. 系统分层设计
4.1 结构层（Structure Layer）

职责：

管理 zone 对象
管理 zone 生命周期
跟踪 zone 状态
处理突破、翻转、失效

不负责：

最终下单
仓位管理
收益统计
4.2 执行层（Execution Layer）

职责：

根据结构层输出决定是否入场/平仓
设置 stop / target / invalidation
与策略引擎对接

执行层只能“消费”结构层结果，不能替代结构层。

5. Zone 分类设计

系统内的支撑/阻力分为三类：

5.1 原生支撑 / 阻力（Native Zones）

来源：

VP Daily
VP Weekly
AVWAP Daily
AVWAP Weekly
Merge 后候选区
5.2 翻转支撑 / 阻力（Flipped Zones）

来源：

旧阻力成功突破后
经回踩确认
转换为新的支撑
5.3 事件型支撑 / 阻力（Event-derived Levels，可选）

来源：

强突破 bar 的 low / high
缺口边界
特定 confirmation candle 的极值

本次重构的重点是 Native Zones + Flipped Zones。

6. 数据结构设计
6.1 基础 Zone 对象
from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class Zone:
    zone_id: str
    role: str                     # "support" | "resistance"
    lower: float
    upper: float
    source: str                   # vp_daily / vp_weekly / avwap_daily / avwap_weekly / merged / flipped


    state: str                    # 见状态机定义
    strength: float = 0.0
    institutional_score: float = 0.0


    first_seen_bar: int = -1
    last_seen_bar: int = -1
    created_at_bar: int = -1
    broken_bar: Optional[int] = None
    retired_bar: Optional[int] = None


    touch_count: int = 0
    retest_count: int = 0
    acceptance_bars: int = 0


    parent_zone_id: Optional[str] = None
    metadata: Dict = field(default_factory=dict)
字段说明
zone_id: zone 全局唯一标识
role: 当前角色，support 或 resistance
lower, upper: 区间边界
source: 来源类型
state: 当前状态
strength: 综合强度评分
institutional_score: 原系统已有强度维度，可保留
first_seen_bar: 第一次识别到的 bar 序号
last_seen_bar: 最近一次匹配到的 bar 序号
created_at_bar: 当前对象创建 bar
broken_bar: 被突破的 bar
retired_bar: 失效/退役 bar
touch_count: 被测试次数
retest_count: 突破后回踩次数
acceptance_bars: breakout 后连续站稳计数
parent_zone_id: 如果是翻转 zone，则指向旧 zone
metadata: 扩展字段
6.2 Registry 结构
@dataclass
class ZoneRegistry:
    zones: Dict[str, Zone] = field(default_factory=dict)
    active_support_ids: List[str] = field(default_factory=list)
    active_resistance_ids: List[str] = field(default_factory=list)
    pending_breakout_ids: List[str] = field(default_factory=list)
    flipped_zone_ids: List[str] = field(default_factory=list)
职责
保存所有 zone 对象
维护当前活跃支撑/阻力列表
维护待突破观察列表
维护翻转支撑列表
6.3 Breakout 事件对象
@dataclass
class BreakoutEvent:
    zone_id: str
    bar_index: int
    breakout_type: str            # "up" | "down"
    breakout_close: float
    breakout_high: float
    breakout_low: float
    boundary_price: float
    acceptance_bars_required: int
    confirmed: bool = False
7. 状态机设计
7.1 Zone 状态定义

每个 zone 必须处于以下状态之一：

原生状态
candidate
confirmed
pending_breakout
突破/翻转状态
broken_pending_flip
confirmed_flip
failed_breakout
生命周期结束状态
retired
7.2 状态迁移图
candidate
-> confirmed
-> pending_breakout
-> broken_pending_flip
-> confirmed_flip


broken_pending_flip
-> failed_breakout


failed_breakout
-> confirmed (可选)
或
-> retired


confirmed / confirmed_flip
-> retired
7.3 状态语义
candidate

刚被识别出来的 zone，尚未经过足够验证。

confirmed

已被重复测试、强度足够、可作为正式 S/R 使用。

pending_breakout

价格已接近关键边界，未来几根 bar 内需优先观察是否突破。

broken_pending_flip

阻力已经被突破，但尚未完成回踩确认，不应立刻无条件视为正式支撑。

confirmed_flip

旧阻力经过回踩和接受，正式翻转为新支撑。

failed_breakout

突破后重新跌回 zone 内部，说明该翻转失败。

retired

长期未出现、被彻底破坏、或不再具备跟踪价值。

8. 核心逻辑流程
8.1 每根 bar 的主流程
1. 从历史数据生成本 bar 的 candidate zones
2. 将新生成 zones 与 registry 中旧 zones 做 matching
3. 更新已有 zone，新增未匹配 zone，淘汰长期失配 zone
4. 更新 zone 状态（candidate -> confirmed 等）
5. 检查是否有 zone 进入 pending_breakout
6. 检查 breakout 是否成立
7. breakout 成立后，生成 broken_pending_flip zone
8. 检查 retest 是否成立
9. retest 成功 -> confirmed_flip
10. 将 active supports / resistances 输出给执行层
8.2 Zone matching 逻辑

目的： 避免每根 bar 生成的新 zones 把旧 zone 完全替换掉。

matching 规则建议

两个 zone 可视为同一对象，当满足以下条件中的若干项：

中心点距离足够近
区间 overlap 比例足够高
width 差异在可接受范围内
source 类似或兼容
示例指标
def zone_center(zone):
    return (zone.lower + zone.upper) / 2


def zone_width(zone):
    return zone.upper - zone.lower

匹配优先级建议：

overlap 高
centre distance 小
width 接近
matching 结果
匹配成功：更新旧 zone
匹配失败：新建 zone
长时间未匹配：retire
8.3 Pending breakout 逻辑
触发条件

当价格足够接近某 confirmed resistance：

abs(close / zone.upper - 1) <= near_breakout_pct
行为
将该 zone 状态改为 pending_breakout
放入 pending_breakout_ids
在未来 N 根 bars 内优先跟踪它
即使新一轮 zone 计算结果变化，也不要立即丢弃它
目的

防止“原本快要突破的阻力位，在真正突破前消失”。

8.4 Breakout 识别逻辑

建议延续现有逻辑思路，但升级为状态机驱动。

breakout 成立条件（向上突破阻力）
high 触及/越过阻力上边界
close > upper * (1 + breakout_buffer_pct)
连续收盘站上达到最小要求
breakout 成立后动作
原 resistance zone 标记为 broken_pending_flip
记录 broken_bar
基于旧阻力边界生成 flipped support zone
flipped zone 的 role 改为 support
flipped zone 初始状态为 broken_pending_flip
8.5 Flipped support 生成逻辑
生成时机

Resistance breakout 确认后立即创建。

示例
flipped_lower = old_zone.upper * (1 - retest_buffer_pct)
flipped_upper = old_zone.upper * (1 + retest_buffer_pct)
属性
source = "flipped"
parent_zone_id = old_zone.zone_id
role = "support"
state = "broken_pending_flip"
8.6 Retest 确认逻辑
retest 成功条件建议

对 flipped support：

low 重新回到翻转支撑区域附近
close 没有有效跌回其下方
后续连续若干 bars 收在支撑上方
成功后动作
flipped zone -> confirmed_flip
retest_count += 1
acceptance_bars 更新
正式加入 active support universe
失败条件

如果价格重新跌破翻转支撑，并连续若干 bars 收在区间下方：

zone -> failed_breakout
后续可选择恢复为 resistance 或直接 retired
9. 输出给执行层的数据

执行层不应直接依赖原始 candidate zones，而应依赖结构层处理后的结果。

9.1 输出结构
@dataclass
class StructureOutput:
    active_support_zones: List[Zone]
    active_resistance_zones: List[Zone]
    pending_breakout_zones: List[Zone]
    confirmed_flipped_support_zones: List[Zone]
说明
active_support_zones: 当前正式可用支撑
active_resistance_zones: 当前正式可用阻力
pending_breakout_zones: 接近突破、需重点跟踪的阻力
confirmed_flipped_support_zones: 已翻转确认的关键支撑
10. 与现有代码的集成要求
10.1 可复用部分

以下部分可保留：

build_ranked_zones_from_history()
VP / AVWAP 候选区生成逻辑
merge_close_zones
rank_zones_for_side
breakout buffer / consecutive close 相关参数
10.2 新增组件

需要新增：

Zone
ZoneRegistry
StructureOutput
BreakoutEvent
match_zones()
update_zone_registry()
update_zone_states()
detect_pending_breakouts()
confirm_breakout_and_create_flip()
process_flip_retest()
10.3 现有策略类需要新增字段
self.zone_registry
self.pending_breakout_zone_ids
self.flipped_support_zone_ids
self.structure_output
10.4 现有 last_boundary 的定位

last_boundary 可以保留，但职责应收缩为：

允许保留的作用
持仓后止损参考线
trade invalidation line
不应继续承担的作用
代替结构层中的翻转支撑对象
代替 zone 生命周期追踪
11. 参数设计建议

建议参数分为四组：

11.1 Zone 生成参数
zone_lookback_bars
daily_vp_lookback_days
weekly_vp_lookback
vp_bins
weekly_vp_bins
merge_pct
zone_expand_pct
11.2 Zone 稳定性参数
zone_match_overlap_min
zone_match_center_tolerance_pct
zone_retire_bars
zone_promote_touch_count
11.3 Breakout / Flip 参数
near_breakout_pct
breakout_buffer_pct
min_close_outside_zone
retest_buffer_pct
hold_outside_bars_required
flip_expiry_bars
failed_breakout_min_consecutive_inside_bars
11.4 执行层参数
initial_stop_pct
atr_multiple
max_holding_bars
12. 边界条件 / 异常处理
12.1 区间重叠过多

若多个 zone 高度重叠，优先：

strength 更高者
confirmed / confirmed_flip 优先于 candidate
flipped support 优先于普通 support
12.2 突破后未回踩，直接走强

这种情况下系统应支持两种模式：

激进模式

突破确认即允许入场。

保守模式

要求回踩确认后才入场。

由 require_retest_success 控制。

12.3 假突破后快速拉回

应记为 failed_breakout，不能直接当成成功翻转。

12.4 长期未再出现的旧翻转支撑

超过 flip_expiry_bars 仍未回踩或无效，应 retired。

12.5 同一条旧阻力多次回踩

应记录 retest_count，供后续强度评估使用。

13. 实现优先级建议
Phase 1：最小可用版本

目标：先建立结构连续性

实现：

Zone 对象
Zone registry
Zone matching
Retire 机制
Phase 2：突破跟踪版本

目标：解决“快突破时消失”问题

实现：

pending_breakout
breakout watchlist
breakout state 更新
Phase 3：翻转支撑版本

目标：真正实现 resistance -> support flip

实现：

flipped support zone 创建
retest 确认
failed breakout 判定
confirmed_flip 输出
Phase 4：执行层联动

目标：让策略真正利用结构输出

实现：

support bounce 使用 confirmed / confirmed_flip
breakout / retest 两套入场模式
last_boundary 收缩为风控用途
14. 验收标准

重构完成后，应满足以下验收标准：

同一关键阻力在多根 bar 间具有稳定 identity
接近突破的阻力不会因一次重算立即消失
突破后的旧阻力能进入 pending flip 状态
回踩成功后，旧阻力能被正式识别为新支撑
假突破会被识别，不会误标为成功翻转
执行层可以同时消费 native support 与 flipped support
last_boundary 不再是唯一结构锚点
15. 一句话总结

当前系统的问题不是“支撑/阻力不够多”，而是“缺少结构身份与状态演化”。

本次重构的核心，就是把支撑/阻力从“每根 bar 临时计算出来的线”，升级为“有 identity、有 lifecycle、有 flip 机制的结构对象”。