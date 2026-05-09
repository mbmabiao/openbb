# Zone 核心逻辑说明

本文档与当前代码对齐，记录 zone 类型与命名、锚点与成交量规则、VP 数据源、合并与 **Rank**、价格带宽度及前端展示。

---

## 1. Zone 的核心字段

每个展示到前端的阻力/支撑 zone 通常包含：

- `side`：`support`、`resistance`（由当前价与 `center` 关系判定）。
- `zone_kind`：`avwap`、`vp`、`composite`（生命周期还支持 `event`）。
- `source_types`：来源标签集合，例如 `avwap_short_rolling`、`vp_short`、`avwap_W_swing`。
- `source_types_label`：由 `format_zone_source_types()` 格式化后的展示串。
- `lower` / `upper` / `center`：价格带。
- `timeframes`：来源标签集合；日线 rolling 使用 **`short` / `long`**（与 VP 短长窗口对齐），周线 AVWAP 使用 **`W`**，VP 窗口使用 **`short` / `long`**（见下文）。
- `zone_status`：生命周期状态（快照读取时常过滤非活跃类）。

前端绘图直接使用 `center`、`lower`、`upper`，不在前端重新计算宽度。

---

## 2. 端到端流水线（Dashboard / `generate_zones_for_replay`）

代码入口：`src/engines/zone_generation.py`。

```text
日线 OHLCV（df_calc_daily）
  ├─ build_avwap_features(D) → 日线 AVWAP 候选（rolling + swing + event）
  ├─ VP short 窗口 → _load_window_volume_profile_context(short)
  ├─ VP long  窗口 → _load_window_volume_profile_context(long)
  ├─ create_candidate_zones_from_vp / from_avwap
  ├─ resample_to_weekly → build_avwap_features(W, 无 rolling, swing/event=52) → 周线 AVWAP 候选
  └─ merge_close_zones（同一侧、重叠或 center 接近则合并）
       → rank_zones_for_side（按「中心价带成交量」排序，截断）
       → assign_zone_display_labels（R1/S1… 按 rank 结果顺序编号）
```

---

## 3. 命名规则

### 3.1 AVWAP

列名与 `source_types` 形如：

```text
avwap_{timeframe_label}_{anchor_name}
```

`timeframe_label` 来自锚点元数据：

- 日线 **rolling**：`short` / `long`（与短/长 VP 回看根数一致，非字面周期 `D`）。
- 日线 **swing / event**：`D`（`find_anchor_points(..., timeframe="D")`）。
- 周线：**`W`**。

`anchor_name` 示例：`rolling_21_high`、`recent_swing_high`、`gap_up`、`big_up`。

合并进 `source_types` 时为：

```text
avwap_{timeframe_label}_{anchor_family}
```

例如 `avwap_short_rolling`、`avwap_D_swing`、`avwap_W_event`。

### 3.2 VP（成交量分布）

VP 使用窗口名 **`short` / `long`**（代码里 `normalized_window`），故：

```text
vp_short   # 短窗口 VP
vp_long    # 长窗口 VP
```

不再使用「VP_D / VP_W」表示短长窗口；周线视角若仅出现在 AVWAP 路径上，仍为 `avwap_W_*`。

---

## 4. VP：窗口长度与数据源

**默认（侧边栏 `SidebarDefaults` / `ZoneGenerationConfig`）**

- 短窗口：**21** 个交易日  
- 长窗口：**63** 个交易日  
- `bins`：默认 48  
- `hv_node_quantile`：由侧边栏百分位换算（默认约 0.75）

**生成**：`build_composite_interval_volume_profile_zones` → `build_vp_zones_from_profile`。

**数据源（`_load_window_volume_profile_context`）**

- 先取窗口内交易日对应的日线切片。
- 若 **整个窗口** 内每个交易日都能从 `interval_history_loader` 拉到完整 **5m** 数据，则全程用 **5m** 叠成成交量分布。
- 否则退化为窗口内 **1d** OHLCV 叠分布。

**高量节点**：按价位 bin 体积分位数取高分位 bin；相邻 bin 在宽度条件下合并后再扩带（见第 8 节）。

---

## 5. 日线 AVWAP：Rolling / Swing / Event

实现：`src/features/volume_profile.py` — `find_anchor_points`、`compute_vwap`。

### 5.1 Rolling（仅日线）

`zone_generation` 传入：

```python
rolling_window_bars=(
    (short_vp_lookback_days, "short"),
    (long_vp_lookback_days, "long"),
)
```

在每个窗口的最后 `N` 根 **日线** 上取区间最高、最低；**只有当触及极值的那根 K 线通过成交量门槛时**，才生成对应 `rolling_N_high` / `rolling_N_low` 锚点（见第 6 节）。

**周线路径不传 rolling**（`rolling_window_bars=()`），因此 **没有周线 rolling AVWAP**。

### 5.2 Swing（日线默认 + 周线）

- 使用 `_find_confirmed_swing_points`（左右各 3 根、反转幅度与 ATR）。  
- **搜索长度**：日线默认最近 **63** 根；`zone_generation` 对周线显式传 **52** 根。  
- 仅保留 **成交量达标** 的 swing 点（见第 6 节）。

### 5.3 Event：Gap / 大实体

均在最近 `event_search_bars` 根内筛选（日线默认 **63**，周线 **52**）。

**成交量门槛**：所有 gap / big 候选 bar 必须先满足 `_volume_qualifies`（见第 6 节）。

**Gap（未回补语义）**

- **gap_up**：`open > prev_close` 且 **`close > prev_close`**（收于昨收之上），再取满足条件的 `gap_pct` **最大** 的 bar。  
- **gap_down**：`open < prev_close` 且 **`close < prev_close`**，再取 `gap_pct` **最小**。

即：**日内回补到昨收另一侧的跳空不会被当作有效 gap 锚点**。

**大实体**

- `big_up` / `big_down`：在 **`volume_qualified`** 为真的 bar 上，对 `(close-open)/open` 取最大 / 最小。

### 5.4 AVWAP 曲线

`compute_vwap`：从锚点索引起，对后续 bar 使用典型价 × volume 累加 / volume 累加，即 **标准 anchored VWAP**，全过程使用 **日线或周线 OHLCV**。

---

## 6. 成交量门槛 `_volume_qualifies`

对索引 `index` 上的 bar：

- 取过去至多 **60** 根 **更早** 的 K 线的成交量序列（不含当日）。  
- 若当日成交量 ≥ 该序列的 **0.8 分位数**，则视为通过。

用于：rolling 极值日、swing、gap、big 的候选过滤。

---

## 7. 候选 Zone 排序（Rank）

实现：`src/engines/validation_engine.py` — `rank_zones_for_side`。

**已不再使用**历史上的 `institutional_score` / `reaction_score` 合成排序（相关参数传入函数但被忽略）。

当前规则：

1. 过滤：只保留当前 `side`；阻力丢弃整带在现价下方者，支撑丢弃整带在现价上方者。  
2. 对每条 zone 计算 **`center_volume`**：在 **`df_reaction`（与生成时传入的日线计算帧一致）** 上，对所有 **日线 K** 若其 `[low, high]` 与价格区间  

   `[center * (1 - band_pct), center * (1 + band_pct)]`  

   有交集，则将该根 K 的 **`volume` 累加**。  
   其中 **`band_pct` = `zone_expand_pct`**（与侧边栏 `zone_expand_bp/10000` 一致）。

3. **按 `center_volume` 降序**；并列时用 **`zone_id` 字符串** 打破平局。  
4. 取前 `max_resistance_zones` / `max_support_zones` 条。  

`assign_zone_display_labels` 按 **当前列表顺序** 标 `R1,R2,…` / `S1,S2,…`，即 **Rank 靠前者标签序号更小**。

---

## 8. Composite 合并

实现：`merge_close_zones`（`src/features/boundaries.py`）。

同一 `side`、且（区间重叠 **或** center 相对距离 ≤ `merge_pct`）则合并为 `zone_kind=composite`。默认 **`merge_pct_bp = 60`** → `merge_pct = 0.006`。

合并后 `vp_volume` 相加；`primary_timeframe`：若 `timeframes` 含 `"W"` 则为 `W`，否则为 **`D`**（纯 `short`/`long` 标签也会落到 `D`）。

---

## 9. VP / AVWAP 价格带宽度（生成侧）

**VP**：由高量 bin 组的 `[bin_left, bin_right]` 得到横向宽度，再：

```text
expand = center * zone_expand_pct
lower = bin_left - expand  （首段逻辑见代码合并循环）
upper = bin_right + expand
```

默认 **`zone_expand_bp = 50`** → **`zone_expand_pct = 0.005`**（0.5%）。

**AVWAP**：无 intrinsic 宽度，仅：

```text
lower = center - center * zone_expand_pct
upper = center + center * zone_expand_pct
```

---

## 10. Event zone 与 TTL（生命周期）

主路径上 gap/big/swing 等多以 **`zone_kind = avwap`**、**`anchor_family = event/swing/rolling`** 出现。持久化用的 **`event`** 大类仍存在于 `constants` / 测试。

`EVENT_ZONE_TTL_BARS`（`zone_lifecycle/constants.py`）等对事件型持久化仍可按周期配置（如日线 63 根等）。

---

## 11. Zone 状态与 Replay 过滤

状态定义见 `zone_lifecycle/constants.py`。从数据库读 replay 快照时，通常只展示活跃意义的状态子集（具体过滤见 `snapshot_queries.py`）。

---

## 12. 持久化与前端

快照与查询：`zone_lifecycle/service.py`、`snapshot_queries.py`。前端 `chart_builder.py` 使用快照中的 `lower`/`upper`/`center` 绘制 center 线与半透明带。

---

## 13. 文档维护说明

逻辑变更时请同步更新本文档，重点核对：

- `zone_generation.py`：窗口天数、周线 swing/event 长度、是否启用 rolling。  
- `volume_profile.py`：锚点与 `_volume_qualifies`、gap/big 条件。  
- `validation_engine.py`：`center_volume` 与排序键。  
