# Equity Data Dashboard

这是一个基于 `Streamlit + OpenBB` 的股票数据看板，入口是 [src/app.py](/d:/code/openbb/src/app.py)。  
应用一方面提供基础面与新闻查询，另一方面把历史价格页做成了一个“机构支撑/阻力区”分析面板，支持：

- 历史价格回放 `replay`
- 日线与高周期 volume profile
- Anchored VWAP 锚点
- ATR 带
- 支撑/阻力区打分与排序

## 入口设计

整个应用围绕 `src/app.py` 展开，执行路径很清晰：

1. 设置 `Streamlit` 页面配置与标题。
2. 调用 [src/ui/sidebar.py](/d:/code/openbb/src/ui/sidebar.py) 收集用户输入。
3. 根据 `TAB_NAMES` 创建 6 个标签页。
4. 将“历史价格分析”交给 [src/dashboard_page.py](/d:/code/openbb/src/dashboard_page.py)。
5. 将财务报表、比率、新闻等交给 [src/ui/panels.py](/d:/code/openbb/src/ui/panels.py) 做通用展示。

也就是说，`app.py` 更像一个总控入口：

- 负责装配页面
- 负责路由 tab
- 不直接承载复杂业务计算

真正的核心分析流程在 `dashboard_page.py`。

## 功能概览

应用当前包含 6 个标签页：

- `Historical Price`
- `Income`
- `Balance Sheet`
- `Cash Flow`
- `Ratios`
- `News`

其中最核心的是 `Historical Price`：

- 加载历史 OHLCV
- 构造回放视角下的计算窗口
- 生成 AVWAP 特征
- 生成日线与高周期 volume profile
- 合并候选 zone
- 对支撑/阻力进行反应验证与排序
- 在图表和表格中展示最终 zone

## Historical Price 页工作流

`Historical Price` 页由 [src/dashboard_page.py](/d:/code/openbb/src/dashboard_page.py) 驱动，大致流程如下。

### 1. 数据加载

通过 [src/data/market_data.py](/d:/code/openbb/src/data/market_data.py)：

- 拉取历史价格
- 归一化 OHLCV 字段
- 检查必需列是否完整

### 2. 计算框架与回放模式

通过 [src/engines/replay_engine.py](/d:/code/openbb/src/engines/replay_engine.py)：

- 划分图表展示数据 `plot frame`
- 划分策略计算数据 `calc frame`
- 支持把任意历史日期当作“今天”重新计算

### 3. AVWAP 锚点

通过 [src/features/volume_profile.py](/d:/code/openbb/src/features/volume_profile.py)：

- 生成 anchored VWAP 特征
- 当前 major high/low 已按“距离”分层，而不是按日线/周线概念分层
- 目前有两档：
  - `20` 个交易日短期高低点
  - `60` 个交易日中期高低点

### 4. Volume Profile

系统会构建两类 profile：

- 日线视角：最近若干交易日的 `5m` 数据合成 profile
- 高周期视角：最近若干周窗口的 `1d` 数据合成 profile

然后提取高成交密度节点，生成候选价格带。

### 5. Zone 合并与打分

通过：

- [src/features/boundaries.py](/d:/code/openbb/src/features/boundaries.py)
- [src/engines/validation_engine.py](/d:/code/openbb/src/engines/validation_engine.py)

完成以下工作：

- 合并相近或重叠 zone
- 识别支撑 / 阻力
- 计算 reaction 指标
- 计算 institutional score
- 分别筛出最终展示的支撑区和阻力区

### 6. 图表渲染

通过 [src/plotting/chart_builder.py](/d:/code/openbb/src/plotting/chart_builder.py)：

- 渲染 K 线
- 渲染成交量
- 渲染 zone 水平线
- 渲染 AVWAP 线
- 渲染 volume profile 侧边叠加层

## 侧边栏参数

侧边栏定义在 [src/ui/sidebar.py](/d:/code/openbb/src/ui/sidebar.py)，主要分为几组：

- 基础输入
  - `Symbol`
  - `Price provider`
  - `Fundamentals provider`
  - `News provider`
  - `Price history range`
- Institutional Zone Settings
  - volume profile 分箱数
  - 高周期回看窗口
  - zone 扩展宽度
  - 高成交量分位数
  - zone 合并阈值
  - 最大支撑/阻力显示数量
- ATR Overlay
  - 是否显示 ATR 带
  - ATR 倍数
- Reaction Validation
  - lookahead bars
  - strong reaction threshold
  - minimum touch gap
- Bar Handling
  - 是否排除最新未收盘 bar 参与计算
  - 是否在图上显示最新 live bar

默认值定义在 [src/config/settings.py](/d:/code/openbb/src/config/settings.py)。

## 目录结构

以 `src/app.py` 为中心，可以把项目理解成下面几层：

```text
src/
├─ app.py                  # 应用入口
├─ dashboard_page.py       # 历史价格主页面
├─ config/                 # 页面常量、默认参数
├─ data/                   # OpenBB 数据拉取与清洗
├─ engines/                # 回放、验证、打分等流程引擎
├─ features/               # AVWAP、VP、边界特征
├─ plotting/               # 图表拼装与渲染
├─ ui/                     # 侧边栏、面板、状态
├─ boundary_tester/        # 批量验证与研究工具
└─ run_boundary_tester.py  # 研究/验证入口
```

## 安装依赖

项目当前依赖见 [requirements.txt](/d:/code/openbb/requirements.txt)：

```bash
pip install -r requirements.txt
```

核心依赖包括：

- `streamlit`
- `openbb`
- `pandas`
- `plotly`
- `PyYAML`
- `streamlit-lightweight-charts`

## 启动方式

在仓库根目录执行：

```bash
streamlit run src/app.py
```

启动后可以在侧边栏输入证券代码，例如：

- `AAPL`
- `MSFT`
- `000300.SS`

## 使用建议

如果你主要关心价格结构，建议优先看 `Historical Price` 页：

1. 输入 `symbol`
2. 选择足够长的 `history range`
3. 调整 VP / zone 参数
4. 用 `replay` 看历史某一天当下系统会给出哪些支撑阻力
5. 结合下方表格检查 zone 来源、反应质量和分数

如果你主要关心公司基本面，则可以直接切到：

- `Income`
- `Balance Sheet`
- `Cash Flow`
- `Ratios`
- `News`

## 开发说明

如果要继续扩展这个项目，建议从 `src/app.py` 往下读：

1. 先看 `app.py` 怎么组织 tab 和 sidebar。
2. 再看 `dashboard_page.py` 怎么串起价格分析流程。
3. 接着看 `features/` 和 `engines/`，理解 zone 是怎么生成和排序的。
4. 最后看 `plotting/chart_builder.py`，理解最终图表是怎么拼出来的。

这是目前最省力、也最接近真实运行路径的阅读顺序。

## 已知特点

- 应用以 `Streamlit` 交互体验为主，适合研究和可视化，不是交易执行系统。
- 多数数据能力依赖 `OpenBB` 提供的数据接口。
- 高周期 zone 与日线 zone 会做合并和加权，所以最终结果是“结构化汇总”，不是简单罗列原始信号。
- 当前 README 以 `src/app.py` 为主线，因此更偏“应用结构说明”，而不是研究方法论文式文档。
