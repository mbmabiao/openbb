# OpenBB Equity Research Dashboard

这是一个基于 `Streamlit + OpenBB + yfinance` 的股票研究应用。入口是 [src/app.py](src/app.py)，顶部导航分为三个一级模块：

- `Stock`：单只股票研究、历史 K 线、VAP/volume profile、支撑阻力 zone、财务报表和新闻。
- `Backtest`：自定义策略回测、策略参数 UI、交易图、资金曲线和交易明细。
- `Market`：ETF 市场轮动、行业/主题/风格/产业链分类、leaders/laggards 和市场广度。

项目定位是研究和可视化工具，不是实盘交易执行系统。

## 快速开始

建议使用 Python 3.11 或 3.12。

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run src/app.py
```

启动后在浏览器中打开 Streamlit 给出的本地地址。侧边栏输入股票代码，例如：

- `AAPL`
- `MSFT`
- `NVDA`
- `000300.SS`

## Stock 模块

`Stock` 是单只股票研究页。它复用侧边栏的 symbol/provider/range 设置，并在页面内提供多个 tab。

### 先运行 warmup

`Stock` 的 Historical Price 页会读取 zone lifecycle 的快照数据。为了让支撑/阻力 zone、pattern event、divergence event 和 replay 查询正常显示，建议先为目标 symbol 生成 warmup 快照。

单只股票：

```powershell
python src\build_zone_snapshots.py --symbol MSFT --lookback-years 5 --provider yfinance --reset
```

指定日期范围：

```powershell
python src\build_zone_snapshots.py --symbol MSFT --start-date 2021-01-01 --end-date 2026-06-05 --provider yfinance --reset
```

批量脚本：

```powershell
.\scripts\warmup.ps1
.\scripts\warmup_etf.ps1
```

warmup 默认写入：

```text
outputs/zone_lifecycle.sqlite
```

常用参数：

- `--symbol`：股票或 ETF 代码。
- `--lookback-years`：从今天往前生成多少年快照。
- `--start-date` / `--end-date`：明确指定快照区间。
- `--provider`：OpenBB provider，例如 `yfinance`。
- `--reset`：先删除该 symbol 的旧 lifecycle 数据再重建。
- `--no-force`：增量处理，而不是重建整个区间。
- `--warmup-config-path`：自定义 warmup 阈值配置，默认 [src/config/warmup.yaml](src/config/warmup.yaml)。

### Historical Price

Historical Price 是 Stock 模块的核心页，由 [src/dashboard_page.py](src/dashboard_page.py) 驱动。

主要功能：

- 加载并清洗 OHLCV 历史价格。
- 使用 lightweight-charts 风格黑底 K 线图。
- 显示 volume histogram。
- 显示 long VAP / volume profile 侧边分布。
- 显示支撑/阻力 zone、zone label 和 zone band。
- 显示 EMA20、EMA50 和 ATR bands。
- 支持 replay date，把历史某一天当作当下重新查看。
- 从 warmup 数据库读取 pattern event、MACD divergence event 和 breakout event。

推荐使用流程：

1. 先为 symbol 运行 warmup。
2. 启动应用并进入 `Stock`。
3. 在侧边栏输入 symbol，选择 history range。
4. 调整 VAP bins、zone expand、EMA、ATR multiple、bar handling 等设置。
5. 在 Historical Price 页面选择 replay date。
6. 查看图表里的 K 线、volume、VAP、zone、事件 marker 和下方 zone 表格。

### 财务和新闻 tabs

Stock 下还包括：

- `Income`
- `Balance Sheet`
- `Cash Flow`
- `Ratios`
- `News`

这些 tab 通过 OpenBB 拉取财务报表、比率和新闻。侧边栏里的 `Fundamentals provider`、`News provider` 可以留空，也可以填 provider 名称。

## Backtest 模块

`Backtest` 是一级页面，由 [src/ui/strategy_backtest_page.py](src/ui/strategy_backtest_page.py) 渲染。

主要作用：

- 自动发现 [src/strategies](src/strategies) 下的策略。
- 读取策略的 `default_config` 和 `config_schema`，自动生成参数表单。
- 支持选择 symbol、primary timeframe、日期范围、provider、extended hours。
- 支持设置 initial capital、slippage、commission、position size、long/short 开关。
- 回测价格图使用 lightweight-charts 风格，显示 K 线、volume、`plot_` overlay、开平仓 marker 和 overlay legend。
- Equity Curve 继续使用 Plotly。
- Trade Details 显示每笔交易、平仓原因、盈亏、盈亏比例和余额。

### 策略开发协议

Backtest 页标题下方有 `download develop protocol` 文本链接，可以下载英文策略开发协议。源文件是：

[src/backtesting/README.md](src/backtesting/README.md)

核心协议：

- 必需信号列：`open_long`、`close_long`、`open_short`、`close_short`。
- 可选 intrabar 触发价列：`open_long_price`、`close_long_price`、`open_short_price`、`close_short_price`。
- 可选 overlay 列：任何 `plot_` 开头的列都会画到回测图上。
- 可选仓位列：`position_size_pct`、`position_notional`、`target_weight`。
- 没有 `*_price` 列时，engine 保持默认 close 成交。
- 有 `*_price` 列时，finite 且大于 0 的价格必须在当前 bar 的 `low <= price <= high` 范围内才触发。

### 添加自己的策略

在 [src/strategies/examples](src/strategies/examples) 或 [src/strategies](src/strategies) 下新增策略文件，并继承 `BaseStrategy`。

最小结构：

```python
from __future__ import annotations

import pandas as pd

from strategies.base import BaseStrategy, StrategyContext


class MyStrategy(BaseStrategy):
    name = "my_strategy"
    display_name = "My Strategy"
    description = "Example strategy."
    required_timeframes = ["1d"]

    default_config = {"lookback": 20}
    config_schema = {
        "lookback": {
            "type": "int",
            "label": "Lookback",
            "default": 20,
            "min": 1,
            "max": 200,
            "step": 1,
            "required": True,
        }
    }

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        df = context.data[context.primary_timeframe].copy()
        df["open_long"] = False
        df["close_long"] = False
        df["open_short"] = False
        df["close_short"] = False
        df["plot_ma"] = df["close"].rolling(int(self.config["lookback"])).mean()
        return df
```

保存文件后，在 Backtest 页点击 `Refresh`，新策略会被发现。

### 从 Python 运行回测

```python
from backtesting.runner import run_backtest

result = run_backtest(
    symbol="MSFT",
    strategy_name="supertrend_atr_trailing",
    strategy_config={
        "atr_period": 10,
        "supertrend_multiplier": 3.0,
        "atr_exit_mult": 1.5,
        "exit_on_opposite_signal": True,
        "direction": "both",
    },
    backtest_config={
        "initial_capital": 10_000,
        "primary_timeframe": "1d",
        "start_date": "2025-01-01",
        "end_date": "2026-06-05",
        "price_provider": "yfinance",
    },
)
```

## Market 模块

`Market` 是 ETF 市场轮动看板，由 [src/market_dashboard.py](src/market_dashboard.py) 渲染，直接使用 `yfinance` 拉取行情。

主要作用：

- 观察市场基准：`SPY`、`QQQ`、`IWM`、`DIA`。
- 观察行业轮动：`XLK`、`XLF`、`XLE`、`XLV` 等。
- 观察主题方向：AI、半导体、云、清洁能源、国防、加密相关等。
- 观察产业链确认：上游、中游、下游、基础设施、电力供应等。
- 观察风格因子：质量、动量、价值、低波动、小盘、股息等。
- 观察商业模式、收入来源和供应链关系。

页面会计算：

- 每个 ETF 的近期表现。
- 20 日均线 / 50 日均线位置。
- 每个分类的 leaders 和 laggards。
- 20MA breadth / 50MA breadth。
- 模块级诊断，用于判断风险偏好、主题扩散、产业链确认或分化。

使用方式：

1. 点击顶部 `Market`。
2. 默认自动刷新，每 `10` 秒更新展示；也可以手动刷新。
3. 先看 Market Benchmark 判断大盘风险偏好。
4. 再看 Sector、Theme、Value Chain、Style Factor 等模块，确认强势方向是否扩散。
5. 用 leaders/laggards 和 breadth 判断行情是集中抱团、扩散，还是防御切换。

## 目录结构

```text
src/
  app.py                         Streamlit 入口和顶部导航
  dashboard_page.py              Stock / Historical Price 页面
  market_dashboard.py            Market ETF 轮动看板
  build_zone_snapshots.py        warmup / zone snapshot 离线生成入口
  backtesting/                   回测 engine、schema、runner、说明文档
  strategies/                    策略基类、自动发现、示例策略
  data/                          OpenBB 数据获取和 OHLCV 清洗
  engines/                       replay、zone generation、validation
  features/                      volume profile、ATR、zone strength
  plotting/                      lightweight-charts 组件和回测图 adapter
  ui/                            sidebar、页面状态、回测页面
  zone_lifecycle/                zone 生命周期、事件、快照和 SQLite repository
  config/                        app 默认值和 warmup 阈值
  test/                          单元测试
scripts/
  warmup.ps1                     单只或少量股票 warmup 示例
  warmup_etf.ps1                 ETF 批量 warmup 示例
```

## 配置文件

- [src/config/settings.py](src/config/settings.py)：页面默认值，例如默认 symbol、history range、ATR multiple。
- [src/config/warmup.yaml](src/config/warmup.yaml)：zone lifecycle、breakout、pattern event、divergence event、zone generation 阈值。
- [src/backtesting.sample.yaml](src/backtesting.sample.yaml)：回测配置示例。

## 测试

安装依赖后可以运行：

```powershell
pytest -q src/test
```

如果只想做语法检查：

```powershell
python -m py_compile src/app.py src/ui/strategy_backtest_page.py src/backtesting/engine.py
```

## 数据和输出

常见输出位置：

- `outputs/zone_lifecycle.sqlite`：zone lifecycle 和 replay snapshot 数据库。
- `src/outputs/`：部分研究工具或测试脚本输出。
- `reports/`、`outputs/`：本地分析报告和导出结果。

这些输出通常是本地生成物，不应和源码逻辑混在一起提交，除非明确需要留档。

## 依赖

依赖见 [requirements.txt](requirements.txt)。核心包包括：

- `streamlit`
- `openbb`
- `yfinance`
- `pandas`
- `numpy`
- `plotly`
- `SQLAlchemy`
- `PyYAML`
- `pytest`

`Stock` 和 `Backtest` 的价格/基本面数据主要通过 OpenBB 获取；`Market` 模块直接使用 `yfinance`。
