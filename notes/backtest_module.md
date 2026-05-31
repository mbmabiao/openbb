Goal:
Design and implement a new custom strategy backtesting module for this project.

The system should allow users to place strategy files inside a fixed strategy folder. The application should automatically discover available strategies from that folder, expose them to the frontend, allow the user to select a symbol and strategy, automatically read the selected strategy's configurable fields, render those fields as adjustable frontend inputs, run the backtest, then visualise the strategy execution and performance results.

Do not break existing functionality. Add the new module in a clean, isolated way. It is acceptable and preferred to add a new embedded page or panel dedicated to strategy backtesting.

Existing project context:
Inspect the current repository before implementing. The project already has market data, dashboard, charting, boundary testing and zone lifecycle modules. Reuse existing utilities where appropriate, but do not tightly couple the new backtesting module to existing experimental code.

Look at these areas first:

- `src/data/market_data.py`
- `src/dashboard_page.py`
- `src/market_dashboard.py`
- `src/plotting/chart_builder.py`
- `src/boundary_tester/`
- `src/zone_lifecycle/`
- existing config/output/report patterns

High-level architecture:

OHLCV data
→ Strategy discovery
→ Strategy selection in frontend
→ Automatic strategy parameter schema loading
→ Dynamic frontend strategy parameter form
→ Strategy receives one or more timeframe dataframes
→ Strategy emits standard trading signals
→ Backtest engine executes signals
→ Results are calculated
→ Chart displays candles, buy/sell/exit markers and equity curve
→ Trade table and summary metrics are shown

Add these new modules:

```text
src/backtesting/
    __init__.py
    schema.py
    engine.py
    metrics.py
    runner.py
    data_context.py

src/strategies/
    __init__.py
    base.py
    registry.py
    examples/
        supertrend_atr_trailing.py

src/pages/ or existing dashboard location:
    strategy_backtest_page.py

Use the actual existing frontend/dashboard structure if the project uses a different page layout. Keep the new page isolated.

Strategy folder and auto discovery

Create a fixed folder for custom strategies:

src/strategies/

The system should automatically discover strategies from this folder.

Each strategy file should define one class that inherits from BaseStrategy and declares:

name = "strategy_name"
display_name = "Human Readable Strategy Name"
description = "..."
required_timeframes = ["1d"]
default_config = {...}
config_schema = {...}

Some strategies may require multiple timeframes:

required_timeframes = ["15m", "1d"]

Implement a registry/discovery mechanism in src/strategies/registry.py.

Required functions:

def discover_strategies() -> dict:
    """
    Import strategy modules from src/strategies and src/strategies/examples.
    Return available strategy classes keyed by strategy name.
    """

def list_strategies() -> list[dict]:
    """
    Return frontend-friendly strategy metadata:
    name, display_name, description, required_timeframes, default_config, config_schema.
    """

def get_strategy(name: str, config: dict | None = None) -> BaseStrategy:
    """
    Instantiate a strategy by name.
    """

Avoid hard-coding only one strategy. Adding a new strategy file should make it available automatically.

Strategy protocol

Create src/strategies/base.py.

The strategy should support both single-timeframe and multi-timeframe inputs.

Define:

@dataclass
class StrategyContext:
    symbol: str
    primary_timeframe: str
    data: dict[str, pd.DataFrame]
    config: dict

A strategy receives a context object where:

context.data["15m"]
context.data["1d"]

are OHLCV dataframes.

The primary execution timeframe is usually the lower timeframe, for example 15m, while higher timeframe data such as 1d can be used as a filter.

Create a base strategy interface:

class BaseStrategy:
    name: str = "base"
    display_name: str = "Base Strategy"
    description: str = ""
    required_timeframes: list[str] = ["1d"]
    default_config: dict = {}
    config_schema: dict = {}

    def __init__(self, config: dict | None = None):
        self.config = {**self.default_config, **(config or {})}

    def prepare(self, context: StrategyContext) -> StrategyContext:
        return context

    def generate_signals(self, context: StrategyContext) -> pd.DataFrame:
        raise NotImplementedError

The returned dataframe should be based on the primary execution timeframe and preserve OHLCV columns.

Required signal columns:

open_long
close_long
open_short
close_short

The engine should fill missing signal columns with False.

Optional columns:

entry_reason
exit_reason
stop_price
take_profit_price
plot_*
signal_score
strategy_state
position_size_pct
position_notional
target_weight

Use four-way signals internally. Do not rely only on buy and sell, because they are ambiguous for long/short and close/open behaviour.

2.1 Strategy parameter UI and dynamic config loading

This is a required part of the implementation.

Each strategy must expose user-configurable parameters through default_config, and should expose a config_schema dictionary.

When a user selects or loads a strategy in the frontend, the system must automatically read that strategy's config_schema and render the strategy-specific configuration fields as editable frontend controls.

The frontend should not hard-code parameters for specific strategies. The form should be generated dynamically from the selected strategy metadata.

Supported config field types for MVP:

int
float
bool
str
select

Each field in config_schema may include:

type
label
default
min
max
step
options
help
required

Example:

class SuperTrendATRTrailingStrategy(BaseStrategy):
    name = "supertrend_atr_trailing"
    display_name = "SuperTrend + ATR Trailing Exit"
    description = "SuperTrend entries with ATR-based dynamic exits."
    required_timeframes = ["1d"]

    default_config = {
        "atr_period": 10,
        "supertrend_multiplier": 3.0,
        "atr_exit_mult": 1.5,
        "exit_on_opposite_signal": True,
        "direction": "both",
    }

    config_schema = {
        "atr_period": {
            "type": "int",
            "label": "ATR Period",
            "default": 10,
            "min": 1,
            "max": 100,
            "step": 1,
            "help": "ATR Wilder smoothing period.",
            "required": True,
        },
        "supertrend_multiplier": {
            "type": "float",
            "label": "SuperTrend Multiplier",
            "default": 3.0,
            "min": 0.5,
            "max": 10.0,
            "step": 0.1,
            "help": "ATR multiplier used to build SuperTrend bands.",
            "required": True,
        },
        "atr_exit_mult": {
            "type": "float",
            "label": "ATR Exit Multiplier",
            "default": 1.5,
            "min": 0.5,
            "max": 10.0,
            "step": 0.1,
            "help": "ATR multiplier used for trailing exits.",
            "required": True,
        },
        "exit_on_opposite_signal": {
            "type": "bool",
            "label": "Exit on Opposite Signal",
            "default": True,
            "help": "Close the current position when the opposite SuperTrend signal appears.",
        },
        "direction": {
            "type": "select",
            "label": "Direction",
            "default": "both",
            "options": ["long", "short", "both"],
            "help": "Allowed trading direction.",
            "required": True,
        },
    }

Frontend dynamic form behaviour:

When the strategy selector changes, call the strategy metadata/list endpoint or registry function.
Read the selected strategy's config_schema.
Render inputs based on field type:
int and float: numeric input
bool: checkbox or switch
str: text input
select: dropdown
Initialise each field with config_schema[field]["default"] if available, otherwise fallback to default_config[field].
Show label as the field name.
Show help as tooltip or helper text if the current frontend framework supports it.
Respect min, max, step, and options.
Validate required fields before running the backtest.
When running a backtest, pass the user-edited values as strategy_config.
If config_schema is missing or empty, fall back to rendering simple inputs inferred from default_config value types.

The strategy list exposed to the frontend must include config_schema.

Example frontend-friendly strategy metadata:

{
  "name": "supertrend_atr_trailing",
  "display_name": "SuperTrend + ATR Trailing Exit",
  "description": "SuperTrend entries with ATR-based dynamic exits.",
  "required_timeframes": ["1d"],
  "default_config": {
    "atr_period": 10,
    "supertrend_multiplier": 3.0,
    "atr_exit_mult": 1.5,
    "exit_on_opposite_signal": true,
    "direction": "both"
  },
  "config_schema": {
    "atr_period": {
      "type": "int",
      "label": "ATR Period",
      "default": 10,
      "min": 1,
      "max": 100,
      "step": 1
    }
  }
}
Strategy-controlled position sizing

The backtest engine should support both global position sizing and optional strategy-controlled sizing.

Default sizing:

position_notional = current_equity * backtest_config.position_size_pct

However, if the strategy output dataframe contains one of these optional columns on the entry bar, the engine should use it with the following priority:

1. position_notional
2. position_size_pct
3. target_weight
4. fallback to backtest_config.position_size_pct

Meaning:

position_notional: absolute cash notional allocated to the trade.
position_size_pct: percentage of current equity allocated to the trade.
target_weight: target portfolio weight for the position.

For MVP:

Clamp position_size_pct and target_weight to [0, 1].
Do not allow negative position size.
Do not allow position notional to exceed available equity unless leverage is explicitly added later.
If a strategy value is missing, null, invalid or non-finite, fall back to the global backtest config.
Record sizing information in each executed trade:
position_notional
position_size_pct
size_source

Possible size_source values:

config
strategy_position_notional
strategy_position_size_pct
strategy_target_weight
Multi-timeframe data support

Create src/backtesting/data_context.py.

Implement a helper that can load and align multiple OHLCV timeframes for a selected symbol.

Example:

build_strategy_context(
    symbol="MSFT",
    primary_timeframe="15m",
    required_timeframes=["15m", "1d"],
    start_date="2025-01-01",
    end_date="2026-05-31",
    strategy_config={...},
)

Requirements:

Fetch data for all required timeframes.
Primary timeframe should drive execution.
Higher timeframe features should be forward-filled onto the primary timeframe where needed.
Avoid look-ahead bias:
A daily signal should only be available to intraday bars after the daily candle is actually complete.
If exact completion handling is difficult in MVP, document the assumption clearly and make the alignment function explicit.
Keep the alignment logic separate from the engine.

The strategy itself may also perform alignment if needed, but provide a clean helper for common cases.

Backtest configuration

Create BacktestConfig in src/backtesting/schema.py.

Include:

initial_capital: float
slippage: float
commission_pct: float
position_size_pct: float
allow_short: bool
allow_long: bool
exit_before_entry: bool
price_col: str = "close"
primary_timeframe: str
start_date: str | None
end_date: str | None

Frontend should allow the user to configure at least:

Symbol
Primary timeframe
Strategy
Backtest start date
Backtest end date
Initial capital
Slippage percentage
Per-trade commission percentage
Position size percentage
Long/short direction options where possible
Strategy-specific parameters dynamically generated from selected strategy config_schema
Backtest engine

Create src/backtesting/engine.py.

Implement BacktestEngine.

The engine should:

Accept a StrategyContext, a strategy instance and BacktestConfig.
Call strategy.generate_signals(context).
Validate OHLCV columns.
Fill missing signal columns with False.
Iterate bar by bar.
Maintain one active position in MVP v1.
Support long and short.
Apply slippage.
Apply commission percentage per transaction.
Support default and strategy-controlled position sizing.
Exit before entry by default.
Record trades.
Record equity curve.
Return a BacktestResult.

Execution price model for MVP:

Use close price by default.
Long entry = close * (1 + slippage)
Long exit  = close * (1 - slippage)
Short entry = close * (1 - slippage)
Short exit  = close * (1 + slippage)

Commission:

commission = trade_notional * commission_pct

Position sizing:

Default:
position_notional = current_equity * position_size_pct

Strategy override:
position_notional / position_size_pct / target_weight from signal dataframe.
Data schemas

Create dataclasses in src/backtesting/schema.py.

Required dataclasses:

Position
Trade
BacktestConfig
BacktestResult
EquityPoint

Trade must support display fields like:

index
type
exit_reason
entry_time
exit_time
entry_price
exit_price
pnl
balance
bars_held
position_notional
position_size_pct
size_source

Example trade table row:

1 | LONG | Stop Loss | 02/03/2026, 18:00 | 02/13/2026, 18:00 | 268.8352 | 257.9527 | -409.60 | 9585.40

Use raw values in the data model and let UI/report formatting handle currency/date formatting.

Metrics

Create src/backtesting/metrics.py.

Calculate and return:

total_return
annualised_return
max_drawdown
sharpe_ratio
win_rate
profit_loss_ratio
profit_factor
trade_count
long_trade_count
short_trade_count
average_win
average_loss
average_bars_held
final_equity

Definitions:

Total return: (final_equity / initial_capital) - 1
Annualised return: based on start/end dates and total return
Max drawdown: calculated from equity curve
Sharpe ratio: use equity returns; assume risk-free rate 0 for MVP
Win rate: winning trades / total trades
Profit/loss ratio: average win / absolute average loss
Profit factor: gross profit / absolute gross loss

Runner

Create src/backtesting/runner.py.

Provide:

def run_backtest(
    symbol: str,
    strategy_name: str,
    strategy_config: dict,
    backtest_config: dict,
) -> BacktestResult:
    ...

Responsibilities:

Discover/load strategy.
Read required timeframes from the strategy.
Build the strategy context.
Run the engine.
Return result.

Also provide a lower-level function:

def run_backtest_from_context(
    context: StrategyContext,
    strategy_name: str,
    strategy_config: dict,
    backtest_config: dict,
) -> BacktestResult:
    ...

This makes testing easier.

Example strategy

Create example strategy:

src/strategies/examples/supertrend_atr_trailing.py

Strategy name:

name = "supertrend_atr_trailing"
display_name = "SuperTrend + ATR Trailing Exit"
required_timeframes = ["1d"]

Default config:

default_config = {
    "atr_period": 10,
    "supertrend_multiplier": 3.0,
    "atr_exit_mult": 1.5,
    "exit_on_opposite_signal": True,
    "direction": "both",
}

Also implement full config_schema for all default config fields.

Logic:

Calculate True Range.
Calculate ATR using Wilder-style smoothing.
Calculate SuperTrend direction.
Open long when SuperTrend flips from short to long.
Open short when SuperTrend flips from long to short.
Long exit:
Track highest high since long entry.
long_stop = highest_high_since_entry - atr_exit_mult * ATR
close_long=True when close < long_stop.
Short exit:
Track lowest low since short entry.
short_stop = lowest_low_since_entry + atr_exit_mult * ATR
close_short=True when close > short_stop.
If exit_on_opposite_signal=True, also close long on short flip and close short on long flip.

Output columns:

atr
supertrend_direction
supertrend
plot_supertrend
plot_atr_stop
open_long
close_long
open_short
close_short
entry_reason
exit_reason
position_size_pct, optional if strategy wants to control sizing
Multi-timeframe example strategy

Create another simple example strategy if time allows:

src/strategies/examples/multi_timeframe_trend_filter.py

Purpose:

Use 1d trend as higher timeframe filter.
Use 15m bars for entries.
Example:
Only allow long entries on 15m when 1d trend is bullish.
Only allow short entries on 15m when 1d trend is bearish.

This demonstrates that strategies can use multiple K-line levels jointly.

This strategy should also define default_config and config_schema, so the frontend can dynamically render its parameters.

New embedded UI page

Add a new embedded page/panel for strategy backtesting.

The exact location should match the existing dashboard structure. If there is a sidebar/page system, add a page such as:

Strategy Backtest

UI requirements:

Inputs:

Symbol selector or text input
Primary timeframe selector
Strategy selector, populated from discovered strategies
Backtest start date
Backtest end date
Initial capital
Slippage percentage
Commission percentage per trade
Position size percentage
Long/short options
Strategy-specific config fields generated dynamically from the selected strategy's config_schema
Run Backtest button

Dynamic strategy config UI requirements:

When a strategy is selected, immediately load and display its configurable fields.
Do not hard-code fields for specific strategies.
Use config_schema to generate input widgets.
If no config_schema is available, infer simple controls from default_config.
User-edited values must be passed into strategy_config when running the backtest.
If the user changes strategy, reset the strategy parameter form to the newly selected strategy's defaults.
The UI should clearly separate:
Backtest environment settings
Strategy-specific settings

Outputs:

Candlestick chart
Buy/sell/exit markers on the chart
Optional strategy overlay lines from plot_* columns
Equity curve chart
Summary metric cards:
Total return
Annualised return
Max drawdown
Sharpe ratio
Win rate
Profit/loss ratio
Trade count
Final equity
Trade detail table with columns:
No.
Type
Exit reason
Entry time
Exit time
Entry price
Exit price
PnL
Balance

The table should support negative PnL display and preserve raw numeric values internally.

Chart marker helper

Add a lightweight helper if needed:

def build_chart_payload(result: BacktestResult) -> dict:
    ...

It should convert result data into a frontend-friendly structure:

{
    "candles": ...,
    "entries": ...,
    "exits": ...,
    "overlays": ...,
    "equity_curve": ...,
    "metrics": ...,
    "trades": ...,
}

Overlay rule:

Any signal dataframe column starting with plot_ can be offered as chart overlay.
Entry/exit markers should come from executed trades, not only raw signals, so the chart reflects actual executed positions.
Config sample

Add:

src/backtesting.sample.yaml

Example:

backtest:
  symbol: MSFT
  primary_timeframe: 1d
  start_date: "2025-01-01"
  end_date: "2026-05-31"
  initial_capital: 10000
  slippage: 0.0005
  commission_pct: 0.0005
  position_size_pct: 1.0
  allow_long: true
  allow_short: true
  exit_before_entry: true
  price_col: close

strategy:
  name: supertrend_atr_trailing
  config:
    atr_period: 10
    supertrend_multiplier: 3.0
    atr_exit_mult: 1.5
    exit_on_opposite_signal: true
    direction: both
Tests

Add tests with synthetic OHLCV data.

Test cases:

Strategy discovery finds example strategies.
Strategy metadata is frontend-friendly and includes config_schema.
Frontend/API-facing metadata includes strategy configurable fields.
Strategy config schema can be used to build default strategy config.
Engine can execute a long trade.
Engine can execute a short trade.
Engine applies slippage and commission.
Engine supports config-based position sizing.
Engine supports strategy-controlled position sizing from position_size_pct.
Engine records equity curve.
Metrics include all required keys.
SuperTrend ATR strategy runs end-to-end.
Multi-timeframe context can hold both 15m and 1d data.
Missing signal columns are filled as False.

Do not rely on external APIs in tests.

Code quality requirements
Keep the new system modular.
Use type hints.
Use dataclasses for schemas.
Keep existing functionality untouched.
Avoid large framework changes.
Avoid over-engineering.
MVP first, but design interfaces cleanly for future expansion.
Document assumptions, especially around multi-timeframe alignment and look-ahead prevention.
Document the strategy parameter schema system clearly.
Deliverables

After implementation, provide:

List of added/modified files.
Explanation of the strategy plugin architecture.
Explanation of how strategies are auto-discovered.
Explanation of how strategy configurable fields are loaded and rendered in the frontend.
Explanation of the backtest execution flow.
Explanation of how the new embedded page works.
Example of adding a new strategy file with default_config and config_schema.
Example of running a backtest from Python.
Any assumptions or TODOs.

Final acceptance criteria:

Strategies live in a fixed folder and are auto-discovered.
Each strategy can declare default_config and config_schema.
When a strategy is selected, the frontend automatically displays its configurable fields.
User can edit strategy-specific parameters before running the backtest.
Frontend can list and select strategies.
User can choose symbol, timeframe, backtest period, initial capital, slippage and commission percentage.
A selected strategy can run on OHLCV data.
Strategies can request multiple timeframes such as 15m and 1d.
The engine executes standard four-way signals.
The engine supports both global position sizing and strategy-controlled position sizing.
The chart displays executed buy/sell/exit points.
The chart displays equity curve.
The result includes:
total return
annualised return
max drawdown
Sharpe ratio
win rate
profit/loss ratio
trade count
equity curve
detailed trade records
Existing project functionality still works