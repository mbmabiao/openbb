# OpenBB Equity Research Dashboard

This is an equity research application built with `Streamlit + OpenBB + yfinance`. The entry point is [src/app.py](src/app.py). The top navigation has three primary modules:

- `Stock`: single-stock research, historical candles, VAP/volume profile, support/resistance zones, financial statements, and news.
- `Backtest`: custom strategy backtesting, strategy parameter UI, trade chart, equity curve, and trade details.
- `Market`: ETF market rotation, sector/theme/style/value-chain classification, leaders/laggards, and market breadth.

The project is designed for research and visualization. It is not a live trading execution system.

## Quick Start

Python 3.11 or 3.12 is recommended.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run src/app.py
```

After startup, open the local URL printed by Streamlit. Enter a symbol in the sidebar, for example:

- `AAPL`
- `MSFT`
- `NVDA`
- `000300.SS`

## Stock Module

`Stock` is the single-stock research page. It uses the symbol/provider/range settings from the sidebar and provides multiple tabs inside the page.

### Run Warmup First

The Historical Price page in `Stock` reads zone lifecycle snapshot data. To show support/resistance zones, pattern events, divergence events, and replay queries correctly, run warmup snapshots for the target symbol first.

Single symbol:

```powershell
python src\build_zone_snapshots.py --symbol MSFT --lookback-years 5 --provider yfinance --reset
```

Specific date range:

```powershell
python src\build_zone_snapshots.py --symbol MSFT --start-date 2021-01-01 --end-date 2026-06-05 --provider yfinance --reset
```

Batch scripts:

```powershell
.\scripts\warmup.ps1
.\scripts\warmup_etf.ps1
```

Warmup writes to this database by default:

```text
outputs/zone_lifecycle.sqlite
```

Common parameters:

- `--symbol`: stock or ETF ticker.
- `--lookback-years`: how many years of snapshots to build back from today.
- `--start-date` / `--end-date`: explicit snapshot date range.
- `--provider`: OpenBB provider, for example `yfinance`.
- `--reset`: delete existing lifecycle data for the symbol before rebuilding.
- `--no-force`: process incrementally instead of rebuilding the whole range.
- `--warmup-config-path`: custom warmup threshold config. Defaults to [src/config/warmup.yaml](src/config/warmup.yaml).

### Historical Price

Historical Price is the core page of the Stock module and is driven by [src/dashboard_page.py](src/dashboard_page.py).

Main features:

- Load and clean historical OHLCV prices.
- Render black-background candlestick charts in a lightweight-charts style.
- Show volume histogram.
- Show long VAP / volume profile distribution on the side.
- Show support/resistance zones, zone labels, and zone bands.
- Show EMA20, EMA50, and ATR bands.
- Support replay date, treating a historical date as the current point in time.
- Read pattern events, MACD divergence events, and breakout events from the warmup database.

Recommended workflow:

1. Run warmup for the symbol first.
2. Start the app and enter `Stock`.
3. Enter the symbol in the sidebar and choose a history range.
4. Adjust VAP bins, zone expand, EMA, ATR multiple, bar handling, and related settings.
5. Select a replay date in Historical Price.
6. Review the chart candles, volume, VAP, zones, event markers, and the zone table below.

### Financial And News Tabs

The Stock module also includes:

- `Income`
- `Balance Sheet`
- `Cash Flow`
- `Ratios`
- `News`

These tabs fetch financial statements, ratios, and news through OpenBB. The sidebar fields `Fundamentals provider` and `News provider` can be left blank or set to a provider name.

## Backtest Module

`Backtest` is a primary page rendered by [src/ui/strategy_backtest_page.py](src/ui/strategy_backtest_page.py).

Main features:

- Automatically discover strategies under [src/strategies](src/strategies).
- Read each strategy's `default_config` and `config_schema` to generate a parameter form automatically.
- Choose symbol, primary timeframe, date range, provider, and extended hours.
- Configure initial capital, slippage, commission, position size, and long/short switches.
- Render the backtest price chart in a lightweight-charts style, including candles, volume, `plot_` overlays, entry/exit markers, and overlay legend.
- Keep Equity Curve in Plotly.
- Show Trade Details with each trade, exit reason, P/L, P/L %, and balance.

### Strategy Development Protocol

The Backtest page has a `download develop protocol` text link under the title. It downloads the English strategy development protocol. The source file is:

[src/backtesting/README.md](src/backtesting/README.md)

Core protocol:

- Required signal columns: `open_long`, `close_long`, `open_short`, `close_short`.
- Optional intrabar trigger price columns: `open_long_price`, `close_long_price`, `open_short_price`, `close_short_price`.
- Optional overlay columns: any column starting with `plot_` is drawn on the backtest chart.
- Optional sizing columns: `position_size_pct`, `position_notional`, `target_weight`.
- If no `*_price` columns are provided, the engine keeps the default close-price execution.
- If a `*_price` column is provided, a finite positive price must be inside the current bar range `low <= price <= high` to trigger.

### Add Your Own Strategy

Add a new strategy file under [src/strategies/examples](src/strategies/examples) or [src/strategies](src/strategies), and inherit from `BaseStrategy`.

Minimal structure:

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

After saving the file, click `Refresh` on the Backtest page. The new strategy will be discovered.

### Run A Backtest From Python

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

## Market Module

`Market` is an ETF market rotation dashboard rendered by [src/market_dashboard.py](src/market_dashboard.py). It fetches market data directly with `yfinance`.

Main uses:

- Watch market benchmarks: `SPY`, `QQQ`, `IWM`, `DIA`.
- Watch sector rotation: `XLK`, `XLF`, `XLE`, `XLV`, and others.
- Watch themes: AI, semiconductors, cloud, clean energy, defense, crypto-linked equities, and more.
- Watch value-chain confirmation: upstream, midstream, downstream, infrastructure, power supply, and related groups.
- Watch style factors: quality, momentum, value, low volatility, small caps, dividends, and more.
- Watch business models, revenue exposure, and supply-chain relationships.

The page computes:

- Recent performance for each ETF.
- Position relative to the 20-day and 50-day moving averages.
- Leaders and laggards for each classification.
- 20MA breadth and 50MA breadth.
- Module-level diagnosis for risk appetite, theme breadth, value-chain confirmation, or divergence.

Usage:

1. Click `Market` in the top navigation.
2. The dashboard auto-refreshes every `10` seconds by default; manual refresh is also available.
3. Start with Market Benchmark to judge broad risk appetite.
4. Then inspect Sector, Theme, Value Chain, Style Factor, and other modules to see whether strength is spreading.
5. Use leaders/laggards and breadth to judge whether the market is concentrated, broadening, or rotating defensively.

## Project Structure

```text
src/
  app.py                         Streamlit entry point and top navigation
  dashboard_page.py              Stock / Historical Price page
  market_dashboard.py            Market ETF rotation dashboard
  build_zone_snapshots.py        Offline warmup / zone snapshot entry point
  backtesting/                   Backtest engine, schema, runner, and docs
  strategies/                    Strategy base class, discovery, and examples
  data/                          OpenBB data fetching and OHLCV cleaning
  engines/                       Replay, zone generation, and validation
  features/                      Volume profile, ATR, and zone strength
  plotting/                      lightweight-charts components and backtest adapter
  ui/                            Sidebar, page state, and backtest page
  zone_lifecycle/                Zone lifecycle, events, snapshots, and SQLite repository
  config/                        App defaults and warmup thresholds
  test/                          Unit tests
scripts/
  warmup.ps1                     Warmup example for one or a few stocks
  warmup_etf.ps1                 Batch warmup example for ETFs
```

## Configuration Files

- [src/config/settings.py](src/config/settings.py): app defaults, such as default symbol, history range, and ATR multiple.
- [src/config/warmup.yaml](src/config/warmup.yaml): thresholds for zone lifecycle, breakout, pattern event, divergence event, and zone generation.
- [src/backtesting.sample.yaml](src/backtesting.sample.yaml): sample backtest configuration.

## Tests

After installing dependencies:

```powershell
pytest -q src/test
```

For syntax checks only:

```powershell
python -m py_compile src/app.py src/ui/strategy_backtest_page.py src/backtesting/engine.py
```

## Data And Outputs

Common output locations:

- `outputs/zone_lifecycle.sqlite`: zone lifecycle and replay snapshot database.
- `src/outputs/`: outputs from some research tools or test scripts.
- `reports/`, `outputs/`: local analysis reports and exported results.

These outputs are usually local generated artifacts and should not be mixed into source commits unless they are intentionally archived.

## Dependencies

Dependencies are listed in [requirements.txt](requirements.txt). Core packages include:

- `streamlit`
- `openbb`
- `yfinance`
- `pandas`
- `numpy`
- `plotly`
- `SQLAlchemy`
- `PyYAML`
- `pytest`

`Stock` and `Backtest` mainly fetch price and fundamental data through OpenBB. The `Market` module uses `yfinance` directly.
