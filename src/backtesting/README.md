# Backtesting 信号协议

策略通过 `generate_signals(context)` 返回主执行周期上的 OHLCV dataframe。engine 逐 bar 执行信号。

## 必需信号列

四方向信号协议保持不变：

- `open_long`
- `close_long`
- `open_short`
- `close_short`

如果策略没有返回这些列，engine 会自动补成 `False`。

## 默认成交价

默认行为不变：如果策略没有提供任何 `*_price` 列，engine 使用 `row[backtest_config.price_col]` 成交，通常是 `close`。

slippage 仍在 engine 内应用：

- long entry: `price * (1 + slippage)`
- long exit: `price * (1 - slippage)`
- short entry: `price * (1 - slippage)`
- short exit: `price * (1 + slippage)`

`Trade.entry_price` 和 `Trade.exit_price` 记录的是实际执行价，也就是包含 slippage 后的价格。

## 可选 intrabar 触发价列

策略可以选择提供 action-specific 价格列：

- `open_long_price`
- `close_long_price`
- `open_short_price`
- `close_short_price`

这些列不是必需的。只有策略提供对应列时，engine 才会在对应 action 上优先使用它。

规则：

- `NaN`、`inf`、非 finite、`<= 0` 的价格无效，engine fallback 到 `row[price_col]`。
- finite 且 `> 0` 的价格必须满足 `low <= price <= high`。
- 如果 finite 正价格不在当前 bar 的 high/low 范围内，该 action 当 bar 不执行。
- 不实现 `next_open`、`warmup` 或新的 execution model。

买入侧触发价：

- `open_long_price`
- `close_short_price`

卖出侧触发价：

- `open_short_price`
- `close_long_price`

## 示例

盘前 gap 策略可以返回：

```python
df["open_short"] = False
df["open_short_price"] = np.nan

trigger = prev_close + gap_atr_mult * atr
if should_short:
    df.at[idx, "open_short"] = True
    df.at[idx, "open_short_price"] = trigger
```

如果当前 5m bar 满足：

```text
low <= open_short_price <= high
```

engine 会用 `open_short_price` 开空；否则该 bar 不开空。没有 `open_short_price` 列的策略继续按默认 close 成交。
