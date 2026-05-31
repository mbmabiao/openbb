from __future__ import annotations

import math

import numpy as np
import pandas as pd

from backtesting.schema import EquityPoint, Trade


def calculate_metrics(
    *,
    initial_capital: float,
    equity_curve: list[EquityPoint],
    trades: list[Trade],
) -> dict[str, float | int | None]:
    final_equity = equity_curve[-1].equity if equity_curve else initial_capital
    total_return = (final_equity / initial_capital) - 1 if initial_capital else 0.0

    equity_df = pd.DataFrame(
        [{"date": point.time, "equity": point.equity} for point in equity_curve]
    )
    annualised_return = _annualised_return(equity_df, total_return)
    max_drawdown = _max_drawdown(equity_df)
    sharpe_ratio = _sharpe_ratio(equity_df)

    wins = [trade.pnl for trade in trades if trade.pnl > 0]
    losses = [trade.pnl for trade in trades if trade.pnl < 0]
    gross_profit = float(sum(wins))
    gross_loss = float(sum(losses))
    average_win = float(np.mean(wins)) if wins else 0.0
    average_loss = float(np.mean(losses)) if losses else 0.0

    return {
        "total_return": float(total_return),
        "annualised_return": annualised_return,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
        "win_rate": len(wins) / len(trades) if trades else 0.0,
        "profit_loss_ratio": average_win / abs(average_loss) if average_loss else None,
        "profit_factor": gross_profit / abs(gross_loss) if gross_loss else None,
        "trade_count": len(trades),
        "long_trade_count": sum(1 for trade in trades if trade.type == "LONG"),
        "short_trade_count": sum(1 for trade in trades if trade.type == "SHORT"),
        "average_win": average_win,
        "average_loss": average_loss,
        "average_bars_held": float(np.mean([trade.bars_held for trade in trades])) if trades else 0.0,
        "final_equity": float(final_equity),
    }


def _annualised_return(equity_df: pd.DataFrame, total_return: float) -> float | None:
    if equity_df.empty or len(equity_df) < 2:
        return None
    start = pd.to_datetime(equity_df["date"].iloc[0], errors="coerce")
    end = pd.to_datetime(equity_df["date"].iloc[-1], errors="coerce")
    days = max((end - start).days, 0)
    if days <= 0:
        return None
    return float((1.0 + total_return) ** (365.0 / days) - 1.0)


def _max_drawdown(equity_df: pd.DataFrame) -> float:
    if equity_df.empty:
        return 0.0
    equity = pd.to_numeric(equity_df["equity"], errors="coerce").dropna()
    if equity.empty:
        return 0.0
    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0
    return float(drawdown.min())


def _sharpe_ratio(equity_df: pd.DataFrame) -> float | None:
    if equity_df.empty or len(equity_df) < 3:
        return None
    returns = pd.to_numeric(equity_df["equity"], errors="coerce").pct_change().dropna()
    std = float(returns.std(ddof=1)) if not returns.empty else 0.0
    if not math.isfinite(std) or std == 0.0:
        return None
    return float((returns.mean() / std) * math.sqrt(252))

