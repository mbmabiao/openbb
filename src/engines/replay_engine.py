from __future__ import annotations

import pandas as pd


def list_replay_dates(df_calc: pd.DataFrame) -> list[pd.Timestamp]:
    if df_calc.empty:
        return []
    return (
        pd.to_datetime(df_calc["date"])
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


def prepare_plot_and_calc_frames(
    df: pd.DataFrame,
    exclude_last_bar_for_calc: bool,
    show_last_bar_on_chart: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_plot = df.copy()
    df_calc = df.copy()

    if exclude_last_bar_for_calc and len(df_calc) > 1:
        df_calc = df_calc.iloc[:-1].copy()

    if (not show_last_bar_on_chart) and len(df_plot) > 1:
        df_plot = df_plot.iloc[:-1].copy()

    return df_plot, df_calc


def prepare_replay_frame(
    df_plot: pd.DataFrame,
    df_calc: pd.DataFrame,
    replay_date_value: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    replay_timestamp = pd.Timestamp(replay_date_value).normalize()

    plot_dates = pd.to_datetime(df_plot["date"]).dt.normalize()
    calc_dates = pd.to_datetime(df_calc["date"]).dt.normalize()

    plot_mask = plot_dates <= replay_timestamp
    df_plot_replay = df_plot.loc[plot_mask].copy().reset_index(drop=True)

    prior_calc_dates = calc_dates[calc_dates < replay_timestamp]
    if prior_calc_dates.empty:
        df_calc_replay = df_calc.iloc[0:0].copy().reset_index(drop=True)
    else:
        calc_cutoff = prior_calc_dates.max()
        calc_mask = calc_dates <= calc_cutoff
        df_calc_replay = df_calc.loc[calc_mask].copy().reset_index(drop=True)

    return df_plot_replay, df_calc_replay
