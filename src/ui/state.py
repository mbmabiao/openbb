from __future__ import annotations

from bisect import bisect_left
from datetime import date, datetime

import pandas as pd
import streamlit as st

from engines.replay_engine import list_replay_dates


def get_replay_session_key(symbol: str) -> str:
    return f"replay_date_{symbol}"


def get_replay_date_state(df_calc: pd.DataFrame, symbol: str) -> pd.Timestamp:
    available_dates = [value.date() for value in list_replay_dates(df_calc)]
    if not available_dates:
        raise ValueError("No dates available for replay.")

    session_key = get_replay_session_key(symbol)

    if session_key not in st.session_state:
        st.session_state[session_key] = available_dates[-1]

    st.session_state[session_key] = _snap_to_available_replay_date(
        st.session_state[session_key],
        available_dates,
    )
    return pd.Timestamp(st.session_state[session_key])


def render_replay_controls(df_calc: pd.DataFrame, symbol: str) -> pd.Timestamp:
    available_dates = [value.date() for value in list_replay_dates(df_calc)]
    if not available_dates:
        raise ValueError("No dates available for replay.")

    session_key = get_replay_session_key(symbol)

    if session_key not in st.session_state:
        st.session_state[session_key] = available_dates[-1]

    st.session_state[session_key] = _snap_to_available_replay_date(
        st.session_state[session_key],
        available_dates,
    )

    date_to_index = {value: index for index, value in enumerate(available_dates)}

    def move_replay(delta: int) -> None:
        current = _coerce_date(st.session_state[session_key])
        if current in date_to_index:
            index = date_to_index[current] + delta
        else:
            insertion_index = bisect_left(available_dates, current)
            index = insertion_index if delta > 0 else insertion_index - 1
        new_index = min(max(index, 0), len(available_dates) - 1)
        st.session_state[session_key] = available_dates[new_index]

    col_prev, col_date, col_next = st.columns([1, 2, 1])

    with col_prev:
        st.button(
            "Prev Day",
            key=f"replay_prev_day_{symbol}",
            on_click=move_replay,
            args=(-1,),
            disabled=(date_to_index[st.session_state[session_key]] == 0),
            use_container_width=True,
        )

    with col_date:
        st.date_input(
            "Replay date (treated as today)",
            min_value=available_dates[0],
            max_value=available_dates[-1],
            key=session_key,
            label_visibility="collapsed",
        )

    with col_next:
        st.button(
            "Next Day",
            key=f"replay_next_day_{symbol}",
            on_click=move_replay,
            args=(1,),
            disabled=(date_to_index[st.session_state[session_key]] == len(available_dates) - 1),
            use_container_width=True,
        )

    current_index = date_to_index[st.session_state[session_key]]
    st.caption(
        f"Replay date: {st.session_state[session_key]} | "
        f"Step {current_index + 1}/{len(available_dates)}"
    )

    return pd.Timestamp(st.session_state[session_key])


def _snap_to_available_replay_date(
    current_value,
    available_dates: list[date],
) -> date:
    current_date = _coerce_date(current_value)
    insertion_index = bisect_left(available_dates, current_date)
    if insertion_index < len(available_dates):
        return available_dates[insertion_index]
    return available_dates[-1]


def _coerce_date(value) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()
