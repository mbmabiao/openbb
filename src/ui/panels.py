from __future__ import annotations

import streamlit as st

from data.market_data import to_dataframe
from ui.sidebar import DashboardControls


def show_dataframe_result(title, fetcher, empty_message: str = "No data returned.") -> None:
    st.subheader(title)
    try:
        result = fetcher()
        frame = to_dataframe(result)
        if frame is not None and not frame.empty:
            st.dataframe(frame, use_container_width=True)
        else:
            st.info(empty_message)
    except Exception as error:
        st.error(f"Error: {error}")


def show_news(
    title,
    fetcher,
    news_limit: int,
    empty_message: str = "No news data returned.",
) -> None:
    st.subheader(title)
    try:
        result = fetcher()
        frame = to_dataframe(result)

        if frame is None or frame.empty:
            st.info(empty_message)
            return

        preferred_columns = [
            column
            for column in ["date", "title", "source", "publisher", "url"]
            if column in frame.columns
        ]
        if preferred_columns:
            st.dataframe(frame[preferred_columns], use_container_width=True)
        else:
            st.dataframe(frame, use_container_width=True)

        if "title" in frame.columns:
            st.markdown("### Latest Headlines")
            for _, row in frame.head(news_limit).iterrows():
                title_value = row.get("title", "Untitled")
                article_date = row.get("date", "")
                source = row.get("source", row.get("publisher", ""))
                url = row.get("url", "")

                st.markdown(f"**{title_value}**")
                meta = " | ".join([str(value) for value in [article_date, source] if value])
                if meta:
                    st.caption(meta)
                if url:
                    st.markdown(f"[Open article]({url})")
                st.divider()
    except Exception as error:
        st.error(f"Error: {error}")


def show_definitions(controls: DashboardControls) -> None:
    st.markdown("### Definitions")
    st.markdown(
        f"""
**This version adds multi-timeframe confluence, reaction validation, and replay mode.**

**1) Daily and Weekly Zones**
- Daily VP input: recent **{controls.vp_lookback_days}** trading days of **1h OHLCV**
- Higher-timeframe VP input: recent **{controls.weekly_vp_lookback}** weekly windows of **1d OHLCV**
- Composite VP method: each source bar distributes volume across all covered price bins
- No fallback to lower-precision VP when the required source interval is unavailable
- Each zone explicitly records timeframe source(s)

**2) Multi-timeframe confluence**
- Daily and weekly zones are merged when close or overlapping
- Zones with both **D** and **W** sources get a confluence bonus

**3) Reaction validation**
For each zone, the system tracks:
- touch count
- first-touch quality
- strong reaction rate
- reclaim rate
- repeated-test decay

**4) Institutional score**
Combines:
- volume structure
- inventory logic
- AVWAP contribution
- timeframe confluence
- historical reaction quality
- width penalty

**5) Replay mode**
- choose any historical trading date
- treat that selected date as "today" for all calculations
- buttons and date input are synchronized

**6) Latest bar handling**
- chart frame: {"show live last bar" if controls.show_live_last_bar_on_chart else "hide live last bar"}
- calculation frame: {"exclude latest bar" if controls.exclude_last_unclosed_bar else "include latest bar"}
"""
    )
