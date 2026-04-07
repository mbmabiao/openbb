from __future__ import annotations

import os
os.environ["OPENBB_AUTO_BUILD"] = "false"

import json
from datetime import date, timedelta

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from openbb import obb


st.set_page_config(page_title="Institutional Support/Resistance Dashboard", layout="wide")
st.title("Equity Data Dashboard")


# =========================================================
# SIDEBAR
# =========================================================
symbol = st.sidebar.text_input("Symbol", value="000300.SS").strip().upper()
price_provider = st.sidebar.text_input("Price provider (optional)", value="").strip() or None
fund_provider = st.sidebar.text_input("Fundamentals provider (optional)", value="").strip() or None
news_provider = st.sidebar.text_input("News provider (optional)", value="").strip() or None

history_range = st.sidebar.selectbox(
    "Price history range",
    options=["1Y", "3Y", "5Y", "10Y", "Max"],
    index=2,
)

news_limit = st.sidebar.slider("News items", min_value=5, max_value=50, value=10, step=5)

st.sidebar.markdown("---")
st.sidebar.subheader("Institutional Zone Settings")

vp_lookback_days = 20

vp_bins = st.sidebar.slider(
    "Composite VP price bins",
    min_value=20,
    max_value=120,
    value=48,
    step=4,
)

weekly_vp_lookback = st.sidebar.slider(
    "Weekly volume profile lookback bars",
    min_value=20,
    max_value=156,
    value=26,
    step=4,
)

weekly_vp_bins = st.sidebar.slider(
    "Weekly volume profile price bins",
    min_value=10,
    max_value=60,
    value=24,
    step=2,
)

zone_expand_bp = st.sidebar.slider(
    "Zone expand (bp)",
    min_value=10,
    max_value=300,
    value=50,
    step=10,
)
zone_expand_pct = zone_expand_bp / 10000.0

hv_node_quantile_pct = st.sidebar.slider(
    "High-volume node quantile (%)",
    min_value=50,
    max_value=95,
    value=75,
    step=5,
)
hv_node_quantile = hv_node_quantile_pct / 100.0

merge_pct_bp = st.sidebar.slider(
    "Merge nearby zones (bp)",
    min_value=10,
    max_value=200,
    value=60,
    step=10,
)
merge_pct = merge_pct_bp / 10000.0

max_resistance_zones = st.sidebar.slider(
    "Maximum resistance zones to display",
    min_value=1,
    max_value=8,
    value=3,
    step=1,
)

max_support_zones = st.sidebar.slider(
    "Maximum support zones to display",
    min_value=1,
    max_value=8,
    value=3,
    step=1,
)

show_avwap_lines = st.sidebar.checkbox("Show anchored VWAP lines", value=True)
show_all_candidate_zones = st.sidebar.checkbox("Show all candidate zones table", value=True)

st.sidebar.markdown("---")
st.sidebar.subheader("ATR Overlay")

show_atr_bands = st.sidebar.checkbox("Show recent 20-day ATR bands", value=False)
atr_multiplier = st.sidebar.slider(
    "ATR multiple",
    min_value=1.5,
    max_value=3.0,
    value=2.0,
    step=0.1,
)

st.sidebar.markdown("---")
st.sidebar.subheader("Reaction Validation")

reaction_lookahead = st.sidebar.slider(
    "Reaction lookahead bars",
    min_value=1,
    max_value=15,
    value=5,
    step=1,
)

reaction_threshold_bp = st.sidebar.slider(
    "Strong reaction threshold (bp)",
    min_value=20,
    max_value=800,
    value=150,
    step=10,
)
reaction_return_threshold = reaction_threshold_bp / 10000.0

min_touch_gap = st.sidebar.slider(
    "Minimum bars between distinct touches",
    min_value=1,
    max_value=20,
    value=3,
    step=1,
)

st.sidebar.markdown("---")
st.sidebar.subheader("Bar Handling")

exclude_last_unclosed_bar = st.sidebar.checkbox(
    "Exclude latest unclosed bar from calculations",
    value=True,
)

show_live_last_bar_on_chart = st.sidebar.checkbox(
    "Show latest live bar on chart",
    value=True,
)

if not symbol:
    st.warning("Enter a symbol in the sidebar.")
    st.stop()


# =========================================================
# HELPERS
# =========================================================
def get_start_date_from_range(range_label: str) -> str | None:
    today = date.today()
    if range_label == "1Y":
        return str(today - timedelta(days=365))
    if range_label == "3Y":
        return str(today - timedelta(days=365 * 3))
    if range_label == "5Y":
        return str(today - timedelta(days=365 * 5))
    if range_label == "10Y":
        return str(today - timedelta(days=365 * 10))
    if range_label == "Max":
        return None
    return str(today - timedelta(days=365 * 5))


def to_dataframe(result):
    if result is None:
        return None
    if hasattr(result, "to_dataframe"):
        return result.to_dataframe()
    if hasattr(result, "to_df"):
        return result.to_df()
    if isinstance(result, pd.DataFrame):
        return result
    try:
        return pd.DataFrame(result)
    except Exception:
        return None


def normalise_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if isinstance(out.index, pd.DatetimeIndex):
        index_name = out.index.name if out.index.name is not None else "date"
        if index_name in out.columns:
            index_name = "__index_date__"
        out = out.reset_index(names=index_name)
    else:
        out = out.reset_index(drop=False)
        if "date" in out.columns and "index" in out.columns:
            out = out.drop(columns=["index"])

    rename_map = {}
    for col in out.columns:
        lower = str(col).lower().strip()
        if lower in ("date", "datetime", "timestamp", "time", "__index_date__"):
            rename_map[col] = "date"
        elif lower in ("open", "adj_open"):
            rename_map[col] = "open"
        elif lower in ("high", "adj_high"):
            rename_map[col] = "high"
        elif lower in ("low", "adj_low"):
            rename_map[col] = "low"
        elif lower in ("close", "adj_close", "price"):
            rename_map[col] = "close"
        elif lower in ("volume", "vol"):
            rename_map[col] = "volume"

    out = out.rename(columns=rename_map)

    if "date" not in out.columns and len(out.columns) > 0:
        first_col = out.columns[0]
        trial = pd.to_datetime(out[first_col], errors="coerce")
        if trial.notna().sum() > 0:
            out["date"] = trial

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        try:
            if getattr(out["date"].dt, "tz", None) is not None:
                out["date"] = out["date"].dt.tz_localize(None)
        except (AttributeError, TypeError):
            pass

    out = out.reset_index(drop=True)
    out.index.name = None

    if "date" in out.columns:
        out = out.dropna(subset=["date"])
        out = out.sort_values(by="date", kind="stable").reset_index(drop=True)

    return out


def fetch_price_history(
    symbol_value: str,
    start_date_value: str | None,
    provider_value: str | None,
    end_date_value: str | None = None,
    interval_value: str | None = None,
    adjustment_value: str | None = None,
    extended_hours_value: bool | None = None,
):
    kwargs = {"symbol": symbol_value}
    if start_date_value is not None:
        kwargs["start_date"] = start_date_value
    if end_date_value is not None:
        kwargs["end_date"] = end_date_value
    if interval_value:
        kwargs["interval"] = interval_value
    if adjustment_value:
        kwargs["adjustment"] = adjustment_value
    if extended_hours_value is not None:
        kwargs["extended_hours"] = extended_hours_value
    if provider_value:
        kwargs["provider"] = provider_value
    return obb.equity.price.historical(**kwargs)


def get_recent_trading_dates(df: pd.DataFrame, lookback_days: int) -> list[pd.Timestamp]:
    if df.empty:
        return []

    trading_dates = (
        pd.to_datetime(df["date"])
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    if not trading_dates:
        return []

    return [pd.Timestamp(d).normalize() for d in trading_dates[-lookback_days:]]


def get_recent_trading_dates_for_weekly_window(
    df: pd.DataFrame,
    weekly_lookback_bars: int,
) -> list[pd.Timestamp]:
    if df.empty or weekly_lookback_bars < 1:
        return []

    daily_dates = pd.to_datetime(df["date"]).dt.normalize()
    weekly_periods = daily_dates.dt.to_period("W-FRI")
    period_df = pd.DataFrame({
        "date": daily_dates,
        "week_period": weekly_periods,
    }).drop_duplicates(subset=["date"]).sort_values("date", kind="stable")

    if period_df.empty:
        return []

    recent_week_periods = period_df["week_period"].drop_duplicates().tolist()[-weekly_lookback_bars:]
    if not recent_week_periods:
        return []

    recent_period_set = set(recent_week_periods)
    selected = period_df.loc[period_df["week_period"].isin(recent_period_set), "date"].tolist()
    return [pd.Timestamp(d).normalize() for d in selected]


def fetch_interval_history_for_dates(
    symbol_value: str,
    trading_dates: list[pd.Timestamp],
    provider_value: str | None,
    interval_value: str,
) -> pd.DataFrame:
    if not trading_dates:
        return pd.DataFrame()

    start_ts = pd.Timestamp(trading_dates[0]).normalize()
    end_ts = pd.Timestamp(trading_dates[-1]).normalize()
    query_end_ts = end_ts + timedelta(days=1)

    result = fetch_price_history(
        symbol_value=symbol_value,
        start_date_value=str(start_ts.date()),
        end_date_value=str(query_end_ts.date()),
        provider_value=provider_value,
        interval_value=interval_value,
        adjustment_value="splits_only",
        extended_hours_value=False,
    )
    interval_df = to_dataframe(result)
    if interval_df is None or interval_df.empty:
        return pd.DataFrame()

    interval_df = normalise_ohlcv_columns(interval_df)
    required_cols = {"date", "open", "high", "low", "close", "volume"}
    if not required_cols.issubset(set(interval_df.columns)):
        return pd.DataFrame()

    for col in ["open", "high", "low", "close", "volume"]:
        interval_df[col] = pd.to_numeric(interval_df[col], errors="coerce")

    interval_df = interval_df.dropna(subset=["date", "open", "high", "low", "close", "volume"]).copy()
    if interval_df.empty:
        return pd.DataFrame()

    target_dates = {pd.Timestamp(d).normalize() for d in trading_dates}
    interval_dates = pd.to_datetime(interval_df["date"]).dt.normalize()
    interval_df = interval_df.loc[interval_dates.isin(target_dates)].copy().reset_index(drop=True)
    return interval_df


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
    replay_ts = pd.Timestamp(replay_date_value).normalize()

    plot_dates = pd.to_datetime(df_plot["date"]).dt.normalize()
    calc_dates = pd.to_datetime(df_calc["date"]).dt.normalize()

    plot_mask = plot_dates <= replay_ts
    df_plot_replay = df_plot.loc[plot_mask].copy().reset_index(drop=True)

    prior_calc_dates = calc_dates[calc_dates < replay_ts]
    if prior_calc_dates.empty:
        df_calc_replay = df_calc.iloc[0:0].copy().reset_index(drop=True)
    else:
        calc_cutoff = prior_calc_dates.max()
        calc_mask = calc_dates <= calc_cutoff
        df_calc_replay = df_calc.loc[calc_mask].copy().reset_index(drop=True)

    return df_plot_replay, df_calc_replay


def get_replay_date_state(df_calc: pd.DataFrame) -> pd.Timestamp:
    available_dates = (
        pd.to_datetime(df_calc["date"])
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    if available_dates.empty:
        raise ValueError("No dates available for replay.")

    available_date_list = [d.date() for d in available_dates.tolist()]
    min_date = available_date_list[0]
    max_date = available_date_list[-1]

    session_key = f"replay_date_{symbol}"

    if session_key not in st.session_state:
        st.session_state[session_key] = max_date

    current_date = st.session_state[session_key]
    if current_date < min_date:
        st.session_state[session_key] = min_date
    if current_date > max_date:
        st.session_state[session_key] = max_date

    return pd.Timestamp(st.session_state[session_key])


def render_replay_controls(df_calc: pd.DataFrame) -> pd.Timestamp:
    available_dates = (
        pd.to_datetime(df_calc["date"])
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    if available_dates.empty:
        raise ValueError("No dates available for replay.")

    available_date_list = [d.date() for d in available_dates.tolist()]
    min_date = available_date_list[0]
    max_date = available_date_list[-1]

    session_key = f"replay_date_{symbol}"

    if session_key not in st.session_state:
        st.session_state[session_key] = max_date

    current_date = st.session_state[session_key]
    if current_date < min_date:
        st.session_state[session_key] = min_date
    if current_date > max_date:
        st.session_state[session_key] = max_date

    date_to_idx = {d: i for i, d in enumerate(available_date_list)}

    def move_replay(delta: int):
        current = st.session_state[session_key]
        idx = date_to_idx[current]
        new_idx = min(max(idx + delta, 0), len(available_date_list) - 1)
        st.session_state[session_key] = available_date_list[new_idx]

    c1, c2, c3 = st.columns([1, 2, 1])

    with c1:
        st.button(
            "← Prev Day",
            key=f"replay_prev_day_{symbol}",
            on_click=move_replay,
            args=(-1,),
            disabled=(date_to_idx[st.session_state[session_key]] == 0),
            use_container_width=True,
        )

    with c2:
        st.date_input(
            "Replay date (treated as today)",
            min_value=min_date,
            max_value=max_date,
            key=session_key,
            label_visibility="collapsed",
        )

    with c3:
        st.button(
            "Next Day →",
            key=f"replay_next_day_{symbol}",
            on_click=move_replay,
            args=(1,),
            disabled=(date_to_idx[st.session_state[session_key]] == len(available_date_list) - 1),
            use_container_width=True,
        )

    current_idx = date_to_idx[st.session_state[session_key]]
    st.caption(
        f"Replay date: {st.session_state[session_key]} | "
        f"Step {current_idx + 1}/{len(available_date_list)}"
    )

    return pd.Timestamp(st.session_state[session_key])


def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    x = df.copy().set_index("date").sort_index()
    weekly = pd.DataFrame({
        "open": x["open"].resample("W-FRI").first(),
        "high": x["high"].resample("W-FRI").max(),
        "low": x["low"].resample("W-FRI").min(),
        "close": x["close"].resample("W-FRI").last(),
        "volume": x["volume"].resample("W-FRI").sum(),
    }).dropna(subset=["open", "high", "low", "close"]).reset_index()

    return weekly


def compute_atr(df: pd.DataFrame, period: int = 20) -> pd.Series:
    if df.empty or period < 1:
        return pd.Series(index=df.index, dtype=float)

    high_low = df["high"] - df["low"]
    high_prev_close = (df["high"] - df["close"].shift(1)).abs()
    low_prev_close = (df["low"] - df["close"].shift(1)).abs()
    true_range = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)

    return true_range.rolling(window=period, min_periods=period).mean()


def compute_vwap(df: pd.DataFrame, start_idx: int) -> pd.Series:
    sub = df.iloc[start_idx:].copy()
    typical_price = (sub["high"] + sub["low"] + sub["close"]) / 3.0
    vol = sub["volume"].fillna(0.0)

    cum_pv = (typical_price * vol).cumsum()
    cum_v = vol.cumsum().replace(0, np.nan)
    avwap = cum_pv / cum_v

    full = pd.Series(index=df.index, dtype=float)
    full.loc[sub.index] = avwap.values
    return full


def find_anchor_points(df: pd.DataFrame, recent_window_cap: int = 126) -> dict[str, int]:
    out: dict[str, int] = {}
    n = len(df)
    if n < 30:
        return out

    recent_window = min(recent_window_cap, n)
    recent_slice = df.iloc[-recent_window:]

    idx_major_high = recent_slice["high"].idxmax()
    idx_major_low = recent_slice["low"].idxmin()
    out["major_high"] = int(idx_major_high)
    out["major_low"] = int(idx_major_low)

    prev_close = df["close"].shift(1)
    gap_pct = (df["open"] - prev_close) / prev_close.replace(0, np.nan)
    gap_slice = gap_pct.iloc[-recent_window:]
    if gap_slice.notna().sum() > 0:
        out["gap_down"] = int(gap_slice.idxmin())
        out["gap_up"] = int(gap_slice.idxmax())

    body_return = (df["close"] - df["open"]) / df["open"].replace(0, np.nan)
    body_slice = body_return.iloc[-recent_window:]
    if body_slice.notna().sum() > 0:
        out["big_down"] = int(body_slice.idxmin())
        out["big_up"] = int(body_slice.idxmax())

    cleaned: dict[str, int] = {}
    seen = set()
    for k, v in out.items():
        if v is not None and v not in seen and 0 <= v < len(df) - 1:
            cleaned[k] = v
            seen.add(v)
    return cleaned


def build_vp_zones_from_profile(
    vp_df: pd.DataFrame,
    zone_expand: float,
    hv_quantile: float,
    timeframe: str,
    source_label: str,
) -> tuple[list[dict], pd.DataFrame]:
    if vp_df.empty:
        return [], vp_df
    if not (pd.to_numeric(vp_df["volume"], errors="coerce").fillna(0.0) > 0).any():
        return [], vp_df

    threshold = vp_df["volume"].quantile(hv_quantile)
    hv_nodes = vp_df[vp_df["volume"] >= threshold].copy()

    zones = []
    if hv_nodes.empty:
        return zones, vp_df

    hv_nodes = hv_nodes.sort_values("bin_center").reset_index(drop=True)

    current_left = float(hv_nodes.loc[0, "bin_left"])
    current_right = float(hv_nodes.loc[0, "bin_right"])
    current_vol = float(hv_nodes.loc[0, "volume"])

    for i in range(1, len(hv_nodes)):
        left = float(hv_nodes.loc[i, "bin_left"])
        right = float(hv_nodes.loc[i, "bin_right"])
        vol = float(hv_nodes.loc[i, "volume"])

        width_ref = max(current_right - current_left, 1e-9)
        close_enough = (left - current_right) <= (width_ref * 0.8)

        if close_enough:
            current_right = right
            current_vol += vol
        else:
            center = (current_left + current_right) / 2.0
            expand = center * zone_expand
            zones.append({
                "type": f"vp_zone_{timeframe}",
                "lower": current_left - expand,
                "upper": current_right + expand,
                "center": center,
                "vp_volume": current_vol,
                "timeframes": {timeframe},
                "source_types": {f"vp_{timeframe}"},
                "primary_timeframe": timeframe,
                "source_label": source_label,
            })
            current_left = left
            current_right = right
            current_vol = vol

    center = (current_left + current_right) / 2.0
    expand = center * zone_expand
    zones.append({
        "type": f"vp_zone_{timeframe}",
        "lower": current_left - expand,
        "upper": current_right + expand,
        "center": center,
        "vp_volume": current_vol,
        "timeframes": {timeframe},
        "source_types": {f"vp_{timeframe}"},
        "primary_timeframe": timeframe,
        "source_label": source_label,
    })

    return zones, vp_df


def build_composite_interval_volume_profile_zones(
    interval_df: pd.DataFrame,
    bins: int,
    zone_expand: float,
    hv_quantile: float,
    timeframe: str,
    source_label: str | None = None,
    source_mode: str = "composite",
) -> tuple[list[dict], pd.DataFrame]:
    if interval_df.empty:
        return [], pd.DataFrame()

    sub = interval_df.copy()
    low_min = float(sub["low"].min())
    high_max = float(sub["high"].max())
    if not np.isfinite(low_min) or not np.isfinite(high_max):
        return [], pd.DataFrame()

    if high_max <= low_min:
        high_max = low_min * (1.0 + 1e-6) if low_min != 0 else 1e-6

    bin_edges = np.linspace(low_min, high_max, bins + 1)
    bin_left = bin_edges[:-1]
    bin_right = bin_edges[1:]
    bin_centers = (bin_left + bin_right) / 2.0
    vol_bins = np.zeros(bins, dtype=float)

    for row in sub.itertuples(index=False):
        low = float(row.low)
        high = float(row.high)
        volume = float(row.volume)

        if not np.isfinite(low) or not np.isfinite(high) or not np.isfinite(volume) or volume <= 0:
            continue

        if high < low:
            low, high = high, low

        low = min(max(low, low_min), high_max)
        high = min(max(high, low_min), high_max)

        if high <= low:
            idx = int(np.searchsorted(bin_edges, low, side="right") - 1)
            idx = int(np.clip(idx, 0, bins - 1))
            vol_bins[idx] += volume
            continue

        overlap_left = np.maximum(bin_left, low)
        overlap_right = np.minimum(bin_right, high)
        overlaps = np.maximum(overlap_right - overlap_left, 0.0)
        total_overlap = float(overlaps.sum())

        if total_overlap <= 0:
            idx = int(np.searchsorted(bin_edges, low, side="right") - 1)
            idx = int(np.clip(idx, 0, bins - 1))
            vol_bins[idx] += volume
            continue

        vol_bins += volume * (overlaps / total_overlap)

    vp_df = pd.DataFrame({
        "bin_left": bin_left,
        "bin_right": bin_right,
        "bin_center": bin_centers,
        "volume": vol_bins,
        "timeframe": timeframe,
        "source_bars": len(sub),
        "source_mode": source_mode,
    })

    return build_vp_zones_from_profile(
        vp_df=vp_df,
        zone_expand=zone_expand,
        hv_quantile=hv_quantile,
        timeframe=timeframe,
        source_label=source_label or f"VP ({timeframe}, composite)",
    )


def build_avwap_features(df: pd.DataFrame, timeframe: str) -> tuple[pd.DataFrame, dict]:
    recent_window_cap = 126 if timeframe == "D" else 52
    anchors = find_anchor_points(df, recent_window_cap=recent_window_cap)
    avwap_cols = {}
    anchor_meta = {}

    for anchor_name, idx in anchors.items():
        col_name = f"avwap_{timeframe}_{anchor_name}"
        avwap_cols[col_name] = compute_vwap(df, idx)
        anchor_meta[col_name] = {
            "anchor_name": anchor_name,
            "start_idx": idx,
            "start_date": df.loc[idx, "date"],
            "start_price": float(df.loc[idx, "close"]),
            "timeframe": timeframe,
        }

    out = df.copy()
    for k, v in avwap_cols.items():
        out[k] = v

    return out, anchor_meta


def create_candidate_zones_from_avwap(df: pd.DataFrame, anchor_meta: dict, zone_expand_pct: float) -> list[dict]:
    zones = []
    if df.empty:
        return zones

    current_price = float(df["close"].iloc[-1])

    for col, meta in anchor_meta.items():
        latest_val = df[col].dropna()
        if latest_val.empty:
            continue

        avwap_now = float(latest_val.iloc[-1])
        center = avwap_now
        expand = center * zone_expand_pct
        timeframe = meta["timeframe"]

        if center >= current_price:
            zone_side = "resistance"
            avwap_strength = max((center - current_price) / max(current_price, 1e-9), 0.0) + 0.5
        else:
            zone_side = "support"
            avwap_strength = max((current_price - center) / max(current_price, 1e-9), 0.0) + 0.5

        zones.append({
            "type": f"avwap_{zone_side}_{timeframe}",
            "side": zone_side,
            "lower": center - expand,
            "upper": center + expand,
            "center": center,
            "vp_volume": 0.0,
            "anchor_count": 1,
            "avwap_strength": avwap_strength,
            "anchor_name": meta["anchor_name"],
            "anchor_start_date": meta["start_date"],
            "timeframes": {timeframe},
            "source_types": {f"avwap_{timeframe}"},
            "primary_timeframe": timeframe,
            "source_label": f"AVWAP ({timeframe})",
        })

    return zones


def create_candidate_zones_from_vp(df: pd.DataFrame, vp_zones: list[dict]) -> list[dict]:
    if df.empty:
        return []

    current_price = float(df["close"].iloc[-1])
    out = []

    for z in vp_zones:
        z2 = z.copy()
        z2["anchor_count"] = 0
        z2["avwap_strength"] = 0.0
        z2["side"] = "resistance" if z["center"] >= current_price else "support"
        out.append(z2)

    return out


def format_zone_source_types(source_types: set[str] | list[str] | tuple[str, ...] | None) -> str:
    if not source_types:
        return ""

    formatted = []
    for source_type in sorted(set(source_types)):
        parts = str(source_type).split("_", 1)
        if len(parts) == 2:
            family, timeframe = parts
            formatted.append(f"{family.upper()}_{timeframe.upper()}")
        else:
            formatted.append(str(source_type).upper())

    return ",".join(formatted)


def merge_close_zones(zones: list[dict], merge_pct: float = 0.006) -> list[dict]:
    if not zones:
        return []

    zones_sorted = sorted(zones, key=lambda z: (z["side"], z["center"]))
    merged = [zones_sorted[0].copy()]

    for z in zones_sorted[1:]:
        last = merged[-1]

        if z["side"] != last["side"]:
            merged.append(z.copy())
            continue

        overlap = not (z["lower"] > last["upper"] or z["upper"] < last["lower"])
        close_center = abs(z["center"] - last["center"]) / max(last["center"], 1e-9) <= merge_pct

        if overlap or close_center:
            new_lower = min(last["lower"], z["lower"])
            new_upper = max(last["upper"], z["upper"])
            timeframes = set(last.get("timeframes", set())) | set(z.get("timeframes", set()))
            source_types = set(last.get("source_types", set())) | set(z.get("source_types", set()))
            merged[-1] = {
                "type": f"merged_{last['side']}",
                "side": last["side"],
                "lower": float(new_lower),
                "upper": float(new_upper),
                "center": float((new_lower + new_upper) / 2.0),
                "vp_volume": float(last.get("vp_volume", 0.0) + z.get("vp_volume", 0.0)),
                "anchor_count": int(last.get("anchor_count", 0) + z.get("anchor_count", 0)),
                "avwap_strength": float(last.get("avwap_strength", 0.0) + z.get("avwap_strength", 0.0)),
                "timeframes": timeframes,
                "source_types": source_types,
                "primary_timeframe": "W" if "W" in timeframes else "D",
                "source_label": format_zone_source_types(source_types),
            }
        else:
            merged.append(z.copy())

    return merged


def compute_inventory_zone_score(zone: dict, current_price: float, vp_df_daily: pd.DataFrame, vp_df_weekly: pd.DataFrame) -> float:
    zone_vol = float(zone.get("vp_volume", 0.0))
    max_daily = max(float(vp_df_daily["volume"].max()), 1e-9) if not vp_df_daily.empty else 1.0
    max_weekly = max(float(vp_df_weekly["volume"].max()), 1e-9) if not vp_df_weekly.empty else 1.0
    max_vol = max(max_daily, max_weekly, 1.0)
    vol_score = zone_vol / max_vol

    distance_pct = abs(zone["center"] - current_price) / max(current_price, 1e-9)
    proximity_score = 1.0 / max(distance_pct, 0.01)

    weekly_bonus = 0.25 if "W" in zone.get("timeframes", set()) else 0.0
    multi_tf_bonus = 0.35 if len(zone.get("timeframes", set())) >= 2 else 0.0

    return 0.5 * vol_score + 0.3 * proximity_score + weekly_bonus + multi_tf_bonus


def validate_zone_reaction(
    df: pd.DataFrame,
    zone: dict,
    lookahead: int,
    return_threshold: float,
    min_gap: int,
) -> dict:
    if df.empty or lookahead < 1:
        return {
            "touch_count": 0,
            "first_touch_score": 0.0,
            "strong_reaction_rate": 0.0,
            "reclaim_rate": 0.0,
            "reaction_score": 0.0,
            "last_reaction_date": pd.NaT,
        }

    lower = float(zone["lower"])
    upper = float(zone["upper"])
    center = float(zone["center"])
    side = zone["side"]

    touched_idx = []
    last_touch = -10_000

    for i in range(len(df) - lookahead):
        row = df.iloc[i]
        touched = float(row["low"]) <= upper and float(row["high"]) >= lower
        if touched and (i - last_touch) >= min_gap:
            touched_idx.append(i)
            last_touch = i

    if not touched_idx:
        return {
            "touch_count": 0,
            "first_touch_score": 0.0,
            "strong_reaction_rate": 0.0,
            "reclaim_rate": 0.0,
            "reaction_score": 0.0,
            "last_reaction_date": pd.NaT,
        }

    strong_reactions = 0
    reclaims = 0
    reactions = []

    for i in touched_idx:
        row = df.iloc[i]
        base_close = float(row["close"])
        future = df.iloc[i + 1:i + 1 + lookahead].copy()
        if future.empty:
            continue

        if side == "support":
            best_forward = float(future["high"].max())
            forward_ret = (best_forward - base_close) / max(base_close, 1e-9)

            pierced = float(row["low"]) < lower
            reclaimed_same_bar = pierced and float(row["close"]) >= lower
            reclaimed_future = bool((future["close"] >= lower).any())
            reclaimed = reclaimed_same_bar or reclaimed_future

            strong = (
                forward_ret >= return_threshold
                or (float(row["close"]) > center and (float(row["close"]) - float(row["open"])) > 0)
                or reclaimed
            )
        else:
            best_forward = float(future["low"].min())
            forward_ret = (base_close - best_forward) / max(base_close, 1e-9)

            pierced = float(row["high"]) > upper
            reclaimed_same_bar = pierced and float(row["close"]) <= upper
            reclaimed_future = bool((future["close"] <= upper).any())
            reclaimed = reclaimed_same_bar or reclaimed_future

            strong = (
                forward_ret >= return_threshold
                or (float(row["close"]) < center and (float(row["open"]) - float(row["close"])) > 0)
                or reclaimed
            )

        if strong:
            strong_reactions += 1
        if reclaimed:
            reclaims += 1

        touch_quality = 0.0
        if strong:
            touch_quality += 1.0
        if reclaimed:
            touch_quality += 0.75
        reactions.append(touch_quality)

    effective_touches = max(len(reactions), 1)
    strong_reaction_rate = strong_reactions / effective_touches
    reclaim_rate = reclaims / effective_touches

    first_touch_score = reactions[0] if reactions else 0.0
    touch_count = len(touched_idx)

    repeated_test_decay = max(touch_count - 2, 0) * 0.12

    reaction_score = (
        1.2 * first_touch_score
        + 1.0 * strong_reaction_rate
        + 0.8 * reclaim_rate
        - repeated_test_decay
    )

    last_reaction_date = df.iloc[touched_idx[-1]]["date"]

    return {
        "touch_count": touch_count,
        "first_touch_score": float(first_touch_score),
        "strong_reaction_rate": float(strong_reaction_rate),
        "reclaim_rate": float(reclaim_rate),
        "reaction_score": float(reaction_score),
        "last_reaction_date": last_reaction_date,
    }


def rank_zones_for_side(
    zones: list[dict],
    vp_df_daily: pd.DataFrame,
    vp_df_weekly: pd.DataFrame,
    current_price: float,
    side: str,
    max_zones: int,
    df_reaction: pd.DataFrame,
    lookahead: int,
    reaction_threshold: float,
    min_gap: int,
) -> list[dict]:
    ranked = []
    max_vp_daily = max(float(vp_df_daily["volume"].max()), 1e-9) if not vp_df_daily.empty else 1.0
    max_vp_weekly = max(float(vp_df_weekly["volume"].max()), 1e-9) if not vp_df_weekly.empty else 1.0
    max_vp = max(max_vp_daily, max_vp_weekly, 1.0)

    for z in zones:
        if z.get("side") != side:
            continue

        if side == "resistance" and z["upper"] < current_price:
            continue
        if side == "support" and z["lower"] > current_price:
            continue

        reaction = validate_zone_reaction(
            df=df_reaction,
            zone=z,
            lookahead=lookahead,
            return_threshold=reaction_threshold,
            min_gap=min_gap,
        )

        distance_pct = abs(z["center"] - current_price) / max(current_price, 1e-9)
        width_pct = (z["upper"] - z["lower"]) / max(z["center"], 1e-9)

        vp_strength = float(z.get("vp_volume", 0.0)) / max_vp
        inventory_score = compute_inventory_zone_score(z, current_price, vp_df_daily, vp_df_weekly)
        avwap_strength = float(z.get("avwap_strength", 0.0))
        anchor_count = int(z.get("anchor_count", 0))
        proximity_score = 1.0 / max(distance_pct, 0.01)
        width_penalty = width_pct * 20.0

        timeframes = z.get("timeframes", set())
        weekly_bonus = 1.0 if "W" in timeframes else 0.0
        multi_tf_bonus = 1.2 if len(timeframes) >= 2 else 0.0
        confluence_count = len(set(z.get("source_types", set())))

        structural_score = (
            2.4 * vp_strength
            + 1.9 * inventory_score
            + 1.3 * avwap_strength
            + 0.7 * anchor_count
            + 0.9 * weekly_bonus
            + 1.1 * multi_tf_bonus
            + 0.3 * confluence_count
            + 1.0 * proximity_score
            - width_penalty
        )

        institutional_score = structural_score + 2.0 * reaction["reaction_score"]

        z2 = z.copy()
        z2["distance_pct"] = distance_pct
        z2["width_pct"] = width_pct
        z2["vp_strength"] = vp_strength
        z2["inventory_score"] = inventory_score
        z2["weekly_bonus"] = weekly_bonus
        z2["multi_tf_bonus"] = multi_tf_bonus
        z2["confluence_count"] = confluence_count
        z2["timeframe_sources"] = ",".join(sorted(timeframes))
        z2["source_types_label"] = format_zone_source_types(z.get("source_types", set()))
        z2.update(reaction)
        z2["structural_score"] = structural_score
        z2["institutional_score"] = institutional_score
        ranked.append(z2)

    ranked = sorted(
        ranked,
        key=lambda x: (
            x["institutional_score"],
            x["reaction_score"],
            x["multi_tf_bonus"],
            x["vp_strength"],
            -x["distance_pct"],
        ),
        reverse=True,
    )

    ranked = ranked[:max_zones]
    ranked = sorted(ranked, key=lambda x: x["center"])
    return ranked


def assign_zone_display_labels(zones: list[dict], prefix: str) -> list[dict]:
    if not zones:
        return []

    ranked_by_distance = sorted(
        zones,
        key=lambda z: (
            z.get("distance_pct", float("inf")),
            abs(float(z.get("center", 0.0))),
        ),
    )
    label_map = {id(zone): f"{prefix}{i}" for i, zone in enumerate(ranked_by_distance, start=1)}

    labeled = []
    for zone in zones:
        zone_copy = zone.copy()
        zone_copy["display_label"] = label_map[id(zone)]
        labeled.append(zone_copy)

    return labeled


def zones_to_dataframe(zones: list[dict]) -> pd.DataFrame:
    if not zones:
        return pd.DataFrame(columns=[
            "side", "type", "lower", "upper", "center",
            "timeframe_sources", "source_types_label", "confluence_count",
            "vp_volume", "anchor_count", "avwap_strength",
            "touch_count", "first_touch_score", "strong_reaction_rate", "reclaim_rate", "reaction_score",
            "distance_pct", "width_pct", "vp_strength",
            "inventory_score", "weekly_bonus", "multi_tf_bonus",
            "structural_score", "institutional_score",
        ])

    df = pd.DataFrame(zones).copy()

    for col in ["timeframes", "source_types"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: ",".join(sorted(x)) if isinstance(x, set) else x)

    return df


def show_dataframe_result(title, fetcher, empty_message="No data returned."):
    st.subheader(title)
    try:
        result = fetcher()
        df = to_dataframe(result)
        if df is not None and not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info(empty_message)
    except Exception as e:
        st.error(f"Error: {e}")


def show_news(title, fetcher, empty_message="No news data returned."):
    st.subheader(title)
    try:
        result = fetcher()
        df = to_dataframe(result)

        if df is None or df.empty:
            st.info(empty_message)
            return

        preferred_cols = [c for c in ["date", "title", "source", "publisher", "url"] if c in df.columns]
        if preferred_cols:
            st.dataframe(df[preferred_cols], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

        if "title" in df.columns:
            st.markdown("### Latest Headlines")
            for _, row in df.head(news_limit).iterrows():
                title_value = row.get("title", "Untitled")
                article_date = row.get("date", "")
                source = row.get("source", row.get("publisher", ""))
                url = row.get("url", "")

                st.markdown(f"**{title_value}**")
                meta = " | ".join([str(x) for x in [article_date, source] if x])
                if meta:
                    st.caption(meta)
                if url:
                    st.markdown(f"[Open article]({url})")
                st.divider()

    except Exception as e:
        st.error(f"Error: {e}")


def show_definitions():
    st.markdown("### Definitions / 定义")
    st.markdown(
        f"""
**This version adds multi-timeframe confluence, reaction validation, and replay mode.**  
**这版新增了多周期共振、历史反应验证与复盘模式。**

**1) Daily and Weekly Zones / 日线与周线区域**
- Daily VP input: recent **{vp_lookback_days}** trading days of **1h OHLCV**
- Higher-timeframe VP input: recent **{weekly_vp_lookback}** weekly windows of **1d OHLCV**
- Composite VP method: each source bar distributes volume across all covered price bins
- No fallback to lower-precision VP when the required source interval is unavailable
- Each zone explicitly records timeframe source(s)

**2) Multi-timeframe confluence / 多周期共振**
- Daily and weekly zones are merged when close/overlapping
- Zones with both **D** and **W** sources get confluence bonus

**3) Reaction validation / 反应验证**
For each zone, the system tracks:
- touch count
- first-touch quality
- strong reaction rate
- reclaim rate
- repeated-test decay

**4) Institutional score / 机构化评分**
Combines:
- volume structure
- inventory logic
- AVWAP contribution
- timeframe confluence
- historical reaction quality
- width penalty

**5) Replay mode / 复盘模式**
- choose any historical trading date
- treat that selected date as “today” for all calculations
- buttons and date input are synchronised

**6) Latest bar handling / 最后一根K处理**
- chart frame: {"show live last bar" if show_live_last_bar_on_chart else "hide live last bar"}
- calculation frame: {"exclude latest bar" if exclude_last_unclosed_bar else "include latest bar"}
"""
    )


def to_lwc_time(x) -> str:
    return pd.to_datetime(x).strftime("%Y-%m-%d")


def build_lwc_series(
    df_plot: pd.DataFrame,
    df_calc_daily_with_features: pd.DataFrame,
    support_zones: list[dict],
    resistance_zones: list[dict],
    daily_anchor_meta: dict,
    show_avwap_lines: bool,
    atr_overlay: dict | None = None,
):
    visible_start = pd.to_datetime(df_plot["date"].iloc[0]) if not df_plot.empty else None
    visible_end = pd.to_datetime(df_plot["date"].iloc[-1]) if not df_plot.empty else None

    candle_data = []
    for _, row in df_plot.iterrows():
        candle_data.append({
            "time": to_lwc_time(row["date"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "change_pct": float(row["change_pct"]) if pd.notna(row.get("change_pct")) else None,
        })

    volume_data = []
    for _, row in df_plot.iterrows():
        up = float(row["close"]) >= float(row["open"])
        volume_data.append({
            "time": to_lwc_time(row["date"]),
            "value": float(row["volume"]) if pd.notna(row["volume"]) else 0.0,
            "color": "rgba(255, 0, 0, 0.8)" if up else "rgba(0, 128, 0, 0.8)",
        })

    series = [
        {
            "type": "Candlestick",
            "data": candle_data,
            "options": {
                "upColor": "#ff0000",
                "downColor": "#008000",
                "borderUpColor": "#ff0000",
                "borderDownColor": "#008000",
                "wickUpColor": "#ff0000",
                "wickDownColor": "#008000",
                "priceLineVisible": True,
            },
        },
        {
            "type": "Histogram",
            "data": volume_data,
            "options": {
                "priceFormat": {"type": "volume"},
                "priceScaleId": "volume",
            },
            "priceScale": {
                "scaleMargins": {
                    "top": 0.82,
                    "bottom": 0.0,
                }
            },
        },
    ]

    if show_avwap_lines:
        for col, meta in daily_anchor_meta.items():
            valid = df_calc_daily_with_features[["date", col]].dropna().copy()
            if valid.empty:
                continue
            if visible_start is not None and visible_end is not None:
                valid = valid[
                    (pd.to_datetime(valid["date"]) >= visible_start)
                    & (pd.to_datetime(valid["date"]) <= visible_end)
                ].copy()
            if valid.empty:
                continue

            line_data = []
            for _, row in valid.iterrows():
                line_data.append({
                    "time": to_lwc_time(row["date"]),
                    "value": float(row[col]),
                })

            series.append({
                "type": "Line",
                "data": line_data,
                "options": {
                    "lineWidth": 1,
                    "priceLineVisible": False,
                    "lastValueVisible": False,
                    "color": "#2962FF",
                    "lineStyle": 2,
                },
            })

    if not df_plot.empty:
        start_time = to_lwc_time(df_plot["date"].iloc[0])
        end_time = to_lwc_time(df_plot["date"].iloc[-1])

        for zone in resistance_zones:
            series.append({
                "type": "Line",
                "data": [
                    {"time": start_time, "value": float(zone["center"])},
                    {"time": end_time, "value": float(zone["center"])},
                ],
                "overlay_label": {
                    "text": zone.get("display_label", ""),
                    "color": "#cc3333",
                },
                "options": {
                    "lineWidth": 3,
                    "priceLineVisible": False,
                    "lastValueVisible": False,
                    "color": "#cc3333",
                    "lineStyle": 2,
                },
            })

        for zone in support_zones:
            series.append({
                "type": "Line",
                "data": [
                    {"time": start_time, "value": float(zone["center"])},
                    {"time": end_time, "value": float(zone["center"])},
                ],
                "overlay_label": {
                    "text": zone.get("display_label", ""),
                    "color": "#2e8b57",
                },
                "options": {
                    "lineWidth": 3,
                    "priceLineVisible": False,
                    "lastValueVisible": False,
                    "color": "#2e8b57",
                    "lineStyle": 2,
                },
            })

        if atr_overlay is not None:
            atr_upper = atr_overlay.get("upper")
            atr_lower = atr_overlay.get("lower")
            atr_label = atr_overlay.get("label", "ATR20")
            atr_color = atr_overlay.get("color", "#7c3aed")

            if atr_upper is not None:
                series.append({
                    "type": "Line",
                    "data": [
                        {"time": start_time, "value": float(atr_upper)},
                        {"time": end_time, "value": float(atr_upper)},
                    ],
                    "overlay_label": {
                        "text": f"{atr_label}+",
                        "color": atr_color,
                    },
                    "options": {
                        "lineWidth": 2,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                        "color": atr_color,
                        "lineStyle": 1,
                    },
                })

            if atr_lower is not None:
                series.append({
                    "type": "Line",
                    "data": [
                        {"time": start_time, "value": float(atr_lower)},
                        {"time": end_time, "value": float(atr_lower)},
                    ],
                    "overlay_label": {
                        "text": f"{atr_label}-",
                        "color": atr_color,
                    },
                    "options": {
                        "lineWidth": 2,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                        "color": atr_color,
                        "lineStyle": 1,
                    },
                })

    return series


def render_lwc_chart_with_focus_header(
    chart_options: dict,
    series: list[dict],
    chart_key: str,
):
    chart_height = int(chart_options.get("height", 700))
    container_id = f"lwc-chart-{abs(hash(chart_key))}"
    payload = json.dumps(
        {
            "chart": chart_options,
            "series": series,
        },
        ensure_ascii=False,
    )

    html = f"""
<div id="{container_id}" class="lwc-wrap">
  <div id="{container_id}-header" class="lwc-header"></div>
  <div id="{container_id}-zone-labels" class="lwc-zone-labels"></div>
  <div id="{container_id}-chart" class="lwc-chart"></div>
</div>

<style>
  html, body {{
    margin: 0;
    padding: 0;
    background: #ffffff;
  }}

  .lwc-wrap {{
    position: relative;
    width: 100%;
    height: {chart_height}px;
    background: #ffffff;
    overflow: hidden;
    font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  }}

  .lwc-chart {{
    width: 100%;
    height: 100%;
  }}

  .lwc-zone-labels {{
    position: absolute;
    inset: 0;
    z-index: 9;
    pointer-events: none;
  }}

  .lwc-zone-label {{
    position: absolute;
    left: 8px;
    transform: translateY(-50%);
    padding: 2px 6px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.95);
    border: 1px solid currentColor;
    font-size: 11px;
    font-weight: 700;
    line-height: 1.2;
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.08);
    white-space: nowrap;
  }}

  .lwc-header {{
    position: absolute;
    top: 12px;
    left: 12px;
    z-index: 10;
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 8px 12px;
    background: rgba(255, 255, 255, 0.92);
    border: 1px solid rgba(15, 23, 42, 0.10);
    border-radius: 10px;
    box-shadow: 0 8px 22px rgba(15, 23, 42, 0.08);
    backdrop-filter: blur(6px);
    color: #1f2937;
    pointer-events: none;
  }}

  .lwc-header-date {{
    font-size: 12px;
    font-weight: 600;
    color: #475569;
    white-space: nowrap;
  }}

  .lwc-header-item {{
    display: flex;
    align-items: baseline;
    gap: 6px;
    white-space: nowrap;
  }}

  .lwc-header-label {{
    font-size: 12px;
    color: #64748b;
  }}

  .lwc-header-value {{
    font-size: 16px;
    font-weight: 700;
    color: #111827;
  }}
</style>

<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
  const payload = {payload};
  const root = document.getElementById("{container_id}");
  const header = document.getElementById("{container_id}-header");
  const zoneLabels = document.getElementById("{container_id}-zone-labels");
  const chartNode = document.getElementById("{container_id}-chart");

  const normalizeTime = (value) => {{
    if (typeof value === "string") {{
      return value;
    }}
    if (typeof value === "number") {{
      return new Date(value * 1000).toISOString().slice(0, 10);
    }}
    if (value && typeof value === "object" && "year" in value) {{
      const y = String(value.year).padStart(4, "0");
      const m = String(value.month).padStart(2, "0");
      const d = String(value.day).padStart(2, "0");
      return `${{y}}-${{m}}-${{d}}`;
    }}
    return "";
  }};

  const formatNumber = (value) => {{
    const num = Number(value);
    if (!Number.isFinite(num)) {{
      return "--";
    }}
    return num.toLocaleString(undefined, {{
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }});
  }};

  const formatPct = (value) => {{
    const num = Number(value);
    if (!Number.isFinite(num)) {{
      return "--";
    }}
    const sign = num > 0 ? "+" : "";
    return `${{sign}}${{(num * 100).toFixed(2)}}%`;
  }};

  const getPctColor = (value) => {{
    const num = Number(value);
    if (!Number.isFinite(num) || num === 0) {{
      return "#475569";
    }}
    return num > 0 ? "#dc2626" : "#15803d";
  }};

  const chart = LightweightCharts.createChart(chartNode, {{
    ...payload.chart,
    width: root.clientWidth || 900,
    height: payload.chart?.height || {chart_height},
  }});

  const seriesBuilders = {{
    Candlestick: (options) => chart.addCandlestickSeries(options || {{}}),
    Histogram: (options) => chart.addHistogramSeries(options || {{}}),
    Line: (options) => chart.addLineSeries(options || {{}}),
  }};

  let candleData = [];
  const candleLookup = new Map();
  const zoneLabelSeries = [];

  (payload.series || []).forEach((item) => {{
    const builder = seriesBuilders[item.type];
    if (!builder) {{
      return;
    }}

    const createdSeries = builder(item.options || {{}});
    createdSeries.setData(item.data || []);

    if (item.priceScale) {{
      createdSeries.priceScale().applyOptions(item.priceScale);
    }}

    if (item.markers && typeof createdSeries.setMarkers === "function") {{
      createdSeries.setMarkers(item.markers);
    }}

    if (item.overlay_label && item.data && item.data.length) {{
      zoneLabelSeries.push({{
        series: createdSeries,
        value: Number(item.data[0].value),
        text: item.overlay_label.text || "",
        color: item.overlay_label.color || "#111827",
      }});
    }}

    if (item.type === "Candlestick" && candleData.length === 0) {{
      candleData = item.data || [];
      candleData.forEach((bar) => {{
        candleLookup.set(normalizeTime(bar.time), bar);
      }});
    }}
  }});

  const renderHeader = (bar) => {{
    if (!bar) {{
      header.innerHTML = '<span class="lwc-header-date">No data</span>';
      return;
    }}

    const dateText = normalizeTime(bar.time) || "--";
    const closeText = formatNumber(bar.close);
    const pctText = formatPct(bar.change_pct);
    const pctColor = getPctColor(bar.change_pct);

    header.innerHTML = `
      <div class="lwc-header-date">${{dateText}}</div>
      <div class="lwc-header-item">
        <span class="lwc-header-label">Close</span>
        <span class="lwc-header-value">${{closeText}}</span>
      </div>
      <div class="lwc-header-item">
        <span class="lwc-header-label">Change</span>
        <span class="lwc-header-value" style="color: ${{pctColor}};">${{pctText}}</span>
      </div>
    `;
  }};

  const defaultBar = candleData.length ? candleData[candleData.length - 1] : null;
  renderHeader(defaultBar);

  const renderZoneLabels = () => {{
    if (!zoneLabels) {{
      return;
    }}

    zoneLabels.innerHTML = "";

    zoneLabelSeries.forEach((item) => {{
      const y = item.series.priceToCoordinate(item.value);
      if (!Number.isFinite(y)) {{
        return;
      }}

      const el = document.createElement("div");
      el.className = "lwc-zone-label";
      el.textContent = item.text;
      el.style.top = `${{y}}px`;
      el.style.color = item.color;
      zoneLabels.appendChild(el);
    }});
  }};

  chart.subscribeCrosshairMove((param) => {{
    const timeKey = normalizeTime(param?.time);
    if (!timeKey) {{
      renderHeader(defaultBar);
      return;
    }}

    renderHeader(candleLookup.get(timeKey) || defaultBar);
  }});

  chart.timeScale().fitContent();
  renderZoneLabels();

  const applyWidth = () => {{
    const width = root.clientWidth || 900;
    chart.applyOptions({{ width }});
    renderZoneLabels();
  }};

  const resizeObserver = new ResizeObserver(() => {{
    applyWidth();
  }});

  resizeObserver.observe(root);
  window.addEventListener("resize", applyWidth);
  chart.timeScale().subscribeVisibleTimeRangeChange(renderZoneLabels);
</script>
"""

    components.html(html, height=chart_height + 6)


def render_zone_left_panel(
    support_zones: list[dict],
    resistance_zones: list[dict],
    current_price: float,
):
    st.markdown("#### Zones / 区域标签")
    st.metric("Calc Close", f"{current_price:.2f}")

    if resistance_zones:
        st.markdown("**Resistance / 压力位**")
        for zone in resistance_zones:
            st.markdown(
                f"""
<div style="margin-bottom:10px; padding:8px 10px; border-left:6px solid #cc3333; background:#fff5f5; border-radius:6px;">
    <div style="font-weight:700;">{zone.get("display_label", "")} [{zone.get("source_types_label", "")}]</div>
    <div>{zone["lower"]:.2f} - {zone["upper"]:.2f}</div>
    <div style="font-size:12px; color:#666;">Score: {zone.get("institutional_score", 0):.2f}</div>
</div>
""",
                unsafe_allow_html=True,
            )
    else:
        st.info("No resistance zones.")

    if support_zones:
        st.markdown("**Support / 支撑位**")
        for zone in support_zones:
            st.markdown(
                f"""
<div style="margin-bottom:10px; padding:8px 10px; border-left:6px solid #2e8b57; background:#f4fff7; border-radius:6px;">
    <div style="font-weight:700;">{zone.get("display_label", "")} [{zone.get("source_types_label", "")}]</div>
    <div>{zone["lower"]:.2f} - {zone["upper"]:.2f}</div>
    <div style="font-size:12px; color:#666;">Score: {zone.get("institutional_score", 0):.2f}</div>
</div>
""",
                unsafe_allow_html=True,
            )
    else:
        st.info("No support zones.")


def show_price_chart():
    st.subheader(f"Historical Price — {symbol}")

    try:
        start_date_value = get_start_date_from_range(history_range)
        result = fetch_price_history(symbol, start_date_value, price_provider)
        raw_df = to_dataframe(result)

        if raw_df is None or raw_df.empty:
            st.info("No historical price data returned.")
            return

        df = normalise_ohlcv_columns(raw_df)
        df = df.copy()
        df.index.name = None
        df = df.reset_index(drop=True)

        required_cols = {"date", "open", "high", "low", "close"}
        missing = required_cols - set(df.columns)
        if missing:
            st.error(
                f"Price data does not contain required OHLC columns: {sorted(missing)}. "
                f"Available columns: {list(df.columns)}"
            )
            st.markdown("### Raw Price Data")
            st.dataframe(raw_df, use_container_width=True)
            st.markdown("### Normalised Price Data")
            st.dataframe(df, use_container_width=True)
            return

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "volume" not in df.columns:
            df["volume"] = np.nan

        df = df.dropna(subset=["date", "open", "high", "low", "close"]).copy()

        if df.empty:
            st.info("No valid OHLC rows available after cleaning.")
            return

        df_plot, df_calc_daily_base = prepare_plot_and_calc_frames(
            df=df,
            exclude_last_bar_for_calc=exclude_last_unclosed_bar,
            show_last_bar_on_chart=show_live_last_bar_on_chart,
        )

        if df_calc_daily_base.empty:
            st.warning("Calculation frame is empty after excluding the latest bar.")
            return

        replay_date = get_replay_date_state(df_calc_daily_base)
        df_plot_replay, df_calc_daily = prepare_replay_frame(df_plot, df_calc_daily_base, replay_date)
        df_plot_replay = df_plot_replay.copy()
        df_plot_replay["prev_close"] = df_plot_replay["close"].shift(1)
        df_plot_replay["change_pct"] = (
            (df_plot_replay["close"] - df_plot_replay["prev_close"])
            / df_plot_replay["prev_close"].replace(0, np.nan)
        )
        initial_visible_bars = 200
        df_plot_display = df_plot_replay.tail(initial_visible_bars).copy()
        if df_calc_daily.empty:
            st.warning("No calculation data available on or before the selected replay date.")
            return
        if df_plot_replay.empty:
            st.warning("No chart data available on or before the selected replay date.")
            return

        current_price = float(df_calc_daily["close"].iloc[-1])
        atr20_series = compute_atr(df_calc_daily, period=20)
        atr20_value = float(atr20_series.iloc[-1]) if not atr20_series.empty and pd.notna(atr20_series.iloc[-1]) else np.nan
        atr_overlay = None
        if show_atr_bands and np.isfinite(atr20_value):
            atr_distance = atr20_value * atr_multiplier
            atr_overlay = {
                "upper": current_price + atr_distance,
                "lower": current_price - atr_distance,
                "label": f"ATR20x{atr_multiplier:.1f}",
                "color": "#6d28d9",
            }

        df_calc_daily_with_features, daily_anchor_meta = build_avwap_features(df_calc_daily, timeframe="D")
        vp_daily_mode = "1h composite"
        vp_daily_note = f"Daily VP uses the most recent {vp_lookback_days} trading days of 1h OHLCV."
        recent_vp_dates = get_recent_trading_dates(df_calc_daily, vp_lookback_days)
        daily_vp_source_df = pd.DataFrame()
        daily_vp_zones_raw = []
        vp_df_daily = pd.DataFrame()

        try:
            daily_vp_source_df = fetch_interval_history_for_dates(
                symbol_value=symbol,
                trading_dates=recent_vp_dates,
                provider_value=price_provider,
                interval_value="1h",
            )
        except Exception as interval_error:
            vp_daily_mode = "1h unavailable"
            vp_daily_note = (
                "1h history could not be loaded for the selected replay window, "
                f"so daily VP was omitted. Details: {interval_error}"
            )

        if not daily_vp_source_df.empty:
            try:
                daily_vp_zones_raw, vp_df_daily = build_composite_interval_volume_profile_zones(
                    interval_df=daily_vp_source_df,
                    bins=vp_bins,
                    zone_expand=zone_expand_pct,
                    hv_quantile=hv_node_quantile,
                    timeframe="D",
                    source_label="VP (D, 1h composite)",
                    source_mode="1h_composite",
                )
            except Exception as interval_profile_error:
                daily_vp_zones_raw, vp_df_daily = [], pd.DataFrame()
                vp_daily_mode = "1h unavailable"
                vp_daily_note = (
                    "1h composite daily VP construction failed for the selected replay window, "
                    f"so daily VP was omitted. Details: {interval_profile_error}"
                )
            else:
                if not vp_df_daily.empty:
                    vp_daily_note = (
                        f"Daily VP uses {len(recent_vp_dates)} trading days / {len(daily_vp_source_df)} bars of 1h OHLCV."
                    )
                else:
                    vp_daily_mode = "1h unavailable"
                    vp_daily_note = (
                        "1h history was returned, but no valid composite daily VP could be built, "
                        "so daily VP was omitted."
                    )
        else:
            if vp_daily_mode != "1h unavailable":
                vp_daily_mode = "1h unavailable"
                vp_daily_note = (
                    "No 1h history was returned for the selected replay window, "
                    "so daily VP was omitted."
                )
        daily_vp_zones = create_candidate_zones_from_vp(df_calc_daily_with_features, daily_vp_zones_raw)
        daily_avwap_zones = create_candidate_zones_from_avwap(
            df=df_calc_daily_with_features,
            anchor_meta=daily_anchor_meta,
            zone_expand_pct=zone_expand_pct,
        )

        df_calc_weekly = resample_to_weekly(df_calc_daily)
        df_calc_weekly_with_features, weekly_anchor_meta = build_avwap_features(df_calc_weekly, timeframe="W")
        vp_weekly_mode = "1d higher-timeframe composite"
        vp_weekly_note = (
            f"Weekly VP uses the most recent {weekly_vp_lookback} weekly windows of 1d OHLCV."
        )
        recent_weekly_vp_dates = get_recent_trading_dates_for_weekly_window(df_calc_daily, weekly_vp_lookback)
        weekly_vp_source_df = pd.DataFrame()
        weekly_vp_zones_raw = []
        vp_df_weekly = pd.DataFrame()

        try:
            weekly_vp_source_df = fetch_interval_history_for_dates(
                symbol_value=symbol,
                trading_dates=recent_weekly_vp_dates,
                provider_value=price_provider,
                interval_value="1d",
            )
        except Exception as interval_error:
            vp_weekly_mode = "1d unavailable"
            vp_weekly_note = (
                "1d higher-timeframe history could not be loaded for the selected replay window, "
                f"so higher-timeframe VP was omitted. Details: {interval_error}"
            )

        if not weekly_vp_source_df.empty:
            try:
                weekly_vp_zones_raw, vp_df_weekly = build_composite_interval_volume_profile_zones(
                    interval_df=weekly_vp_source_df,
                    bins=weekly_vp_bins,
                    zone_expand=zone_expand_pct,
                    hv_quantile=hv_node_quantile,
                    timeframe="W",
                    source_label="VP (W, 1d higher-timeframe composite)",
                    source_mode="1d_higher_timeframe_composite",
                )
            except Exception as interval_profile_error:
                weekly_vp_zones_raw, vp_df_weekly = [], pd.DataFrame()
                vp_weekly_mode = "1d unavailable"
                vp_weekly_note = (
                    "1d higher-timeframe VP construction failed for the selected replay window, "
                    f"so higher-timeframe VP was omitted. Details: {interval_profile_error}"
                )
            else:
                if not vp_df_weekly.empty:
                    vp_weekly_note = (
                        f"Weekly VP uses {len(recent_weekly_vp_dates)} trading days / "
                        f"{len(weekly_vp_source_df)} bars of 1d OHLCV."
                    )
                else:
                    vp_weekly_mode = "1d unavailable"
                    vp_weekly_note = (
                        "1d higher-timeframe history was returned, but no valid composite VP could be built, "
                        "so higher-timeframe VP was omitted."
                    )

        if weekly_vp_source_df.empty:
            if vp_weekly_mode != "1d unavailable":
                vp_weekly_mode = "1d unavailable"
                vp_weekly_note = (
                    "No 1d higher-timeframe history was returned for the selected replay window, "
                    "so higher-timeframe VP was omitted."
                )
        weekly_vp_zones = create_candidate_zones_from_vp(df_calc_weekly_with_features, weekly_vp_zones_raw)
        weekly_avwap_zones = create_candidate_zones_from_avwap(
            df=df_calc_weekly_with_features,
            anchor_meta=weekly_anchor_meta,
            zone_expand_pct=zone_expand_pct,
        )

        all_candidate_zones = merge_close_zones(
            daily_vp_zones + daily_avwap_zones + weekly_vp_zones + weekly_avwap_zones,
            merge_pct=merge_pct,
        )

        resistance_zones = rank_zones_for_side(
            zones=all_candidate_zones,
            vp_df_daily=vp_df_daily,
            vp_df_weekly=vp_df_weekly,
            current_price=current_price,
            side="resistance",
            max_zones=max_resistance_zones,
            df_reaction=df_calc_daily,
            lookahead=reaction_lookahead,
            reaction_threshold=reaction_return_threshold,
            min_gap=min_touch_gap,
        )
        resistance_zones = assign_zone_display_labels(resistance_zones, prefix="R")

        support_zones = rank_zones_for_side(
            zones=all_candidate_zones,
            vp_df_daily=vp_df_daily,
            vp_df_weekly=vp_df_weekly,
            current_price=current_price,
            side="support",
            max_zones=max_support_zones,
            df_reaction=df_calc_daily,
            lookahead=reaction_lookahead,
            reaction_threshold=reaction_return_threshold,
            min_gap=min_touch_gap,
        )
        support_zones = assign_zone_display_labels(support_zones, prefix="S")

        chart_data = build_lwc_series(
            df_plot=df_plot_display,
            df_calc_daily_with_features=df_calc_daily_with_features,
            support_zones=support_zones,
            resistance_zones=resistance_zones,
            daily_anchor_meta=daily_anchor_meta,
            show_avwap_lines=show_avwap_lines,
            atr_overlay=atr_overlay,
        )

        chart_options = {
            "layout": {
                "background": {"type": "solid", "color": "#ffffff"},
                "textColor": "#222",
                "fontSize": 12,
            },
            "grid": {
                "vertLines": {"color": "rgba(197, 203, 206, 0.3)"},
                "horzLines": {"color": "rgba(197, 203, 206, 0.3)"},
            },
            "crosshair": {
                "mode": 1
            },
            "rightPriceScale": {
                "borderColor": "rgba(197, 203, 206, 0.8)",
                "scaleMargins": {
                    "top": 0.08,
                    "bottom": 0.22,
                },
            },
            "timeScale": {
                "borderColor": "rgba(197, 203, 206, 0.8)",
                "timeVisible": True,
                "secondsVisible": False,
                "rightOffset": 5,
                "barSpacing": 12,
                "minBarSpacing": 4,
            },
            "height": 700,
        }

        replay_date = render_replay_controls(df_calc_daily_base)

        left_col, right_col = st.columns([1.15, 6.2], vertical_alignment="top")

        with left_col:
            render_zone_left_panel(
                support_zones=support_zones,
                resistance_zones=resistance_zones,
                current_price=current_price,
            )

        with right_col:
            render_lwc_chart_with_focus_header(
                chart_options=chart_options,
                series=chart_data,
                chart_key=f"lwc_{symbol}_{pd.Timestamp(replay_date).strftime('%Y%m%d')}",
            )

        st.caption(f"Daily VP mode: {vp_daily_mode}. {vp_daily_note}")
        st.caption(f"Weekly VP mode: {vp_weekly_mode}. {vp_weekly_note}")
        if show_atr_bands:
            if np.isfinite(atr20_value):
                st.caption(
                    f"ATR overlay: ATR20 = {atr20_value:.2f}; "
                    f"band distance = {atr20_value * atr_multiplier:.2f} ({atr_multiplier:.1f}x)."
                )
            else:
                st.caption("ATR overlay: insufficient daily bars to compute ATR20.")

        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

        nearest_res = min(resistance_zones, key=lambda z: abs(z["center"] - current_price)) if resistance_zones else None
        nearest_sup = min(support_zones, key=lambda z: abs(z["center"] - current_price)) if support_zones else None

        if show_atr_bands and np.isfinite(atr20_value):
            col1, col2, col3, col4 = st.columns(4)
        else:
            col1, col2, col3 = st.columns(3)
            col4 = None

        col1.metric("Replay Date", str(pd.Timestamp(replay_date).date()))
        col2.metric(
            "Nearest Resistance",
            (
                f"{nearest_res['lower']:.2f} - {nearest_res['upper']:.2f}"
                f" [{nearest_res.get('source_types_label', '')}]"
            ) if nearest_res else "N/A"
        )
        col3.metric(
            "Nearest Support",
            (
                f"{nearest_sup['lower']:.2f} - {nearest_sup['upper']:.2f}"
                f" [{nearest_sup.get('source_types_label', '')}]"
            ) if nearest_sup else "N/A"
        )
        if col4 is not None:
            col4.metric("ATR20", f"{atr20_value:.2f}", f"{atr_multiplier:.1f}x = {atr20_value * atr_multiplier:.2f}")

        show_definitions()

        st.markdown("### Selected Resistance Zones")
        if resistance_zones:
            st.dataframe(zones_to_dataframe(resistance_zones), use_container_width=True)
        else:
            st.info("No important resistance zones found.")

        st.markdown("### Selected Support Zones")
        if support_zones:
            st.dataframe(zones_to_dataframe(support_zones), use_container_width=True)
        else:
            st.info("No important support zones found.")

        if show_all_candidate_zones:
            st.markdown("### All Candidate Zones")
            if all_candidate_zones:
                st.dataframe(zones_to_dataframe(all_candidate_zones), use_container_width=True)
            else:
                st.info("No candidate zones detected.")

        st.markdown("### Daily AVWAP Anchor Points")
        if daily_anchor_meta:
            anchor_rows = []
            for col, meta in daily_anchor_meta.items():
                latest_avwap = df_calc_daily_with_features[col].dropna()
                avwap_now = float(latest_avwap.iloc[-1]) if not latest_avwap.empty else np.nan
                anchor_rows.append({
                    "timeframe": meta["timeframe"],
                    "avwap_column": col,
                    "anchor_name": meta["anchor_name"],
                    "start_date": meta["start_date"],
                    "start_price": meta["start_price"],
                    "latest_avwap": avwap_now,
                })
            st.dataframe(pd.DataFrame(anchor_rows), use_container_width=True)
        else:
            st.info("No daily AVWAP anchors available.")

        st.markdown("### Weekly AVWAP Anchor Points")
        if weekly_anchor_meta:
            anchor_rows = []
            for col, meta in weekly_anchor_meta.items():
                latest_avwap = df_calc_weekly_with_features[col].dropna()
                avwap_now = float(latest_avwap.iloc[-1]) if not latest_avwap.empty else np.nan
                anchor_rows.append({
                    "timeframe": meta["timeframe"],
                    "avwap_column": col,
                    "anchor_name": meta["anchor_name"],
                    "start_date": meta["start_date"],
                    "start_price": meta["start_price"],
                    "latest_avwap": avwap_now,
                })
            st.dataframe(pd.DataFrame(anchor_rows), use_container_width=True)
        else:
            st.info("No weekly AVWAP anchors available.")

        st.markdown("### Daily Composite Volume Profile Bins")
        if not vp_df_daily.empty:
            st.dataframe(vp_df_daily, use_container_width=True)
        else:
            st.info("No daily composite volume profile data available.")

        st.markdown("### Weekly / Higher-Timeframe Volume Profile Bins")
        if not vp_df_weekly.empty:
            st.dataframe(vp_df_weekly, use_container_width=True)
        else:
            st.info("No weekly or higher-timeframe volume profile data available.")

        st.markdown("### Data Frames Used")
        st.markdown(f"- Plot rows (replay): **{len(df_plot_replay)}**")
        st.markdown(f"- Daily calc rows: **{len(df_calc_daily_with_features)}**")
        st.markdown(f"- Weekly calc rows: **{len(df_calc_weekly_with_features)}**")

        st.markdown("### Historical Price Data (Replay Plot Frame)")
        st.dataframe(df_plot_replay, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {e}")


# =========================================================
# TABS
# =========================================================
tabs = st.tabs([
    "Historical Price",
    "Income",
    "Balance Sheet",
    "Cash Flow",
    "Ratios",
    "News",
])

with tabs[0]:
    show_price_chart()

with tabs[1]:
    show_dataframe_result(
        f"Income Statement — {symbol}",
        lambda: obb.equity.fundamental.income(symbol, provider=fund_provider)
        if fund_provider else obb.equity.fundamental.income(symbol),
        empty_message="No income statement data returned.",
    )

with tabs[2]:
    show_dataframe_result(
        f"Balance Sheet — {symbol}",
        lambda: obb.equity.fundamental.balance(symbol, provider=fund_provider)
        if fund_provider else obb.equity.fundamental.balance(symbol),
        empty_message="No balance sheet data returned.",
    )

with tabs[3]:
    show_dataframe_result(
        f"Cash Flow — {symbol}",
        lambda: obb.equity.fundamental.cash(symbol, provider=fund_provider)
        if fund_provider else obb.equity.fundamental.cash(symbol),
        empty_message="No cash flow data returned.",
    )

with tabs[4]:
    show_dataframe_result(
        f"Ratios — {symbol}",
        lambda: fetch_ratios_fmp(symbol),
        empty_message="No ratios data returned.",
    )

with tabs[5]:
    show_news(
        f"Company News — {symbol}",
        lambda: obb.news.company(symbol, limit=news_limit, provider=news_provider)
        if news_provider else obb.news.company(symbol, limit=news_limit),
    )
