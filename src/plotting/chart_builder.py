from __future__ import annotations

import json

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config.settings import ChartDefaults


def build_chart_options(defaults: ChartDefaults | None = None) -> dict:
    defaults = defaults or ChartDefaults()
    return {
        "layout": {
            "background": {"type": "solid", "color": "#05070d"},
            "textColor": "#d7e2ee",
            "fontSize": 12,
        },
        "grid": {
            "vertLines": {"color": "rgba(148, 163, 184, 0.16)"},
            "horzLines": {"color": "rgba(148, 163, 184, 0.16)"},
        },
        "crosshair": {"mode": 1},
        "rightPriceScale": {
            "borderColor": "rgba(148, 163, 184, 0.36)",
            "scaleMargins": {
                "top": 0.08,
                "bottom": 0.22,
            },
        },
        "timeScale": {
            "borderColor": "rgba(148, 163, 184, 0.36)",
            "timeVisible": True,
            "secondsVisible": False,
            "rightOffset": defaults.right_offset,
            "barSpacing": defaults.bar_spacing,
            "minBarSpacing": defaults.min_bar_spacing,
        },
        "height": defaults.height,
    }


def to_lwc_time(value) -> str | int:
    timestamp = pd.to_datetime(value)
    if pd.isna(timestamp):
        return ""
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    if timestamp.time() == pd.Timestamp(timestamp.date()).time():
        return timestamp.strftime("%Y-%m-%d")
    return int(timestamp.timestamp())


def build_volume_profile_overlay_data(profile_df: pd.DataFrame) -> list[dict]:
    required_columns = {"bin_left", "bin_right", "bin_center", "volume"}
    if profile_df.empty or not required_columns.issubset(profile_df.columns):
        return []

    selected_columns = ["bin_left", "bin_right", "bin_center", "volume"]
    if "buy_volume" in profile_df.columns:
        selected_columns.append("buy_volume")
    if "sell_volume" in profile_df.columns:
        selected_columns.append("sell_volume")

    overlay_df = profile_df.loc[:, selected_columns].copy()
    for column in overlay_df.columns:
        overlay_df[column] = pd.to_numeric(overlay_df[column], errors="coerce")

    if "buy_volume" not in overlay_df.columns:
        overlay_df["buy_volume"] = 0.0
    if "sell_volume" not in overlay_df.columns:
        overlay_df["sell_volume"] = overlay_df["volume"]

    overlay_df["buy_volume"] = overlay_df["buy_volume"].fillna(0.0)
    overlay_df["sell_volume"] = overlay_df["sell_volume"].fillna(0.0)
    overlay_df["volume"] = overlay_df["buy_volume"] + overlay_df["sell_volume"]
    overlay_df = overlay_df.dropna(subset=["bin_left", "bin_right", "bin_center", "volume"]).copy()
    overlay_df = overlay_df.loc[overlay_df["volume"] > 0].copy()
    if overlay_df.empty:
        return []

    max_volume = float(overlay_df["volume"].max())
    total_volume = float(overlay_df["volume"].sum())
    overlay_df["is_poc"] = overlay_df["volume"] >= max_volume
    overlay_df["concentration_pct"] = (
        (overlay_df["volume"] / total_volume) * 100.0 if total_volume > 0 else 0.0
    )
    overlay_df["buy_share_pct"] = np.where(
        overlay_df["volume"] > 0,
        (overlay_df["buy_volume"] / overlay_df["volume"]) * 100.0,
        0.0,
    )
    overlay_df = overlay_df.sort_values("bin_center", kind="stable").reset_index(drop=True)

    return [
        {
            "bin_left": float(row.bin_left),
            "bin_right": float(row.bin_right),
            "bin_center": float(row.bin_center),
            "volume": float(row.volume),
            "buy_volume": float(row.buy_volume),
            "sell_volume": float(row.sell_volume),
            "is_poc": bool(row.is_poc),
            "concentration_pct": int(round(float(row.concentration_pct))),
            "buy_share_pct": int(round(float(row.buy_share_pct))),
        }
        for row in overlay_df.itertuples(index=False)
    ]


def build_lwc_series(
    df_plot: pd.DataFrame,
    support_zones: list[dict],
    resistance_zones: list[dict],
    pattern_events: list[dict] | None = None,
    atr_overlay: dict | None = None,
    ema_overlay: list[dict] | None = None,
) -> list[dict]:
    visible_start = pd.to_datetime(df_plot["date"].iloc[0]) if not df_plot.empty else None
    visible_end = pd.to_datetime(df_plot["date"].iloc[-1]) if not df_plot.empty else None

    candle_data = [
        {
            "time": to_lwc_time(row["date"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "change_pct": float(row["change_pct"]) if pd.notna(row.get("change_pct")) else None,
        }
        for _, row in df_plot.iterrows()
    ]

    volume_data = [
        {
            "time": to_lwc_time(row["date"]),
            "value": float(row["volume"]) if pd.notna(row["volume"]) else 0.0,
            "color": "rgba(255, 0, 0, 0.8)"
            if float(row["close"]) >= float(row["open"])
            else "rgba(0, 128, 0, 0.8)",
        }
        for _, row in df_plot.iterrows()
    ]

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
            "pattern_event_markers": _build_pattern_event_markers(pattern_events or []),
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

    for overlay in ema_overlay or []:
        line_data = overlay.get("data") or []
        if not line_data:
            continue
        series.append(
            {
                "type": "Line",
                "data": line_data,
                "overlay_label": {
                    "text": str(overlay.get("label", "EMA")),
                    "color": str(overlay.get("color", "#111827")),
                },
                "options": {
                    "lineWidth": 2,
                    "priceLineVisible": False,
                    "lastValueVisible": False,
                    "color": str(overlay.get("color", "#111827")),
                    "lineStyle": 0,
                },
            }
        )

    if not df_plot.empty:
        start_time = to_lwc_time(df_plot["date"].iloc[0])
        end_time = to_lwc_time(df_plot["date"].iloc[-1])

        for zone in resistance_zones:
            series.append(
                {
                    "type": "Line",
                    "data": [
                        {"time": start_time, "value": float(zone["center"])},
                        {"time": end_time, "value": float(zone["center"])},
                    ],
                    "overlay_label": {
                        "text": zone.get("display_label", ""),
                        "color": "#cc3333",
                        "fillColor": "rgba(204, 51, 51, 0.20)",
                        "lower": float(zone["lower"]),
                        "upper": float(zone["upper"]),
                    },
                    "options": {
                        "lineWidth": 3,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                        "color": "#cc3333",
                        "lineStyle": 2,
                    },
                }
            )

        for zone in support_zones:
            series.append(
                {
                    "type": "Line",
                    "data": [
                        {"time": start_time, "value": float(zone["center"])},
                        {"time": end_time, "value": float(zone["center"])},
                    ],
                    "overlay_label": {
                        "text": zone.get("display_label", ""),
                        "color": "#2e8b57",
                        "fillColor": "rgba(46, 139, 87, 0.20)",
                        "lower": float(zone["lower"]),
                        "upper": float(zone["upper"]),
                    },
                    "options": {
                        "lineWidth": 3,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                        "color": "#2e8b57",
                        "lineStyle": 2,
                    },
                }
            )

        if atr_overlay is not None:
            atr_label = atr_overlay.get("label", "ATR20")
            atr_color = atr_overlay.get("color", "#7c3aed")
            atr_upper_data = atr_overlay.get("upper_data") or []
            atr_lower_data = atr_overlay.get("lower_data") or []

            if visible_start is not None and visible_end is not None:
                atr_upper_data = [
                    row
                    for row in atr_upper_data
                    if visible_start <= pd.to_datetime(row["time"]) <= visible_end
                ]
                atr_lower_data = [
                    row
                    for row in atr_lower_data
                    if visible_start <= pd.to_datetime(row["time"]) <= visible_end
                ]

            if atr_upper_data:
                series.append(
                    {
                        "type": "Line",
                        "data": atr_upper_data,
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
                    }
                )

            if atr_lower_data:
                series.append(
                    {
                        "type": "Line",
                        "data": atr_lower_data,
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
                    }
                )

    return series


def _build_pattern_event_markers(pattern_events: list[dict]) -> list[dict]:
    markers: list[dict] = []
    stack_counts: dict[tuple[str, str], int] = {}
    for event in pattern_events:
        direction = str(event.get("direction", "")).strip().lower()
        is_up = direction in {"bullish", "up"}
        source = str(event.get("source", "")).strip().lower()
        is_divergence = source == "macd_divergence" or "divergence" in str(event.get("event_type", "")).lower()
        is_breakout = source == "breakout"
        marker_time = to_lwc_time(event.get("event_time"))
        marker_position = "aboveBar" if is_breakout and is_up else "belowBar" if is_breakout else "belowBar" if is_up else "aboveBar"
        stack_key = (str(marker_time), marker_position)
        stack_index = stack_counts.get(stack_key, 0)
        stack_counts[stack_key] = stack_index + 1
        markers.append(
            {
                "time": marker_time,
                "position": marker_position,
                "color": _event_marker_color(event, is_up=is_up, is_divergence=is_divergence, is_breakout=is_breakout),
                "shape": "arrowDown" if marker_position == "aboveBar" else "arrowUp",
                "text": _event_display_name(event),
                "stackIndex": stack_index,
            }
        )
    return markers


EVENT_DISPLAY_NAMES = {
    "volume_stall_up": "\u653e\u91cf\u4e0a\u6da8\u505c\u6ede",
    "volume_hold_down": "\u653e\u91cf\u4e0b\u8dcc\u627f\u63a5",
    "volume_long_upper_wick": "\u653e\u91cf\u957f\u4e0a\u5f71",
    "volume_long_lower_wick": "\u653e\u91cf\u957f\u4e0b\u5f71",
    "macd_bearish_divergence": "\u9876\u80cc\u79bb",
    "macd_bearish_divergence_risk": "\u9876\u80cc\u79bb",
    "macd_bullish_divergence": "\u5e95\u80cc\u79bb",
    "macd_bullish_divergence_risk": "\u5e95\u80cc\u79bb",
    "confirmed": "\u7a81\u7834",
    "true_breakout_weak": "\u5f31\u7a81\u7834",
    "retest_success": "\u56de\u8e29\u786e\u8ba4",
}


def _event_marker_color(event: dict, *, is_up: bool, is_divergence: bool, is_breakout: bool) -> str:
    if is_breakout:
        return "#dc2626" if is_up else "#15803d"
    if is_divergence:
        return "#2563eb" if is_up else "#ea580c"
    return "#15803d" if is_up else "#dc2626"


def _event_display_name(event: dict) -> str:
    event_type = str(event.get("event_type", "")).strip().lower()
    if event_type in EVENT_DISPLAY_NAMES:
        return EVENT_DISPLAY_NAMES[event_type]
    event_name = str(event.get("event_name") or "").strip()
    return event_name or event_type


def render_lwc_chart_with_focus_header(
    chart_options: dict,
    series: list[dict],
    chart_key: str,
    volume_profile_data: list[dict] | None = None,
):
    chart_height = int(chart_options.get("height", 700))
    container_id = f"lwc-chart-{abs(hash(chart_key))}"
    payload = json.dumps(
        {
            "chart": chart_options,
            "series": series,
            "volumeProfile": volume_profile_data or [],
        },
        ensure_ascii=False,
    )

    html = f"""
<div id="{container_id}" class="lwc-wrap">
  <div id="{container_id}-header" class="lwc-header"></div>
  <div id="{container_id}-zone-labels" class="lwc-zone-labels"></div>
  <div id="{container_id}-pattern-markers" class="lwc-pattern-markers"></div>
  <div id="{container_id}-overlay-legend" class="lwc-overlay-legend"></div>
  <div id="{container_id}-volume-profile-panel" class="lwc-volume-profile-panel"></div>
  <div id="{container_id}-volume-profile" class="lwc-volume-profile"></div>
  <div id="{container_id}-chart" class="lwc-chart"></div>
</div>

<style>
  html, body {{
    margin: 0;
    padding: 0;
    background: #05070d;
  }}

  .lwc-wrap {{
    position: relative;
    width: 100%;
    height: {chart_height}px;
    background: #05070d;
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

  .lwc-pattern-markers {{
    position: absolute;
    inset: 0;
    z-index: 12;
    pointer-events: none;
  }}

  .lwc-pattern-marker {{
    position: absolute;
    width: 0;
    display: block;
  }}

  .lwc-pattern-label {{
    position: absolute;
    left: 0;
    display: flex;
    align-items: center;
    padding: 2px 5px;
    border-radius: 999px;
    background: rgba(13, 20, 32, 0.94);
    border: 1px solid currentColor;
    font-size: 10px;
    font-weight: 700;
    line-height: 1.2;
    white-space: nowrap;
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.12);
  }}

  .lwc-pattern-marker.above .lwc-pattern-label {{
    top: 0;
    transform: translate(-50%, -100%);
  }}

  .lwc-pattern-marker.below .lwc-pattern-label {{
    bottom: 0;
    transform: translate(-50%, 100%);
  }}

  .lwc-pattern-stem {{
    position: absolute;
    left: 0;
    width: 1px;
    background: currentColor;
    opacity: 0.82;
    transform: translateX(-50%);
  }}

  .lwc-pattern-marker.above .lwc-pattern-stem {{
    top: 4px;
    bottom: 8px;
  }}

  .lwc-pattern-marker.below .lwc-pattern-stem {{
    top: 8px;
    bottom: 4px;
  }}

  .lwc-pattern-arrow {{
    position: absolute;
    left: 0;
    width: 0;
    height: 0;
    transform: translateX(-50%);
  }}

  .lwc-pattern-marker.above .lwc-pattern-arrow {{
    bottom: 1px;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 9px solid currentColor;
  }}

  .lwc-pattern-marker.below .lwc-pattern-arrow {{
    top: 1px;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-bottom: 9px solid currentColor;
  }}

  .lwc-volume-profile {{
    position: absolute;
    inset: 0;
    z-index: 8;
    pointer-events: none;
  }}

  .lwc-volume-profile-panel {{
    position: absolute;
    top: 0;
    right: 64px;
    bottom: 0;
    width: 196px;
    z-index: 7;
    pointer-events: none;
    background:
      linear-gradient(
        to right,
        rgba(148, 163, 184, 0.14) 0,
        rgba(148, 163, 184, 0.14) 1px,
        rgba(5, 7, 13, 0.88) 1px,
        rgba(5, 7, 13, 0.88) 100%
      );
    border-left: 1px solid rgba(148, 163, 184, 0.14);
  }}

  .lwc-zone-label {{
    position: absolute;
    left: 8px;
    z-index: 2;
    transform: translateY(-50%);
    padding: 2px 6px;
    border-radius: 999px;
    background: rgba(13, 20, 32, 0.94);
    border: 1px solid currentColor;
    font-size: 11px;
    font-weight: 700;
    line-height: 1.2;
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.08);
    white-space: nowrap;
  }}

  .lwc-zone-band {{
    position: absolute;
    left: 0;
    right: 64px;
    z-index: 1;
    min-height: 2px;
    pointer-events: none;
  }}

  .lwc-volume-profile-bar {{
    position: absolute;
    right: 76px;
    display: flex;
    flex-direction: row;
    overflow: hidden;
    border-radius: 999px 0 0 999px;
    background: rgba(15, 23, 42, 0.52);
    border: 1px solid rgba(148, 163, 184, 0.35);
    box-sizing: border-box;
  }}

  .lwc-volume-profile-bar.poc {{
    border-color: rgba(245, 158, 11, 0.75);
    box-shadow: 0 0 0 1px rgba(245, 158, 11, 0.2);
  }}

  .lwc-volume-profile-segment {{
    height: 100%;
  }}

  .lwc-volume-profile-segment.buy {{
    background: rgba(220, 38, 38, 0.62);
  }}

  .lwc-volume-profile-segment.sell {{
    background: rgba(21, 128, 61, 0.62);
  }}

  .lwc-volume-profile-tag {{
    position: absolute;
    top: 8px;
    left: 8px;
    right: 8px;
    padding: 4px 7px;
    border-radius: 6px;
    background: rgba(13, 20, 32, 0.96);
    border: 1px solid rgba(245, 158, 11, 0.75);
    color: #fbbf24;
    font-size: 10px;
    font-weight: 700;
    line-height: 1.25;
    white-space: normal;
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.10);
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
    background: rgba(13, 20, 32, 0.86);
    border: 1px solid rgba(148, 163, 184, 0.22);
    border-radius: 10px;
    box-shadow: 0 8px 22px rgba(0, 0, 0, 0.22);
    backdrop-filter: blur(6px);
    color: #d7e2ee;
    pointer-events: none;
  }}

  .lwc-overlay-legend {{
    position: absolute;
    top: 12px;
    right: 76px;
    z-index: 11;
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 6px;
    max-width: min(52%, 620px);
    pointer-events: none;
  }}

  .lwc-overlay-legend-item {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 7px;
    border-radius: 999px;
    background: rgba(13, 20, 32, 0.86);
    border: 1px solid rgba(148, 163, 184, 0.22);
    color: #d7e2ee;
    font-size: 11px;
    font-weight: 700;
    line-height: 1.15;
    white-space: nowrap;
  }}

  .lwc-overlay-legend-swatch {{
    width: 16px;
    height: 2px;
    border-radius: 999px;
    background: currentColor;
  }}

  .lwc-header-date {{
    font-size: 12px;
    font-weight: 600;
    color: #d7e2ee;
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
    color: #94a3b8;
  }}

  .lwc-header-value {{
    font-size: 16px;
    font-weight: 700;
    color: #ffffff;
  }}
</style>

<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
  const payload = {payload};
  const root = document.getElementById("{container_id}");
  const header = document.getElementById("{container_id}-header");
  const zoneLabels = document.getElementById("{container_id}-zone-labels");
  const patternMarkersLayer = document.getElementById("{container_id}-pattern-markers");
  const overlayLegend = document.getElementById("{container_id}-overlay-legend");
  const volumeProfilePanel = document.getElementById("{container_id}-volume-profile-panel");
  const volumeProfile = document.getElementById("{container_id}-volume-profile");
  const chartNode = document.getElementById("{container_id}-chart");
  const priceScaleRightOffset = 64;
  const patternMarkerGapPx = 72;
  const patternMarkerStackGapPx = 30;

  const getProfilePanelWidth = () => {{
    const chartWidth = chartNode.clientWidth || root.clientWidth || 900;
    return Math.max(Math.min(chartWidth * 0.2, 196), 108);
  }};

  const getZoneBandRightOffset = () => priceScaleRightOffset + getProfilePanelWidth();

  const normalizeTime = (value) => {{
    if (typeof value === "string") {{
      return value;
    }}
    if (typeof value === "number") {{
      const iso = new Date(value * 1000).toISOString();
      return iso.endsWith("T00:00:00.000Z") ? iso.slice(0, 10) : iso.slice(0, 19);
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
      return "#94a3b8";
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
  let primaryPriceSeries = null;
  const candleLookup = new Map();
  const zoneLabelSeries = [];
  const overlayLegendItems = [];
  let patternEventMarkers = [];

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

    if (item.pattern_event_markers && item.pattern_event_markers.length) {{
      patternEventMarkers = item.pattern_event_markers;
    }}

    if (item.overlay_label && item.data && item.data.length) {{
      if (item.overlay_label.showInLegend) {{
        overlayLegendItems.push({{
          text: item.overlay_label.text || "",
          color: item.overlay_label.color || "#d7e2ee",
        }});
      }}
      if (item.overlay_label.labelOnChart !== false) {{
        zoneLabelSeries.push({{
          series: createdSeries,
          value: Number(item.data[0].value),
          text: item.overlay_label.text || "",
          color: item.overlay_label.color || "#d7e2ee",
          fillColor: item.overlay_label.fillColor || "",
          lower: Number(item.overlay_label.lower),
          upper: Number(item.overlay_label.upper),
        }});
      }}
    }}

    if (item.type === "Candlestick" && candleData.length === 0) {{
      candleData = item.data || [];
      primaryPriceSeries = createdSeries;
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

  const renderOverlayLegend = () => {{
    if (!overlayLegend) {{
      return;
    }}

    overlayLegend.innerHTML = "";
    overlayLegendItems.forEach((item) => {{
      const el = document.createElement("div");
      el.className = "lwc-overlay-legend-item";
      el.style.color = item.color || "#d7e2ee";

      const swatch = document.createElement("span");
      swatch.className = "lwc-overlay-legend-swatch";
      el.appendChild(swatch);

      const text = document.createElement("span");
      text.textContent = item.text || "";
      el.appendChild(text);
      overlayLegend.appendChild(el);
    }});
  }};

  renderOverlayLegend();

  const renderZoneLabels = () => {{
    if (!zoneLabels) {{
      return;
    }}

    zoneLabels.innerHTML = "";
    const coordinateSeries = primaryPriceSeries || zoneLabelSeries[0]?.series;
    if (!coordinateSeries) {{
      return;
    }}

    zoneLabelSeries.forEach((item) => {{
      const upperY = coordinateSeries.priceToCoordinate(item.upper);
      const lowerY = coordinateSeries.priceToCoordinate(item.lower);
      if (item.fillColor && Number.isFinite(upperY) && Number.isFinite(lowerY)) {{
        const band = document.createElement("div");
        band.className = "lwc-zone-band";
        band.style.top = `${{Math.min(upperY, lowerY)}}px`;
        band.style.height = `${{Math.max(Math.abs(lowerY - upperY), 2)}}px`;
        band.style.right = `${{getZoneBandRightOffset()}}px`;
        band.style.background = item.fillColor;
        zoneLabels.appendChild(band);
      }}

      const y = coordinateSeries.priceToCoordinate(item.value);
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

  const renderPatternMarkers = () => {{
    if (!patternMarkersLayer || !primaryPriceSeries) {{
      return;
    }}

    patternMarkersLayer.innerHTML = "";

    patternEventMarkers.forEach((marker) => {{
      const timeKey = normalizeTime(marker.time);
      const bar = candleLookup.get(timeKey);
      if (!bar) {{
        return;
      }}

      const x = chart.timeScale().timeToCoordinate(marker.time);
      const anchorPrice = marker.position === "belowBar" ? Number(bar.low) : Number(bar.high);
      const anchorY = primaryPriceSeries.priceToCoordinate(anchorPrice);
      if (![x, anchorY].every((value) => Number.isFinite(value))) {{
        return;
      }}

      const isBelow = marker.position === "belowBar";
      const stackIndex = Math.max(0, Number(marker.stackIndex) || 0);
      const markerHeight = patternMarkerGapPx + stackIndex * patternMarkerStackGapPx;
      const el = document.createElement("div");
      el.className = `lwc-pattern-marker ${{isBelow ? "below" : "above"}}`;
      el.style.left = `${{x}}px`;
      el.style.top = `${{anchorY + (isBelow ? 0 : -markerHeight)}}px`;
      el.style.height = `${{markerHeight}}px`;
      el.style.color = marker.color || "#d7e2ee";

      const stem = document.createElement("span");
      stem.className = "lwc-pattern-stem";
      el.appendChild(stem);

      const arrow = document.createElement("span");
      arrow.className = "lwc-pattern-arrow";
      el.appendChild(arrow);

      const text = document.createElement("span");
      text.className = "lwc-pattern-label";
      text.textContent = marker.text || "";
      el.appendChild(text);
      patternMarkersLayer.appendChild(el);
    }});
  }};

  const renderVolumeProfile = () => {{
    if (!volumeProfile) {{
      return;
    }}

    volumeProfile.innerHTML = "";
    if (volumeProfilePanel) {{
      volumeProfilePanel.innerHTML = "";
    }}

    const profileRows = payload.volumeProfile || [];
    if (!profileRows.length || !primaryPriceSeries) {{
      if (volumeProfilePanel) {{
        volumeProfilePanel.style.display = "none";
      }}
      return;
    }}

    if (volumeProfilePanel) {{
      volumeProfilePanel.style.display = "block";
    }}

    const profilePanelWidth = getProfilePanelWidth();
    const profileWidth = Math.max(profilePanelWidth - 18, 72);
    const profilePanelRightOffset = priceScaleRightOffset;

    if (volumeProfilePanel) {{
      volumeProfilePanel.style.right = `${{profilePanelRightOffset}}px`;
      volumeProfilePanel.style.width = `${{profilePanelWidth}}px`;
    }}

    const maxVolume = Math.max(
      ...profileRows.map((row) => Number(row.volume)).filter((value) => Number.isFinite(value)),
      0
    );

    if (!(maxVolume > 0)) {{
      if (volumeProfilePanel) {{
        volumeProfilePanel.style.display = "none";
      }}
      return;
    }}

    let pocTagAdded = false;
    profileRows.forEach((row) => {{
      const topY = primaryPriceSeries.priceToCoordinate(Number(row.bin_right));
      const bottomY = primaryPriceSeries.priceToCoordinate(Number(row.bin_left));
      const centerY = primaryPriceSeries.priceToCoordinate(Number(row.bin_center));
      const volume = Number(row.volume);
      const buyVolume = Number(row.buy_volume);
      const sellVolume = Number(row.sell_volume);

      if (![topY, bottomY, centerY, volume, buyVolume, sellVolume].every((value) => Number.isFinite(value))) {{
        return;
      }}

      const top = Math.min(topY, bottomY);
      const rawHeight = Math.abs(bottomY - topY);
      const height = Math.max(rawHeight - 1, 3);
      const width = Math.max((volume / maxVolume) * profileWidth, 2);
      const buyRatio = volume > 0 ? Math.max(Math.min(buyVolume / volume, 1), 0) : 0;
      const buyWidth = width * buyRatio;
      const sellWidth = Math.max(width - buyWidth, 0);

      const bar = document.createElement("div");
      bar.className = `lwc-volume-profile-bar${{row.is_poc ? " poc" : ""}}`;
      bar.style.top = `${{top}}px`;
      bar.style.width = `${{width}}px`;
      bar.style.height = `${{height}}px`;
      bar.style.right = `${{profilePanelRightOffset + 12}}px`;

      if (buyWidth > 0) {{
        const buySegment = document.createElement("div");
        buySegment.className = "lwc-volume-profile-segment buy";
        buySegment.style.width = `${{buyWidth}}px`;
        bar.appendChild(buySegment);
      }}

      if (sellWidth > 0) {{
        const sellSegment = document.createElement("div");
        sellSegment.className = "lwc-volume-profile-segment sell";
        sellSegment.style.width = `${{sellWidth}}px`;
        bar.appendChild(sellSegment);
      }}

      volumeProfile.appendChild(bar);

      if (row.is_poc && volumeProfilePanel && !pocTagAdded) {{
        pocTagAdded = true;
        const tag = document.createElement("div");
        tag.className = "lwc-volume-profile-tag";
        const concentrationText = Number.isFinite(Number(row.concentration_pct))
          ? `${{Math.round(Number(row.concentration_pct))}}%`
          : "";
        const buyShareText = Number.isFinite(Number(row.buy_share_pct))
          ? ` buy ${{Math.round(Number(row.buy_share_pct))}}%`
          : "";
        tag.textContent = concentrationText
          ? `POC ${{concentrationText}}${{buyShareText}}`
          : `POC${{buyShareText}}`;
        volumeProfilePanel.appendChild(tag);
      }}
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
  renderPatternMarkers();
  renderVolumeProfile();

  const applyRightOffsetForProfile = () => {{
    const timeScaleOptions = payload.chart?.timeScale || {{}};
    const profilePanelWidth = getProfilePanelWidth();
    const dividerPadding = 20;
    const barSpacing = Number(timeScaleOptions.barSpacing) || 12;
    const baseRightOffset = Number(timeScaleOptions.rightOffset) || 0;
    const profileRightOffset = Math.ceil((profilePanelWidth + dividerPadding) / Math.max(barSpacing, 1));

    chart.applyOptions({{
      timeScale: {{
        ...timeScaleOptions,
        rightOffset: Math.max(baseRightOffset, profileRightOffset),
      }},
    }});
  }};

  applyRightOffsetForProfile();
  chart.timeScale().fitContent();

  const applyWidth = () => {{
    const width = root.clientWidth || 900;
    chart.applyOptions({{ width }});
    applyRightOffsetForProfile();
    renderZoneLabels();
    renderPatternMarkers();
    renderVolumeProfile();
  }};

  const resizeObserver = new ResizeObserver(() => {{
    applyWidth();
  }});

  resizeObserver.observe(root);
  window.addEventListener("resize", applyWidth);
  chartNode.addEventListener("wheel", () => {{
    window.requestAnimationFrame(renderPatternMarkers);
    window.setTimeout(renderPatternMarkers, 80);
  }}, {{ passive: true }});
  chart.timeScale().subscribeVisibleTimeRangeChange(() => {{
    renderZoneLabels();
    renderPatternMarkers();
    renderVolumeProfile();
  }});
  window.setInterval(() => {{
    renderZoneLabels();
    renderPatternMarkers();
    renderVolumeProfile();
  }}, 500);
</script>
"""

    components.html(html, height=chart_height + 6)


def render_zone_left_panel(
    support_zones: list[dict],
    resistance_zones: list[dict],
    current_price: float,
):
    st.markdown("#### Zones")
    st.metric("Calc Close", f"{current_price:.2f}")

    if resistance_zones:
        st.markdown("**Resistance**")
        for zone in resistance_zones:
            zone_id = zone.get("zone_id", "")
            st.markdown(
                f"""
<div style="margin-bottom:10px; padding:8px 10px; border-left:6px solid #cc3333; background:#fff5f5; border-radius:6px;">
    <div style="font-weight:700;">{zone.get("display_label", "")} [{zone.get("source_types_label", "")}]</div>
    <div style="font-size:12px; color:#555;">{zone.get("zone_status", "active")} · {zone.get("zone_kind", "")}</div>
    <div>{zone["lower"]:.2f} - {zone["upper"]:.2f}</div>
    <div style="font-size:12px; color:#444;">Center {zone["center"]:.2f}</div>
    <div style="font-size:12px; color:#444;">Strength {float(zone.get("zone_strength_pct", 0.0)):.2f}%</div>
    <div style="font-size:11px; color:#777; overflow-wrap:anywhere;">ID {zone_id}</div>
    <div style="font-size:12px; color:#666;">
      T {zone.get("touch_count", 0)} · B {zone.get("break_count", 0)} · CB {zone.get("confirmed_breakout_count", 0)}
    </div>
</div>
""",
                unsafe_allow_html=True,
            )
    else:
        st.info("No resistance zones.")

    if support_zones:
        st.markdown("**Support**")
        for zone in support_zones:
            zone_id = zone.get("zone_id", "")
            st.markdown(
                f"""
<div style="margin-bottom:10px; padding:8px 10px; border-left:6px solid #2e8b57; background:#f4fff7; border-radius:6px;">
    <div style="font-weight:700;">{zone.get("display_label", "")} [{zone.get("source_types_label", "")}]</div>
    <div style="font-size:12px; color:#555;">{zone.get("zone_status", "active")} · {zone.get("zone_kind", "")}</div>
    <div>{zone["lower"]:.2f} - {zone["upper"]:.2f}</div>
    <div style="font-size:12px; color:#444;">Center {zone["center"]:.2f}</div>
    <div style="font-size:12px; color:#444;">Strength {float(zone.get("zone_strength_pct", 0.0)):.2f}%</div>
    <div style="font-size:11px; color:#777; overflow-wrap:anywhere;">ID {zone_id}</div>
    <div style="font-size:12px; color:#666;">
      T {zone.get("touch_count", 0)} · B {zone.get("break_count", 0)} · CB {zone.get("confirmed_breakout_count", 0)}
    </div>
</div>
""",
                unsafe_allow_html=True,
            )
    else:
        st.info("No support zones.")
