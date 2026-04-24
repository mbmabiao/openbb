from __future__ import annotations

import json

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config.settings import ChartDefaults


def build_chart_options(defaults: ChartDefaults | None = None) -> dict:
    defaults = defaults or ChartDefaults()
    return {
        "layout": {
            "background": {"type": "solid", "color": "#ffffff"},
            "textColor": "#222222",
            "fontSize": 12,
        },
        "grid": {
            "vertLines": {"color": "rgba(197, 203, 206, 0.3)"},
            "horzLines": {"color": "rgba(197, 203, 206, 0.3)"},
        },
        "crosshair": {"mode": 1},
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
            "rightOffset": defaults.right_offset,
            "barSpacing": defaults.bar_spacing,
            "minBarSpacing": defaults.min_bar_spacing,
        },
        "height": defaults.height,
    }


def to_lwc_time(value) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def build_volume_profile_overlay_data(profile_df: pd.DataFrame) -> list[dict]:
    required_columns = {"bin_left", "bin_right", "bin_center", "volume"}
    if profile_df.empty or not required_columns.issubset(profile_df.columns):
        return []

    overlay_df = profile_df.loc[:, ["bin_left", "bin_right", "bin_center", "volume"]].copy()
    for column in overlay_df.columns:
        overlay_df[column] = pd.to_numeric(overlay_df[column], errors="coerce")
    overlay_df = overlay_df.dropna(subset=["bin_left", "bin_right", "bin_center", "volume"]).copy()
    overlay_df = overlay_df.loc[overlay_df["volume"] > 0].copy()
    if overlay_df.empty:
        return []

    max_volume = float(overlay_df["volume"].max())
    overlay_df["is_poc"] = overlay_df["volume"] >= max_volume
    overlay_df = overlay_df.sort_values("bin_center", kind="stable").reset_index(drop=True)

    return [
        {
            "bin_left": float(row.bin_left),
            "bin_right": float(row.bin_right),
            "bin_center": float(row.bin_center),
            "volume": float(row.volume),
            "is_poc": bool(row.is_poc),
        }
        for row in overlay_df.itertuples(index=False)
    ]


def build_lwc_series(
    df_plot: pd.DataFrame,
    df_calc_daily_with_features: pd.DataFrame,
    support_zones: list[dict],
    resistance_zones: list[dict],
    daily_anchor_meta: dict,
    show_avwap_lines: bool,
    atr_overlay: dict | None = None,
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
        for column_name, meta in daily_anchor_meta.items():
            valid = df_calc_daily_with_features[["date", column_name]].dropna().copy()
            if valid.empty:
                continue
            if visible_start is not None and visible_end is not None:
                valid = valid[
                    (pd.to_datetime(valid["date"]) >= visible_start)
                    & (pd.to_datetime(valid["date"]) <= visible_end)
                ].copy()
            if valid.empty:
                continue

            line_data = [
                {
                    "time": to_lwc_time(row["date"]),
                    "value": float(row[column_name]),
                }
                for _, row in valid.iterrows()
            ]
            series.append(
                {
                    "type": "Line",
                    "data": line_data,
                    "options": {
                        "lineWidth": 1,
                        "priceLineVisible": False,
                        "lastValueVisible": False,
                        "color": "#2962FF",
                        "lineStyle": 2,
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
            atr_upper = atr_overlay.get("upper")
            atr_lower = atr_overlay.get("lower")
            atr_label = atr_overlay.get("label", "ATR20")
            atr_color = atr_overlay.get("color", "#7c3aed")

            if atr_upper is not None:
                series.append(
                    {
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
                    }
                )

            if atr_lower is not None:
                series.append(
                    {
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
                    }
                )

    return series


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
  <div id="{container_id}-volume-profile" class="lwc-volume-profile"></div>
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

  .lwc-volume-profile {{
    position: absolute;
    inset: 0;
    z-index: 8;
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

  .lwc-volume-profile-bar {{
    position: absolute;
    right: 76px;
    border-radius: 999px 0 0 999px;
    background: rgba(59, 130, 246, 0.22);
    border: 1px solid rgba(59, 130, 246, 0.35);
    box-sizing: border-box;
  }}

  .lwc-volume-profile-bar.poc {{
    background: rgba(245, 158, 11, 0.42);
    border-color: rgba(245, 158, 11, 0.75);
    box-shadow: 0 0 0 1px rgba(245, 158, 11, 0.2);
  }}

  .lwc-volume-profile-tag {{
    position: absolute;
    right: 76px;
    transform: translateY(-50%);
    padding: 1px 6px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.96);
    border: 1px solid rgba(245, 158, 11, 0.75);
    color: #92400e;
    font-size: 10px;
    font-weight: 700;
    line-height: 1.2;
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
  const volumeProfile = document.getElementById("{container_id}-volume-profile");
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
  let primaryPriceSeries = null;
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

  const renderVolumeProfile = () => {{
    if (!volumeProfile) {{
      return;
    }}

    volumeProfile.innerHTML = "";

    const profileRows = payload.volumeProfile || [];
    if (!profileRows.length || !primaryPriceSeries) {{
      return;
    }}

    const chartWidth = chartNode.clientWidth || root.clientWidth || 900;
    const profileWidth = Math.max(Math.min(chartWidth * 0.18, 180), 72);
    const maxVolume = Math.max(
      ...profileRows.map((row) => Number(row.volume)).filter((value) => Number.isFinite(value)),
      0
    );

    if (!(maxVolume > 0)) {{
      return;
    }}

    profileRows.forEach((row) => {{
      const topY = primaryPriceSeries.priceToCoordinate(Number(row.bin_right));
      const bottomY = primaryPriceSeries.priceToCoordinate(Number(row.bin_left));
      const centerY = primaryPriceSeries.priceToCoordinate(Number(row.bin_center));
      const volume = Number(row.volume);

      if (![topY, bottomY, centerY, volume].every((value) => Number.isFinite(value))) {{
        return;
      }}

      const top = Math.min(topY, bottomY);
      const rawHeight = Math.abs(bottomY - topY);
      const height = Math.max(rawHeight - 1, 3);
      const width = Math.max((volume / maxVolume) * profileWidth, 2);

      const bar = document.createElement("div");
      bar.className = `lwc-volume-profile-bar${{row.is_poc ? " poc" : ""}}`;
      bar.style.top = `${{top}}px`;
      bar.style.width = `${{width}}px`;
      bar.style.height = `${{height}}px`;
      volumeProfile.appendChild(bar);

      if (row.is_poc) {{
        const tag = document.createElement("div");
        tag.className = "lwc-volume-profile-tag";
        tag.textContent = "POC";
        tag.style.top = `${{centerY}}px`;
        tag.style.transform = `translate(-${{Math.min(width + 8, profileWidth + 8)}}px, -50%)`;
        volumeProfile.appendChild(tag);
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
  renderVolumeProfile();

  const applyWidth = () => {{
    const width = root.clientWidth || 900;
    chart.applyOptions({{ width }});
    renderZoneLabels();
    renderVolumeProfile();
  }};

  const resizeObserver = new ResizeObserver(() => {{
    applyWidth();
  }});

  resizeObserver.observe(root);
  window.addEventListener("resize", applyWidth);
  chart.timeScale().subscribeVisibleTimeRangeChange(() => {{
    renderZoneLabels();
    renderVolumeProfile();
  }});
  window.setInterval(renderVolumeProfile, 500);
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
        st.markdown("**Support**")
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
