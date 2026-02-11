"""
EcoPulse 动态交互式数据看板 · 主入口
====================================
Streamlit 多页面应用的主页 — 核心指标总览 (CEO 驾驶舱)。

运行方式:
    streamlit run CoreCode6/app.py
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.animations import animated_number, animation_control_panel
from src.dashboard.utils import (
    COLOR_MAP,
    init_plotly_theme,
    inject_custom_css,
    load_data,
)

# 屏蔽 Streamlit 内部日志噪声
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(
    logging.ERROR
)

st.set_page_config(
    page_title="EcoPulse · 核心指标总览",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── 日期范围过滤 ────────────────────────────────────────────
def _filter_by_date(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if df.empty or "dt" not in df.columns:
        return df
    mask = (df["dt"].dt.date >= start) & (df["dt"].dt.date <= end)
    return df.loc[mask].copy()


def main() -> None:
    inject_custom_css()
    init_plotly_theme()
    animation_control_panel()

    # ── 数据加载 ──────────────────────────────────────────
    df_funnel, _, _, _ = load_data()
    if df_funnel.empty:
        st.error("Serving 数据未就绪，请先运行 `scripts/prepare_serving_data.py` 生成 CSV。")
        st.stop()

    # ── 侧边栏 ───────────────────────────────────────────
    st.sidebar.title("🔍 筛选控制台")
    df_daily = df_funnel[df_funnel["dimension"] == "daily"].copy()
    if df_daily.empty:
        st.error("funnel_stats.csv 缺少 dimension='daily' 数据。")
        st.stop()

    min_date = df_daily["dt"].min().date()
    max_date = df_daily["dt"].max().date()
    selected_range = st.sidebar.date_input(
        "选择日期范围",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date,
    )
    if len(selected_range) < 2:
        st.info("请选择完整的日期范围。")
        st.stop()

    start_date, end_date = selected_range[0], selected_range[1]
    df_daily_f = _filter_by_date(df_daily, start_date, end_date)

    # ── 页面标题 ──────────────────────────────────────────
    st.markdown(
        '<div class="big-title">📊 核心指标总览 · CEO 驾驶舱</div>',
        unsafe_allow_html=True,
    )
    st.caption(
        f"数据范围：{start_date} → {end_date}｜指标按日粒度聚合，跨日去重差异已在 ETL 层处理。"
    )

    # ── 核心 KPI（动画卡片） ──────────────────────────────
    pv = int(
        df_daily_f.loc[df_daily_f["event_type"] == "view", "session_count"].sum()
    )
    uv = int(
        df_daily_f.loc[df_daily_f["event_type"] == "view", "user_count"].sum()
    )
    orders = int(
        df_daily_f.loc[
            df_daily_f["event_type"] == "purchase", "session_count"
        ].sum()
    )
    cvr = (orders / pv * 100) if pv else 0.0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        animated_number(
            value=float(pv),
            label="访问量 (PV)",
            format="integer",
            duration_ms=1400,
            color="#00d4ff",
            font_size="2.6rem",
            component_key="overview-pv",
        )
    with c2:
        animated_number(
            value=float(uv),
            label="用户量 (UV)",
            format="integer",
            duration_ms=1400,
            delay_ms=120,
            color="#00ff88",
            font_size="2.6rem",
            component_key="overview-uv",
        )
    with c3:
        animated_number(
            value=float(orders),
            label="订单数",
            format="integer",
            duration_ms=1400,
            delay_ms=240,
            color="#ff9500",
            font_size="2.6rem",
            component_key="overview-orders",
        )
    with c4:
        animated_number(
            value=cvr,
            label="转化率",
            format="percent",
            precision=2,
            suffix="%",
            duration_ms=1400,
            delay_ms=360,
            color="#a855f7",
            font_size="2.6rem",
            component_key="overview-cvr",
        )

    st.markdown("---")

    # ── 每日行为趋势 ─────────────────────────────────────
    st.markdown(
        '<div class="section-title">📅 每日行为趋势</div>',
        unsafe_allow_html=True,
    )

    event_names = {"view": "浏览", "cart": "加购", "purchase": "购买"}
    fig_trend = go.Figure()
    for event_type, color in COLOR_MAP.items():
        sub = df_daily_f[df_daily_f["event_type"] == event_type].sort_values("dt")
        if sub.empty:
            continue
        fig_trend.add_trace(
            go.Scatter(
                x=sub["dt"],
                y=sub["session_count"],
                mode="lines+markers",
                name=event_names.get(event_type, event_type),
                line=dict(color=color, width=2.5),
                marker=dict(size=5),
                hovertemplate=(
                    f"<b>{event_names.get(event_type, event_type)}</b><br>"
                    "日期: %{x|%Y-%m-%d}<br>"
                    "会话数: %{y:,.0f}<extra></extra>"
                ),
            )
        )

    fig_trend.update_layout(
        height=420,
        margin=dict(l=50, r=30, t=40, b=50),
        xaxis_title="日期",
        yaxis_title="会话数",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        hovermode="x unified",
    )
    st.plotly_chart(fig_trend, width="stretch")

    # ── 数据下载 ──────────────────────────────────────────
    st.download_button(
        label="📥 下载当前趋势数据 (CSV)",
        data=df_daily_f.to_csv(index=False).encode("utf-8-sig"),
        file_name="funnel_daily_filtered.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
