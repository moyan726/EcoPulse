"""
EcoPulse · 客户价值与留存 (Customer Value & Retention)
=====================================================
定位: CRM / 数据分析师视角的深度用户挖掘。

组件:
1. 用户核心 KPI (动画数值)
2. RFM 价值分层 (环形图)
3. K-Means 聚类画像 (可选择的动画雷达图)
4. 留存热力图
5. 明细数据查询 (Data Grid)
"""
from __future__ import annotations

import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.animations import animated_number, animated_radar, animation_control_panel
from src.dashboard.utils import (
    CLUSTER_COLORS,
    RETENTION_HEATMAP_SCALE,
    init_plotly_theme,
    inject_custom_css,
    load_data,
)

st.set_page_config(page_title="Customer Value & Retention", page_icon="💎", layout="wide")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(
    logging.ERROR
)


# ── 辅助函数 ──────────────────────────────────────────────
def _scale(val: float, lo: float, hi: float, invert: bool = False) -> float:
    """将 val 归一化到 [0, 100]，可选反转。"""
    if hi <= lo:
        return 50.0
    ratio = (val - lo) / (hi - lo)
    if invert:
        ratio = 1 - ratio
    return max(0.0, min(1.0, ratio)) * 100


def _build_all_cluster_radars(df: pd.DataFrame):
    """
    计算所有聚类的雷达图数据。
    返回 dict[cluster_id -> (categories, values, user_count)] 或 None（数据不可用时）。
    """
    required = {"prediction", "recency", "frequency", "monetary"}
    if df.empty or not required.issubset(df.columns):
        return None

    means = df.groupby("prediction")[["recency", "frequency", "monetary"]].mean()
    counts = df.groupby("prediction").size()
    if means.empty:
        return None

    # 基于 Cluster 均值的 min/max 做归一化（各 Cluster 之间可比较）
    # 使用均值而非用户级 min/max，避免被极端异常值压缩
    r_lo, r_hi = means["recency"].min(), means["recency"].max()
    f_lo, f_hi = means["frequency"].min(), means["frequency"].max()
    m_lo, m_hi = means["monetary"].min(), means["monetary"].max()

    categories = ["活跃度", "购买频次", "消费能力", "忠诚度", "价值潜力"]
    result = {}

    for cid, row in means.iterrows():
        rs = _scale(float(row["recency"]), r_lo, r_hi, invert=True)
        fs = _scale(float(row["frequency"]), f_lo, f_hi)
        ms = _scale(float(row["monetary"]), m_lo, m_hi)
        loyalty = (rs + fs) / 2
        potential = (fs + ms) / 2

        result[cid] = (
            categories,
            [round(rs, 1), round(fs, 1), round(ms, 1), round(loyalty, 1), round(potential, 1)],
            int(counts.get(cid, 0)),
        )

    return result if result else None


# ── 聚类标签（基于实际数据特征动态适配）────────────────
CLUSTER_LABEL_MAP = {
    0: "🛒 中等活跃客户",      # recency 中 / frequency 中 / monetary 中
    1: "💤 沉睡客户",          # recency 高(久未活跃) / frequency 低 / monetary 低
    2: "🔥 超级用户",          # recency 低(最近活跃) / frequency 高 / monetary 高
    3: "🌱 低价值客户",        # recency 低 / frequency 最低 / monetary 最低
}


def _cluster_display_name(cid) -> str:
    """根据 Cluster ID 返回可读名称。"""
    return CLUSTER_LABEL_MAP.get(int(cid), f"Cluster {cid}")


# ── 主页面 ────────────────────────────────────────────────
def main() -> None:
    inject_custom_css()
    init_plotly_theme()
    animation_control_panel()

    df_funnel, df_retention, df_rfm, df_clusters = load_data()
    if df_rfm.empty:
        st.error("RFM 数据未就绪。")
        st.stop()

    st.markdown(
        '<div class="big-title">💎 Customer Value & Retention (客户价值与留存)</div>',
        unsafe_allow_html=True,
    )

    # ── 用户核心 KPI ──────────────────────────────────────
    # 从 Global 维度精确取值以避免跨维度重复累加
    pv_val = 0.0
    uv_val = 0.0
    if not df_funnel.empty and "dimension" in df_funnel.columns:
        global_view = df_funnel[
            (df_funnel["dimension"] == "global") & (df_funnel["event_type"] == "view")
        ]
        if not global_view.empty:
            pv_val = float(global_view["session_count"].iloc[0])
            uv_val = float(global_view["user_count"].iloc[0])

    paying_users = float(df_rfm["user_id"].nunique()) if "user_id" in df_rfm.columns else float(len(df_rfm))
    penetration = (paying_users / uv_val * 100) if uv_val > 0 else 0.0
    avg_freq = float(df_rfm["frequency"].mean()) if "frequency" in df_rfm.columns and pd.notna(df_rfm["frequency"].mean()) else 0.0

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        animated_number(
            value=pv_val, label="总访问量 (PV)", format="integer",
            duration_ms=1000, color="#00d4ff", font_size="2.2rem",
            component_key="ui-kpi-pv", height=120,
        )
    with k2:
        animated_number(
            value=paying_users, label="付费客户数 (Paying)", format="integer",
            duration_ms=1000, delay_ms=100, color="#ff9500", font_size="2.2rem",
            component_key="ui-kpi-paying", height=120,
        )
    with k3:
        animated_number(
            value=penetration, label="付费渗透率 (UV→Pay)", format="percent",
            precision=2, suffix="%", duration_ms=1000, delay_ms=200,
            color="#a855f7", font_size="2.2rem",
            component_key="ui-kpi-rate", height=120,
        )
    with k4:
        animated_number(
            value=avg_freq, label="平均购买频次", format="float",
            precision=1, duration_ms=1000, delay_ms=300,
            color="#00ff88", font_size="2.2rem",
            component_key="ui-kpi-freq", height=120,
        )

    st.markdown("---")

    # ── RFM 分群 + K-Means 雷达 ──────────────────────────
    if "rfm_segment" in df_rfm.columns:
        seg = df_rfm["rfm_segment"].value_counts().reset_index()
        seg.columns = ["segment", "count"]
        seg_label_map = {
            "Need Attention (需要关注用户)": "需关注",
            "Potential Loyalist (潜力客户)": "潜力客户",
            "New Customers (新客户)": "新客户",
            "Loyal (一般价值客户)": "忠诚客户",
            "Champions (重要价值客户)": "重要客户",
            "At Risk (潜在流失用户)": "潜在流失",
            "Promising (成长客户)": "成长客户",
            "Hibernating (沉睡用户)": "沉睡用户",
        }
        seg["segment_short"] = seg["segment"].map(lambda s: seg_label_map.get(str(s), str(s)))

        left, right = st.columns([1, 2])

        with left:
            st.markdown('<div class="section-title">📊 用户价值分层</div>', unsafe_allow_html=True)
            fig_pie = go.Figure(
                go.Pie(
                    labels=seg["segment_short"],
                    values=seg["count"],
                    customdata=seg["segment"],
                    hole=0.6,
                    marker=dict(colors=CLUSTER_COLORS + ["#4ecdc4", "#95e1d3", "#ff9ff3", "#feca57"]),
                    textinfo="label+percent",
                    textposition="outside",
                    textfont=dict(size=11),
                    automargin=True,
                    hovertemplate=(
                        "<b>%{customdata}</b><br>"
                        "占比: %{percent}<br>"
                        "人数: %{value:,}<extra></extra>"
                    ),
                )
            )
            fig_pie.update_layout(
                height=430,
                margin=dict(l=36, r=100, t=20, b=60),
                showlegend=False,
                uniformtext_minsize=10,
                uniformtext_mode="show",
            )
            st.plotly_chart(fig_pie, width="stretch")

        with right:
            st.markdown(
                '<div class="section-title">🧠 K-Means 聚类画像</div>',
                unsafe_allow_html=True,
            )

            radar_data = _build_all_cluster_radars(df_clusters)
            if radar_data:
                cluster_ids = sorted(radar_data.keys(), key=lambda x: int(x))
                display_names = {cid: f"{_cluster_display_name(cid)} ({radar_data[cid][2]:,}人)" for cid in cluster_ids}

                selected_cid = st.selectbox(
                    "选择聚类群体查看画像",
                    options=cluster_ids,
                    format_func=lambda cid: display_names[cid],
                    key="cluster-selector",
                )

                cats, vals, cnt = radar_data[selected_cid]
                animated_radar(
                    categories=cats, values=vals, max_value=100,
                    title=f"{_cluster_display_name(selected_cid)}",
                    total_duration_ms=1400, stagger_ms=100,
                    fill_color="rgba(0, 212, 255, 0.24)", stroke_color="#00d4ff",
                    component_key=f"user-radar-c{selected_cid}", height=450,
                )

                # 显示该群体的特征摘要
                st.caption(
                    f"该群体共 **{cnt:,}** 人 | "
                    f"活跃度 {vals[0]}  购买频次 {vals[1]}  消费能力 {vals[2]}  "
                    f"忠诚度 {vals[3]}  价值潜力 {vals[4]}"
                )
            else:
                st.info("暂无可用聚类数据。")

    st.markdown("---")

    # ── 留存热力图 ────────────────────────────────────────
    st.markdown(
        '<div class="section-title">🔥 用户留存热力图</div>',
        unsafe_allow_html=True,
    )

    if not df_retention.empty:
        df_ret = df_retention.dropna(subset=["cohort_date"]).copy()
        pivot = df_ret.pivot(index="cohort_date", columns="period", values="retention_count")

        if 0 in pivot.columns:
            cohort_sizes = pivot[0]
            retention_rate = pivot.divide(cohort_sizes, axis=0)
            cols = [c for c in retention_rate.columns if 1 <= c <= 14]
            plot_df = retention_rate[cols] if cols else retention_rate

            scale_src = plot_df.stack(future_stack=True).dropna()
            hmax = float(scale_src.quantile(0.95)) if not scale_src.empty else 1.0
            hmax = max(0.15, min(1.0, hmax))

            fig_hm = px.imshow(
                plot_df,
                labels=dict(x="留存天数", y="群组日期", color="留存率"),
                color_continuous_scale=RETENTION_HEATMAP_SCALE,
                aspect="auto",
                zmin=0.0,
                zmax=hmax,
            )
            fig_hm.update_coloraxes(colorbar=dict(title="留存率", tickformat=".0%"))
            fig_hm.update_layout(height=360)
            st.plotly_chart(fig_hm, width="stretch")
        else:
            st.warning("留存数据缺少 period=0。")
    else:
        st.info("留存数据暂未加载。")

    st.markdown("---")

    # ── 明细数据查询 ──────────────────────────────────────
    st.markdown(
        '<div class="section-title">📋 用户明细数据查询</div>',
        unsafe_allow_html=True,
    )

    with st.expander("🔍 打开高级筛选"):
        uid_filter = st.text_input("搜索 User ID")
        seg_filter = st.multiselect(
            "筛选 RFM 分群",
            df_rfm["rfm_segment"].unique().tolist() if "rfm_segment" in df_rfm.columns else [],
        )

    display = df_rfm.copy()
    if uid_filter:
        display = display[display["user_id"].astype(str).str.contains(uid_filter)]
    if seg_filter:
        display = display[display["rfm_segment"].isin(seg_filter)]

    st.dataframe(
        display.sort_values("monetary", ascending=False).head(1000),
        column_config={
            "user_id": "用户 ID",
            "recency": "最近购买间隔 (天)",
            "frequency": "购买频次",
            "monetary": st.column_config.NumberColumn("消费金额", format="¥ %.2f"),
            "rfm_segment": "价值分层",
        },
        width="stretch",
        hide_index=True,
    )
    st.caption("注：仅展示前 1,000 条记录，按消费金额降序排列。")


if __name__ == "__main__":
    main()
