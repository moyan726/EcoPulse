"""
EcoPulse · 用户画像洞察 (User Insights)
========================================
定位: CRM / 数据分析师视角的深度用户挖掘。

组件:
1. 用户核心 KPI (动画数值)
2. RFM 价值分层 (环形图)
3. K-Means 聚类画像 (动画雷达图)
4. 留存热力图
5. 明细数据查询 (Data Grid)

改进点 (vs VIEW 旧版):
- 留存热力图去掉 Day0 避免色阶被 100% 拉满
- 雷达图聚类评分逻辑独立函数，可读性更高
- 表格显示优化，增加排序说明
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

st.set_page_config(page_title="用户画像洞察", page_icon="�", layout="wide")
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


def _best_cluster_radar(df: pd.DataFrame):
    """从聚类结果中选出最佳群体，返回雷达图参数 (cluster_id, categories, values)。"""
    required = {"prediction", "recency", "frequency", "monetary"}
    if df.empty or not required.issubset(df.columns):
        return None

    means = df.groupby("prediction")[["recency", "frequency", "monetary"]].mean()
    if means.empty:
        return None

    r_lo, r_hi = means["recency"].min(), means["recency"].max()
    f_lo, f_hi = means["frequency"].min(), means["frequency"].max()
    m_lo, m_hi = means["monetary"].min(), means["monetary"].max()

    best_id, best_score = None, -1.0
    for cid, row in means.iterrows():
        rs = _scale(float(row["recency"]), r_lo, r_hi, invert=True)
        fs = _scale(float(row["frequency"]), f_lo, f_hi)
        ms = _scale(float(row["monetary"]), m_lo, m_hi)
        score = rs * 0.30 + fs * 0.35 + ms * 0.35
        if score > best_score:
            best_score, best_id = score, cid

    if best_id is None:
        return None

    row = means.loc[best_id]
    rs = _scale(float(row["recency"]), r_lo, r_hi, invert=True)
    fs = _scale(float(row["frequency"]), f_lo, f_hi)
    ms = _scale(float(row["monetary"]), m_lo, m_hi)
    loyalty = (rs + fs) / 2
    potential = (fs + ms) / 2

    return (
        str(best_id),
        ["活跃度", "购买频次", "消费能力", "忠诚度", "价值潜力"],
        [round(rs, 1), round(fs, 1), round(ms, 1), round(loyalty, 1), round(potential, 1)],
    )


# ── 主页面 ────────────────────────────────────────────────
def main() -> None:
    inject_custom_css()
    init_plotly_theme()
    animation_control_panel()

    _, df_retention, df_rfm, df_clusters = load_data()
    if df_rfm.empty:
        st.error("RFM 数据未就绪。")
        st.stop()

    st.markdown(
        '<div class="big-title">👥 用户画像洞察 (User Insights)</div>',
        unsafe_allow_html=True,
    )

    # ── 用户核心 KPI ──────────────────────────────────────
    total_users = float(df_rfm["user_id"].nunique()) if "user_id" in df_rfm.columns else float(len(df_rfm))
    avg_freq = float(df_rfm["frequency"].mean()) if "frequency" in df_rfm.columns and pd.notna(df_rfm["frequency"].mean()) else 0.0
    repeat_rate = float((df_rfm["frequency"] > 1).mean() * 100) if "frequency" in df_rfm.columns else 0.0

    k1, k2, k3 = st.columns(3)
    with k1:
        animated_number(
            value=total_users, label="活跃用户数", format="integer",
            duration_ms=1200, color="#00d4ff", font_size="2.4rem",
            component_key="ui-kpi-users", height=125,
        )
    with k2:
        animated_number(
            value=avg_freq, label="平均购买频次", format="float", precision=1,
            duration_ms=1200, delay_ms=120, color="#00ff88", font_size="2.4rem",
            component_key="ui-kpi-freq", height=125,
        )
    with k3:
        animated_number(
            value=repeat_rate, label="复购用户占比", format="percent",
            precision=1, suffix="%", duration_ms=1200, delay_ms=240,
            color="#a855f7", font_size="2.4rem",
            component_key="ui-kpi-repeat", height=125,
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
                '<div class="section-title">🧠 K-Means 聚类画像 (动画雷达图)</div>',
                unsafe_allow_html=True,
            )
            payload = _best_cluster_radar(df_clusters)
            if payload:
                cid, cats, vals = payload
                animated_radar(
                    categories=cats, values=vals, max_value=100,
                    title=f"高价值群体画像 (Cluster {cid})",
                    total_duration_ms=1400, stagger_ms=100,
                    fill_color="rgba(0, 212, 255, 0.24)", stroke_color="#00d4ff",
                    component_key="user-radar-main", height=470,
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
            # 只展示 Day1-14，去掉 Day0 的 100% 以避免色阶失真
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
