"""
EcoPulse · 品牌深度分析 (Brand Deep Dive)
==========================================
定位: 市场 / 品牌运营视角的竞品分析工具。

改进点 (vs VIEW 旧版):
- 品牌柱状图新增 highlight 高亮选中品牌，其余灰色
- 漏斗图流失率标注位置更精确
- 数据校验更完善，空品牌场景不崩溃
"""
from __future__ import annotations

import logging

import plotly.graph_objects as go
import streamlit as st

from src.dashboard.animations import (
    animated_bar_chart,
    animated_number,
    animation_control_panel,
)
from src.dashboard.utils import COLOR_MAP, init_plotly_theme, inject_custom_css, load_data

st.set_page_config(page_title="品牌深度分析", page_icon="📊", layout="wide")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(
    logging.ERROR
)


def main() -> None:
    inject_custom_css()
    init_plotly_theme()
    animation_control_panel()

    df_funnel, _, _, _ = load_data()
    if df_funnel.empty:
        st.error("数据未就绪，请先运行数据准备脚本。")
        st.stop()

    st.markdown(
        '<div class="big-title">🏢 品牌深度分析 (Brand Deep Dive)</div>',
        unsafe_allow_html=True,
    )

    # ── 品牌筛选 ──────────────────────────────────────────
    st.sidebar.title("🔍 品牌筛选")
    df_brand_agg = df_funnel[df_funnel["dimension"] == "brand"].copy()
    if df_brand_agg.empty:
        st.warning("funnel_stats.csv 中无 brand 维度数据。")
        st.stop()

    top_brands = (
        df_brand_agg[df_brand_agg["event_type"] == "purchase"]
        .groupby("brand")["session_count"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )
    if not top_brands:
        st.warning("无品牌购买数据。")
        st.stop()

    selected = st.sidebar.selectbox("选择要分析的品牌", top_brands)
    if not selected:
        st.info("请在左侧选择一个品牌。")
        st.stop()

    # ── 品牌核心 KPI ──────────────────────────────────────
    brand_data = df_brand_agg[df_brand_agg["brand"] == selected]
    b_pv = int(brand_data.loc[brand_data["event_type"] == "view", "session_count"].sum())
    b_cart = int(brand_data.loc[brand_data["event_type"] == "cart", "session_count"].sum())
    b_orders = int(brand_data.loc[brand_data["event_type"] == "purchase", "session_count"].sum())
    b_rate = (b_orders / b_pv * 100) if b_pv else 0.0

    # 大盘均值
    all_pv = df_brand_agg.loc[df_brand_agg["event_type"] == "view", "session_count"].sum()
    all_orders = df_brand_agg.loc[df_brand_agg["event_type"] == "purchase", "session_count"].sum()
    avg_rate = (all_orders / all_pv * 100) if all_pv else 0.0

    st.markdown(
        '<div class="section-title">🎯 品牌核心指标</div>', unsafe_allow_html=True
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        animated_number(
            value=float(b_pv),
            label=f"{selected} 访问量",
            format="integer",
            duration_ms=1200,
            color="#00d4ff",
            font_size="2.4rem",
            component_key=f"brand-pv-{selected}",
            height=130,
        )
    with c2:
        animated_number(
            value=float(b_orders),
            label=f"{selected} 销量",
            format="integer",
            duration_ms=1200,
            delay_ms=100,
            color="#ff9500",
            font_size="2.4rem",
            component_key=f"brand-orders-{selected}",
            height=130,
        )
    with c3:
        animated_number(
            value=b_rate,
            label="转化率",
            format="percent",
            precision=2,
            suffix="%",
            duration_ms=1200,
            delay_ms=200,
            color="#a855f7",
            font_size="2.4rem",
            component_key=f"brand-rate-{selected}",
            height=130,
        )

    # 趋势箭头
    diff = b_rate - avg_rate
    sym = "↑" if diff >= 0 else "↓"
    clr = "#00ff88" if diff >= 0 else "#ff6b6b"
    st.markdown(
        f'<p style="color:{clr};font-weight:700;text-align:center;">'
        f'{sym} {abs(diff):.2f}% vs 大盘均值 ({avg_rate:.2f}%)</p>',
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # ── 漏斗 + 竞品对比 ──────────────────────────────────
    left, right = st.columns([1, 1])

    with left:
        st.markdown(
            f'<div class="section-title">🔻 {selected} 转化漏斗</div>',
            unsafe_allow_html=True,
        )

        funnel_rows = brand_data.groupby("event_type", as_index=False)["session_count"].sum()
        sorter = {"view": 1, "cart": 2, "purchase": 3}
        funnel_rows["sort_id"] = funnel_rows["event_type"].map(sorter).fillna(99).astype(int)
        funnel_rows = funnel_rows.sort_values("sort_id")

        # 流失率标注
        annotations = []
        if len(funnel_rows) >= 3:
            v = funnel_rows.iloc[0]["session_count"]
            c = funnel_rows.iloc[1]["session_count"]
            p = funnel_rows.iloc[2]["session_count"]

            if v > 0:
                annotations.append(
                    dict(
                        x=0.5, y=0.85,
                        xref="paper", yref="paper",
                        text=f"流失 {(v - c) / v:.1%}",
                        showarrow=False,
                        font=dict(color="#ff6b6b", size=14),
                    )
                )
            if c > 0:
                annotations.append(
                    dict(
                        x=0.5, y=0.45,
                        xref="paper", yref="paper",
                        text=f"转化 {p / c:.1%}",
                        showarrow=False,
                        font=dict(color="#00ff88", size=14),
                    )
                )

        fig_funnel = go.Figure(
            go.Funnel(
                y=funnel_rows["event_type"],
                x=funnel_rows["session_count"],
                textinfo="value+percent initial",
                marker={"color": [COLOR_MAP.get(t, "#888") for t in funnel_rows["event_type"]]},
            )
        )
        fig_funnel.update_layout(
            height=400,
            margin=dict(l=20, r=80, t=30, b=20),
            annotations=annotations,
        )
        st.plotly_chart(fig_funnel, width="stretch")

    with right:
        st.markdown(
            f'<div class="section-title">📊 {selected} vs Top 品牌销量</div>',
            unsafe_allow_html=True,
        )

        purchase_rank = (
            df_brand_agg[df_brand_agg["event_type"] == "purchase"]
            .groupby("brand")["session_count"]
            .sum()
            .sort_values(ascending=False)
        )

        # 确保选中品牌在列表中
        top_names = purchase_rank.head(6).index.tolist()
        if selected not in top_names:
            top_names = [selected] + [b for b in top_names if b != selected][:5]

        compare = purchase_rank.reindex(top_names).fillna(0).sort_values(ascending=False)
        cat_list = compare.index.tolist()
        val_list = [float(v) for v in compare.values.tolist()]

        # 高亮选中品牌
        hi_idx = cat_list.index(selected) if selected in cat_list else -1

        animated_bar_chart(
            categories=cat_list,
            values=val_list,
            title="销量对比（柱状生长 + 品牌高亮）",
            duration_ms=900,
            stagger_ms=90,
            bounce=True,
            bar_color="#00ff88",
            highlight_index=hi_idx,
            highlight_color="#ff9500",
            show_values=True,
            component_key=f"brand-compare-{selected}",
            height=420,
        )


if __name__ == "__main__":
    main()
