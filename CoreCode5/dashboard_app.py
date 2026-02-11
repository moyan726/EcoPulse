"""
模块名称：Python 可视化看板 (Streamlit Dashboard)
作者：moyan726
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    读取 data/serving/ 下的 CSV 数据，展示交互式分析看板。
    包含：总览、漏斗分析、用户画像三个核心页面。

运行方式：
    streamlit run CoreCode5/dashboard_app.py
"""

import os
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# 设置页面配置
st.set_page_config(
    page_title="电商行为分析看板",
    page_icon="📊",
    layout="wide"
)

# 1. 数据加载函数 (带缓存)
@st.cache_data
def load_data():
    # 动态获取项目根目录
    try:
        # 当通过 streamlit run 运行时，__file__ 可能不准确，使用 os.getcwd() 辅助判断
        # 假设从项目根目录运行
        base_path = "data/serving"
        if not os.path.exists(base_path):
            # 尝试向上寻找
            base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "serving")
    except:
        base_path = "data/serving"

    def read_csv_safe(name):
        file_path = os.path.join(base_path, f"{name}.csv")
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        return pd.DataFrame()

    df_funnel = read_csv_safe("funnel_stats")
    df_retention = read_csv_safe("user_retention")
    df_rfm = read_csv_safe("user_rfm")
    df_clusters = read_csv_safe("user_clusters")

    # 类型转换
    if not df_funnel.empty and 'dt' in df_funnel.columns:
        df_funnel['dt'] = pd.to_datetime(df_funnel['dt'])
    
    return df_funnel, df_retention, df_rfm, df_clusters

# 加载数据
try:
    df_funnel, df_retention, df_rfm, df_clusters = load_data()
except Exception as e:
    st.error(f"数据加载失败: {e}")
    st.stop()

# 2. 侧边栏 (Sidebar)
st.sidebar.title("🔍 筛选控制台")

# 日期筛选
if not df_funnel.empty:
    min_date = df_funnel['dt'].min().date()
    max_date = df_funnel['dt'].max().date()
    date_range = st.sidebar.date_input("选择日期范围", [min_date, max_date], min_value=min_date, max_value=max_date)
else:
    st.sidebar.warning("漏斗数据为空")

# 品牌筛选
if not df_funnel.empty:
    all_brands = df_funnel[df_funnel['dimension'] == 'brand']['brand'].dropna().unique()
    selected_brands = st.sidebar.multiselect("选择品牌 (用于漏斗对比)", all_brands, default=all_brands[:5] if len(all_brands) > 0 else None)
else:
    selected_brands = []

# 3. 页面内容 (Tabs)
tab1, tab2, tab3 = st.tabs(["📈 核心指标总览", "📉 转化漏斗分析", "👥 用户画像洞察"])

# --- Tab 1: 总览 ---
with tab1:
    st.header("核心指标总览 (Overview)")
    
    if not df_funnel.empty:
        # 计算 KPI (基于 Global 维度)
        df_global = df_funnel[df_funnel['dimension'] == 'global']
        total_pv = df_global[df_global['event_type'] == 'view']['session_count'].sum()
        total_uv = df_global[df_global['event_type'] == 'view']['user_count'].sum()
        total_orders = df_global[df_global['event_type'] == 'purchase']['session_count'].sum()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("总访问量 (PV)", f"{total_pv:,}")
        col2.metric("总用户数 (UV)", f"{total_uv:,}")
        col3.metric("总订单数", f"{total_orders:,}")
        col4.metric("整体转化率", f"{total_orders / total_pv * 100:.2f}%" if total_pv > 0 else "0%")

        # 每日趋势图
        st.subheader("📅 每日流量与订单趋势")
        df_daily = df_funnel[df_funnel['dimension'] == 'daily'].sort_values('dt')
        
        fig_trend = px.line(df_daily, x='dt', y='session_count', color='event_type', 
                            title="每日各行为 Session 数趋势", markers=True)
        st.plotly_chart(fig_trend, use_container_width=True)

# --- Tab 2: 漏斗分析 ---
with tab2:
    st.header("转化漏斗分析 (Funnel Analysis)")
    
    col_funnel_1, col_funnel_2 = st.columns(2)
    
    with col_funnel_1:
        st.subheader("全站转化漏斗")
        if not df_funnel.empty:
            df_global = df_funnel[df_funnel['dimension'] == 'global']
            # 确保顺序
            sorter = {'view': 1, 'cart': 2, 'purchase': 3}
            df_global['sort_id'] = df_global['event_type'].map(sorter)
            df_global = df_global.sort_values('sort_id')
            
            fig_funnel = go.Figure(go.Funnel(
                y = df_global['event_type'],
                x = df_global['session_count'],
                textposition = "inside",
                textinfo = "value+percent initial",
                opacity = 0.65, marker = {"color": ["deepskyblue", "lightsalmon", "teal"]}
            ))
            st.plotly_chart(fig_funnel, use_container_width=True)
    
    with col_funnel_2:
        st.subheader("品牌转化率对比")
        if selected_brands is not None and len(selected_brands) > 0:
            df_brand = df_funnel[(df_funnel['dimension'] == 'brand') & (df_funnel['brand'].isin(selected_brands))]
            # 计算转化率 (Purchase / View) - 简化逻辑：这里展示 Purchase 绝对值对比
            # 实际计算转化率需要 Pivot，这里为演示直接展示各环节
            fig_brand = px.bar(df_brand, x='brand', y='session_count', color='event_type', 
                               title="各品牌行为分布", barmode='group')
            st.plotly_chart(fig_brand, use_container_width=True)
        else:
            st.info("请在左侧侧边栏选择品牌以查看对比。")

# --- Tab 3: 用户画像 ---
with tab3:
    st.header("用户画像洞察 (User Profile)")
    
    col_user_1, col_user_2 = st.columns(2)
    
    with col_user_1:
        st.subheader("👥 RFM 用户分层占比")
        if not df_rfm.empty:
            df_seg = df_rfm['rfm_segment'].value_counts().reset_index()
            df_seg.columns = ['segment', 'count']
            fig_pie = px.pie(df_seg, values='count', names='segment', title="用户价值分层占比")
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.warning("RFM 数据未加载")

    with col_user_2:
        st.subheader("🔥 留存热力图 (Cohort Analysis)")
        if not df_retention.empty:
            # Pivot 为矩阵形式
            # 确保 cohort_date 是字符串或日期类型以便展示
            df_ret_pivot = df_retention.pivot(index='cohort_date', columns='period', values='retention_count')
            # 计算留存率
            # 注意：实际数据中 period=0 的 count 即为 cohort_size
            cohort_sizes = df_ret_pivot[0]
            retention_rate = df_ret_pivot.divide(cohort_sizes, axis=0)
            
            fig_heatmap = px.imshow(retention_rate, 
                                    labels=dict(x="Period (Days)", y="Cohort Date", color="Retention Rate"), 
                                    x=retention_rate.columns, 
                                    y=retention_rate.index, 
                                    color_continuous_scale="Blues", text_auto=".1%")
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.warning("留存数据未加载")
    
    st.subheader("🧠 K-Means 聚类结果")
    if not df_clusters.empty:
        # 散点图展示聚类
        fig_cluster = px.scatter_3d(df_clusters.sample(min(1000, len(df_clusters))), 
                                    x='recency', y='frequency', z='monetary', 
                                    color='prediction', title="用户聚类 3D 视图 (抽样 1000 人)")
        st.plotly_chart(fig_cluster, use_container_width=True)
