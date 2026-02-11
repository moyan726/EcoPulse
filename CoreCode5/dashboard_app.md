# dashboard_app.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **应用脚本**: [dashboard_app.py](file:///e:/a_VibeCoding/EcoPulse/CoreCode5/dashboard_app.py)
*   **技术栈**: Streamlit (Web框架) + Plotly (交互式图表) + Pandas (数据处理)
*   **数据源**: `data/serving/*.csv` (由 `export_to_serving.py` 产出)

### 2. 看板架构与功能 (Dashboard Architecture)

#### 2.1 核心指标总览 (Overview Tab)
*   **KPI 卡片**: 实时展示 PV (浏览量), UV (访客数), 订单量, 整体转化率。
*   **趋势监控**: 每日各行为类型 (`view`/`cart`/`purchase`) 的流量波动折线图，支持缩放查看特定时间段。

#### 2.2 转化漏斗分析 (Funnel Tab)
*   **全站漏斗**: 经典的倒三角漏斗图，直观展示 `View -> Cart -> Purchase` 的各环节流失率。
*   **品牌对比**: 交互式柱状图，支持通过侧边栏筛选特定品牌，横向对比不同品牌的转化效率。

#### 2.3 用户画像洞察 (User Profile Tab)
*   **RFM 分层**: 饼图展示“高价值用户”、“流失用户”等各分层的占比，辅助精准营销。
*   **留存热力图**: 颜色深浅直观反映用户留存衰减情况 (Cohort Analysis)，无需人工计算。
*   **3D 聚类视图**: 利用 K-Means 结果绘制 R/F/M 三维散点图，可视化展示用户群体的空间分布差异。

### 3. 部署与交付
*   **轻量化**: 无需部署复杂的 BI Server，仅需 Python 环境即可运行。
*   **交互性**: 侧边栏提供日期范围和品牌筛选，图表支持悬停提示 (Tooltip) 和局部放大。
*   **纯代码交付**: 整个看板逻辑封装在一个 `.py` 文件中，版本控制友好，易于迁移和复用。

---
*记录日期: 2026/2/11*
