# Python 可视化看板实施方案 (Streamlit)

本方案旨在替代 Power BI，使用纯 Python 技术栈 (`Streamlit` + `Plotly`) 构建交互式数据分析看板。

---

## 1. 系统架构

```mermaid
graph LR
    CSV[Serving Layer (CSV)] --> Pandas[Pandas DataFrame]
    Pandas --> Streamlit[Streamlit App]
    Streamlit --> Plotly[Plotly Charts]
    User[用户浏览器] --> Streamlit
```

## 2. 页面布局设计

### 2.1 侧边栏 (Sidebar)
- **标题**：电商行为分析看板
- **全局筛选**：
    - 日期范围选择器 (Date Range Picker)
    - 品牌多选框 (Multiselect)

### 2.2 核心页面 (Tabs)

#### Tab 1: 核心指标 (Overview)
- **KPI 卡片**：总访问量 (PV)、总用户数 (UV)、总成交额 (GMV)、整体转化率。
- **趋势图**：
    - 双轴折线图：左轴显示 PV/UV，右轴显示 GMV。
    - 维度：按 `dt` (日期) 聚合。

#### Tab 2: 漏斗分析 (Funnel)
- **漏斗图**：展示 `view` -> `cart` -> `purchase` 的留存人数与转化率。
- **对比分析**：
    - 不同品牌/类目的转化率对比条形图。

#### Tab 3: 用户画像 (User Profile)
- **RFM 分布**：
    - 3D 散点图 (R, F, M) 或 2D 散点图 (F vs M, 颜色=R)。
    - 用户分群 (Segment) 占比饼图。
- **留存热力图**：
    - X轴：Period (天数)
    - Y轴：Cohort Date
    - 颜色：留存率

## 3. 依赖库要求
- `streamlit`: Web 应用框架
- `pandas`: 数据处理
- `plotly`: 交互式绘图
- `plotly-express`: 简化绘图接口

## 4. 数据源映射
| 页面组件 | 数据源文件 | 关键字段 |
| :--- | :--- | :--- |
| KPI 卡片 | `funnel_stats` | `user_count`, `session_count` |
| 每日趋势 | `funnel_stats` (dimension='daily') | `dt`, `event_type`, `session_count` |
| 漏斗图 | `funnel_stats` (dimension='global') | `event_type`, `session_count` |
| 留存热力图 | `user_retention` | `cohort_date`, `period`, `retention_count` |
| RFM 分布 | `user_rfm` | `recency`, `frequency`, `monetary`, `rfm_segment` |
| 聚类结果 | `user_clusters` | `prediction` |

---

## 5. 运行方式
```bash
pip install streamlit plotly pandas
streamlit run dashboard_app.py
```
