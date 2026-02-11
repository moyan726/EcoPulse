# Power BI 可视化配置手册

本手册指导如何利用 `data/serving/` 下的 CSV 数据在 Power BI 中构建专业级电商分析看板。

---

## 1. 数据导入
1. 打开 Power BI Desktop。
2. 点击 **获取数据** -> **文本/CSV**。
3. 分别导入 `funnel_stats`, `user_retention`, `user_rfm`, `user_clusters`。
4. 在“转换数据”中，确保日期字段（如 `dt`, `cohort_date`）被识别为日期类型。

---

## 2. 核心 DAX 度量值 (Measures)

在 Power BI 中点击“新建度量值”，复制以下公式：

### 2.1 漏斗转化率
```dax
加购率 = 
DIVIDE(
    CALCULATE(SUM('funnel_stats'[session_count]), 'funnel_stats'[event_type] = "cart"),
    CALCULATE(SUM('funnel_stats'[session_count]), 'funnel_stats'[event_type] = "view")
)

购买转化率 = 
DIVIDE(
    CALCULATE(SUM('funnel_stats'[session_count]), 'funnel_stats'[event_type] = "purchase"),
    CALCULATE(SUM('funnel_stats'[session_count]), 'funnel_stats'[event_type] = "view")
)
```

### 2.2 留存率
```dax
留存率 = DIVIDE(SUM('user_retention'[retention_count]), SUM('user_retention'[cohort_size]))
```

---

## 3. 看板布局建议

### 页面 1：核心指标总览 (Executive Summary)
- **切片器**：日期 (`dt`)、品牌 (`brand`)。
- **卡片图**：总成交额 (SUM `monetary`)、总用户数 (DistinctCount `user_id`)。
- **折线图**：每日 `view` / `purchase` 趋势。
- **环形图**：各品牌销售额占比。

### 页面 2：转化漏斗分析 (Funnel Analysis)
- **漏斗图 (Funnel Chart)**：
    - 组：`event_type`
    - 值：`session_count`
- **簇状柱形图**：不同类目 (`category_code`) 的转化率对比。

### 页面 3：留存矩阵 (Retention Matrix)
- **矩阵图 (Matrix)**：
    - 行：`cohort_date` (按周或月分组)
    - 列：`period` (第 0, 1, 2... 天/周)
    - 值：`留存率` (设置条件格式为热力图颜色)

### 页面 4：用户价值分层 (RFM & Clusters)
- **散点图**：X轴 `Frequency`, Y轴 `Monetary`，颜色 `rfm_segment`。
- **树状图 (Treemap)**：展示各聚类 (`prediction`) 的用户规模占比。

---

## 4. 样式建议
- **配色**：建议使用 Power BI 自带的 "Executive" 或 "Innovate" 主题。
- **交互**：确保点击一个图表（如点击某个品牌）时，其他图表能联动过滤。
