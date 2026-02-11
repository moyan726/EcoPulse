# data_quality.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **分析依据**: [data_quality_report.json](file:///e:/a_VibeCoding/EcoPulse/outputs/data_quality/data_quality_report.json)
*   **统计明细**: `skew_top_brands.csv`, `skew_top_categories.csv`
*   **执行日志**: `logs/data_quality_oct_2019_20260211.log`

### 2. 产出结果 (Outputs)
*   **结果目录**: [outputs/data_quality/](file:///e:/a_VibeCoding/EcoPulse/outputs/data_quality/)
*   **关键产出**:
    *   `price_outliers_summary.csv`: 价格异常值详细审计。
    *   `skew_top_brands.csv`: 品牌数据倾斜分析。

### 3. 运行结果深度分析
1.  **价格异常审计**:
    *   **price = 0**: 发现 **68,673** 条记录（占比约 0.16%）。
    *   **price < 0**: 0 条。
    *   *结论*: 价格异常主要集中在 0 元数据，需判定为赠品或噪点并进行过滤。
2.  **品牌倾斜 (Skewness)**:
    *   **NaN (品牌缺失)**: 占比 **14.4%** (约 611 万条)。
    *   **Top 品牌**: Samsung (12.4%), Apple (9.7%), Xiaomi (7.2%)。
    *   *结论*: 前三大品牌占据了约 30% 的流量，品牌缺失值较高，后续推荐逻辑需考虑对 NaN 的处理。
3.  **类目倾斜**:
    *   **NaN (类目缺失)**: 占比高达 **31.8%** (约 1351 万条)。
    *   **热门类目**: `electronics.smartphone` 占比 27.1%。
    *   *结论*: 类目缺失情况非常严重，对类目相关的特征工程构成了挑战。

---
*记录日期: 2026/2/11*
