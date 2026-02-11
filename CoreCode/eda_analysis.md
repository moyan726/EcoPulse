# eda_analysis.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **分析依据**: 终端输出 (Terminal #4-74)
*   **原始快照**: [summary.json](file:///e:/a_VibeCoding/EcoPulse/outputs/eda/summary.json)
*   **统计明细**: [event_type_counts.csv](file:///e:/a_VibeCoding/EcoPulse/outputs/eda/event_type_counts.csv)

### 2. 产出结果 (Outputs)
*   **结果目录**: [outputs/eda/](file:///e:/a_VibeCoding/EcoPulse/outputs/eda/)
*   **关键产出**:
    *   `price_hist.png`: 价格分布直方图。
    *   `missing_rates.csv`: 各字段数据缺失率统计。
    *   `session_event_count_stats.json`: 会话活跃度分布。

### 3. 运行结果深度分析
1.  **数据规模概览**:
    *   **总行数**: 42,448,764 条。
    *   **UV (活跃用户)**: 约 330.8 万。
    *   **Sessions (会话数)**: 约 885.7 万。
    *   *解读*: 用户活跃度高，单月样本足以支撑深度行为预测。
2.  **业务行为分布**:
    *   **view (浏览)**: 96%
    *   **cart (加购)**: 2.2%
    *   **purchase (购买)**: 1.7%
    *   *转化率*: 从浏览到购买的转化率约为 1.8%，呈现明显的漏斗结构，且购买样本相对稀疏。
3.  **价格区间特征**:
    *   **中位数 (P50)**: $161.83。
    *   **极高价 (P99.9)**: $2574.07。
    *   **预警**: 存在价格为 0 的异常记录，需在清洗阶段剔除。
4.  **性能表现**:
    *   得益于 Parquet 格式，4200 万行数据的多维统计在 45 秒内完成，性能极佳。

---
*记录日期: 2026/2/11*
