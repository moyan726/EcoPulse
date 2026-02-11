# analysis_rfm.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **输入数据**: `data/dwd/user_behavior` (DWD 明细层)
*   **执行日志**: `logs/analysis_rfm_20260211.log`
*   **分析口径**: 基于 Recency (最近购买), Frequency (频次), Monetary (金额) 的用户分层模型。

### 2. 产出结果 (Outputs)
*   **输出目录**: [ads_user_rfm](file:///e:/a_VibeCoding/EcoPulse/data/ads/ads_user_rfm)
*   **存储格式**: Parquet (Snappy 压缩)
*   **数据粒度**: 用户级 (One row per user_id)

### 3. RFM 模型运行深度解析
1.  **数据分布特征**:
    *   **分析基准日**: 2019-10-31
    *   **分桶降级**: 由于大部分用户在单月内仅购买 1 次（Frequency 分布极度长尾），导致 `QuantileDiscretizer` 无法将频次分为 5 组，自动降级为 3 组。这是低频电商数据的典型特征。
2.  **用户分层统计**:
    *   **普通客户 (General Users)**: 302,643 人 (占比 ~87%)。
    *   **新客户 (New Users)**: 44,475 人 (占比 ~13%)。
    *   *注*: 由于频次打分偏低，导致符合“核心客户(R>3, F>3, M>3)”条件的用户极少（在此样本集中为 0）。
3.  **业务启示**:
    *   平台用户以低频消费为主，**拉新**和**首单转化**是核心增长动力。
    *   针对 4.4 万名“新客户”，建议设计“第二单半价”或“满减券”活动以刺激复购。

---
*记录日期: 2026/2/11*
