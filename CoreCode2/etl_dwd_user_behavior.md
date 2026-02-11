# etl_dwd_user_behavior.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **输入数据**: [sample_oct_2019](file:///e:/a_VibeCoding/EcoPulse/data/dwd/sample_oct_2019) (第一阶段产出的 Parquet 样本)
*   **执行日志**: `logs/etl_dwd_20260211.log`
*   **逻辑口径**: DWD (Data Warehouse Detail) 明细层清洗，包含去重、类型转换及异常值标记。

### 2. 产出结果 (Outputs)
*   **输出目录**: [user_behavior](file:///e:/a_VibeCoding/EcoPulse/data/dwd/user_behavior)
*   **存储格式**: Parquet (Snappy 压缩)
*   **分区方式**: 按日期 (`dt`) 进行目录分区存储。

### 3. ETL 清洗逻辑与结果分析
1.  **字段规范化**:
    *   将原始的 `event_time` 字符串转换为标准的 `event_timestamp`。
    *   衍生出 `dt` (日期) 字段，用于支撑数仓的分区查询。
2.  **数据去重 (Deduplication)**:
    *   **处理前**: 约 42,448,764 条。
    *   **处理后**: 42,418,544 条。
    *   *结论*: 成功识别并剔除了约 **3 万条** 重复记录，提升了数据的唯一性。
3.  **异常值治理**:
    *   **标记逻辑**: 根据第一阶段审计结果，将价格 ≤ 0 的记录标记为 `price_is_illegal = 1`。
    *   **统计结果**: 标记了 **68,670** 条非法价格记录。
    *   *意义*: 这种“不删除但标记”的策略符合数仓治理规范，既保留了数据的完整性，又为后续特征工程提供了过滤依据。

---
*记录日期: 2026/2/11*
