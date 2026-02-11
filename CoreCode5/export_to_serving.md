# export_to_serving.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **执行脚本**: [export_to_serving.py](file:///e:/a_VibeCoding/EcoPulse/CoreCode5/export_to_serving.py)
*   **输入数据**: ADS 层 Parquet 文件
    *   `data/ads/ads_funnel_stats`
    *   `data/ads/ads_user_retention`
    *   `data/ads/ads_user_rfm`
    *   `data/ads/ads_user_clusters`
*   **输出目标**: Serving 层 CSV 文件 (`data/serving/`)

### 2. 核心功能解析
该模块作为数据工程链路的**最后一公里**，承担着将分布式计算结果转换为应用层可用数据的重任。

*   **数据清洗与合并**: 
    *   利用 Spark 的 `coalesce(1)` 算子，将原本分散在多个 Partition 中的数据合并为单一文件。
    *   自动处理 Spark 输出的临时目录结构，将其重命名为标准 CSV 文件（如 `user_rfm.csv`），便于 Power BI、Tableau 或 Excel 直接读取。
*   **接口扩展性**:
    *   脚本内置了 MySQL JDBC 连接模版。未来若需上线实时大屏，只需配置数据库连接参数，即可无缝切换为数据库写入模式。

### 3. 产出清单 (Deliverables)
执行成功后，`data/serving/` 目录下将生成以下核心数据表：

| 文件名 | 描述 | 关键字段 | 下游应用 |
| :--- | :--- | :--- | :--- |
| **funnel_stats.csv** | 转化漏斗数据 | `event_type`, `session_count` | 漏斗图、转化率仪表盘 |
| **user_retention.csv** | 留存率分析 | `cohort_date`, `period`, `retention_rate` | 留存热力图 (Cohort Matrix) |
| **user_rfm.csv** | 用户价值评分 | `user_id`, `R_score`, `F_score`, `M_score` | 用户价值分布图 |
| **user_clusters.csv** | 机器学习分群 | `user_id`, `prediction` (0-3), `rfm_segment` | 用户画像散点图、营销圈人 |

### 4. 可视化对接指南
请参考配套的 [VISUALIZATION_GUIDE.md](file:///e:/a_VibeCoding/EcoPulse/olddocs/design/VISUALIZATION_GUIDE.md) 进行 BI 看板搭建。
*   **Power BI**: 直接使用“获取数据 -> 文本/CSV”连接上述文件。
*   **Tableau**: 连接文本文件，确保分隔符设置为逗号 (`,`)。

---
*记录日期: 2026/2/11*
