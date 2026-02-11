# benchmark_storage.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **输入数据**: 
    *   CSV: `data/raw/2019-Oct.csv` (原始行存储)
    *   Parquet: `data/dwd/user_behavior` (DWD 分区列存储)
*   **执行日志**: `logs/benchmark_20260211.log`
*   **测试场景**: 对比全表扫描、列聚合、分区裁剪三种场景下的查询耗时。

### 2. 产出结果 (Outputs)
*   **结果目录**: [outputs/benchmark](file:///e:/a_VibeCoding/EcoPulse/outputs/benchmark)
*   **关键产出**:
    *   `storage_performance.csv`: 详细的性能对比数据表。
    *   `benchmark_summary.json`: 测试结果摘要。

### 3. 性能对比深度解析 (Performance Insights)

| 测试场景 | CSV 耗时 (秒) | Parquet 耗时 (秒) | 性能提升倍数 | 原理解析 |
| :--- | :--- | :--- | :--- | :--- |
| **全表计数 (Count)** | 2.66s | 2.50s | ~1.06x | 首次读取差异不明显，但 Parquet 元数据包含 Count 信息，在大规模下优势会扩大。 |
| **单列聚合 (Sum)** | 4.04s | **1.21s** | **3.3x** | **列式存储优势**：Spark 仅需读取 `price` 列，无需加载其他无关字段，IO 开销大幅降低。 |
| **条件过滤 (Filter)** | 3.92s | **0.22s** | **17.8x** | **分区裁剪 (Partition Pruning)**：Spark 直接定位到 `dt=2019-10-01` 目录，跳过 97% 的无关文件扫描。 |

### 4. 结论
采用 **Snappy 压缩的 Parquet 分区存储** 方案，在特定查询场景下（如按日期筛选）能带来 **10倍以上** 的性能提升。这有力证明了第二阶段 ETL 架构升级的必要性。

---
*记录日期: 2026/2/11*
