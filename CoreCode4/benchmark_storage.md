# benchmark_storage.py (Phase 4) 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **测试脚本**: [benchmark_storage.py](file:///e:/a_VibeCoding/EcoPulse/CoreCode4/benchmark_storage.py)
*   **执行时间**: 2026-02-11 20:04
*   **对比对象**: 
    *   **CSV**: `data/row/2019-Oct.csv` (原始行式存储)
    *   **Parquet**: `data/dwd/user_behavior` (DWD层列式存储，按 `dt` 分区)

### 2. 核心性能指标 (Key Performance Indicators)

| 测试场景 (Scenario) | CSV 耗时 (s) | Parquet 耗时 (s) | 性能提升 (Speedup) | 技术原理 |
| :--- | :--- | :--- | :--- | :--- |
| **全表扫描 (Count)** | 2.78 | **2.11** | **1.3x** | 虽数据量相近，Parquet 元数据统计加速了扫描。 |
| **单列聚合 (Sum Price)** | 4.40 | **1.22** | **3.6x** | **列式存储 (Columnar Storage)**：只读取需要的 `price` 列，避免IO浪费。 |
| **条件过滤 (dt='2019-10-01')** | 4.21 | **0.24** | **17.5x** | **分区裁剪 (Partition Pruning)**：直接跳过无关文件，仅读取目标分区。 |

### 3. 数据一致性与治理成果
*   **数据量对比**:
    *   CSV (Raw): 42,448,764 行
    *   Parquet (Cleaned): 42,418,544 行
    *   **差异**: 减少了约 3 万行，这与 ETL 阶段记录的“剔除重复记录”和“异常治理”吻合。
*   **结论**: 
    *   DWD 层在保持数据完整性的同时，通过格式转换和分区设计，实现了**数量级**的查询性能飞跃。
    *   对于后续的机器学习任务（如 K-Means），直接读取 Parquet 将节省大量特征工程时间。

---
*记录日期: 2026/2/11*
