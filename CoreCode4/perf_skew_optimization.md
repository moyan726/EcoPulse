# perf_skew_optimization.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **输入数据**: `data/dwd/user_behavior`
*   **执行日志**: `logs/perf_skew_optimization_20260211.log`
*   **优化技术**: Two-Stage Aggregation with Salting (两阶段加盐聚合)

### 2. 实验结果 (Benchmark Results)
| 方案 | 耗时 (Duration) | 性能提升 | 适用场景 |
| :--- | :--- | :--- | :--- |
| **A. 普通聚合** | 4.45s | - | 数据分布均匀 |
| **B. 加盐聚合** | **3.73s** | **+16.3%** | 存在极端热点 Key (如 Top 1 品牌占 50% 数据) |

### 3. 技术原理解析
1.  **Phase 1 (Partial Aggregation)**:
    *   给原始 Key 加上 `0-9` 的随机后缀（Salt），将原本集中在一个 Task 的热点数据打散到 10 个 Task 中并行处理。
    *   `key` -> `key_0`, `key_1`, ..., `key_9`
2.  **Phase 2 (Global Aggregation)**:
    *   去除后缀，将分散的中间结果进行最终汇总。
    *   由于 Phase 1 已经大幅减少了数据行数，Phase 2 的计算量极小，从而消除了 Shuffle 阶段的长尾等待。

---
*记录日期: 2026/2/11*
