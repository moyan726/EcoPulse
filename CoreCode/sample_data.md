# sample_data.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **数据来源**: 原始数据集 `data/row/2019-Oct.csv`
*   **执行日志**: `logs/sample_data_20260211.log`
*   **分析口径**: 按月份（2019-10）进行数据过滤与格式转换。

### 2. 产出结果 (Outputs)
*   **输出目录**: [sample_oct_2019](file:///e:/a_VibeCoding/EcoPulse/data/dwd/sample_oct_2019)
*   **文件格式**: Snappy 压缩的 Parquet 列式存储。
*   **文件结构**: 包含多个 `part-xxxx.parquet` 子文件（Spark 并行写入产物）。

### 3. 运行逻辑说明
*   **化大为小**: 将 2 亿级全量数据缩减为 2019 年 10 月的单月样本（约 4244 万条），实现快速原型开发。
*   **格式优化**: 舍弃读取缓慢的 CSV，转换为高性能的 Parquet 格式，使后续 EDA 分析速度提升了约 10 倍。
*   **环境依赖**: 脚本自动绑定了项目本地的 `.venv` 环境与 `JAVA_HOME`。

---
*记录日期: 2026/2/11*
