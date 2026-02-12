# ODS 数据可读性验证脚本 (verify_data_read.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/verify_data_read.py`
*   **作用**: 绕过 Hive Metastore，直接使用 PySpark 读取 HDFS 上的原始 CSV 数据，验证数据内容的正确性与 Schema 解析能力。

## 2. 核心逻辑
*   **Schema 定义**: 在代码中显式定义了包含 `event_time`, `user_id`, `price` 等 9 个字段的 StructType，与 Hive DDL 保持一致。
*   **直接读取**: 使用 `spark.read.csv` 直接加载 HDFS 文件，不依赖 Hive 表元数据。
*   **统计分析**: 执行 `count()` 和 `show()` 操作，触发真实的 MapReduce/Spark 任务，确保数据块可被计算引擎读取。

## 3. 验证结果 (2019-10)
*   **读取状态**: ✅ Success
*   **行数统计**: 42,448,764 行 (约 4245 万条)
*   **数据抽样**: 字段对齐准确，无乱码，Double 类型字段 (`price`) 解析正常。

## 4. 变更记录
*   **2026-02-12**: 初始版本，作为 Metastore 不可用时的兜底验证方案。
