# DWD 层 ETL 脚本 (etl_dwd_enterprise.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/etl_dwd_enterprise.py`
*   **作用**: 执行从 ODS (原始数据) 到 DWD (明细数据) 的清洗与转换任务。

## 2. 核心逻辑
### 2.1 数据读取
*   直接读取 Hive 表 `ecop.ods_user_behavior`，获取全量 7 个月的数据。
*   依赖 `common.spark_config` 提供的统一 SparkSession，连接远程 Hive Metastore。

### 2.2 数据清洗 (Data Cleaning)
1.  **Bot 流量过滤**:
    *   **规则**: 统计每个 `user_session` 的事件总数。
    *   **阈值**: 若单次会话包含超过 **500** 个事件，判定为机器人 (Bot)。
    *   **操作**: 使用 `left_anti` join 剔除这些异常会话。
2.  **异常价格标记**:
    *   新增字段 `price_is_illegal`。
    *   若 `price <= 0`，标记为 `1`，否则为 `0`。保留数据但打标，便于后续审计。
3.  **类型转换**:
    *   `event_time`: String -> Timestamp
    *   `dt`: 从 event_time 提取日期，作为 DWD 表的分区字段。

### 2.3 数据写入
*   **目标表**: `ecop.dwd_user_behavior`
*   **格式**: Parquet (Snappy 压缩)
*   **分区**: 动态分区写入 (`partitionBy("dt")`)。

## 3. 执行方式
```bash
python CoreCode_Enterprise/etl_dwd_enterprise.py
```

## 4. 变更记录
*   **2026-02-12**: 初始版本，实现了企业级清洗规范 (Bot 过滤 + 价格审计)。
