# 全链路每日调度脚本说明

## 1. 功能概述
`run_daily_batch.py` 是 EcoPulse 数据仓库的每日批处理调度入口。它按依赖顺序执行 ODS -> DWD -> ADS 的全链路数据处理任务。

## 2. 执行流程
1.  **ODS Partition Repair** (`repair_partitions.py`): 修复 Hive Metastore 中的 ODS 分区信息。
2.  **DWD ETL** (`etl_dwd_enterprise.py`): 执行数据清洗、去重、转换，并写入 DWD 层 (Parquet)。
3.  **ADS Calculation**:
    *   `ads_user_rfm.py`: 计算用户 RFM 模型指标。
    *   `ads_funnel_stats.py`: 计算漏斗转化率。
    *   `ads_user_retention.py`: 计算用户留存率。

## 3. 错误处理
*   **Critical Tasks**: ODS 和 DWD 任务被标记为关键任务 (Critical)。如果失败，整个 Pipeline 立即终止。
*   **Non-Critical Tasks**: ADS 任务如果单个失败，不会影响其他 ADS 任务的执行（但会在日志中报错）。

## 4. 使用方法
```bash
python CoreCode_Enterprise/run_daily_batch.py
```
