# Hive ODS/DWD/ADS 建表脚本 (hive_ddl.sql)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/hive_ddl.sql`
*   **作用**: 定义 EcoPulse 项目在 Hive 数仓中的分层架构，包含 ODS、DWD、ADS 三层表的 DDL 语句。

## 2. 数仓分层设计
### 2.1 ODS 层 (原始数据)
*   **表名**: `ecop.ods_user_behavior`
*   **类型**: 外部表 (EXTERNAL TABLE)
*   **存储**: TEXTFILE (CSV)
*   **位置**: `/ecop/ods/user_behavior`
*   **分区**: 按月分区 (`dt_month`)，如 `2019-10`。
*   **说明**: 直接映射 HDFS 上的原始 CSV 文件，不移动数据。

### 2.2 DWD 层 (明细数据)
*   **表名**: `ecop.dwd_user_behavior`
*   **类型**: 管理表 (MANAGED TABLE)
*   **存储**: Parquet + Snappy 压缩
*   **分区**: 按日分区 (`dt`)
*   **说明**: 存储清洗后的高质量明细数据，去除了 Bot 流量和异常价格。

### 2.3 ADS 层 (应用数据)
*   **表名**: 
    *   `ecop.ads_user_rfm`: 用户 RFM 分层结果
    *   `ecop.ads_funnel_stats`: 漏斗分析结果
    *   `ecop.ads_user_retention`: 留存分析结果
    *   `ecop.ads_user_clusters`: K-Means 聚类结果
*   **存储**: Parquet
*   **说明**: 存储高度聚合的业务指标，供 HBase 和 BI 看板直接查询。

## 3. 执行方式
推荐使用 Beeline 或 Hive CLI 执行：
```bash
hive -f CoreCode_Enterprise/hive_ddl.sql
```
或者在 Spark SQL 中运行。

## 4. 变更记录
*   **2026-02-12**: 初始版本，确立了 ODS -> DWD -> ADS 的标准分层规范。
