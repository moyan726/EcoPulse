# 数据全量接入脚本 (ingest_to_hdfs.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/ingest_to_hdfs.py`
*   **作用**: 将本地 `data/row/` 目录下的 7 个月度 CSV 文件（共 54GB）自动上传至 HDFS，并修复 Hive 分区元数据。

## 2. 核心逻辑
### 2.1 文件名解析与映射
脚本内置了文件名到 Hive 分区的映射规则：
*   `2019-Oct.csv` -> `/ecop/ods/user_behavior/dt_month=2019-10`
*   `2019-Nov.csv` -> `/ecop/ods/user_behavior/dt_month=2019-11`
*   ... (涵盖 2019/10 至 2020/04)

### 2.2 智能上传
*   利用 Spark 的 Hadoop FileSystem API 进行上传。
*   **幂等性**: 上传前会自动检查目标文件是否存在，若存在则跳过，避免重复传输浪费时间。
*   **目录管理**: 自动创建 HDFS 上的分区目录结构。

### 2.3 元数据同步
*   上传完成后，自动执行 `MSCK REPAIR TABLE ecop.ods_user_behavior`。
*   **作用**: 通知 Hive Metastore 扫描 HDFS 目录，自动识别并注册新上传的分区，无需手动 `ALTER TABLE ADD PARTITION`。

## 3. 执行方式
```bash
python CoreCode_Enterprise/ingest_to_hdfs.py
```
*   **前置条件**: 
    1.  已执行 `hive_ddl.sql` 创建了表结构。
    2.  `data/row/` 目录下存在原始 CSV 文件。

## 4. 变更记录
*   **2026-02-12**: 初始版本，实现了从 Local FS 到 HDFS 的自动化 ETL 接入流程。
