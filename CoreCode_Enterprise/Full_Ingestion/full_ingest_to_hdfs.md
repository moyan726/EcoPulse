# 全量数据接入说明 (full_ingest_to_hdfs.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/Full_Ingestion/full_ingest_to_hdfs.py`
*   **作用**: 负责将项目全周期 (2019.10 - 2020.04) 的 7 个 CSV 数据文件完整上传至 HDFS。

## 2. 核心功能
*   **批量处理**: 自动遍历 `data/row/` 目录，匹配文件名并映射到对应的 Hive 分区 (`dt_month=YYYY-MM`)。
*   **智能断点续传**: 上传前校验文件是否存在及大小是否一致。如果文件已完整上传，则自动跳过；如果大小不匹配，则强制覆盖。
*   **内存优化**: 考虑到数据总量较大 (54GB)，调整了 Driver Memory 为 8GB 以确保稳定性。

## 3. 目录结构
```text
CoreCode_Enterprise/
└── Full_Ingestion/           # 新增的全量接入子模块
    ├── full_ingest_to_hdfs.py
    └── full_ingest_to_hdfs.md
```

## 4. 执行方式
```bash
python CoreCode_Enterprise/Full_Ingestion/full_ingest_to_hdfs.py
```

## 5. 变更记录
*   **2026-02-12**: 初始版本，从 Phase E2 的单月验证脚本升级而来，增加了全量映射和文件大小校验逻辑。
