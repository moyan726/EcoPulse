# export_serving_from_hive.py

## 用途

从 Hive 管理表直接导出看板所需的 `data/serving/*.csv`，用于在 “企业链路（Hive/HDFS）” 与 “本地看板（CoreCode6）” 之间做数据同步。

该脚本的定位是：当 ADS 结果写在 Hive（`ecop.ads_*`）而不是本地 `data/ads/` 时，仍能一键生成看板可读 CSV。

## 导出范围

| Hive 表 | 输出 CSV |
|---|---|
| `ecop.ads_funnel_stats` | `data/serving/funnel_stats.csv` |
| `ecop.ads_user_retention` | `data/serving/user_retention.csv` |
| `ecop.ads_user_rfm` | `data/serving/user_rfm.csv` |
| `ecop.ads_user_clusters` | `data/serving/user_clusters.csv` |

## 运行方式

```bash
python scripts/export_serving_from_hive.py --force
```

参数：

- `--force`：覆盖已有 CSV（用于从“旧的 0 结果”强制刷新为最新结果）

## 技术实现要点

- 通过 [common/spark_config.py](file:///e:/a_VibeCoding/EcoPulse/common/spark_config.py) 创建启用 Hive 的 SparkSession
- 每张表 `coalesce(1)` 导出为单文件 CSV（便于看板读取）
- 临时目录写入使用 `file:///` URI，避免在启用 HDFS `defaultFS` 时被误当作 HDFS 路径

## 与 prepare_serving_data.py 的区别

- `prepare_serving_data.py`：读取本地 `data/ads/*.parquet` → 导出 CSV（纯 pandas/pyarrow）
- `export_serving_from_hive.py`：读取 Hive `ecop.ads_*` → 导出 CSV（Spark/Hive）

优先建议：

- 如果 ADS 已经落到 Hive：用 `export_serving_from_hive.py`
- 如果 ADS 在本地 Parquet：用 `prepare_serving_data.py`

