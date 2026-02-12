# check_dwd_hdfs_path.py

## 用途

用于在不依赖 Hive Metastore 的情况下，直接通过 HDFS API 检查 `ecop.dwd_user_behavior` 的底层目录是否已经产生分区与数据文件。

适用场景：

- DWD ETL 正在运行/刚结束，但 Hive `count(*)` 还没跑完或你想先确认“目录里到底有没有文件”
- 怀疑 ETL 失败导致 “表存在但目录为空” 的情况

## 检查目标

- HDFS 目录：`/user/hive/warehouse/ecop.db/dwd_user_behavior`
- 输出：
  - `FOUND N children under dwd_user_behavior`
  - `SAMPLE: dt=YYYY-MM-DD, ...`（显示前 10 个子目录/文件名）

## 运行方式

在项目根目录激活环境后执行：

```bash
python CoreCode_Enterprise/check_dwd_hdfs_path.py
```

## 依赖与前置条件

- 通过 [common/spark_config.py](file:///e:/a_VibeCoding/EcoPulse/common/spark_config.py) 统一创建 SparkSession（无需启用 Hive）
- 需要能访问 HDFS（`fs.defaultFS` 已在 `spark_config.py` 内配置）

## 典型解读

- `FOUND 0 children ...`：
  - 目录存在但没有任何分区/文件，通常意味着 DWD 还没写入成功，或写入还没提交完成
- `FOUND 30+ children ... dt=...`：
  - 已产生按天分区目录，说明 DWD 已经落盘（即使 Hive 层面的统计/元数据刷新还未完成）

