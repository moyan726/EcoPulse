# 全量数据全局推广指南

本手册指导如何将基于样本验证通过的逻辑应用到全量数据集（100GB+）上。

---

## 1. 推广时机建议
全量运行应在以下条件满足后进行：
1. **逻辑验证完成**：所有 ETL、分析和建模脚本在 10 月样本上运行无误。
2. **性能调优完成**：已验证加盐聚合、广播连接等策略可有效处理数据倾斜。
3. **硬件资源充足**：建议在具有至少 16GB 内存的环境中运行全量任务。

---

## 2. 脚本修改步骤

### 2.1 修改数据源路径
在以下脚本中，将输入路径从样本目录修改为全量 CSV 目录：
- `etl_dwd_user_behavior.py`
    - 修改前：`input_path = "data/raw/2019-Oct.csv"`
    - 修改后：`input_path = "data/raw/*.csv"` (读取目录下所有月份)

### 2.2 调整 Spark 配置
由于全量数据规模剧增，需在每个脚本的 `get_spark_session()` 中调整参数：
```python
.config("spark.driver.memory", "12g")      # 增加 Driver 内存
.config("spark.executor.memory", "8g")     # 如果是集群模式
.config("spark.sql.shuffle.partitions", "200") # 增加 Shuffle 分区数，防止单个 Task 过大
```

---

## 3. 全量运行流水线顺序
1. `etl_dwd_user_behavior.py`：生成全量 DWD 明细。
2. `analysis_funnel.py`：计算全量漏斗指标。
3. `analysis_retention.py`：计算全量留存矩阵。
4. `analysis_rfm.py`：计算全量用户评分。
5. `ml_kmeans_clustering.py`：对全量用户进行聚类（注意：全量聚类耗时较长）。
6. `export_to_serving.py`：同步最终结果到可视化层。

---

## 4. 常见问题排查
- **OOM (内存溢出)**：如果报错内存不足，请检查是否启用了 `broadcast` 连接了过大的表，或尝试进一步增大 `shuffle.partitions`。
- **磁盘空间**：全量 Parquet 数据约占 15-20GB，请预留足够的存储空间。
