# PySpark 性能优化对齐要点（第一阶段）

本文件用于将后续“加分模块”的优化策略，提前落到可执行的工程约束与可验证证据上。

## 数据倾斜（Data Skew）

- **常见触发**：`groupBy(brand)`、`join` 高基数字段、头部品牌/类目长尾分布
- **诊断证据**：Spark UI 同一 stage 内 task 的 `Max Duration` 显著大于 `Median Duration`
- **治理策略**：
  - 两阶段聚合加盐（Salting）
  - 维表小且稳定时使用广播连接（Broadcast Join）
  - 分区数与数据量匹配，避免过大分区导致 OOM

## 存储与扫描

- **CSV**：文本解析开销大，`inferSchema` 额外扫描成本高
- **Parquet**：列式存储，适合聚合与筛选，具备投影下推与谓词下推潜力
- **第一阶段约束**：Track A 优先将样本转为 Parquet，降低迭代成本

## 缓存与物化

- **适用场景**：同一 DataFrame 被多次统计（缺失率 + 分布 + 会话统计）
- **工程约束**：只缓存“会重复使用”的中间结果，避免缓存过大触发 GC/OOM

## 近似统计（可接受的论文口径）

- **去重计数**：优先 `approx_count_distinct`，可显著降低全局去重成本
- **分位数**：优先 `approxQuantile`，适合大数据量的价格分布审计

## Windows 本机约束（与方案 A 一致）

- 不修改系统环境变量
- 通过 Conda 环境隔离 Python 依赖
- 通过会话级或进程内设置 `JAVA_HOME` 运行 Spark
