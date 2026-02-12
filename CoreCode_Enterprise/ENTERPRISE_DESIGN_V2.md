# EcoPulse Enterprise Architecture — 落地实施方案 V2.0

> **版本**：V2.0 (完全重写)
> **日期**：2026-02-12
> **定位**：可直接执行的工程施工图，非概念性设计

---

## 0. 现状审计 (As-Is Audit)

在写方案之前，必须先承认现状的真实情况：

| 维度 | 你以为的 | 实际的 |
| :--- | :--- | :--- |
| 数据规模 | 2 亿+ 条 (7 个月) | **仅处理了 10 月**，约 4200 万条 |
| 数仓分层 | Raw → ODS → DWD → ADS | **ODS 层不存在**，DWD 直接从 Phase1 采样读取 |
| 原始数据 | 已接入管道 | 7 个 CSV (共 54 GB) **静静躺在 `data/row/`**，未被任何脚本使用 |
| Serving 层 | 高性能查询 | **CSV 本地文件**，全量加载到内存 |
| RFM 分群 | 8 段精细化 | Spark 端只有 4 群，**Pandas 端偷偷改成 8 群** |
| 存储架构 | 分布式 | **纯单机本地文件系统** |

### 可用集群资源

| 节点 | 角色 | 组件 |
| :--- | :--- | :--- |
| **hadoop1** | Master | NameNode, HBase Master, Hive Metastore, ResourceManager |
| **hadoop2** | Worker | DataNode, HBase RegionServer, NodeManager |
| **hadoop3** | Worker | DataNode, HBase RegionServer, NodeManager |
| **Windows 宿主机** | 开发/计算 | Spark Local, Streamlit, Python 3.11, JDK 1.8 |

---

## 1. 升级总体目标 (Upgrade Objectives)

本次升级要解决 **评审意见书** 中 4 个致命问题：

| # | 评审问题 | 对应升级动作 | 完成标志 |
| :--- | :--- | :--- | :--- |
| P0-1 | ODS 层缺失 | 7 个月 CSV 全量接入 HDFS → Hive ODS 外部表 | `hdfs dfs -ls /ecop/ods` 可见 7 个分区 |
| P0-2 | RFM 分群前后矛盾 | 8 分群逻辑迁回 Spark 端 (analysis_rfm.py) | ADS Parquet 中 `rfm_segment` 直接包含 8 个值 |
| P0-3 | Serving 层为 CSV | ADS 结果写入 HBase，Streamlit 从 HBase 读 | 看板启动后 `scan 'ecop:user_profile'` 有数据 |
| P0-4 | Spark 配置重复 | 抽取公共模块 `common/spark_config.py` | 所有 CoreCode 脚本 import 统一配置 |

---

## 2. 架构设计 (To-Be Architecture)

### 2.1 目标架构拓扑

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    Windows 宿主机 (开发 & 计算)                       │
│                                                                     │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │ Raw CSV  │  │  Spark Local │  │ CoreCode_Ent │  │ Streamlit  │  │
│  │ data/row │→→│  PySpark 3.5 │→→│ (新增脚本)    │  │ CoreCode6  │  │
│  │ 54 GB    │  │  driver 8g   │  │              │  │            │  │
│  └──────────┘  └──────┬───────┘  └──────────────┘  └─────┬──────┘  │
│                       │ HDFS Protocol (hdfs://hadoop1:9000)│         │
│                       │ Thrift (hadoop1:9083 / 16010)      │ HBase   │
│                       │                                    │ Thrift  │
├───────────────────────┼────────────────────────────────────┼─────────┤
│                       ▼                                    ▼         │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │              VMware 三节点 Hadoop 集群                        │   │
│  │                                                              │   │
│  │  ┌─────────────────────────────────────────────────────┐     │   │
│  │  │ HDFS                                                │     │   │
│  │  │  /ecop/ods/       ← 原始 CSV (Hive 外部表)          │     │   │
│  │  │  /ecop/dwd/       ← 清洗后 Parquet (Hive 管理表)     │     │   │
│  │  │  /ecop/ads/       ← 指标聚合 Parquet                │     │   │
│  │  └─────────────────────────────────────────────────────┘     │   │
│  │                                                              │   │
│  │  ┌──────────────┐    ┌──────────────────────────────┐        │   │
│  │  │ Hive         │    │ HBase                        │        │   │
│  │  │ Metastore    │    │  ecop:user_profile (RFM/KM)  │        │   │
│  │  │ (元数据管理)  │    │  ecop:funnel_stats (漏斗)     │        │   │
│  │  │ ODS/DWD/ADS  │    │  ecop:user_retention (留存)   │        │   │
│  │  └──────────────┘    └──────────────────────────────┘        │   │
│  │                                                              │   │
│  │  hadoop1 (Master)   hadoop2 (Worker)   hadoop3 (Worker)      │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 数据流转全景 (End-to-End Data Flow)

```text
Phase E1             Phase E2             Phase E3            Phase E4
环境打通              ODS 全量接入          DWD/ADS 重跑         HBase Serving
                                          (修复分群逻辑)
                                          
data/row/*.csv  ──→  HDFS /ecop/ods/  ──→  Hive DWD 表   ──→  HBase 表
  (54 GB)            (Hive 外部表)         (Parquet)          (KV 存储)
  7 个月度文件         7 个月度分区           全量清洗              │
                                          8 段 RFM              ▼
                                          K-Means 聚类       Streamlit
                                          漏斗/留存            (happybase 读取)
```

### 2.3 Hive 数仓分层设计

```sql
-- 数据库
CREATE DATABASE IF NOT EXISTS ecop;

-- ═══════════════════════════════════════════════════════
-- ODS 层：原始数据 (外部表，指向 HDFS 上的 CSV)
-- ═══════════════════════════════════════════════════════
CREATE EXTERNAL TABLE IF NOT EXISTS ecop.ods_user_behavior (
    event_time      STRING,
    event_type      STRING,
    product_id      BIGINT,
    category_id     BIGINT,
    category_code   STRING,
    brand           STRING,
    price           DOUBLE,
    user_id         BIGINT,
    user_session    STRING
)
PARTITIONED BY (dt_month STRING)          -- 按月分区: '2019-10', '2019-11', ...
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/ecop/ods/user_behavior'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ═══════════════════════════════════════════════════════
-- DWD 层：清洗后明细 (管理表，Parquet + Snappy)
-- ═══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ecop.dwd_user_behavior (
    event_time        TIMESTAMP,
    event_type        STRING,
    product_id        BIGINT,
    category_id       BIGINT,
    category_code     STRING,
    brand             STRING,
    price             DOUBLE,
    user_id           BIGINT,
    user_session      STRING,
    price_is_illegal  INT               -- 异常价格标记
)
PARTITIONED BY (dt DATE)                 -- 按日分区
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ═══════════════════════════════════════════════════════
-- ADS 层：业务指标 (管理表，Parquet)
-- ═══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ecop.ads_user_rfm (
    user_id       BIGINT,
    recency       INT,
    frequency     INT,
    monetary      DOUBLE,
    r_score       INT,
    f_score       INT,
    m_score       INT,
    rfm_segment   STRING              -- 8 段分群，在 Spark 端直接计算
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ecop.ads_funnel_stats (
    event_type      STRING,
    session_count   BIGINT,
    user_count      BIGINT,
    dimension       STRING,
    dt              DATE,
    brand           STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ecop.ads_user_retention (
    cohort_date      DATE,
    period           INT,
    retention_count  BIGINT,
    cohort_size      BIGINT,
    retention_rate   DOUBLE
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ecop.ads_user_clusters (
    user_id     BIGINT,
    recency     INT,
    frequency   INT,
    monetary    DOUBLE,
    prediction  INT                   -- K-Means 簇 ID
)
STORED AS PARQUET;
```

### 2.4 HBase Schema 设计

```text
┌─────────────────────────────────────────────────────────────────┐
│ Table: ecop:user_profile                                        │
│ RowKey: MD5(user_id)[0:4] + user_id  (前缀散列防热点)            │
├─────────────────────────────────────────────────────────────────┤
│ Column Family: rfm                                              │
│   rfm:recency        (INT)                                      │
│   rfm:frequency      (INT)                                      │
│   rfm:monetary       (DOUBLE)                                   │
│   rfm:r_score        (INT)                                      │
│   rfm:f_score        (INT)                                      │
│   rfm:m_score        (INT)                                      │
│   rfm:segment        (STRING, 8 段分群名)                        │
│                                                                 │
│ Column Family: cluster                                          │
│   cluster:id         (INT, K-Means 簇 ID)                       │
│   cluster:label      (STRING, 簇标签名)                          │
├─────────────────────────────────────────────────────────────────┤
│ 设计要点:                                                        │
│   • 两个 Column Family: rfm (高频读) + cluster (低频读)          │
│   • RowKey 前缀散列: 避免 user_id 单调递增导致 Region 热点        │
│   • 预分区: CREATE 时指定 SPLITS => ['4','8','c']  (16 进制前缀)  │
│   • TTL: 不设置 (用户画像长期有效)                                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Table: ecop:funnel_daily                                        │
│ RowKey: dimension#dt#event_type  (复合 RowKey)                   │
│   例: "daily#2019-10-01#view"                                    │
├─────────────────────────────────────────────────────────────────┤
│ Column Family: metrics                                          │
│   metrics:session_count  (BIGINT)                               │
│   metrics:user_count     (BIGINT)                               │
│   metrics:brand          (STRING, nullable)                     │
├─────────────────────────────────────────────────────────────────┤
│ 设计要点:                                                        │
│   • RowKey 按 dimension 前缀，支持 Scan("daily#") 快速读取日趋势  │
│   • 全局漏斗: RowKey = "global##view"                            │
│   • 品牌漏斗: RowKey = "brand#samsung#purchase"                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Table: ecop:user_retention                                      │
│ RowKey: cohort_date#period (零填充)                              │
│   例: "2019-10-01#007"                                           │
├─────────────────────────────────────────────────────────────────┤
│ Column Family: metrics                                          │
│   metrics:retention_count  (BIGINT)                             │
│   metrics:cohort_size      (BIGINT)                             │
│   metrics:retention_rate   (DOUBLE)                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 分阶段实施计划 (Implementation Phases)

### Phase E1: 环境打通与基础设施 (Day 1)

**目标**：Windows 宿主机能成功访问集群 HDFS / HBase / Hive。

#### 3.1.1 网络与 hosts 配置

```powershell
# 在 Windows 上编辑 hosts 文件 (需管理员权限)
# C:\Windows\System32\drivers\etc\hosts 追加:
# <VM_IP_1>  hadoop1
# <VM_IP_2>  hadoop2
# <VM_IP_3>  hadoop3
```

**验证方法**:
```powershell
ping hadoop1
ping hadoop2
ping hadoop3
```

#### 3.1.2 集群配置文件同步

在项目根目录创建 `conf/` 目录，从集群复制以下文件：

```text
EcoPulse/
└── conf/                          # ← 新增目录
    ├── core-site.xml              # HDFS 连接地址 (fs.defaultFS)
    ├── hdfs-site.xml              # HDFS 副本/块配置
    ├── hive-site.xml              # Hive Metastore 连接 (Thrift)
    └── hbase-site.xml             # HBase ZooKeeper Quorum
```

关键配置项检查清单：

| 文件 | 关键属性 | 预期值 |
| :--- | :--- | :--- |
| `core-site.xml` | `fs.defaultFS` | `hdfs://hadoop1:9000` (或 8020) |
| `hive-site.xml` | `hive.metastore.uris` | `thrift://hadoop1:9083` |
| `hbase-site.xml` | `hbase.zookeeper.quorum` | `hadoop1,hadoop2,hadoop3` |
| `hbase-site.xml` | `hbase.zookeeper.property.clientPort` | `2181` |

#### 3.1.3 Python 依赖安装

```powershell
# 在项目虚拟环境中安装
pip install happybase thrift pyhive
```

| 包 | 版本建议 | 用途 |
| :--- | :--- | :--- |
| `happybase` | ≥ 1.2.0 | HBase Python 客户端 (Thrift) |
| `thrift` | ≥ 0.16 | Thrift 协议基础库 |
| `pyhive` | ≥ 0.7.0 | Hive Python 客户端 (可选，主要用 Spark SQL) |

#### 3.1.4 连通性验证脚本

**文件**：`CoreCode_Enterprise/test_connectivity.py`

```python
"""
验证 Windows → Hadoop 集群连通性。
测试项: HDFS 读写 / HBase 连接 / Hive Metastore。
"""
import os
import subprocess
import sys

def test_hdfs():
    """测试 HDFS 连通性"""
    print("=" * 50)
    print("[Test 1] HDFS Connectivity")
    print("=" * 50)
    # 方式一：通过 Spark 的 hadoop fs 命令
    # 方式二：通过 PySpark 代码
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("Test_HDFS") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop1:9000") \
        .getOrCreate()
    
    # 创建测试目录
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    test_path = sc._jvm.org.apache.hadoop.fs.Path("/ecop/_test_connectivity")
    
    if fs.exists(test_path):
        fs.delete(test_path, True)
    fs.mkdirs(test_path)
    
    exists = fs.exists(test_path)
    print(f"  HDFS mkdir /ecop/_test_connectivity: {'✅ OK' if exists else '❌ FAIL'}")
    
    # 清理
    fs.delete(test_path, True)
    spark.stop()
    return exists

def test_hbase():
    """测试 HBase Thrift 连通性"""
    print("\n" + "=" * 50)
    print("[Test 2] HBase Connectivity")
    print("=" * 50)
    import happybase
    try:
        # HBase Thrift Server 默认端口 9090
        conn = happybase.Connection('hadoop1', port=9090)
        tables = conn.tables()
        print(f"  HBase tables: {tables}")
        print(f"  HBase connection: ✅ OK")
        conn.close()
        return True
    except Exception as e:
        print(f"  HBase connection: ❌ FAIL ({e})")
        print(f"  → 请确认 HBase Thrift Server 已启动:")
        print(f"    hadoop1$ hbase thrift start &")
        return False

if __name__ == "__main__":
    r1 = test_hdfs()
    r2 = test_hbase()
    print("\n" + "=" * 50)
    print(f"Results: HDFS={'✅' if r1 else '❌'}  HBase={'✅' if r2 else '❌'}")
    print("=" * 50)
```

#### 3.1.5 集群端前置操作 (在 hadoop1 上执行)

```bash
# 1. 启动 HBase Thrift Server (如果尚未启动)
#    Streamlit 和 Python 都通过 Thrift 协议访问 HBase
hbase thrift start &

# 2. 在 HDFS 上创建项目根目录
hdfs dfs -mkdir -p /ecop/ods/user_behavior
hdfs dfs -mkdir -p /ecop/dwd
hdfs dfs -mkdir -p /ecop/ads

# 3. 在 HBase 中预创建表 (hbase shell)
hbase shell <<'EOF'
create_namespace 'ecop'

create 'ecop:user_profile', \
  {NAME => 'rfm', VERSIONS => 1, COMPRESSION => 'SNAPPY'}, \
  {NAME => 'cluster', VERSIONS => 1, COMPRESSION => 'SNAPPY'}, \
  SPLITS => ['4', '8', 'c']

create 'ecop:funnel_daily', \
  {NAME => 'metrics', VERSIONS => 1, COMPRESSION => 'SNAPPY'}

create 'ecop:user_retention', \
  {NAME => 'metrics', VERSIONS => 1, COMPRESSION => 'SNAPPY'}

list_namespace_tables 'ecop'
EOF

# 4. 启动 Hive Metastore (如果尚未启动)
nohup hive --service metastore &
```

---

### Phase E2: ODS 全量接入 (Day 2)

**目标**：将本地 54 GB 原始 CSV 上传到 HDFS，注册为 Hive ODS 外部表。

#### 3.2.1 数据上传策略

```text
本地文件                                    HDFS 路径
data/row/2019-Oct.csv     →  /ecop/ods/user_behavior/dt_month=2019-10/data.csv
data/row/2019-Nov.csv     →  /ecop/ods/user_behavior/dt_month=2019-11/data.csv
data/row/2019-Dec.csv     →  /ecop/ods/user_behavior/dt_month=2019-12/data.csv
data/row/2020-Jan.csv     →  /ecop/ods/user_behavior/dt_month=2020-01/data.csv
data/row/2020-Feb.csv     →  /ecop/ods/user_behavior/dt_month=2020-02/data.csv
data/row/2020-Mar.csv     →  /ecop/ods/user_behavior/dt_month=2020-03/data.csv
data/row/2020-Apr.csv     →  /ecop/ods/user_behavior/dt_month=2020-04/data.csv
```

**文件**：`CoreCode_Enterprise/ingest_to_hdfs.py`

核心逻辑：
```python
"""
ODS 全量接入：将本地 CSV 上传至 HDFS，并注册 Hive 分区。

运行方式 (在激活虚拟环境后):
    python CoreCode_Enterprise/ingest_to_hdfs.py

耗时预估: 54 GB / ~100 MB/s (虚拟网卡) ≈ 9-10 分钟
"""
# 核心流程:
# 1. 遍历 data/row/*.csv
# 2. 解析文件名提取月份 (2019-Oct → 2019-10)
# 3. 通过 Spark 的 Hadoop FileSystem API 上传到 HDFS 对应分区目录
# 4. 执行 Hive MSCK REPAIR TABLE 自动识别所有分区
#
# 月份映射: {'Oct':'10', 'Nov':'11', 'Dec':'12', 'Jan':'01', ...}
```

#### 3.2.2 Hive 分区注册

上传完成后，执行以下命令让 Hive 识别所有分区：

```sql
-- 在 Hive 或 Spark SQL 中执行
MSCK REPAIR TABLE ecop.ods_user_behavior;

-- 验证
SELECT dt_month, COUNT(*) as cnt
FROM ecop.ods_user_behavior
GROUP BY dt_month
ORDER BY dt_month;
```

**预期结果**：

| dt_month | cnt (预估) |
| :--- | :--- |
| 2019-10 | ~42,000,000 |
| 2019-11 | ~67,000,000 |
| 2019-12 | ~69,000,000 |
| 2020-01 | ~57,000,000 |
| 2020-02 | ~56,000,000 |
| 2020-03 | ~58,000,000 |
| 2020-04 | ~69,000,000 |

---

### Phase E3: DWD/ADS 重构 (Day 3-4)

**目标**：ETL 读写 Hive 表；RFM 8 分群在 Spark 端完成。

#### 3.3.1 公共模块抽取

**文件**：`common/spark_config.py`

```python
"""
EcoPulse 公共 Spark 配置模块。
所有 CoreCode 脚本通过此模块获取 SparkSession，消除重复代码。
"""
import os
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def get_spark_session(
    app_name: str = "EcoPulse",
    driver_memory: str = "8g",
    enable_hive: bool = False,
    hdfs_uri: str = "hdfs://hadoop1:9000",
) -> SparkSession:
    """
    构建统一的 SparkSession。
    
    Args:
        app_name:      Spark 应用名
        driver_memory:  Driver 内存
        enable_hive:    是否启用 Hive 支持
        hdfs_uri:       HDFS 地址 (设为空字符串则使用本地模式)
    """
    # 从环境变量读取，不再硬编码
    venv_python = (PROJECT_ROOT / ".venv" / "Scripts" / "python.exe").as_posix()
    os.environ.setdefault("PYSPARK_PYTHON", venv_python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
    
    # JAVA_HOME 必须通过 dev_shell.ps1 或 .env 设置，此处仅做 fallback
    os.environ.setdefault("JAVA_HOME", os.getenv("JAVA_HOME", ""))

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    
    if hdfs_uri:
        builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_uri)
    
    if enable_hive:
        # 指向集群的 Hive Metastore
        conf_dir = (PROJECT_ROOT / "conf").as_posix()
        builder = builder \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://hadoop1:9083") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hadoop1:9083") \
            .enableHiveSupport()
    
    return builder.getOrCreate()
```

#### 3.3.2 ETL DWD 重构

**文件**：`CoreCode_Enterprise/etl_dwd_enterprise.py`

关键变更点：
```python
# 变更 1: 读取 Hive ODS 表 (全量 7 个月)，而非本地 Parquet
df_raw = spark.sql("SELECT * FROM ecop.ods_user_behavior")

# 变更 2: 增加 Bot 过滤 (Session 频率异常检测)
from pyspark.sql.window import Window
from pyspark.sql.functions import count as spark_count, col

# 统计每个 user_session 的事件数
session_counts = df_raw.groupBy("user_session").agg(
    spark_count("*").alias("event_cnt")
)
# 阈值: 单 Session 超过 500 次事件视为 Bot
bot_sessions = session_counts.filter(col("event_cnt") > 500)
df_clean = df_raw.join(bot_sessions, on="user_session", how="left_anti")

# 变更 3: 写入 Hive DWD 管理表 (按日分区)
df_final.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .format("hive") \
    .saveAsTable("ecop.dwd_user_behavior")
```

#### 3.3.3 RFM 8 分群修复 (关键!)

**文件**：`CoreCode_Enterprise/analysis_rfm_enterprise.py`

```python
# 核心变更: 将 8 段分群逻辑从 prepare_serving_data.py 迁移到 Spark 端

def assign_rfm_segment(df_rfm):
    """
    在 Spark 端直接计算 8 段 RFM 分群，写入 ADS 层。
    不再依赖下游 Pandas 脚本做 "后门修补"。
    """
    from pyspark.sql.functions import when, col
    
    # fm_score = f_score + m_score
    df = df_rfm.withColumn("fm_score", col("f_score") + col("m_score"))
    
    # 动态计算中位数阈值
    fm_median = df.stat.approxQuantile("fm_score", [0.5], 0.01)[0]
    fm_hi = int(fm_median) + 1
    
    df = df.withColumn("rfm_segment",
        when((col("r_score") >= 4) & (col("fm_score") >= fm_hi + 1),
             "Champions (重要价值客户)")
        .when((col("r_score") >= 4) & (col("fm_score") >= fm_hi),
             "Potential Loyalist (潜力客户)")
        .when((col("r_score") >= 4) & (col("fm_score") < fm_hi),
             "Promising (成长客户)")
        .when((col("r_score") == 3) & (col("fm_score") < fm_hi) & (col("frequency") <= 1),
             "New Customers (新客户)")
        .when((col("r_score") == 3) & (col("fm_score") >= fm_hi),
             "Loyal (一般价值客户)")
        .when((col("r_score") == 3) & (col("fm_score") < fm_hi),
             "Need Attention (需要关注)")
        .when((col("r_score") <= 2) & (col("fm_score") >= fm_hi),
             "At Risk (流失预警)")
        .otherwise("Hibernating (沉睡客户)")
    )
    
    return df.drop("fm_score")
    
# 写入 Hive ADS 表
rfm_result.write.mode("overwrite").format("hive").saveAsTable("ecop.ads_user_rfm")
```

#### 3.3.4 其他 ADS 指标重跑

| 脚本 | 变更要点 |
| :--- | :--- |
| `analysis_funnel_enterprise.py` | 读 Hive DWD → 写 Hive ADS；增加 `filter(price_is_illegal == 0)` |
| `analysis_retention_enterprise.py` | 读 Hive DWD → 写 Hive ADS；数据量从 1 个月扩展到 7 个月 |
| `ml_kmeans_enterprise.py` | 读 Hive ADS RFM → 输出 Elbow 图 → 写 Hive ADS |

---

### Phase E4: HBase Serving 层 (Day 5)

**目标**：ADS 结果灌入 HBase，Streamlit 从 HBase 读取。

#### 3.4.1 ADS → HBase 写入工具

**文件**：`CoreCode_Enterprise/utils/hbase_writer.py`

```python
"""
HBase 批量写入工具。
将 Spark DataFrame / Pandas DataFrame 批量灌入 HBase 表。
"""
import hashlib
import struct
from typing import Dict, List, Optional

import happybase

HBASE_HOST = "hadoop1"
HBASE_PORT = 9090
BATCH_SIZE = 5000      # 每批写入行数，防止 Thrift 超时


def _make_rowkey_user(user_id: int) -> bytes:
    """
    生成 user_profile 的 RowKey。
    格式: MD5(user_id)[0:4] + user_id
    目的: 前缀散列，避免 user_id 单调递增导致 Region 热点。
    """
    md5_prefix = hashlib.md5(str(user_id).encode()).hexdigest()[:4]
    return f"{md5_prefix}_{user_id}".encode()


def write_user_profiles(rfm_df, cluster_df):
    """
    将 RFM 分群和 K-Means 聚类结果合并写入 HBase ecop:user_profile。
    
    Args:
        rfm_df:     Pandas DataFrame with columns: user_id, recency, frequency,
                    monetary, r_score, f_score, m_score, rfm_segment
        cluster_df: Pandas DataFrame with columns: user_id, prediction
    """
    # 合并
    merged = rfm_df.merge(cluster_df[['user_id', 'prediction']], 
                          on='user_id', how='left')
    
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = conn.table('ecop:user_profile')
    
    batch = table.batch(batch_size=BATCH_SIZE)
    for _, row in merged.iterrows():
        rowkey = _make_rowkey_user(int(row['user_id']))
        batch.put(rowkey, {
            b'rfm:recency':    str(row['recency']).encode(),
            b'rfm:frequency':  str(row['frequency']).encode(),
            b'rfm:monetary':   str(row['monetary']).encode(),
            b'rfm:r_score':    str(row['r_score']).encode(),
            b'rfm:f_score':    str(row['f_score']).encode(),
            b'rfm:m_score':    str(row['m_score']).encode(),
            b'rfm:segment':    str(row['rfm_segment']).encode(),
            b'cluster:id':     str(int(row.get('prediction', -1))).encode(),
        })
    batch.send()
    conn.close()
    print(f"[HBase] 写入 ecop:user_profile 完成: {len(merged):,} 行")


def write_funnel(funnel_df):
    """将漏斗统计写入 HBase ecop:funnel_daily。"""
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = conn.table('ecop:funnel_daily')
    
    batch = table.batch(batch_size=BATCH_SIZE)
    for _, row in funnel_df.iterrows():
        dim = str(row.get('dimension', 'global'))
        dt = str(row.get('dt', ''))
        et = str(row.get('event_type', ''))
        brand = str(row.get('brand', ''))
        
        # 复合 RowKey
        rowkey = f"{dim}#{dt}#{et}#{brand}".encode()
        batch.put(rowkey, {
            b'metrics:session_count': str(row['session_count']).encode(),
            b'metrics:user_count':    str(row['user_count']).encode(),
        })
    batch.send()
    conn.close()


def write_retention(retention_df):
    """将留存数据写入 HBase ecop:user_retention。"""
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = conn.table('ecop:user_retention')
    
    batch = table.batch(batch_size=BATCH_SIZE)
    for _, row in retention_df.iterrows():
        cohort = str(row['cohort_date'])
        period = str(int(row['period'])).zfill(3)
        rowkey = f"{cohort}#{period}".encode()
        batch.put(rowkey, {
            b'metrics:retention_count': str(row['retention_count']).encode(),
            b'metrics:cohort_size':     str(row['cohort_size']).encode(),
            b'metrics:retention_rate':  str(row['retention_rate']).encode(),
        })
    batch.send()
    conn.close()
```

#### 3.4.2 Streamlit 数据源适配

**文件**：`CoreCode_Enterprise/utils/hbase_reader.py`

```python
"""
HBase 读取适配器 — 供 Streamlit 看板使用。
提供与原 CSV load_data() 完全兼容的接口。
"""
import pandas as pd
import happybase

HBASE_HOST = "hadoop1"
HBASE_PORT = 9090


def load_data_from_hbase():
    """
    从 HBase 读取所有看板数据，返回与原 load_data() 相同的
    4 个 DataFrame: (df_funnel, df_retention, df_rfm, df_clusters)
    
    这样 CoreCode6 的页面代码无需任何修改。
    """
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    
    # 1. 读取漏斗数据
    df_funnel = _scan_funnel(conn)
    
    # 2. 读取留存数据
    df_retention = _scan_retention(conn)
    
    # 3. 读取用户画像 (RFM + Cluster)
    df_rfm, df_clusters = _scan_user_profiles(conn)
    
    conn.close()
    return df_funnel, df_retention, df_rfm, df_clusters


def _scan_user_profiles(conn):
    """扫描 ecop:user_profile，拆分为 rfm_df 和 cluster_df。"""
    table = conn.table('ecop:user_profile')
    
    rows = []
    for key, data in table.scan():
        user_id = key.decode().split('_', 1)[1]  # 去掉 MD5 前缀
        rows.append({
            'user_id': int(user_id),
            'recency':    int(data.get(b'rfm:recency', b'0')),
            'frequency':  int(data.get(b'rfm:frequency', b'0')),
            'monetary':   float(data.get(b'rfm:monetary', b'0')),
            'r_score':    int(data.get(b'rfm:r_score', b'0')),
            'f_score':    int(data.get(b'rfm:f_score', b'0')),
            'm_score':    int(data.get(b'rfm:m_score', b'0')),
            'rfm_segment': data.get(b'rfm:segment', b'').decode(),
            'prediction':  int(data.get(b'cluster:id', b'-1')),
        })
    
    df = pd.DataFrame(rows)
    df_rfm = df[['user_id','recency','frequency','monetary',
                  'r_score','f_score','m_score','rfm_segment']]
    df_clusters = df[['user_id','recency','frequency','monetary','prediction']]
    return df_rfm, df_clusters


# _scan_funnel 和 _scan_retention 类似，解析各自的 RowKey 和 Column
```

#### 3.4.3 CoreCode6 适配改造

在 `CoreCode6/src/dashboard/utils.py` 的 `load_data()` 函数中增加 **数据源切换**：

```python
import os

def load_data():
    """
    统一数据加载入口。
    优先尝试 HBase，失败或未配置时 fallback 到本地 CSV。
    """
    use_hbase = os.getenv("ECOPULSE_USE_HBASE", "false").lower() == "true"
    
    if use_hbase:
        try:
            from CoreCode_Enterprise.utils.hbase_reader import load_data_from_hbase
            return load_data_from_hbase()
        except Exception as e:
            logger.warning(f"HBase 连接失败，降级为 CSV 模式: {e}")
    
    # Fallback: 原有 CSV 加载逻辑
    return _load_from_csv()
```

启动时指定数据源：
```powershell
# HBase 模式
$env:ECOPULSE_USE_HBASE = "true"
streamlit run CoreCode6/app.py

# CSV 模式 (默认，向后兼容)
streamlit run CoreCode6/app.py
```

---

## 4. 文件结构规划 (New Directory Structure)

```text
EcoPulse/
├── common/                             # ← 新增: 公共模块
│   ├── __init__.py
│   ├── spark_config.py                 # 统一 Spark Session 配置
│   └── logger_config.py                # 统一日志配置
│
├── conf/                               # ← 新增: Hadoop 集群配置
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── hive-site.xml
│   └── hbase-site.xml
│
├── CoreCode_Enterprise/                # ← 新增: 企业级重构脚本
│   ├── ENTERPRISE_DESIGN_V2.md         # 本文档
│   ├── test_connectivity.py            # E1: 连通性验证
│   ├── ingest_to_hdfs.py              # E2: ODS 全量接入
│   ├── hive_ddl.sql                   # E2: Hive 建表 DDL
│   ├── etl_dwd_enterprise.py          # E3: DWD ETL (读写 Hive)
│   ├── analysis_rfm_enterprise.py     # E3: RFM 8 分群 (Spark 端)
│   ├── analysis_funnel_enterprise.py  # E3: 漏斗分析
│   ├── analysis_retention_enterprise.py # E3: 留存分析
│   ├── ml_kmeans_enterprise.py        # E3: K-Means (含 Elbow 图)
│   ├── export_to_hbase.py            # E4: ADS → HBase
│   └── utils/
│       ├── __init__.py
│       ├── hbase_writer.py            # HBase 写入工具
│       └── hbase_reader.py            # HBase 读取适配器
│
├── CoreCode/                          # 保留: Phase 1 (仅作历史参考)
├── CoreCode2/                         # 保留: Phase 2
├── CoreCode3/                         # 保留: Phase 3
├── CoreCode4/                         # 保留: Phase 4
├── CoreCode5/                         # 保留: Phase 5
├── CoreCode6/                         # 保留: Phase 6 (看板，增加 HBase 适配)
└── ...
```

---

## 5. 风险评估与应对 (Risk Management)

| # | 风险 | 概率 | 影响 | 应对策略 |
| :--- | :--- | :--- | :--- | :--- |
| R1 | VM 内存不足导致 HDFS 写入失败 | 中 | 高 | 设 HDFS 副本数为 1 (`dfs.replication=1`)，减少存储压力 |
| R2 | 54 GB CSV 上传到 HDFS 耗时过长 | 中 | 中 | 先上传 10 月 (5.4 GB) 做全流程验证，再批量上传其余 6 个月 |
| R3 | HBase Thrift Server 未启动 | 高 | 高 | 连通性测试脚本 (`test_connectivity.py`) 先行；写 README 提醒 |
| R4 | Spark on Windows 读 HDFS 性能差 | 中 | 中 | 采用存算分离：Spark Local 只做计算，大文件存取走 HDFS |
| R5 | Hive Metastore 版本不兼容 | 低 | 高 | 通过 `spark.sql()` 直接操作 Hive，不依赖 Hive CLI |
| R6 | HBase scan 全表 34 万行看板卡顿 | 中 | 高 | 加入 `st.cache_data(ttl=300)` 5 分钟缓存；或用 Filter 限制扫描范围 |

---

## 6. 验收标准 (Acceptance Criteria)

### Phase E1 验收

```text
□ ping hadoop1/2/3 全部通
□ test_connectivity.py 输出 HDFS=✅ HBase=✅
□ conf/ 目录包含 4 个 XML 配置文件
□ happybase, thrift 已安装
```

### Phase E2 验收

```text
□ hdfs dfs -ls /ecop/ods/user_behavior/ 显示 7 个月度分区目录
□ Hive: SELECT dt_month, COUNT(*) FROM ecop.ods_user_behavior GROUP BY dt_month
  返回 7 行，总量 ≈ 2 亿
□ ODS 外部表数据可被 Spark SQL 查询
```

### Phase E3 验收

```text
□ Hive DWD 表: SELECT COUNT(*) FROM ecop.dwd_user_behavior 返回 ≈ 2 亿 (去重后)
□ Hive ADS RFM 表: SELECT DISTINCT rfm_segment FROM ecop.ads_user_rfm 返回 8 行
□ Hive ADS 漏斗表: SELECT * FROM ecop.ads_funnel_stats WHERE dimension='global' 有数据
□ outputs/ml/elbow_analysis.png 生成了 K vs Silhouette 折线图
□ common/spark_config.py 被所有新脚本引用，无硬编码 JAVA_HOME
```

### Phase E4 验收

```text
□ hbase shell: scan 'ecop:user_profile', LIMIT => 5  有数据
□ hbase shell: scan 'ecop:funnel_daily', LIMIT => 5   有数据
□ hbase shell: scan 'ecop:user_retention', LIMIT => 5  有数据
□ ECOPULSE_USE_HBASE=true streamlit run CoreCode6/app.py 看板正常显示
□ ECOPULSE_USE_HBASE=false 时看板仍可 fallback 到 CSV 模式
```

---

## 7. 时间线与里程碑 (Timeline)

```text
Day 1  ──────  Day 2  ──────  Day 3-4  ──────  Day 5  ──────  Day 6
  │               │               │               │              │
Phase E1       Phase E2       Phase E3         Phase E4       文档 & 答辩准备
环境打通       ODS 接入       DWD/ADS 重跑     HBase Serving   更新 README
连通性测试     54GB→HDFS      8 分群修复       看板适配         项目时间轨迹
              Hive 建表       Elbow 图输出     双模式验证       论文素材整理
```

---

## 8. 答辩话术准备 (Defense Talking Points)

这次升级完成后，你在答辩时可以自信地说：

> **架构层面**："项目采用混合云部署 — Windows 宿主机作为计算节点运行 PySpark，VMware 三节点 Hadoop 集群提供分布式存储。数仓严格遵循 ODS → DWD → ADS → Serving 四层架构，ODS 层通过 Hive 外部表管理原始数据，DWD 和 ADS 为 Hive 管理表使用 Parquet + Snappy 压缩，Serving 层采用 HBase 提供毫秒级 KV 查询。"

> **数据治理层面**："ETL 过程增加了 Bot 流量过滤（Session 频率异常检测）和异常价格过滤，RFM 8 段精细化分群在 Spark 端一次性完成，保证了从 ADS 到 Serving 的数据一致性，避免了数据漂移。"

> **性能层面**："HBase 的 RowKey 设计采用了 MD5 前缀散列 + 预分区策略，避免了 Region 热点问题。看板数据源支持 HBase / CSV 双模式热切换，通过环境变量控制，实现了优雅降级。"

---

## 附录 A: 与评审意见的对应关系

| 评审意见 | V2 方案对应章节 | 解决程度 |
| :--- | :--- | :--- |
| 1.1 Serving 层伪分布式 | Phase E4: HBase Serving | ✅ 完全解决 |
| 1.3 ODS 层缺失 | Phase E2: ODS 全量接入 | ✅ 完全解决 |
| 1.4 Spark 配置复制粘贴 | §3.3.1 公共模块抽取 | ✅ 完全解决 |
| 2.3 RFM 分群前后矛盾 | §3.3.3 RFM 8 分群修复 | ✅ 完全解决 |
| 2.4 Elbow Method 有名无实 | Phase E3 ml_kmeans_enterprise | ✅ 完全解决 |
| 3.3 ETL 清洗深度不足 | §3.3.2 ETL DWD 重构 (Bot 过滤) | ✅ 完全解决 |
| 5.2 依赖管理混乱 | 需额外更新 requirements.txt | ⏳ 待配合 |
| 5.5 JAVA_HOME 硬编码 | §3.3.1 环境变量化 | ✅ 完全解决 |
