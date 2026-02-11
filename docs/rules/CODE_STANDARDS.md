# 代码规范约束

本文件定义项目的代码风格与编写规范，确保代码质量一致、可读性高、便于答辩展示和后续维护。

---

## 1. 文件命名规范

### 1.1 Python 脚本
- 使用 `snake_case` 命名：`eda_oct_2019.py`、`rfm_analysis.py`
- 按功能模块命名：`etl_dwd_clean.py`、`analysis_funnel.py`
- 禁止使用中文、空格、特殊字符

### 1.2 SQL 脚本
- 格式：`<层级>_<表名>.sql`
- 示例：`dwd_user_behavior.sql`、`ads_funnel_daily.sql`

### 1.3 输出文件
- 格式：`<模块>/<指标名>.<格式>`
- 示例：`outputs/eda/event_type_counts.csv`

### 1.4 配置文件
- 环境配置：`environment.yml`
- 项目配置：`config.py` 或 `config.yaml`

---

## 2. Python 代码风格

### 2.1 基本规范
- 遵循 PEP 8 风格指南
- 缩进使用 4 个空格（禁止 Tab）
- 每行最大长度 100 字符
- 文件末尾保留一个空行

### 2.2 Type Hints（强制）
```python
# ✅ 正确
def calculate_conversion_rate(views: int, purchases: int) -> float:
    return purchases / views if views > 0 else 0.0

# ❌ 错误
def calculate_conversion_rate(views, purchases):
    return purchases / views if views > 0 else 0.0
```

### 2.3 入口函数（强制）
```python
# 每个可执行脚本必须有以下结构
def main():
    """脚本主入口"""
    pass

if __name__ == "__main__":
    main()
```

### 2.4 路径处理（强制）
```python
# ✅ 正确：使用相对路径或配置
import os
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "dwd", "sample_oct_2019")

# ❌ 错误：硬编码绝对路径
DATA_PATH = "E:\\a_VibeCoding\\GraduationProject\\data\\sample.parquet"
```

### 2.5 日志输出（强制）
```python
# ✅ 正确：使用 logging 模块
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Processing started...")

# ❌ 错误：使用 print 调试
print("Processing started...")  # 禁止残留在正式代码中
```

---

## 3. PySpark 代码约束

### 3.1 链式调用限制
```python
# ✅ 正确：拆分为多个变量（超过 5 层时）
df_filtered = df.filter(col("event_type") == "purchase")
df_grouped = df_filtered.groupBy("user_id")
df_agg = df_grouped.agg(count("*").alias("purchase_count"))
df_result = df_agg.orderBy(col("purchase_count").desc())

# ❌ 避免：过长的链式调用
df_result = df.filter(...).groupBy(...).agg(...).orderBy(...).limit(...).select(...)
```

### 3.2 Schema 显式指定（强制）
```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# ✅ 正确：显式定义 Schema
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])
df = spark.read.csv(path, header=True, schema=schema)

# ❌ 禁止：使用 inferSchema（性能差，类型可能不准确）
df = spark.read.csv(path, header=True, inferSchema=True)
```

### 3.3 缓存管理（强制成对）
```python
# ✅ 正确：cache 和 unpersist 成对出现
df_cached = df.cache()
# ... 多次使用 df_cached ...
result1 = df_cached.groupBy("brand").count()
result2 = df_cached.groupBy("category_code").count()
df_cached.unpersist()  # 使用完毕后释放

# ❌ 错误：只 cache 不 unpersist
df.cache()
# ... 使用后忘记释放 ...
```

### 3.4 聚合操作注释（强制）
```python
# ✅ 正确：添加业务口径注释
# 计算用户级转化率
# 口径：purchase_uv / view_uv，同一用户多次购买只算一次
conversion_df = df.groupBy("user_id").agg(
    count(when(col("event_type") == "view", 1)).alias("view_count"),
    count(when(col("event_type") == "purchase", 1)).alias("purchase_count")
)
```

### 3.5 环境变量设置（强制）
```python
# 每个 PySpark 脚本开头必须包含
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("JAVA_HOME", r"E:\Java\jdk1.8.0_291")  # 唯一允许硬编码的路径
```

---

## 4. 注释与文档

### 4.1 模块文档（强制）
```python
"""
模块名称：EDA 探索性分析 - 2019年10月数据
作者：[姓名]
创建日期：2026-01-29
最后修改：2026-01-29

功能描述：
    对 2019 年 10 月的电商行为数据进行探索性分析，
    包括事件分布、缺失率统计、价格分位数等。

输入：
    - data/dwd/sample_oct_2019

输出：
    - outputs/eda/summary.json
    - outputs/eda/event_type_counts.csv
    - outputs/eda/missing_rates.csv

依赖：
    - PySpark 3.5.0
    - Pandas
"""
```

### 4.2 函数文档（强制）
```python
def calculate_rfm_scores(df: DataFrame, reference_date: str) -> DataFrame:
    """
    计算用户 RFM 指标。

    Args:
        df: 包含 purchase 事件的 DataFrame，必须包含字段：
            - user_id: 用户ID
            - event_time: 事件时间
            - price: 价格
        reference_date: 参考日期，格式 'YYYY-MM-DD'

    Returns:
        包含 user_id, recency, frequency, monetary 的 DataFrame

    业务口径：
        - Recency: 参考日期与最近一次购买的天数差
        - Frequency: 购买次数（基于 event 去重）
        - Monetary: 购买总金额
    """
    pass
```

### 4.3 复杂逻辑注释（强制）
```python
# 两阶段聚合加盐策略 - 解决品牌维度数据倾斜
# Step 1: 添加随机盐值，将热门品牌数据打散到多个分区
# Step 2: 第一次聚合（带盐）
# Step 3: 去盐后二次聚合
salt_range = 20
df_salted = df.withColumn("brand_salt", concat(col("brand"), lit("_"), (rand() * salt_range).cast("int")))
```

---

## 5. 禁止事项清单

| 禁止行为 | 原因 | 替代方案 |
|----------|------|----------|
| `inferSchema=True` | 性能差，需额外扫描 | 显式定义 Schema |
| 残留 `print()` 调试 | 不专业，难以追踪 | 使用 `logging` |
| 硬编码数据路径 | 不可移植 | 使用相对路径或配置 |
| 链式调用超过 5 层 | 可读性差，难调试 | 拆分变量 |
| 只 cache 不 unpersist | 内存泄漏 | 成对使用 |
| 中文变量名 | 兼容性问题 | 英文 + 中文注释 |
| 魔法数字 | 难以理解 | 定义常量并注释 |

---

## 6. 代码审查检查点

提交代码前，请确认：

- [ ] 所有函数都有 Type Hints
- [ ] 所有可执行脚本都有 `main()` 入口
- [ ] 没有残留的 `print()` 调试语句
- [ ] 没有硬编码的绝对路径（JAVA_HOME 除外）
- [ ] PySpark 代码使用显式 Schema
- [ ] 缓存操作成对出现
- [ ] 复杂逻辑有中文注释
- [ ] 聚合操作标注了业务口径
