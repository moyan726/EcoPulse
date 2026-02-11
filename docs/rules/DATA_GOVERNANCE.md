# 数据治理约束

本文件定义项目的数据质量标准、分层规范和口径一致性要求，确保分析结论准确可靠。

---

## 1. 数据分层规范

### 1.1 分层架构

```
data/
├── raw/                  # ODS 原始层
│   └── *.csv
├── dwd/                  # DWD 明细层（清洗后）
│   └── *.parquet
├── dws/                  # DWS 汇总层（聚合表）
│   └── *.parquet
└── ads/                  # ADS 应用层（面向展示）
    └── *.parquet / *.csv
```

### 1.2 各层定义

| 层级 | 全称 | 数据状态 | 处理原则 | 存储格式 |
|------|------|----------|----------|----------|
| ODS | Operational Data Store | 原始数据 | **只读不改**，保留原始文件 | CSV |
| DWD | Data Warehouse Detail | 清洗后明细 | 类型转换、去重、**保留 NULL** | Parquet（分区） |
| DWS | Data Warehouse Summary | 聚合宽表 | 业务聚合、可填充未知值 | Parquet |
| ADS | Application Data Service | 应用结果 | 面向 BI/报表，口径明确 | Parquet/CSV |

### 1.3 分区策略（DWD 层）

```
dwd/user_behavior/
├── dt=2019-10-01/
│   ├── event_type=view/
│   ├── event_type=cart/
│   └── event_type=purchase/
├── dt=2019-10-02/
│   └── ...
```

**分区字段**：
- `dt`：日期分区，格式 `YYYY-MM-DD`，从 `event_time` 派生
- `event_type`：事件类型分区（可选，视查询模式决定）

---

## 2. 缺失值处理规范

### 2.1 DWD 层（强制保真）

| 原则 | 说明 |
|------|------|
| **禁止填充** | 不允许将 NULL 填充为 "unknown"、"other"、0 等 |
| **保留原貌** | 保持数据原始状态，便于后续灵活处理 |
| **记录统计** | 必须输出缺失率统计到 `outputs/data_quality/` |

```python
# ✅ DWD 层正确做法：保留 NULL
df_dwd = df.withColumn("brand", col("brand"))  # 不做任何填充

# ❌ DWD 层错误做法
df_dwd = df.withColumn("brand", when(col("brand").isNull(), "unknown").otherwise(col("brand")))
```

### 2.2 DWS/ADS 层（可填充，需标记）

如需填充，必须同时保留原始缺失标记：

```python
# ✅ 正确：填充的同时保留标记
df_dws = df.withColumn(
    "brand_display", 
    when(col("brand").isNull(), "未知品牌").otherwise(col("brand"))
).withColumn(
    "brand_is_null",
    col("brand").isNull().cast("int")
)
```

### 2.3 高缺失字段处理

| 缺失率 | 处理策略 |
|--------|----------|
| < 5% | 正常使用，可在 ADS 层填充 |
| 5% - 30% | 谨慎使用，需在论文中说明影响 |
| > 30% | **禁止用于关键指标**，仅作辅助分析 |
| > 50% | 考虑是否纳入分析范围，需单独说明 |

**当前数据集关键字段缺失率**：
- `category_code`：约 31.83% → 禁止作为主分析维度
- `brand`：约 14.39% → 可用，需说明影响
- `user_session`：接近 0% → 可放心使用

---

## 3. 异常值处理规范

### 3.1 价格异常

| 异常类型 | 判定标准 | 处理策略 |
|----------|----------|----------|
| 负价格 | `price < 0` | 先统计占比，若 < 0.1% 可剔除 |
| 零价格 | `price = 0` | 区分免费商品 vs 数据错误 |
| 极端高价 | `price > P99.9` | 截尾或标记 `is_outlier` |

```python
# 异常值处理示例
from pyspark.sql.functions import col, when, lit

# 计算分位数
price_p999 = df.stat.approxQuantile("price", [0.999], 0.01)[0]

# 标记异常值（推荐做法：标记而非删除）
df_marked = df.withColumn(
    "price_is_outlier",
    when(col("price") <= 0, lit(1))
    .when(col("price") > price_p999, lit(1))
    .otherwise(lit(0))
)

# 若需剔除，保留记录
outlier_count = df_marked.filter(col("price_is_outlier") == 1).count()
print(f"异常值数量: {outlier_count}, 占比: {outlier_count / df.count() * 100:.4f}%")
```

### 3.2 异常值审计输出

所有异常值处理必须输出审计报告：

```
outputs/data_quality/
├── price_outliers_summary.csv    # 价格异常统计
├── null_analysis.csv             # 缺失值分析
└── data_quality_report.json      # 综合质量报告
```

---

## 4. 口径一致性约束

### 4.1 核心指标定义

| 指标 | 口径定义 | 计算方法 | 备注 |
|------|----------|----------|------|
| **UV（独立用户数）** | 基于 `user_id` 去重 | `approx_count_distinct("user_id")` | 使用近似去重 |
| **PV（页面浏览量）** | `event_type = 'view'` 的事件数 | `count(*)` where view | - |
| **会话数** | 基于 `user_session` 去重 | `approx_count_distinct("user_session")` | 复用源端字段 |
| **购买用户数** | 发生过 purchase 的独立用户 | `countDistinct("user_id")` where purchase | - |
| **GMV** | 购买事件的价格总和 | `sum("price")` where purchase | 单位：原始货币 |

### 4.2 转化率口径（强制统一）

```python
# ✅ 标准口径：UV 基准
# 转化率 = 购买UV / 浏览UV
conversion_rate_uv = purchase_uv / view_uv

# ⚠️ 备选口径：Session 基准（需明确标注）
# Session转化率 = 发生购买的Session数 / 总Session数
conversion_rate_session = purchase_sessions / total_sessions

# ❌ 禁止混用
# 禁止在同一分析中混用 UV 和 PV 作为分母
```

### 4.3 时间口径

| 时间维度 | 定义 | 格式 |
|----------|------|------|
| 日 | 基于 `event_time` 的日期部分 | `YYYY-MM-DD` |
| 周 | ISO 周（周一为起始） | `YYYY-Www` |
| 月 | 自然月 | `YYYY-MM` |

```python
from pyspark.sql.functions import to_date, date_format, weekofyear, year

# 时间维度派生
df = df.withColumn("dt", to_date(col("event_time"))) \
       .withColumn("week", concat(year(col("dt")), lit("-W"), weekofyear(col("dt")))) \
       .withColumn("month", date_format(col("dt"), "yyyy-MM"))
```

### 4.4 会话口径（强制）

```
┌─────────────────────────────────────────────────────────────┐
│  本项目会话定义：直接复用源端 `user_session` 字段            │
│  禁止：基于时间窗口（如 30 分钟静默）进行二次会话重构       │
│  原因：降低复杂度，保持与源端口径一致                        │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. 数据血缘追溯

### 5.1 血缘记录要求

每个输出表/文件必须在 `data_dictionary.md` 中注明：

```markdown
## ads_funnel_daily

| 属性 | 值 |
|------|-----|
| 来源表 | dwd_user_behavior |
| 处理逻辑 | 按日期聚合，计算各事件类型的 UV |
| 过滤条件 | dt >= '2019-10-01' AND dt <= '2019-10-31' |
| 生成脚本 | scripts/analysis_funnel.py |
| 更新频率 | 手动执行 |
```

### 5.2 字段血缘

```markdown
## 字段来源追溯

| 目标字段 | 来源字段 | 转换逻辑 |
|----------|----------|----------|
| dt | event_time | `to_date(event_time)` |
| hour | event_time | `hour(event_time)` |
| brand_display | brand | `coalesce(brand, '未知品牌')` |
```

---

## 6. 数据质量检查点

### 6.1 DWD 层入库前检查

- [ ] 总行数与 ODS 层一致（允许 < 0.1% 的差异，需说明原因）
- [ ] 关键字段（user_id, event_time, event_type）无 NULL
- [ ] event_type 只包含预期值（view, cart, purchase, remove_from_cart）
- [ ] price 无负值（或已标记/剔除）
- [ ] 时间范围符合预期（如 2019-10-01 ~ 2019-10-31）

### 6.2 指标层输出前检查

- [ ] 转化率在合理范围（0% ~ 100%）
- [ ] UV <= PV（用户数不能大于事件数）
- [ ] 购买用户数 <= 总用户数
- [ ] 日期连续性检查（无缺失日期）

---

## 7. 禁止事项清单

| 禁止行为 | 原因 | 正确做法 |
|----------|------|----------|
| DWD 层填充 NULL 为 "unknown" | 丢失信息，影响后续分析灵活性 | 在 DWS/ADS 层填充 |
| 混用 UV 和 PV 作为转化率分母 | 口径不一致，结论不可比 | 统一使用 UV |
| 二次会话重构 | 增加复杂度，偏离源端口径 | 复用 user_session |
| 删除异常值不记录 | 无法追溯，影响复现 | 先统计、标记，再决定处理方式 |
| 指标计算不写口径注释 | 答辩时无法解释 | 每个指标必须有口径说明 |
