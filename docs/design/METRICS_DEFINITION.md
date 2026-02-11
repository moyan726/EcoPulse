# 指标体系设计文档

本文件定义了第三阶段核心分析模块的计算口径与指标逻辑。

---

## 1. 转化漏斗 (Conversion Funnel)

### 1.1 漏斗步骤
1. **View (浏览)**: `event_type = 'view'`
2. **Cart (加购)**: `event_type = 'cart'`
3. **Purchase (购买)**: `event_type = 'purchase'`

### 1.2 计算口径
- **Session 维度 (推荐)**: 在同一个 `user_session` 内完成的步骤转换。反映单次访问的转化效率。
- **User 维度**: 在整个分析周期内，同一个 `user_id` 完成的步骤转换。反映用户的长周期转化价值。

### 1.3 核心指标
- **加购率 (View-to-Cart)**: Cart 数 / View 数
- **下单率 (Cart-to-Purchase)**: Purchase 数 / Cart 数
- **总转化率 (View-to-Purchase)**: Purchase 数 / View 数

---

## 2. 留存分析 (Retention Analysis)

### 2.1 留存定义
- **起始行为 (Initial Action)**: 用户在分析周期内的首次访问（`view`）或首次购买（`purchase`）。
- **回访行为 (Returning Action)**: 用户在后续天/周发生的任意行为（通常指 `view`）。

### 2.2 核心指标
- **N日留存率**: 第 N 天回访人数 / 起始行为人数
- **Cohort 矩阵**: 按周/月划分用户群组，观察其在后续生命周期内的活跃度变化。

---

## 3. RFM 用户分层模型

### 3.1 字段定义
- **Recency (R) 最近一次消费**: 距离分析基准日（数据集中最后一天）的天数。越小代表越活跃。
- **Frequency (F) 消费频率**: 在分析周期内的 `purchase` 总次数。越大代表忠诚度越高。
- **Monetary (M) 消费金额**: 在分析周期内的 `purchase` 总金额（`sum(price)`）。越大代表贡献越高。

### 3.2 评分规则
- 采用 **分位数法 (Quantiles)** 将 R/F/M 分别划分为 1-5 分。
- **分群示例**:
    - **核心客户**: R高 F高 M高
    - **流失预警**: R低 F高 M高
    - **新客户**: R高 F低 M低

---

## 4. ADS 层输出结构

### 4.1 `ads_funnel_stats`
- `dt`, `event_type`, `user_count`, `session_count`, `category_code`, `brand`

### 4.2 `ads_user_retention`
- `cohort_date`, `period`, `retention_count`, `cohort_size`

### 4.3 `ads_user_rfm`
- `user_id`, `r_score`, `f_score`, `m_score`, `rfm_segment`, `total_amount`
