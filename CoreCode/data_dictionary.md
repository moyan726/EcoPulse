# 数据字典 (Data Dictionary)

本项目使用公开电商行为日志数据集，以下是详细的字段结构与业务含义说明。

---

## 1. 核心字段结构

数据集包含 9 个核心字段，每一行代表一次用户与商品之间的交互事件。

| 字段名          | 类型             | 说明                                      |
| --------------- | ---------------- | ----------------------------------------- |
| `event_time`    | string/timestamp | 行为发生时间（UTC 格式）                  |
| `event_type`    | string           | 行为类型（`view`: 浏览, `cart`: 加购, `purchase`: 购买） |
| `product_id`    | int64            | 商品唯一标识 ID                           |
| `category_id`   | int64            | 类目唯一标识 ID                           |
| `category_code` | string           | 类目层级路径（如 `electronics.smartphone`，可能为空） |
| `brand`         | string           | 品牌名称（可能为空）                       |
| `price`         | float            | 商品单价                                  |
| `user_id`       | int64            | 用户唯一标识 ID                           |
| `user_session`  | string           | 会话标识 ID（用于会话级行为分析）          |

---

## 2. 业务分析口径

- **数据粒度**：单次点击事件级。
- **行为漏斗**：分析流程遵循 `view -> cart -> purchase` 的转化路径。
- **采样策略**：为保证性能，第一阶段分析采用 2019 年 10 月的样本数据。

---

## 3. 数据来源

- **平台**：Hugging Face / REES46
- **数据集名称**：eCommerce behavior data from multi category store
- **文件规模**：约 2.85 亿条记录（全量）

---
*更新日期：2026/2/11*
