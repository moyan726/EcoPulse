# analysis_funnel.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **输入数据**: `data/dwd/user_behavior` (DWD 明细层)
*   **执行日志**: `logs/analysis_funnel_20260211.log`
*   **分析维度**: 全站 (Global)、按日 (Daily)、按品牌 (Brand)。

### 2. 产出结果 (Outputs)
*   **输出目录**: [ads_funnel_stats](file:///e:/a_VibeCoding/EcoPulse/data/ads/ads_funnel_stats)
*   **存储格式**: Parquet (Snappy 压缩)
*   **核心指标**: Session Count (会话数), User Count (人数)。

### 3. 全站漏斗深度解析 (Global Funnel)
| 阶段 (Event Type) | 会话数 (Sessions) | 用户数 (Users) | 转化率 (Session Based) | 转化率 (User Based) |
| :--- | :--- | :--- | :--- | :--- |
| **View (浏览)** | 9,242,653 | 3,022,130 | 100% | 100% |
| **Cart (加购)** | 573,097 | 337,117 | 6.2% | 11.1% |
| **Purchase (购买)** | 629,560 | 347,118 | **6.8%** | **11.5%** |

### 4. 关键业务洞察
1.  **"倒挂"现象**: 购买数 (62.9万) 高于加购数 (57.3万)。
    *   *解释*: 这表明平台存在大量的 **"Direct Buy" (直接购买)** 行为，用户跳过了购物车环节。
    *   *启示*: 平台的“一键下单”路径体验良好，建议在 UI 设计上继续强化“立即购买”按钮的权重。
2.  **转化率**: 从浏览到购买的整体转化率达到了 **6.8%** (Session口径)，这是一个相当健康的电商指标（行业平均通常在 2%-5%）。

---
*记录日期: 2026/2/11*
