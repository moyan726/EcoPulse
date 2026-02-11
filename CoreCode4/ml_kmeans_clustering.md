# ml_kmeans_clustering.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **输入数据**: `data/ads/ads_user_rfm`
*   **执行日志**: `logs/ml_kmeans_20260211.log`
*   **算法**: K-Means (K=4)
*   **特征**: Log(Recency), Log(Frequency), Log(Monetary)

### 2. 产出结果 (Outputs)
*   **输出目录**: [ads_user_clusters](file:///e:/a_VibeCoding/EcoPulse/data/ads/ads_user_clusters)
*   **聚类中心 (Centroids)**:

| Cluster | R (Days) | F (Count) | M ($) | 用户标签定义 | 运营策略 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **0** | 7.1 | **8.8** | **3415** | **SVIP (超级核心)** | 尊享服务、VIP权益、防流失 |
| **1** | **2.5** | 1.5 | 341 | **New Active (新活跃)** | 促复购、二单礼包 |
| **2** | 16.3 | 1.9 | 753 | **Potential Lost (高价值流失)** | **重点召回**、大额优惠券 |
| **3** | 17.7 | 1.1 | 118 | **Lost Cheap (低价值流失)** | 批量短信召回、低成本触达 |

### 3. 模型表现
*   **轮廓系数 (Silhouette Score)**:
    *   K=2: 0.595 (最高，但业务区分度不够)
    *   **K=4**: 0.478 (最佳平衡点，业务解释性强)
*   **结论**: K=4 成功将用户划分为特征鲜明的四个群体，特别是识别出了 **Cluster 2 (高价值流失用户)**，这是规则模型 (RFM) 难以自动发现的隐蔽群体。

---
*记录日期: 2026/2/11*
