# verify_ads.py 运行结果分析

---

### 1. 分析来源 (Analysis Source)
*   **输入数据**: 
    *   `data/ads/ads_funnel_stats`
    *   `data/ads/ads_user_retention`
    *   `data/ads/ads_user_rfm`
*   **执行日志**: `logs/verify_ads_20260211.log`
*   **校验目的**: 确保 ADS 层各主题表的数据逻辑自洽性与完整性。

### 2. 产出结果 (Outputs)
*   **校验报告**: [outputs/ads_verification.txt](file:///e:/a_VibeCoding/EcoPulse/outputs/ads_verification.txt)
*   **验证状态**: **PASS (通过)**

### 3. 核心校验点 (Key Verification Points)
1.  **数据一致性 (Consistency)**:
    *   漏斗模型中的购买用户数 (`347,118`) 与 RFM 模型中的总用户数 (`302,643 + 44,475 = 347,118`) **完全一致**。这证明了不同分析脚本处理逻辑的严谨性。
2.  **业务逻辑合理性 (Rationality)**:
    *   留存率计算正确：所有 Cohort 的 Day 0 留存率均为 1.0 (100%)。
    *   漏斗维度完整：Global/Daily/Brand 维度均有数据产出。
3.  **异常监控**:
    *   未发现空文件或 Schema 错乱问题。

---
*记录日期: 2026/2/11*
