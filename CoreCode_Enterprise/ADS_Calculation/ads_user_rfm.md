# ADS 计算: 用户 RFM 模型 (ads_user_rfm.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/ADS_Calculation/ads_user_rfm.py`
*   **作用**: 基于 DWD 层购买记录，计算用户的 Recency (最近购买), Frequency (频次), Monetary (金额) 指标。

## 2. 计算逻辑
*   **数据源**: `ecop.dwd_user_behavior` (过滤 `event_type = 'purchase'`)
*   **指标定义**:
    *   **Recency**: 分析日期 (2020-05-01) - 最近一次购买日期
    *   **Frequency**: 购买次数 (`count`)
    *   **Monetary**: 消费总金额 (`sum(price)`)
*   **目标表**: `ecop.ads_user_rfm`

## 3. 执行方式
```bash
python CoreCode_Enterprise/ADS_Calculation/ads_user_rfm.py
```
