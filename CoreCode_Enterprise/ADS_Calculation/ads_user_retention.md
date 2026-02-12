# ADS 计算: 用户留存率 (ads_user_retention.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/ADS_Calculation/ads_user_retention.py`
*   **作用**: 计算每日用户的次日留存率 (Day 1 Retention)。

## 2. 计算逻辑
*   **DAU 定义**: 当日有任意行为记录的用户。
*   **留存逻辑**: `t1.user_id = t2.user_id` 且 `t1.dt + 1 day = t2.dt`。
*   **输出指标**:
    *   `cohort_date`: 基准日期
    *   `total_count`: 基准日活跃用户数
    *   `retention_count`: 次日仍活跃用户数
    *   `retention_rate_1d`: 次日留存率
*   **目标表**: `ecop.ads_user_retention`

## 3. 执行方式
```bash
python CoreCode_Enterprise/ADS_Calculation/ads_user_retention.py
```
