# ADS 批量调度脚本 (run_all_ads.py)

## 1. 脚本定位
*   **路径**: `run_all_ads.py` (项目根目录)
*   **作用**: 作为 Phase E4 的总入口，串行调度执行所有 ADS 层计算任务。

## 2. 调度列表
1.  **RFM 模型**: `ads_user_rfm.py`
2.  **漏斗分析**: `ads_funnel_stats.py`
3.  **留存分析**: `ads_user_retention.py`

## 3. 使用方式
```bash
python run_all_ads.py
```
