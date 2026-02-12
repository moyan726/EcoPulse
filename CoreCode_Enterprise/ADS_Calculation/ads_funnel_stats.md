# ADS 计算: 漏斗转化分析 (ads_funnel_stats.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/ADS_Calculation/ads_funnel_stats.py`
*   **作用**: 统计全站流量的转化漏斗 (浏览 -> 加购 -> 购买)。

## 2. 计算逻辑
*   **口径**: 基于 Session 粒度的转化 (即本次会话内是否发生了后续行为)。
*   **步骤**:
    1.  统计各事件类型的去重 Session 数。
    2.  计算相邻步骤的转化率 (Conversion Rate)。
*   **目标表**: `ecop.ads_funnel_stats`

## 3. 执行方式
```bash
python CoreCode_Enterprise/ADS_Calculation/ads_funnel_stats.py
```
