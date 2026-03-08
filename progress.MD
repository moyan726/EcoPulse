# EcoPulse 项目变更记录

---

## 2026-02-13 09:45 — 修复 RFM 分群筛选功能

- **时间**: 2026-02-13 09:45 (UTC+8)
- **变更摘要**: 修复 User Insights 页面「筛选 RFM 分群」功能失效的问题
- **问题说明**: `data/serving/user_rfm.csv` 仅包含 `user_id, recency, frequency, monetary` 四列，缺少 `rfm_segment` 列，导致前端筛选控件无数据可选、RFM 饼图不显示。根因是 Hive 企业版的 `ads_user_rfm.py` 仅输出原始 RFM 值（无评分），而 `prepare_serving_data.py` 中的 `_resegment_rfm()` 依赖 `r_score/f_score/m_score` 列，缺失时直接跳过分群计算。
- **策略方向**: 在 `_resegment_rfm()` 增加兜底逻辑——若上游数据缺少评分列，则自动基于原始 `recency/frequency/monetary` 使用 `pd.qcut()` 分位数法打 1-5 分，再执行 8 段分群。
- **具体改动**:
  - `[修改] scripts/prepare_serving_data.py`:
    - 新增 `_qcut_score()` 辅助函数（分位数打分，支持正/反向）
    - 改写 `_resegment_rfm()`：评分列缺失时自动计算，打印评分分布日志
    - 统一分群标签名称（与前端 `2_User_Insights.py` 的 `seg_label_map` 对齐）
  - `[重新生成] data/serving/user_rfm.csv`: 347,118 行，新增 `r_score, f_score, m_score, rfm_segment, fm_score` 列
- **影响面**: 仅影响数据准备脚本和 serving CSV，不影响核心 ADS/DWD 层数据
- **风险与回滚**: 低风险。回滚方式：`git checkout scripts/prepare_serving_data.py` + 重新运行 `--force` 生成旧版 CSV
- **验证**:
  - ✅ CSV 列检查：`rfm_segment` 列存在，8 个分群全部覆盖
  - ✅ 分群分布合理：Hibernating 87,307 / Promising 59,879 / At Risk 56,913 / Champions 50,460 / Loyal 34,574 / New Customers 34,494 / Potential Loyalist 18,915 / Need Attention 4,576
  - ⏳ 看板 UI 验证：需用户手动启动看板确认筛选功能正常

---

## 2026-02-13 10:24 — 看板维度与命名优化

- **时间**: 2026-02-13 10:24 (UTC+8)
- **变更摘要**: 将看板页面进行品牌化升级，并修正关键用户指标
- **问题说明**: 
  1. 页面命名过于通用 ("Brand Analysis", "User Insights")，缺乏业务洞察感。
  2. "活跃用户数" 指标定义模糊，实际展示的是 "付费用户数"，与漏斗中的真实活跃流量 (UV, 6.2M) 差距较大，容易误导决策。
- **策略方向**: 
  - **Rebranding**: 采用更具行动导向的命名 ("Market & Brand Performance", "Customer Value & Retention")。
  - **Metrics Upgrade**: 引入 "全站活跃用户 (Active Users)" 和 "付费渗透率 (Penetration Rate)"，构建完整的流量转化视图。
- **具体改动**:
  - `[修改] CoreCode6/pages/1_Brand_Analysis.py`: 页面标题更新为 `🚀 Market & Brand Performance`。
  - `[修改] CoreCode6/pages/2_User_Insights.py`: 
    - 页面标题更新为 `💎 Customer Value & Retention`。
    - KPI 模块重构：新增全站 UV (6.2M)、付费客户数 (697K)、渗透率 (11.2%)、购买频次。
    - 修复了文件头部可能存在的导入复制问题。
- **影响面**: 看板 UI 及指标展示，不影响底层数据计算
- **风险与回滚**: 低风险。如需回滚，可使用 git checkout 恢复相关 .py 文件。
- **验证**: 
  - ✅ 代码静态语法检查通过 (py_compile)
  - ✅ KPI 逻辑验证：Active Users (View Count) > Paying Users (Purchase Count)
  - ⏳ 看板 UI 验证：需用户刷新浏览器查看效果

---

## 2026-02-13 10:35 — KPI 数据修正与聚类雷达图交互增强

- **时间**: 2026-02-13 10:35 (UTC+8)
- **变更摘要**: 修正活跃用户数虚高问题，并将聚类雷达图改为可交互选择模式
- **问题说明**:
  1. "全站活跃用户" 显示 3048万，远超用户预期的 2300万（PV）。根因：`sum(user_count)` 错误累加了 `global` + `daily` + `brand` 多个维度的行。
  2. K-Means 雷达图仅展示"最佳群体"，各项指标均满分（六边形战士），缺乏区分度和参考价值。
- **策略方向**:
  - KPI：改为从 `funnel_stats.csv` 的 `dimension='global'` 行精确取值，避免跨维度重复累加。
  - 雷达图：移除 `_best_cluster_radar()`，新增 `_build_all_cluster_radars()` + `st.selectbox` 下拉选择。
- **具体改动**:
  - `[修改] CoreCode6/pages/2_User_Insights.py`:
    - KPI 第一卡片改为"总访问量 (PV)"（23,005,586），取自 Global view 的 `session_count`。
    - 新增 `_build_all_cluster_radars()` 函数，计算所有 Cluster 的归一化特征值。
    - 雷达图区域增加 `st.selectbox`，用户可选择 Cluster 0-3 分别查看画像。
    - 归一化基于全局 min/max（而非 Cluster 内部），确保跨 Cluster 可比较。
- **影响面**: 仅影响看板 UI 展示逻辑，不影响底层数据
- **风险与回滚**: 低风险。`git checkout CoreCode6/pages/2_User_Insights.py` 即可回退。
- **验证**:
  - ✅ 代码语法检查通过 (py_compile)
  - ✅ PV 取值逻辑：`global` 行 `session_count` = 23,005,586（符合用户预期）
  - ⏳ 看板 UI 验证：需用户刷新浏览器查看效果
