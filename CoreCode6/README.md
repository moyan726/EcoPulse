# EcoPulse · Phase 6：动态交互式数据看板

## 概述

CoreCode6 是 EcoPulse 项目的**可视化展示层**，构建了一个动静结合、深度交互的电商数据决策中心。

## 架构

```
CoreCode6/
├── app.py                              # 主入口 — 核心指标总览 (CEO 驾驶舱)
├── pages/
│   ├── 1_🏢_Brand_Analysis.py          # 品牌深度分析
│   ├── 2_👥_User_Insights.py           # 用户画像洞察
│   └── 3_🎬_Animations_Lab.py          # 动画演示实验室
├── src/
│   ├── __init__.py
│   └── dashboard/
│       ├── __init__.py
│       ├── animations.py               # 动画引擎 (JS requestAnimationFrame)
│       └── utils.py                    # 样式/主题/数据加载
├── requirements.txt
└── README.md
```

## 数据依赖

看板读取 `data/serving/` 下的 4 个 CSV 文件（由 Phase 5 生成）：

| 文件 | 说明 |
|------|------|
| `funnel_stats.csv` | 转化漏斗统计（全局/日维度/品牌维度） |
| `user_retention.csv` | 用户留存数据 |
| `user_rfm.csv` | RFM 用户价值分层 |
| `user_clusters.csv` | K-Means 聚类结果 |

## 快速启动

```bash
# 方式 1：使用项目根目录的一键脚本
双击 启动看板.bat

# 方式 2：手动启动
cd EcoPulse
.venv\Scripts\activate
streamlit run CoreCode6\app.py
```

## 核心功能

### 动画系统
- **数值递增动画**：KPI 数字从 0 滚动至目标值，支持千分位格式化
- **雷达图辐射动画**：SVG 绘制，数据多边形从中心向外辐射展开
- **柱状图生长动画**：柱子从底部向上生长，可选 Bounce 回弹效果
- **全局动画控制面板**：右下角悬浮设置按钮，支持开关/速度/延迟/fps/reduced-motion

### 页面
1. **核心指标总览** — PV/UV/订单数/转化率 + 每日行为趋势折线图
2. **品牌深度分析** — 品牌筛选 + 漏斗流失率 + 竞品柱状图（高亮对比）
3. **用户画像洞察** — RFM 环形图 + K-Means 雷达图 + 留存热力图 + 明细查询
4. **动画演示实验室** — 所有动画组件独立展示 + 重播按钮

## 设计规范

- **配色**：深色模式 (`#0c1929`)，强调色 `#00d4ff`(青蓝) / `#00ff88`(绿) / `#ff9500`(橙) / `#a855f7`(紫)
- **无障碍**：WCAG AA 对比度 4.5:1+，respect prefers-reduced-motion
- **性能**：CSS will-change + GPU 加速 + requestAnimationFrame 帧率限流
