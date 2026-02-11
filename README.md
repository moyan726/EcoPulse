<h1 align="center">
  # EcoPulse: Intelligent Ecommerce Analytics Platform
  <br>
  EcoPulse> <b>基于 Spark 的亿级电商用户行为深度分析与可视化系统</b>
</h1>

<p align="center">
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License">
  <img src="https://img.shields.io/badge/python-3.8+-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/PySpark-3.5.3-E25A1C?logo=apachespark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Streamlit-1.41+-FF4B4B?logo=streamlit&logoColor=white" alt="Streamlit">
  <img src="https://img.shields.io/badge/Plotly-5.20+-3F4F75?logo=plotly&logoColor=white" alt="Plotly">
</p>

## 项目简介

EcoPulse 是一个端到端的电商数据智能分析平台，旨在通过大数据技术解决海量点击流数据的价值挖掘难题。项目采用经典的数仓分层架构，实现了从原始日志清洗、用户画像建模（RFM/K-Means）到交互式可视化看板的全链路闭环，为业务决策提供精准的数据支撑。

面向 **REES46 多品类电商点击流数据集**（2019-10 至 2020-04，总量超 2 亿条记录），EcoPulse 实现了从原始日志清洗、数仓分层建设、用户画像建模到交互式可视化看板的 **100% Python 全链路闭环**。

---

## 🌟 核心特性 (Key Features)

### 1. 深度用户洞察

- **多维用户画像**: 基于 **RFM 模型** (Recency, Frequency, Monetary) 将用户精准划分为 8 类价值分层（如 Champions, At Risk）。
- **智能聚类分析**: 集成 **K-Means 算法**，自动识别用户群体的潜在特征，辅助精细化运营。
- **留存热力图**: 自动计算 Cohort Analysis（同期群分析），直观展示用户生命周期内的留存衰减。

### 2. 全链路转化分析

- **漏斗模型 (Funnel)**: 追踪 `View -> Cart -> Purchase` 核心路径，精准定位流失节点。
- **归因分析**: 支持按品牌、品类等多维度下钻，识别高转化与低转化特征。

### 3. 高性能架构

- **分布式计算**: 基于 **PySpark** 引擎，支持亿级数据的高效清洗与聚合。
- **列式存储优化**: 采用 **Parquet (Snappy)** 格式，查询性能较传统 CSV 提升 10 倍以上。
- **工程化看板**: 采用 **CoreCode6** 前后端分离架构，通过 GPU 硬件加速与客户端渲染技术，实现 60FPS 流畅交互。

#### 核心能力指标

| 能力 | 说明 |
| :--- | :--- |
| **数仓分层架构** | Raw → DWD → ADS → Serving 四层流转，Parquet 列式存储，查询性能较 CSV 提升 **10-17 倍** |
| **RFM 用户分层** | 基于 Recency / Frequency / Monetary 三维打分，划分为 **8 类** 精细化用户群体 |
| **K-Means 聚类** | 对 RFM 特征做 Log 转换 + 标准化后无监督聚类，轮廓系数 0.478，识别出 4 大核心客群 |
| **转化漏斗分析** | View → Cart → Purchase 全链路追踪，支持品牌/品类多维下钻 |
| **Cohort 留存分析** | 按首购日期分组，计算次日/3 日/7 日留存率，量化用户生命周期粘性 |
| **数据倾斜治理** | 两阶段加盐聚合 (Salting) 技术，热点 Key 场景性能提升 **16.3%** |
| **动画可视化看板** | requestAnimationFrame 驱动的数值递增、雷达辐射、柱状生长动画引擎 |

---

## 🏗️ 系统架构 (System Architecture)

EcoPulse 遵循标准的数仓分层设计，确保数据流转的规范性与可维护性：

```mermaid
graph LR
    Raw[Raw Data (CSV)] --> ODS[ODS Layer]
    ODS --> ETL((Spark ETL))
    ETL --> DWD[DWD Layer (Parquet)]
    DWD --> DWS[DWS Layer (Aggregated)]
    DWS --> ADS[ADS Layer (Metrics)]
    ADS --> Serving[Serving Layer (CSV)]
    Serving --> Dashboard[Streamlit Dashboard]
```

```text
┌──────────────────────────────────────────────────────────────────┐
│                        EcoPulse 数据流转架构                      │
├─────────┬─────────┬──────────┬──────────┬────────────────────────┤
│  Raw    │  DWD    │   ADS    │ Serving  │      Dashboard         │
│  Layer  │  Layer  │  Layer   │  Layer   │      (CoreCode6)       │
│         │         │          │          │                        │
│ CSV     │Parquet  │ Parquet  │  CSV     │  Streamlit + Plotly    │
│ 原始日志 │去重/清洗 │RFM/漏斗  │ 轻量导出  │  动画引擎 + 深色主题    │
│         │分区存储  │留存/聚类  │          │                        │
│ 2亿+条  │4200万条 │ 34.7万   │ 69.5万行 │  3 页交互看板           │
│         │         │  用户    │ 0.31s    │                        │
└────┬────┴────┬────┴────┬────┴────┬─────┴────────────────────────┘
     │         │         │         │
  CoreCode  CoreCode2  CoreCode3  scripts/
  Phase 1   Phase 2    Phase 3   prepare_serving_data.py
  数据探索   ETL清洗    指标计算
            CoreCode4  CoreCode5
            Phase 4    Phase 5
            ML+优化    导出原型
```

| 层级 | 目录 | 描述 | 技术栈 |
| :--- | :--- | :--- | :--- |
| **Raw** | `data/raw/` | 原始电商点击流日志 | CSV |
| **DWD** | `data/dwd/` | 清洗后的明细数据（去重/类型转换） | Parquet |
| **ADS** | `data/ads/` | 业务指标结果（漏斗/留存/RFM） | Parquet |
| **Serving** | `data/serving/` | 面向看板的轻量级结果集 | CSV (Optimized) |
| **View** | `CoreCode6/` | 交互式可视化前台 | Streamlit + Plotly |

---

## 🚀 快速开始 (Quick Start)

### 前置要求

- Windows 10/11 (推荐) 或 Linux/macOS
- Python 3.8+
- Java JDK 1.8+ (用于 Spark)

### 1. 环境初始化

项目提供了自动化的环境配置脚本。在项目根目录下打开终端（PowerShell）：

```powershell
# 激活开发环境（自动设置 JAVA_HOME 和 Python 虚拟环境）
.\scripts\dev_shell.ps1
```

### 2. 数据准备

如果 `data/serving` 目录为空，请运行以下命令生成看板所需数据：

```bash
python scripts/prepare_serving_data.py
```

> 该脚本会自动对 RFM 数据执行 8 段精细化重分群（Champions / Potential Loyalist / Promising / New Customers / Loyal / Need Attention / At Risk / Hibernating）。

### 3. 启动看板

**方式一** — 双击根目录下的 `启动看板.bat`。

**方式二** — 命令行启动：

```bash
streamlit run CoreCode6/app.py
```

访问浏览器 [http://localhost:8501](http://localhost:8501) 即可体验。

### 1. 克隆与初始化

```powershell
git clone https://github.com/moyan726/EcoPulse.git
cd EcoPulse

# 创建虚拟环境
python -m venv .venv
```

```powershell
# 激活环境 (PowerShell)
.\scripts\dev_shell.ps1

# 安装依赖
pip install -r requirements.txt
```

---

## 📂 项目结构 (Project Structure)

```text
EcoPulse/
├── CoreCode/                  # Phase 1: 数据探索与采样
│   ├── sample_data.py         #   CSV → Parquet 高性能采样
│   ├── eda_analysis.py        #   探索性数据分析 (行为分布/缺失率/价格分位)
│   ├── data_quality.py        #   数据质量审计 (异常值/分布倾斜)
│   └── test_spark.py          #   Spark 环境验证
│
├── CoreCode2/                 # Phase 2: ETL 清洗与数仓构建
│   ├── etl_dwd_user_behavior.py  # DWD 明细层 (去重/异常标记/日期分区)
│   └── benchmark_storage.py      # CSV vs Parquet 性能基准
│
├── CoreCode3/                 # Phase 3: 业务指标计算 (ADS 层)
│   ├── analysis_rfm.py        #   RFM 用户分层模型
│   ├── analysis_funnel.py     #   View → Cart → Purchase 转化漏斗
│   ├── analysis_retention.py  #   Cohort 留存分析
│   └── verify_ads.py          #   ADS 层交叉验证
│
├── CoreCode4/                 # Phase 4: 机器学习与性能优化
│   ├── ml_kmeans_clustering.py   # K-Means 用户聚类 (K=4, Silhouette=0.478)
│   ├── perf_skew_optimization.py # 数据倾斜治理 (两阶段加盐聚合)
│   └── benchmark_storage.py      # Phase 4 存储性能复测
│
├── CoreCode5/                 # Phase 5: Serving 层导出
│   ├── export_to_serving.py   #   Spark 批量导出 ADS → CSV
│   └── dashboard_app.py       #   早期看板原型
│
├── CoreCode6/                 # Phase 6: 工程化可视化看板 [当前主力]
│   ├── app.py                 #   主入口 — Overview 总览页
│   ├── pages/
│   │   ├── 1_Brand_Analysis.py   # 品牌深度分析页
│   │   └── 2_User_Insights.py    # 用户画像洞察页
│   ├── src/dashboard/
│   │   ├── animations.py      #   JS 动画引擎 (requestAnimationFrame)
│   │   └── utils.py           #   CSS/数据加载/Plotly 主题
│   ├── benchmark/
│   │   └── benchmark_corecode6.py  # 数据完整性 & 性能验证
│   └── requirements.txt       #   看板依赖清单
│
├── data/
│   ├── row/                   # 原始 CSV (7 个月度文件, 2 亿+条)
│   ├── dwd/                   # DWD 明细层 (Parquet 分区)
│   ├── ads/                   # ADS 指标层 (Parquet)
│   └── serving/               # Serving 层 (CSV, 供看板读取)
│       ├── funnel_stats.csv   #   漏斗统计 (154 行)
│       ├── user_retention.csv #   用户留存 (496 行)
│       ├── user_rfm.csv       #   RFM 分层 (347,118 行, 8 段分群)
│       └── user_clusters.csv  #   K-Means 聚类 (347,118 行, 4 簇)
│
├── scripts/
│   ├── dev_shell.ps1          # 环境激活 (JAVA_HOME + venv)
│   ├── prepare_serving_data.py # ADS Parquet → Serving CSV (含 RFM 重分群)
│   └── count_data_rows.py     # 多进程大文件行数统计
│
├── docs/                      # 项目文档中心
│   ├── design/                #   架构设计 / 指标定义 / 算法原理
│   ├── guides/                #   操作指南 / 性能调优
│   └── rules/                 #   数据治理规范
│
├── preparedoc/                # 学术规划文档 (选题/开题/论文框架)
├── ProjectDescription/        # 项目名称与数据说明
├── 启动看板.bat                # 一键启动看板
├── 项目时间轨迹.md             # 开发时间线
├── 项目开发总进程说明.md        # 项目总览与目录索引
├── requirements.txt           # 全局 Python 依赖
└── environment.yml            # Conda 环境配置
```

---

## 关键性能指标

| 指标 | 数值 |
| :--- | :--- |
| 原始数据总量 | **2 亿+** 条点击流记录 (7 个月度 CSV) |
| DWD 层处理量 | **4,241.8 万** 条 (去重 3 万 / 标记异常 6.8 万) |
| Parquet vs CSV 单列聚合 | 快 **3.6 倍** (1.22s vs 4.40s) |
| Parquet vs CSV 条件过滤 | 快 **17.5 倍** (0.24s vs 4.21s) |
| 数据倾斜优化 (Salting) | 性能提升 **16.3%** (4.45s → 3.73s) |
| K-Means 最佳轮廓系数 | **0.478** (K=4) |
| Serving 数据加载 (4 CSV) | **69.5 万行 / 0.31 秒** |
| RFM 用户分群 | **8 段** (Champions → Hibernating) |
| 全站转化率 (Session 口径) | **6.8%** |
| 次日留存率 | **17.3%** |

---

## 技术栈

| 类别 | 技术 |
| :--- | :--- |
| 分布式计算 | PySpark 3.5.3 |
| 数据存储 | Parquet (Snappy) + 日期分区 |
| 机器学习 | Spark MLlib (K-Means / QuantileDiscretizer) |
| 可视化框架 | Streamlit ≥ 1.41 + Plotly ≥ 5.20 |
| 动画引擎 | 原生 JavaScript (requestAnimationFrame) |
| 数据处理 | Pandas ≥ 2.0 + PyArrow ≥ 17.0 |
| 环境管理 | Python venv / Conda |
| 版本控制 | Git + GitHub |

---

## 开发阶段

| 阶段 | 目录 | 核心产出 |
| :--- | :--- | :--- |
| Phase 1 — 数据摸底 | `CoreCode/` | 数据采样 + EDA 画像 + 质量审计 |
| Phase 2 — ETL 清洗 | `CoreCode2/` | DWD 明细层 + 存储性能基准 |
| Phase 3 — 指标计算 | `CoreCode3/` | RFM 分层 + 转化漏斗 + 留存分析 + ADS 校验 |
| Phase 4 — ML 优化 | `CoreCode4/` | K-Means 聚类 + 数据倾斜治理 |
| Phase 5 — 数据导出 | `CoreCode5/` | Serving 层自动化导出 |
| Phase 6 — 可视化 | `CoreCode6/` | 工程化交互看板 (动画引擎 + 深色主题) |

---

## 📊 可视化展示 (Dashboard Preview)

EcoPulse 的看板设计遵循 "Executive Dashboard" 理念，提供沉浸式的数据体验：

*   **总览页 (Overview)**: 实时 KPI 监控、流量趋势追踪、Top 品牌排行。
*   **转化页 (Funnel)**: 全局漏斗分析、各环节转化率雷达图。
*   **用户页 (User Insights)**: 3D 用户聚类视图、RFM 价值分层环形图。

> **技术亮点**: 看板采用了 React 风格的组件化开发，内置了 GPU 加速的数字递增动画与响应式布局，完美适配不同尺寸屏幕。

### 看板页面

| 页面 | 定位 | 核心组件 |
| :--- | :--- | :--- |
| **Overview (总览)** | CEO / 管理层视角 | 四大 KPI 动画卡片 (PV / UV / 订单 / 转化率) + 每日行为趋势折线图 |
| **Brand Analysis (品牌分析)** | 市场 / 品牌运营视角 | 品牌转化漏斗 + 流失率标注 + 竞品销量高亮对比柱状图 |
| **User Insights (用户洞察)** | CRM / 数据分析师视角 | RFM 8 段价值分层环形图 + K-Means 聚类雷达图 + Cohort 留存热力图 + 明细数据网格 |

**动画引擎特性**：

- `animated_number` — 数值从 0 递增至目标值，支持百分比/整数/浮点格式
- `animated_radar` — 雷达图中心辐射展开，数据点依次出现
- `animated_bar_chart` — 柱状图自底向上生长 + 可选弹跳效果 + 品牌高亮
- 全局控制面板 — 右下角齿轮图标，可调速度/延迟/帧率/reduced-motion 适配

---

## 🛠️ 贡献与维护 (Contribution)

本项目由 **moyan726** 维护。欢迎提交 Issue 或 Pull Request。

*   **Bug 反馈**: 请附上详细的复现步骤与日志。
*   **功能建议**: 欢迎在 Discussion 区讨论新的分析模型。

---

## 许可证

本项目采用 [MIT License](LICENSE) 开源。

**EcoPulse** © 2026. All Rights Reserved.
