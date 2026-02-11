# 输出物规范约束

本文件定义项目所有输出文件的命名规范、目录结构和格式要求，确保输出可追溯、格式统一、便于论文引用。

---

## 1. 目录结构

```
outputs/
├── eda/                    # 探索性分析结果
│   ├── summary.json
│   ├── event_type_counts.csv
│   ├── missing_rates.csv
│   └── price_hist.png
│
├── data_quality/           # 数据质量报告
│   ├── null_analysis.csv
│   ├── outlier_summary.csv
│   └── quality_report.json
│
├── benchmark/              # 性能对比实验
│   ├── csv_vs_parquet.csv
│   ├── skew_optimization.csv
│   └── benchmark_summary.json
│
├── funnel/                 # 漏斗分析
│   ├── funnel_daily.csv
│   ├── funnel_by_category.csv
│   └── funnel_summary.json
│
├── retention/              # 留存分析
│   ├── cohort_matrix.csv
│   ├── retention_weekly.csv
│   └── retention_summary.json
│
├── rfm/                    # RFM 分层
│   ├── rfm_scores.csv
│   ├── rfm_clusters.csv
│   ├── kmeans_elbow.png
│   └── rfm_summary.json
│
└── figures/                # 论文用图表
    ├── fig_3_1_architecture.png
    ├── fig_4_1_funnel_chart.png
    └── fig_5_1_cohort_heatmap.png

logs/
├── sample_data_20260129.log
├── eda_oct_2019_20260129.log
└── etl_dwd_20260129.log
```

---

## 2. 文件命名规范

### 2.1 数据文件

| 类型 | 命名格式 | 示例 |
|------|----------|------|
| 指标结果 | `<指标名>.csv` | `event_type_counts.csv` |
| 带日期范围 | `<指标名>_<日期范围>.csv` | `funnel_2019_10.csv` |
| 聚合汇总 | `<指标名>_summary.json` | `rfm_summary.json` |
| 中间结果 | `<模块>_<步骤>_temp.parquet` | `etl_step1_temp.parquet` |

### 2.2 图表文件

| 类型 | 命名格式 | 示例 |
|------|----------|------|
| 论文图表 | `fig_<章节>_<序号>_<描述>.png` | `fig_3_1_data_architecture.png` |
| 分析图表 | `<模块>_<图表类型>.png` | `rfm_cluster_scatter.png` |
| Spark UI 截图 | `sparkui_<stage>_<描述>.png` | `sparkui_stage5_skew_before.png` |

### 2.3 日志文件

| 类型 | 命名格式 | 示例 |
|------|----------|------|
| 脚本日志 | `<脚本名>_<YYYYMMDD>.log` | `eda_oct_2019_20260129.log` |
| 实验日志 | `exp_<实验名>_<YYYYMMDD>.log` | `exp_kmeans_k5_20260129.log` |

---

## 3. CSV 输出规范

### 3.1 基本要求

| 属性 | 要求 |
|------|------|
| 编码 | `utf-8-sig`（带 BOM，避免 Excel 乱码） |
| 分隔符 | 逗号 `,` |
| 表头 | **必须包含** |
| 引号 | 字符串字段包含逗号时使用双引号 |

### 3.2 数值格式

| 类型 | 格式要求 | 示例 |
|------|----------|------|
| 整数 | 无小数点 | `42112054` |
| 小数 | 最多 4 位小数 | `0.3183` |
| 百分比 | 0-1 之间的小数，不带 % 符号 | `0.0175`（表示 1.75%） |
| 金额 | 2 位小数 | `489.07` |

### 3.3 代码示例

```python
import pandas as pd

# ✅ 正确的 CSV 输出
df.to_csv(
    "outputs/eda/event_type_counts.csv",
    index=False,
    encoding="utf-8-sig",
    float_format="%.4f"
)

# ❌ 错误做法
df.to_csv("output.csv")  # 缺少编码设置
```

---

## 4. JSON 输出规范

### 4.1 基本要求

| 属性 | 要求 |
|------|------|
| 编码 | `utf-8` |
| 中文 | `ensure_ascii=False` |
| 格式化 | `indent=2` |
| 时间戳 | **必须包含** `generated_at` 字段 |

### 4.2 标准结构

```json
{
  "generated_at": "2026-01-29T14:30:00",
  "script": "eda_oct_2019.py",
  "input": {
    "path": "data/dwd/sample_oct_2019",
    "row_count": 42112054
  },
  "env": {
    "python": "3.11.0",
    "spark_version": "3.5.0"
  },
  "results": {
    "key1": "value1",
    "key2": "value2"
  },
  "notes": "可选的备注信息"
}
```

### 4.3 代码示例

```python
import json
from datetime import datetime

def save_json(path: str, payload: dict):
    """保存 JSON 文件，自动添加时间戳"""
    payload["generated_at"] = datetime.now().isoformat(timespec="seconds")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# 使用示例
summary = {"row_count": 42112054, "uv": 3134159}
save_json("outputs/eda/summary.json", summary)
```

---

## 5. 图表输出规范

### 5.1 基本要求

| 属性 | 要求 |
|------|------|
| 分辨率 | 至少 300 DPI |
| 格式 | PNG（位图）、PDF（矢量图，推荐） |
| 尺寸 | 宽度 8-12 英寸，高度按比例 |
| 字体大小 | 标题 14pt，轴标签 12pt，刻度 10pt |

### 5.2 必须包含元素

- [ ] **标题**：清晰描述图表内容
- [ ] **坐标轴标签**：含单位（如有）
- [ ] **图例**：多系列时必须有
- [ ] **数据来源**：可在标题或脚注中注明

### 5.3 中文字体配置

```python
import matplotlib.pyplot as plt

# 设置中文字体（Windows）
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 设置 DPI
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
```

### 5.4 完整示例

```python
import matplotlib.pyplot as plt
import seaborn as sns

# 配置
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 创建图表
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=df, x='event_type', y='count', ax=ax)

# 添加元素
ax.set_title('2019年10月事件类型分布', fontsize=14, fontweight='bold')
ax.set_xlabel('事件类型', fontsize=12)
ax.set_ylabel('事件数量', fontsize=12)

# 添加数据标签
for p in ax.patches:
    ax.annotate(f'{int(p.get_height()):,}', 
                (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='bottom', fontsize=10)

# 保存
plt.tight_layout()
plt.savefig('outputs/figures/fig_3_1_event_distribution.png', 
            dpi=300, bbox_inches='tight')
plt.close()
```

---

## 6. 论文引用规范

### 6.1 图表编号规则

| 类型 | 编号格式 | 示例 |
|------|----------|------|
| 图 | `图 <章节>-<序号>` | 图 3-1、图 4-2 |
| 表 | `表 <章节>-<序号>` | 表 3-1、表 4-2 |
| 公式 | `(<章节>.<序号>)` | (3.1)、(4.2) |

### 6.2 图表目录对照表

在 `outputs/figures/` 目录下维护一个索引文件：

```markdown
# 论文图表索引

| 论文编号 | 文件名 | 描述 | 所在章节 |
|----------|--------|------|----------|
| 图 3-1 | fig_3_1_architecture.png | 系统总体架构图 | 第3章 系统设计 |
| 图 4-1 | fig_4_1_funnel_chart.png | 转化漏斗分析图 | 第4章 核心分析 |
| 图 4-2 | fig_4_2_cohort_heatmap.png | 留存热力图 | 第4章 核心分析 |
| 图 5-1 | fig_5_1_kmeans_elbow.png | K-Means 肘部图 | 第5章 高级建模 |
```

---

## 7. 日志输出规范

### 7.1 日志配置

```python
import logging
import os
from datetime import datetime

def setup_logger(script_name: str) -> logging.Logger:
    """配置日志记录器"""
    os.makedirs("logs", exist_ok=True)
    
    log_file = f"logs/{script_name}_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(script_name)

# 使用示例
logger = setup_logger("eda_oct_2019")
logger.info("开始执行 EDA 分析...")
```

### 7.2 日志内容要求

```
2026-01-29 14:30:00 - INFO - 开始执行 EDA 分析...
2026-01-29 14:30:01 - INFO - 输入文件: data/dwd/sample_oct_2019
2026-01-29 14:30:05 - INFO - 数据行数: 42,112,054
2026-01-29 14:30:10 - INFO - 近似 UV: 3,134,159
2026-01-29 14:30:15 - WARNING - brand 字段缺失率: 14.39%
2026-01-29 14:30:20 - INFO - 结果已保存至: outputs/eda/summary.json
2026-01-29 14:30:20 - INFO - 执行完成，耗时: 20.5 秒
```

---

## 8. 输出检查清单

### 8.1 每次输出前检查

- [ ] 文件名符合命名规范
- [ ] 存放在正确的目录下
- [ ] CSV 使用 `utf-8-sig` 编码
- [ ] JSON 包含 `generated_at` 时间戳
- [ ] 图表分辨率至少 300 DPI
- [ ] 图表包含标题和轴标签

### 8.2 阶段性检查

- [ ] 所有输出文件有对应的生成脚本
- [ ] 论文引用的图表已更新到 `figures/` 目录
- [ ] 图表索引文件已更新
- [ ] 日志文件记录完整
