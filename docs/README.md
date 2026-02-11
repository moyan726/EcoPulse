# 📚 项目文档中心

> **版本**：v1.0.0  
> **最后更新**：2026-01-29  
> **维护者**：[项目负责人]

本目录包含项目的所有规范文档、开发指南和过程记录，是项目开发的核心参考资料。

---

## 📁 目录结构

```
docs/
├── README.md                   # 本文件 - 文档索引
│
├── rules/                      # 🔒 约束规则（必读）
│   ├── PROJECT_RULES.md        #    环境隔离规则
│   ├── CODE_STANDARDS.md       #    代码规范
│   ├── DATA_GOVERNANCE.md      #    数据治理规范
│   ├── GIT_WORKFLOW.md         #    Git 版本控制
│   └── OUTPUT_STANDARDS.md     #    输出物规范
│
├── guides/                     # 📖 指南文档
│   ├── ENVIRONMENT.md          #    环境配置指南
│   └── PYSPARK_PERF_NOTES.md   #    性能优化指南
│
├── logs/                       # 📝 过程记录
│   ├── EXPERIMENT_LOG.md       #    实验记录
│   └── MILESTONE_CHECKLIST.md  #    阶段检查点
│
└── screenshots/                # 🖼️ 截图资源
```

---

## � 约束规则 (`rules/`)

开发过程中**必须遵守**的规范，违反可能导致环境污染、数据错误或协作困难。

| 文档 | 描述 | 优先级 | 状态 |
|:-----|:-----|:------:|:----:|
| [PROJECT_RULES.md](rules/PROJECT_RULES.md) | 环境隔离，确保不污染本机 | `P0` | ✅ |
| [CODE_STANDARDS.md](rules/CODE_STANDARDS.md) | Python / PySpark 代码规范 | `P0` | ✅ |
| [DATA_GOVERNANCE.md](rules/DATA_GOVERNANCE.md) | 数据分层、缺失值、口径一致性 | `P0` | ✅ |
| [GIT_WORKFLOW.md](rules/GIT_WORKFLOW.md) | 分支策略、提交规范、标签管理 | `P1` | ✅ |
| [OUTPUT_STANDARDS.md](rules/OUTPUT_STANDARDS.md) | 输出文件命名、格式、目录 | `P1` | ✅ |

> **优先级说明**：`P0` = 强制执行，`P1` = 建议遵守

---

## 📖 指南文档 (`guides/`)

帮助快速上手和解决常见问题的参考文档。

| 文档 | 描述 | 适用场景 |
|:-----|:-----|:---------|
| [ENVIRONMENT.md](guides/ENVIRONMENT.md) | Conda 环境、JAVA_HOME、工作流程 | 环境配置 |
| [PYSPARK_PERF_NOTES.md](guides/PYSPARK_PERF_NOTES.md) | 数据倾斜、存储优化、缓存策略 | 性能调优 |

---

## 📝 过程记录 (`logs/`)

项目执行过程中的实验数据和进度追踪。

| 文档 | 描述 | 更新频率 |
|:-----|:-----|:--------:|
| [EXPERIMENT_LOG.md](logs/EXPERIMENT_LOG.md) | 实验环境、参数、结果记录 | 每次实验后 |
| [MILESTONE_CHECKLIST.md](logs/MILESTONE_CHECKLIST.md) | 阶段检查点与完成状态 | 每周更新 |

---

## 阶段成果
- **第一阶段**：环境隔离方案落地，10月数据样本生成，基础 EDA 完成。
- **第二阶段**：DWD 层 ETL 开发完成，实现分区存储与去重治理，CSV vs Parquet 性能基准测试完成（加速比最高达 44x）。

---

## 🚀 快速入口

### 🆕 新人入门（按顺序阅读）

```
Step 1 ──► PROJECT_RULES.md    了解环境隔离要求
       │
Step 2 ──► ENVIRONMENT.md      配置开发环境
       │
Step 3 ──► CODE_STANDARDS.md   熟悉代码规范
       │
Step 4 ──► DATA_GOVERNANCE.md  掌握数据处理规范
```

### 💻 日常开发

| 场景 | 参考文档 |
|:-----|:---------|
| 编写代码前 | [CODE_STANDARDS.md](rules/CODE_STANDARDS.md) |
| 处理数据时 | [DATA_GOVERNANCE.md](rules/DATA_GOVERNANCE.md) |
| 输出文件时 | [OUTPUT_STANDARDS.md](rules/OUTPUT_STANDARDS.md) |
| 提交代码前 | [GIT_WORKFLOW.md](rules/GIT_WORKFLOW.md) |
| 性能问题时 | [PYSPARK_PERF_NOTES.md](guides/PYSPARK_PERF_NOTES.md) |

### 📊 进度管理

| 场景 | 参考文档 |
|:-----|:---------|
| 查看项目进度 | [MILESTONE_CHECKLIST.md](logs/MILESTONE_CHECKLIST.md) |
| 记录实验结果 | [EXPERIMENT_LOG.md](logs/EXPERIMENT_LOG.md) |

---

## 📌 核心原则

```
┌─────────────────────────────────────────────────────────────────┐
│                        五大核心原则                              │
├─────────────────────────────────────────────────────────────────┤
│  1️⃣  环境隔离    所有依赖在 Conda 环境内，不污染本机             │
│  2️⃣  数据保真    DWD 层保留 NULL，不随意填充                    │
│  3️⃣  口径一致    所有指标有明确定义和计算公式                    │
│  4️⃣  可复现性    实验记录完整，他人可按文档重现                  │
│  5️⃣  版本追溯    代码变更有迹可循，关键节点打标签               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📋 文档状态说明

| 状态 | 含义 |
|:----:|:-----|
| ✅ | 已完成，可正常使用 |
| 🔄 | 持续更新中 |
| 📝 | 草稿，待完善 |
| ⚠️ | 需要修订 |

---

## 📜 更新日志

| 版本 | 日期 | 更新内容 | 更新人 |
|:----:|:----:|:---------|:------:|
| v1.0.0 | 2026-01-29 | 初始化文档结构，创建所有约束文档 | - |

---

## 🔗 相关链接

- **项目根目录**：[../README.md](../README.md)
- **数据字典**：[../data_dictionary.md](../data_dictionary.md)
- **环境配置**：[../environment.yml](../environment.yml)

---

<div align="center">

**如有问题或建议，请更新本文档或联系项目负责人**

</div>
