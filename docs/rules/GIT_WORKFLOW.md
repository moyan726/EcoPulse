# Git 版本控制约束

本文件定义项目的版本控制规范，确保代码历史清晰、便于回滚和协作。

---

## 0. 仓库初始化（首次）

- 初始化仓库：`git init`
- 首次提交前必须先检查 `.gitignore`，确保不会提交 `data/`、`outputs/`、`logs/` 等大文件/易变文件
- **标签前置条件**：Git 标签必须指向某个提交（commit），因此在没有首个提交之前无法创建里程碑标签

## 1. 分支策略

### 1.1 分支结构

```
main (稳定版本)
  │
  ├── dev (日常开发)
  │     │
  │     ├── feature/eda-analysis
  │     ├── feature/etl-dwd
  │     ├── feature/funnel-analysis
  │     ├── feature/rfm-clustering
  │     └── feature/powerbi-dashboard
  │
  └── fix/price-outlier-filter
```

### 1.2 分支命名规范

| 分支类型 | 命名格式 | 示例 | 用途 |
|----------|----------|------|------|
| 主分支 | `main` | `main` | 稳定版本，仅在阶段性成果完成后合并 |
| 开发分支 | `dev` | `dev` | 日常开发，可随时提交 |
| 功能分支 | `feature/<功能名>` | `feature/rfm-analysis` | 新功能开发 |
| 修复分支 | `fix/<问题描述>` | `fix/price-outlier` | Bug 修复 |
| 实验分支 | `exp/<实验名>` | `exp/kmeans-k-selection` | 实验性代码 |

### 1.3 分支合并规则

```
feature/* ──► dev ──► main
    │
    └── 完成后删除 feature 分支
```

- `feature/*` → `dev`：功能完成且测试通过后合并
- `dev` → `main`：阶段性成果验收后合并
- **禁止**直接向 `main` 提交代码

---

## 2. 提交规范（Conventional Commits）

### 2.1 提交信息格式

```
<type>: <subject>

[optional body]

[optional footer]
```

### 2.2 Type 类型定义

| Type | 含义 | 示例 |
|------|------|------|
| `feat` | 新功能 | `feat: 新增 RFM 指标计算脚本` |
| `fix` | Bug 修复 | `fix: 修复 price 异常值过滤逻辑` |
| `docs` | 文档更新 | `docs: 更新数据字典` |
| `perf` | 性能优化 | `perf: 优化品牌聚合倾斜问题` |
| `refactor` | 代码重构 | `refactor: 重构 ETL 脚本结构` |
| `test` | 测试相关 | `test: 添加转化率计算单元测试` |
| `chore` | 杂项（依赖、配置等） | `chore: 更新 environment.yml` |
| `data` | 数据相关 | `data: 生成 2019-10 月样本数据` |
| `exp` | 实验记录 | `exp: K-Means K=5 实验结果` |

### 2.3 提交示例

```bash
# ✅ 正确示例
git commit -m "feat: 新增转化漏斗分析脚本"
git commit -m "fix: 修复缺失值统计逻辑错误"
git commit -m "docs: 补充 RFM 指标口径说明"
git commit -m "perf: 使用加盐策略优化品牌聚合"
git commit -m "chore: 添加 pyarrow 依赖"

# ❌ 错误示例
git commit -m "更新代码"          # 太模糊
git commit -m "fix bug"          # 没有说明修复了什么
git commit -m "完成了一些功能"    # 中文且不规范
```

### 2.4 提交粒度建议

- 每个提交只做**一件事**
- 功能开发可以多次提交，合并前 squash
- 禁止"一次性提交所有改动"的大杂烩

---

## 3. .gitignore 配置

### 3.1 必须忽略的内容

```gitignore
# ========== 数据文件 ==========
data/
*.csv
*.parquet
*.json
!data_dictionary.md

# ========== 输出文件 ==========
outputs/
logs/

# ========== Python ==========
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
*.egg-info/
dist/
build/
*.egg

# ========== Jupyter ==========
.ipynb_checkpoints/
*.ipynb_checkpoints

# ========== IDE ==========
.idea/
.vscode/
*.swp
*.swo
*~

# ========== 系统文件 ==========
.DS_Store
Thumbs.db
desktop.ini

# ========== 环境 ==========
.env
*.env.local
venv/
env/

# ========== Spark ==========
spark-warehouse/
metastore_db/
derby.log

# ========== 临时文件 ==========
*.tmp
*.temp
*.bak
```

### 3.2 例外情况

某些配置文件需要保留在仓库中：

```gitignore
# 不忽略环境配置模板
!environment.yml
!.env.example

# 不忽略文档中的示例数据
!docs/examples/*.csv
```

---

## 4. 里程碑标签（Tags）

### 4.1 标签命名规范

| 阶段 | 标签 | 说明 |
|------|------|------|
| 环境搭建完成 | `v0.1-env-setup` | Conda 环境、dev_shell.ps1 可用 |
| EDA 完成 | `v1.0-eda` | 探索性分析完成，数据字典定稿 |
| ETL 完成 | `v2.0-etl` | DWD 层清洗脚本完成 |
| 核心分析完成 | `v3.0-analysis` | 漏斗、留存、RFM 分析完成 |
| 性能优化完成 | `v3.5-optimization` | 倾斜治理、性能对比实验完成 |
| 可视化完成 | `v4.0-visualization` | Power BI 看板完成 |
| 论文定稿 | `v5.0-final` | 代码冻结，论文完成 |

### 4.2 打标签命令

```bash
# 打标签
git tag -a v1.0-eda -m "完成探索性分析，数据字典定稿"

# 推送标签
git push origin v1.0-eda

# 查看所有标签
git tag -l
```

---

## 5. 禁止事项

### 5.1 绝对禁止

| 禁止行为 | 原因 | 后果 |
|----------|------|------|
| 提交大数据文件（CSV/Parquet） | 仓库膨胀，clone 缓慢 | 需重写历史清理 |
| 提交 `logs/` 目录 | 日志会频繁变化 | 污染提交历史 |
| 提交 `__pycache__/` | 编译缓存，无意义 | 仓库冗余 |
| 强制推送到 `main` | 破坏历史 | 协作灾难 |
| 提交包含密钥/密码的文件 | 安全风险 | 需立即撤销密钥 |

### 5.2 不建议但可接受

| 行为 | 建议替代方案 |
|------|--------------|
| 直接在 `dev` 分支开发 | 使用 `feature/*` 分支 |
| 提交信息全中文 | 使用英文 type + 中文描述 |
| 提交未测试的代码 | 先本地运行验证 |

---

## 6. 日常工作流

### 6.1 开始新功能

```bash
# 1. 切换到 dev 分支并更新
git checkout dev
git pull origin dev

# 2. 创建功能分支
git checkout -b feature/rfm-analysis

# 3. 开发并提交
git add .
git commit -m "feat: 新增 RFM 指标计算"

# 4. 推送到远程
git push -u origin feature/rfm-analysis
```

### 6.2 完成功能合并

```bash
# 1. 切换到 dev 分支
git checkout dev

# 2. 合并功能分支
git merge feature/rfm-analysis

# 3. 删除功能分支
git branch -d feature/rfm-analysis
git push origin --delete feature/rfm-analysis

# 4. 推送 dev
git push origin dev
```

### 6.3 阶段性成果合并到 main

```bash
# 1. 确保 dev 是最新的
git checkout dev
git pull origin dev

# 2. 切换到 main
git checkout main

# 3. 合并 dev
git merge dev

# 4. 打标签
git tag -a v2.0-etl -m "ETL 阶段完成"

# 5. 推送
git push origin main
git push origin v2.0-etl
```

---

## 7. 紧急回滚流程

### 7.1 回滚最近一次提交

```bash
# 保留改动，撤销提交
git reset --soft HEAD~1

# 丢弃改动，撤销提交
git reset --hard HEAD~1
```

### 7.2 回滚到指定标签

```bash
# 查看标签对应的 commit
git show v1.0-eda

# 回滚（创建新分支保护当前状态）
git checkout -b backup-current
git checkout main
git reset --hard v1.0-eda
```

---

## 8. 检查点

每次提交前确认：

- [ ] 运行 `.gitignore` 检查，确保不提交大文件
- [ ] 提交信息符合 Conventional Commits 规范
- [ ] 代码已在本地测试通过
- [ ] 没有包含敏感信息（路径、密钥等）

每个阶段完成后确认：

- [ ] 已打版本标签
- [ ] 已合并到 `main` 分支
- [ ] 已清理临时分支
