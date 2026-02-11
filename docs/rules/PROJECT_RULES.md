# 项目规则（环境隔离 / 不污染本机）

本项目以“**不污染 Windows 本机环境**”为最高优先级。所有开发与运行必须遵守以下规则。

## 1. 强制方案

- **强制采用方案 A**：Conda 环境隔离 Python 依赖 + 会话级（或进程内）设置 `JAVA_HOME`
- **禁止采用方案 B**：修改系统环境变量 / 全局安装依赖 / 将运行时依赖写入系统目录

## 2. 禁止事项（任何时候都不做）

- 禁止修改 Windows 的**系统环境变量**（系统 `PATH`、系统 `JAVA_HOME`、系统 `HADOOP_HOME` 等）
- 禁止在 `base` 环境或系统 Python 上执行 `pip install ...` 安装项目依赖
- 禁止把 JDK / Hadoop / Spark / winutils 等运行时文件复制到 `C:\Windows\`、`System32`、或其他系统目录
- 禁止将个人机器的绝对路径写死到论文/代码的核心逻辑中（例外：本项目的 `JAVA_HOME` 由会话脚本统一注入）

## 3. 必做事项（每次开发都执行）

- 仅在 Conda 环境 `graduation_project` 中运行本项目代码
- 启动前必须在当前会话内设置：`JAVA_HOME`、`PYSPARK_PYTHON`、`PYSPARK_DRIVER_PYTHON`
- 推荐通过脚本进入开发环境：

```powershell
.\scripts\dev_shell.ps1
```

## 4. 运行规范（可复现）

- 依赖以项目文件为准：`environment.yml`
- 环境隔离说明以文档为准：`docs/ENVIRONMENT.md`
- 运行脚本以会话脚本为入口：`scripts/dev_shell.ps1`
- 所有输出写入项目目录（禁止写入用户系统目录）：`outputs/`、`logs/`

## 5. 审计与回滚（发现污染立即处理）

- 若发现误在 `base` 或系统 Python 安装了依赖：立即卸载并只在 `graduation_project` 中重装
- 若发现误改系统环境变量：立即恢复并在后续仅使用会话脚本注入变量

