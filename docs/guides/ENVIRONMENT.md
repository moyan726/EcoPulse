# 环境隔离规范（强制执行：方案 A）

本项目必须始终采用 **方案 A：Conda 环境隔离 + 会话级 JAVA_HOME**，以确保不污染 Windows 本机环境。

## 原则

- Python 依赖仅安装在 Conda 环境内，不使用全局 pip 安装依赖
- 不修改 Windows 系统环境变量（系统 PATH/JAVA_HOME 等）
- `JAVA_HOME` 仅在当前终端会话或 Python 进程内临时设置

## 一次性准备

- 安装 Anaconda/Miniconda
- 准备 JDK 8/11（仅供 Spark 运行，不编写 Java 代码）

## 日常工作流程（推荐）

1. 打开 PowerShell，进入项目根目录
2. 运行脚本进入隔离环境：

```powershell
.\scripts\dev_shell.ps1
```

3. 在同一终端内运行 Python/PySpark 脚本：

```powershell
python test_spark.py
python sample_data.py
```

## 备用流程（不依赖脚本）

```powershell
conda activate graduation_project
$env:JAVA_HOME = "E:\Java\jdk1.8.0_291"
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
```

## 说明

- Spark 依赖 JVM，因此必须有 JDK；但项目代码全部使用 Python（PySpark）
- 项目脚本会在进程内 `setdefault` 关键变量，避免依赖系统全局配置
