@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
title EcoPulse 数据预览看板

cd /d "%~dp0"

echo.
echo ╔══════════════════════════════════════════════╗
echo ║     EcoPulse 数据预览看板 · 一键启动脚本       ║
echo ╚══════════════════════════════════════════════╝
echo.

rem ────────────────────────────────────────────────
rem  第 1 步：激活虚拟环境
rem ────────────────────────────────────────────────
if not exist ".venv\Scripts\activate.bat" (
    echo [错误] 未找到 .venv 虚拟环境。
    echo        请先运行：python -m venv .venv
    echo        然后安装依赖：.venv\Scripts\pip install -r CoreCode6\requirements.txt
    pause
    goto end
)

call ".venv\Scripts\activate.bat"
echo [√] 虚拟环境已激活

rem ────────────────────────────────────────────────
rem  第 2 步：准备 Serving 数据（Parquet → CSV）
rem ────────────────────────────────────────────────
echo.
echo [·] 检查 Serving 数据 ...

set "SERVING_OK=1"
if not exist "data\serving\funnel_stats.csv"   set "SERVING_OK=0"
if not exist "data\serving\user_retention.csv"  set "SERVING_OK=0"
if not exist "data\serving\user_rfm.csv"        set "SERVING_OK=0"
if not exist "data\serving\user_clusters.csv"   set "SERVING_OK=0"

if "%SERVING_OK%"=="0" (
    echo [·] 首次运行，正在从 Parquet 导出 CSV 数据 ...
    echo.
    python scripts\prepare_serving_data.py
    if errorlevel 1 (
        echo.
        echo [错误] 数据准备失败，请检查上方错误信息。
        pause
        goto end
    )
) else (
    echo [√] Serving 数据已就绪，跳过导出步骤
)

rem ────────────────────────────────────────────────
rem  第 3 步：启动 Streamlit 看板
rem ────────────────────────────────────────────────
echo.
echo ══════════════════════════════════════════════
echo   正在启动看板，浏览器将自动打开 ...
echo   按 Ctrl+C 可停止服务
echo ══════════════════════════════════════════════
echo.

python -m streamlit run CoreCode6\app.py --server.headless false

:end
endlocal
