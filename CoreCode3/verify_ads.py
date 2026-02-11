"""
模块名称：ADS 数据校验
作者：moyan726
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    对 ADS 层生成的各个结果表（漏斗、留存、RFM）进行抽样读取与展示，
    用于人工核对数据逻辑的正确性。

输入：
    - data/ads/ads_funnel_stats
    - data/ads/ads_user_retention
    - data/ads/ads_user_rfm

输出：
    - 终端打印校验结果
    - outputs/ads_verification.txt (校验报告)
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession

def main():
    # 动态获取项目根目录和虚拟环境 Python 路径
    project_root = Path(__file__).resolve().parents[1]
    venv_python = (project_root / ".venv" / "Scripts" / "python.exe").as_posix()
    
    os.environ.setdefault("PYSPARK_PYTHON", venv_python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
    os.environ.setdefault("JAVA_HOME", r"E:\Java\jdk1.8.0_291")
    
    spark = SparkSession.builder \
        .appName("Verify_ADS") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    # 准备输出文件
    output_file = project_root / "outputs" / "ads_verification.txt"
    os.makedirs(output_file.parent, exist_ok=True)
    
    with open(output_file, "w", encoding="utf-8") as f:
        def log_and_print(msg):
            print(msg)
            f.write(msg + "\n")

        log_and_print(f"=== ADS Verification Report ({datetime.now()}) ===\n")

        # 1. 验证转化漏斗
        log_and_print("--- 1. Funnel Summary (Global) ---")
        funnel_path = (project_root / "data/ads/ads_funnel_stats").as_posix()
        if os.path.exists(funnel_path):
            df_funnel = spark.read.parquet(funnel_path)
            # 获取 Pandas DataFrame 以便格式化输出
            pdf = df_funnel.filter("dimension = 'global'").toPandas()
            log_and_print(pdf.to_string())
        else:
            log_and_print(f"[Error] Path not found: {funnel_path}")
        log_and_print("\n")

        # 2. 验证用户留存
        log_and_print("--- 2. Retention Sample (First 5 Cohorts, Period 0 & 1) ---")
        ret_path = (project_root / "data/ads/ads_user_retention").as_posix()
        if os.path.exists(ret_path):
            df_ret = spark.read.parquet(ret_path)
            pdf = df_ret.filter("period <= 1").orderBy("cohort_date", "period").limit(10).toPandas()
            log_and_print(pdf.to_string())
        else:
            log_and_print(f"[Error] Path not found: {ret_path}")
        log_and_print("\n")

        # 3. 验证 RFM 分层
        log_and_print("--- 3. RFM Segmentation Summary ---")
        rfm_path = (project_root / "data/ads/ads_user_rfm").as_posix()
        if os.path.exists(rfm_path):
            df_rfm = spark.read.parquet(rfm_path)
            pdf = df_rfm.groupBy("rfm_segment").count().orderBy("count", ascending=False).toPandas()
            log_and_print(pdf.to_string())
        else:
            log_and_print(f"[Error] Path not found: {rfm_path}")
        log_and_print("\n")

    print(f"\nVerification report saved to: {output_file}")
    spark.stop()

if __name__ == "__main__":
    main()
