"""
模块名称：存储格式性能基准测试 (CSV vs Parquet) - Phase 4
作者：Trae AI
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    对比原始 CSV 格式与分区 Parquet 格式在不同查询场景下的性能差异。
    测试场景包括：全表扫描、单列聚合、条件过滤（分区裁剪）。
    本脚本作为第四阶段的验证性测试，确认数据治理后的性能基准。

输入：
    - data/row/2019-Oct.csv (原始 CSV)
    - data/dwd/user_behavior (Parquet)

输出：
    - outputs/benchmark/storage_performance_phase4.csv
    - outputs/benchmark/benchmark_summary_phase4.json

依赖：
    - PySpark 3.5.3
    - Pandas
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)

# 确保日志和输出目录存在
os.makedirs("logs", exist_ok=True)
os.makedirs("outputs/benchmark", exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/benchmark_phase4_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_spark_session():
    # 动态获取项目根目录和虚拟环境 Python 路径
    project_root = Path(__file__).resolve().parents[1]
    venv_python = (project_root / ".venv" / "Scripts" / "python.exe").as_posix()
    
    os.environ.setdefault("PYSPARK_PYTHON", venv_python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
    os.environ.setdefault("JAVA_HOME", r"E:\Java\jdk1.8.0_291")
    
    return SparkSession.builder \
        .appName("StorageBenchmark_Phase4") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()

def run_test(name, func):
    logger.info(f"Running test: {name}...")
    start_time = time.time()
    result = func()
    duration = time.time() - start_time
    logger.info(f"Test {name} completed in {duration:.2f}s (Result: {result})")
    return duration

def main():
    spark = get_spark_session()
    
    # 定义输入路径
    project_root = Path(__file__).resolve().parents[1]
    # 使用 2019-Oct.csv 作为 CSV 基准，因为它包含测试日期 2019-10-01
    csv_path = (project_root / "data/row/2019-Oct.csv").as_posix()
    parquet_path = (project_root / "data/dwd/user_behavior").as_posix()
    
    if not os.path.exists(csv_path) or not os.path.exists(parquet_path):
        logger.error(f"Missing input data for benchmark.\nCSV: {csv_path}\nParquet: {parquet_path}")
        return

    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ])

    # 1. 加载数据源
    logger.info("Loading datasets...")
    df_csv = spark.read.csv(csv_path, header=True, schema=schema)
    df_parquet = spark.read.parquet(parquet_path)

    results = []

    # --- 场景 1: 全表扫描 Count ---
    # 注意：CSV 只是单月数据，Parquet 是全量数据，Count 结果会不同，主要看速率
    results.append({
        "scenario": "Full Scan Count",
        "format": "Parquet",
        "duration": run_test("Parquet Full Count", lambda: df_parquet.count())
    })
    results.append({
        "scenario": "Full Scan Count",
        "format": "CSV",
        "duration": run_test("CSV Full Count", lambda: df_csv.count())
    })

    # --- 场景 2: 单列聚合 Sum(price) ---
    results.append({
        "scenario": "Columnar Sum",
        "format": "Parquet",
        "duration": run_test("Parquet Sum Price", lambda: df_parquet.agg(spark_sum("price")).collect()[0][0])
    })
    results.append({
        "scenario": "Columnar Sum",
        "format": "CSV",
        "duration": run_test("CSV Sum Price", lambda: df_csv.agg(spark_sum("price")).collect()[0][0])
    })

    # --- 场景 3: 条件过滤 (Parquet 分区裁剪) ---
    # 过滤 2019-10-01 的数据
    test_dt = "2019-10-01"
    results.append({
        "scenario": "Filtered Query (dt=2019-10-01)",
        "format": "Parquet",
        "duration": run_test("Parquet Partition Filtered Count", lambda: df_parquet.filter(col("dt") == test_dt).count())
    })
    results.append({
        "scenario": "Filtered Query (dt=2019-10-01)",
        "format": "CSV",
        "duration": run_test("CSV Filtered Count", lambda: df_csv.filter(col("event_time").contains(test_dt)).count())
    })

    # 5. 保存结果 (使用 _phase4 后缀区分)
    output_csv = (project_root / "outputs/benchmark/storage_performance_phase4.csv").as_posix()
    output_json = (project_root / "outputs/benchmark/benchmark_summary_phase4.json").as_posix()
    
    res_df = pd.DataFrame(results)
    res_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    
    summary = {
        "generated_at": datetime.now().isoformat(),
        "spark_version": spark.version,
        "results": results,
        "note": "Phase 4 validation with optimized environment"
    }
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logger.info("Benchmark completed. Results saved to outputs/benchmark/")
    spark.stop()

if __name__ == "__main__":
    main()
