"""
模块名称：数据采样与格式转换
作者：moyan726
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    从原始 CSV 数据中按月份过滤数据，并保存为 Parquet（Track A 样本阶段）。
    针对本地环境进行了路径与环境变量优化。

输入：
    - data/row/2019-Oct.csv (或用户指定的其他月份文件)

输出：
    - data/dwd/sample_oct_2019 (Parquet 目录)
    - logs/sample_data_<YYYYMMDD>.log
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def setup_logger(script_name: str) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/{script_name}_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )
    return logging.getLogger(script_name)


def build_schema() -> StructType:
    """定义原始 CSV 的数据结构，提高读取性能并确保类型准确"""
    return StructType(
        [
            StructField("event_time", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按月份抽取电商数据并转为 Parquet 格式")
    parser.add_argument(
        "--input",
        default="data/row/2019-Oct.csv",
        help="输入 CSV 路径（相对于项目根目录）",
    )
    parser.add_argument(
        "--output",
        default="data/dwd/sample_oct_2019",
        help="输出 Parquet 目录（相对于项目根目录）",
    )
    parser.add_argument("--month", default="2019-10", help="过滤月份，格式 YYYY-MM")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = setup_logger("sample_data")

    # 1. 环境适配：确保 Spark 能找到正确的 Python 和 Java
    project_root = Path(__file__).resolve().parents[1]
    venv_python = (project_root / ".venv" / "Scripts" / "python.exe").as_posix()
    
    os.environ.setdefault("PYSPARK_PYTHON", venv_python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
    os.environ.setdefault("JAVA_HOME", r"E:\Java\jdk1.8.0_291")

    input_path = (project_root / args.input).as_posix()
    output_dir = (project_root / args.output).as_posix()

    logger.info("Starting data sampling task...")
    logger.info("Project Root: %s", project_root)
    logger.info("Input CSV: %s", input_path)
    logger.info("Output Parquet: %s", output_dir)
    logger.info("Filter Month: %s", args.month)

    # 2. 初始化 Spark Session (配置本地运行资源)
    spark = (
        SparkSession.builder.appName("EcoPulse_DataSampling")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        # 3. 读取数据 (使用预定义的 Schema)
        logger.info("Reading CSV data...")
        schema = build_schema()
        df = spark.read.csv(input_path, header=True, schema=schema)

        # 4. 执行过滤逻辑
        logger.info("Filtering data for month: %s", args.month)
        df_month = df.filter(col("event_time").startswith(args.month))

        # 5. 写入 Parquet (Snappy 压缩，性能最优)
        logger.info("Writing to Parquet format...")
        df_month.write.mode("overwrite").parquet(output_dir)
        
        # 统计结果输出
        count = df_month.count()
        logger.info("Sampling completed successfully. Total records sampled: %d", count)

    except Exception as e:
        logger.error("An error occurred during sampling: %s", str(e), exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")


if __name__ == "__main__":
    main()
