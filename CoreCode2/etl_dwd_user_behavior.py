"""
模块名称：DWD 明细层 ETL - 用户行为数据清洗
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    将原始 ODS 层数据清洗转换为 DWD 明细层。执行类型转换、去重、
    异常值标记及分区存储。

输入：
    - data/dwd/sample_oct_2019 (第一阶段的 Parquet 样本)

输出：
    - data/dwd/user_behavior (Parquet 分区存储)

依赖：
    - PySpark 3.5.3
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, when, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)

# 确保日志目录存在
os.makedirs("logs", exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_dwd_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_spark_session():
    """初始化 Spark 会话"""
    # 动态获取项目根目录和虚拟环境 Python 路径
    project_root = Path(__file__).resolve().parents[1]
    venv_python = (project_root / ".venv" / "Scripts" / "python.exe").as_posix()
    
    os.environ.setdefault("PYSPARK_PYTHON", venv_python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
    os.environ.setdefault("JAVA_HOME", r"E:\Java\jdk1.8.0_291")
    
    return SparkSession.builder \
        .appName("ETL_DWD_UserBehavior") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    logger.info("Spark Session started.")

    # 1. 定义项目根路径
    project_root = Path(__file__).resolve().parents[1]

    # 2. 读取 ODS 数据 (使用第一阶段的样本数据)
    input_path = (project_root / "data" / "dwd" / "sample_oct_2019").as_posix()
    if not os.path.exists(input_path):
        logger.error(f"Input path not found: {input_path}. Please run sample_data.py first.")
        return

    logger.info(f"Reading data from {input_path}...")
    df_raw = spark.read.parquet(input_path)

    # 3. 数据清洗与转换
    logger.info("Starting data cleaning and transformation...")
    
    # 类型转换与派生分区字段
    # event_time 示例: 2019-10-01 00:00:00 UTC
    df_cleaned = df_raw \
        .withColumn("event_timestamp", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'")) \
        .withColumn("dt", to_date(col("event_timestamp")))

    # 去重逻辑 (基于所有核心字段)
    logger.info("Dropping duplicates...")
    df_dedup = df_cleaned.dropDuplicates()
    
    # 异常值处理：根据第一阶段审计结果，标记 price <= 0 的记录
    logger.info("Flagging illegal prices...")
    df_final = df_dedup.withColumn(
        "price_is_illegal",
        when(col("price") <= 0, lit(1)).otherwise(lit(0))
    )

    # 4. 写入 DWD 层 (按 dt 分区)
    output_path = (project_root / "data" / "dwd" / "user_behavior").as_posix()
    logger.info(f"Writing to DWD layer: {output_path} (partitioned by dt)...")
    
    df_final.write.mode("overwrite") \
        .partitionBy("dt") \
        .parquet(output_path)

    # 统计清洗结果
    total_count = df_final.count()
    illegal_count = df_final.filter(col("price_is_illegal") == 1).count()
    
    logger.info("--------------------------------------------------")
    logger.info(f"ETL DWD Process completed successfully.")
    logger.info(f"Total records processed: {total_count}")
    logger.info(f"Illegal price records flagged: {illegal_count}")
    logger.info("--------------------------------------------------")
    
    spark.stop()

if __name__ == "__main__":
    main()
