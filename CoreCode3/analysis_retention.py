"""
模块名称：核心分析 - 留存分析 (Cohort Analysis)
作者：moyan726
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    计算用户的次日、三日、七日留存率。
    按用户首次访问日期（Cohort Date）进行分群。

输入：
    - data/dwd/user_behavior
输出：
    - data/ads/ads_user_retention (Parquet)
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, datediff, countDistinct

# 确保日志目录存在
os.makedirs("logs", exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/analysis_retention_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
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
        .appName("Analysis_Retention") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    logger.info("Spark Session started for Retention Analysis.")

    project_root = Path(__file__).resolve().parents[1]
    input_path = (project_root / "data/dwd/user_behavior").as_posix()
    
    if not os.path.exists(input_path):
        logger.error(f"Input path not found: {input_path}")
        return

    df_dwd = spark.read.parquet(input_path)

    # 1. 计算每个用户的首次访问日期 (Cohort Date)
    logger.info("Determining Cohort Date for each user...")
    user_first_date = df_dwd.groupBy("user_id").agg(
        spark_min("dt").alias("cohort_date")
    )

    # 2. 将首次访问日期关联回原表
    logger.info("Joining Cohort Date with user behaviors...")
    df_with_cohort = df_dwd.join(user_first_date, on="user_id", how="inner")

    # 3. 计算活跃天数与首次访问天数的差值 (Period)
    logger.info("Calculating retention periods...")
    df_retention = df_with_cohort.select(
        "user_id",
        "cohort_date",
        "dt"
    ).distinct().withColumn(
        "period", datediff(col("dt"), col("cohort_date"))
    )

    # 4. 按 Cohort Date 和 Period 聚合人数
    logger.info("Aggregating retention counts...")
    ads_retention = df_retention.groupBy("cohort_date", "period").agg(
        countDistinct("user_id").alias("retention_count")
    )

    # 5. 计算 Cohort Size (Period = 0 的人数)
    cohort_size = ads_retention.filter(col("period") == 0) \
        .select("cohort_date", col("retention_count").alias("cohort_size"))
    
    ads_retention_final = ads_retention.join(cohort_size, on="cohort_date", how="inner") \
        .withColumn("retention_rate", col("retention_count") / col("cohort_size")) \
        .orderBy("cohort_date", "period")

    # 6. 写入 ADS 层
    output_path = (project_root / "data/ads/ads_user_retention").as_posix()
    logger.info(f"Writing Retention results to {output_path}...")
    ads_retention_final.write.mode("overwrite").parquet(output_path)
    
    # 打印前几天的留存预览
    logger.info("Retention Rate Preview (Top 10 rows):")
    ads_retention_final.filter(col("period") <= 7).show(10)

    logger.info("Retention Analysis completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
