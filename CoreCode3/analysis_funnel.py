"""
模块名称：核心分析 - 转化漏斗分析
作者：Trae AI
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    计算 Session 维度和 User 维度的转化漏斗指标。
    包含：全站漏斗、按日趋势、按品牌/类目拆解。

输入：
    - data/dwd/user_behavior
输出：
    - data/ads/ads_funnel_stats (Parquet)
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, lit

# 确保日志目录存在
os.makedirs("logs", exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/analysis_funnel_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
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
        .appName("Analysis_Funnel") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    logger.info("Spark Session started for Funnel Analysis.")

    project_root = Path(__file__).resolve().parents[1]
    input_path = (project_root / "data/dwd/user_behavior").as_posix()
    
    if not os.path.exists(input_path):
        logger.error(f"Input path not found: {input_path}")
        return

    df_dwd = spark.read.parquet(input_path)

    # 1. 全站漏斗 (Session 维度)
    logger.info("Calculating Global Funnel (Session & User level)...")
    funnel_global = df_dwd.groupBy("event_type").agg(
        countDistinct("user_session").alias("session_count"),
        countDistinct("user_id").alias("user_count")
    ).withColumn("dimension", lit("global"))

    # 2. 按日漏斗趋势
    logger.info("Calculating Daily Funnel Trend...")
    funnel_daily = df_dwd.groupBy("dt", "event_type").agg(
        countDistinct("user_session").alias("session_count"),
        countDistinct("user_id").alias("user_count")
    ).withColumn("dimension", lit("daily"))

    # 3. 按品牌漏斗 (Top 20 品牌)
    # 先找出 Top 品牌
    logger.info("Identifying Top 20 Brands...")
    top_brands = df_dwd.filter(col("brand").isNotNull()) \
        .groupBy("brand").count().orderBy(col("count").desc()).limit(20) \
        .select("brand").collect()
    top_brand_list = [row['brand'] for row in top_brands]

    logger.info("Calculating Brand Funnel...")
    funnel_brand = df_dwd.filter(col("brand").isin(top_brand_list)) \
        .groupBy("brand", "event_type").agg(
            countDistinct("user_session").alias("session_count"),
            countDistinct("user_id").alias("user_count")
        ).withColumn("dimension", lit("brand"))

    # 4. 合并结果并写入 ADS 层
    output_path = (project_root / "data/ads/ads_funnel_stats").as_posix()
    logger.info(f"Writing Funnel results to {output_path}...")
    
    # 统一 Schema 结构以便合并 (增加缺失列)
    funnel_global_final = funnel_global.withColumn("dt", lit(None).cast("date")).withColumn("brand", lit(None).cast("string"))
    funnel_daily_final = funnel_daily.withColumn("brand", lit(None).cast("string"))
    funnel_brand_final = funnel_brand.withColumn("dt", lit(None).cast("date"))

    ads_funnel = funnel_global_final.unionByName(funnel_daily_final).unionByName(funnel_brand_final)

    ads_funnel.write.mode("overwrite").parquet(output_path)
    
    # 打印全局漏斗预览
    logger.info("Global Funnel Preview:")
    funnel_global_final.select("event_type", "session_count", "user_count").show()

    logger.info("Funnel Analysis completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
