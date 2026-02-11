"""
模块名称：性能优化实验 - 数据倾斜治理 (加盐聚合演示)
作者：moyan726
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    对比普通聚合与加盐聚合在处理倾斜数据时的性能差异。
    使用 brand 维度进行聚合演示。

输入：
    - data/dwd/user_behavior
"""

import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, concat, lit, rand, split

# 确保日志目录存在
os.makedirs("logs", exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/perf_skew_optimization_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
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
        .appName("Perf_Skew_Optimization") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    logger.info("Spark Session started for Skew Optimization.")

    project_root = Path(__file__).resolve().parents[1]
    input_path = (project_root / "data/dwd/user_behavior").as_posix()
    
    if not os.path.exists(input_path):
        logger.error(f"Input path not found: {input_path}")
        return

    df_dwd = spark.read.parquet(input_path)

    # 1. 场景 A: 普通聚合 (可能产生倾斜)
    logger.info("Starting Scenario A: Normal Aggregation...")
    start_a = time.time()
    res_normal = df_dwd.groupBy("brand").agg(count("*").alias("cnt"))
    # 使用 collect() 触发 Action，确保计算执行
    res_normal.collect() 
    duration_a = time.time() - start_a
    logger.info(f"Normal Aggregation duration: {duration_a:.2f}s")

    # 2. 场景 B: 两阶段加盐聚合 (Salting)
    logger.info("Starting Scenario B: Two-Stage Salting Aggregation...")
    start_b = time.time()
    
    # 第一阶段：局部聚合 (加盐)
    # 将 key 加上 0-9 的随机后缀，打散到不同的 Partition
    salt_range = 10
    df_salted = df_dwd.withColumn("salt", (rand() * salt_range).cast("int")) \
                      .withColumn("salted_key", concat(col("brand"), lit("_"), col("salt")))
    
    partial_agg = df_salted.groupBy("salted_key").agg(count("*").alias("partial_cnt"))
    
    # 第二阶段：全局聚合 (去盐)
    # 去掉后缀，恢复原始 key，再次聚合
    final_agg = partial_agg.withColumn("original_key", split(col("salted_key"), "_")[0]) \
                           .groupBy("original_key").agg({"partial_cnt": "sum"})
    
    final_agg.collect() # 触发计算
    duration_b = time.time() - start_b
    logger.info(f"Salting Aggregation duration: {duration_b:.2f}s")

    # 3. 记录结果
    logger.info("--------------------------------------------------")
    logger.info(f"Optimization Summary:")
    logger.info(f"Normal Aggregation: {duration_a:.2f}s")
    logger.info(f"Salting Aggregation: {duration_b:.2f}s")
    
    # 计算性能变化
    if duration_a > 0:
        diff = (duration_b - duration_a) / duration_a * 100
        logger.info(f"Performance Change: {diff:.2f}% (Note: Overhead may exceed gains on small data)")
    logger.info("--------------------------------------------------")
    
    spark.stop()

if __name__ == "__main__":
    main()
