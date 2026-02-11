"""
模块名称：核心分析 - RFM 用户分层
作者：Trae AI
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    计算用户的 Recency (最近一次购买时间), Frequency (购买频率), Monetary (购买总金额)。
    使用分位数法对 RFM 指标打分 (1-5分)，并划分用户群体。

输入：
    - data/dwd/user_behavior (Parquet)
输出：
    - data/ads/ads_user_rfm (Parquet)
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, count, sum as spark_sum, datediff, lit, to_date, when
from pyspark.ml.feature import QuantileDiscretizer

# 确保日志目录存在
os.makedirs("logs", exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/analysis_rfm_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
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
        .appName("Analysis_RFM") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    logger.info("Spark Session started for RFM Analysis.")

    project_root = Path(__file__).resolve().parents[1]
    input_path = (project_root / "data/dwd/user_behavior").as_posix()
    
    if not os.path.exists(input_path):
        logger.error(f"Input path not found: {input_path}")
        return

    df_dwd = spark.read.parquet(input_path)

    # 0. 过滤只计算 purchase 行为的用户
    df_purchase = df_dwd.filter(col("event_type") == "purchase")

    # 1. 确定分析基准日期 (数据中的最大日期)
    analysis_date_row = df_dwd.agg(spark_max("dt")).collect()[0][0]
    analysis_date = str(analysis_date_row)
    logger.info(f"Analysis baseline date: {analysis_date}")

    # 2. 计算原生 R, F, M 指标
    logger.info("Calculating raw R, F, M metrics...")
    rfm_raw = df_purchase.groupBy("user_id").agg(
        datediff(lit(analysis_date), spark_max("dt")).alias("recency"),
        count("user_session").alias("frequency"),
        spark_sum("price").alias("monetary")
    )

    # 3. 使用分位数法打分 (1-5 分)
    # 注意：Recency 越小越好 (反向打分)，Frequency/Monetary 越大越好
    logger.info("Discretizing RFM metrics into scores (1-5)...")
    
    # R (需要后续反转分数，因为 QuantileDiscretizer 默认从小到大给分)
    q_r = QuantileDiscretizer(numBuckets=5, inputCol="recency", outputCol="r_rank", relativeError=0.01, handleInvalid="keep")
    # F
    q_f = QuantileDiscretizer(numBuckets=5, inputCol="frequency", outputCol="f_score", relativeError=0.01, handleInvalid="keep")
    # M
    q_m = QuantileDiscretizer(numBuckets=5, inputCol="monetary", outputCol="m_score", relativeError=0.01, handleInvalid="keep")

    # Pipeline 处理
    # 注意：QuantileDiscretizer 需要非空数据，我们在前面已经 filter 了 purchase，理论上不会有 null，但为了稳健性可以 dropna
    rfm_clean = rfm_raw.dropna()
    
    rfm_scored = q_r.fit(rfm_clean).transform(rfm_clean)
    rfm_scored = q_f.fit(rfm_scored).transform(rfm_scored)
    rfm_scored = q_m.fit(rfm_scored).transform(rfm_scored)

    # 修正 R 分数 (rank 0->5分, rank 4->1分)
    # 修正输出列类型为 integer
    rfm_final_score = rfm_scored.withColumn("r_score", (5 - col("r_rank")).cast("integer")) \
        .withColumn("f_score", (col("f_score") + 1).cast("integer")) \
        .withColumn("m_score", (col("m_score") + 1).cast("integer")) \
        .drop("r_rank")

    # 4. 简单用户分层逻辑 (示例：根据平均分判断高低)
    # 假设 > 3 分为 "高", <= 3 分为 "低"
    logger.info("Segmenting users based on RFM scores...")
    rfm_segmented = rfm_final_score.withColumn(
        "rfm_segment",
        when((col("r_score") > 3) & (col("f_score") > 3) & (col("m_score") > 3), "Core Users (核心客户)")
        .when((col("r_score") <= 3) & (col("f_score") > 3) & (col("m_score") > 3), "At-Risk Users (流失预警)")
        .when((col("r_score") > 3) & (col("m_score") <= 2), "New Users (新客户)")
        .otherwise("General Users (普通客户)")
    )

    # 5. 写入 ADS 层
    output_path = (project_root / "data/ads/ads_user_rfm").as_posix()
    logger.info(f"Writing RFM results to {output_path}...")
    rfm_segmented.write.mode("overwrite").parquet(output_path)
    
    # 统计分层结果
    segment_stats = rfm_segmented.groupBy("rfm_segment").count().orderBy("count", ascending=False)
    logger.info("RFM Segmentation Stats:")
    segment_stats.show(truncate=False)

    logger.info("RFM Analysis completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
