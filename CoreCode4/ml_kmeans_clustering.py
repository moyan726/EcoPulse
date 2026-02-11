"""
模块名称：高级建模 - K-Means 聚类分析
作者：Trae AI
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    1. 读取 RFM 结果数据。
    2. 对 R/F/M 特征进行 Log 转换和标准化。
    3. 使用肘部法 (Elbow Method) 寻找最佳 K 值。
    4. 训练最终模型并产出用户画像聚类结果。

输入：
    - data/ads/ads_user_rfm
输出：
    - data/ads/ads_user_clusters (Parquet)
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log1p
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# 确保日志目录存在
os.makedirs("logs", exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/ml_kmeans_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
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
        .appName("ML_KMeans_Clustering") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def main():
    spark = get_spark_session()
    logger.info("Spark Session started for KMeans Modeling.")

    project_root = Path(__file__).resolve().parents[1]
    input_path = (project_root / "data/ads/ads_user_rfm").as_posix()
    
    if not os.path.exists(input_path):
        logger.error(f"Input path not found: {input_path}")
        return

    df_rfm = spark.read.parquet(input_path)

    # 1. 特征预处理
    logger.info("Preprocessing features: Log transformation...")
    # 对 F 和 M 进行 log1p 转换（R 已经是较小天数，可选）
    df_prepped = df_rfm.withColumn("f_log", log1p(col("frequency"))) \
                       .withColumn("m_log", log1p(col("monetary"))) \
                       .withColumn("r_log", log1p(col("recency")))

    # 2. 向量化
    assembler = VectorAssembler(
        inputCols=["r_log", "f_log", "m_log"],
        outputCol="features"
    )
    df_vector = assembler.transform(df_prepped)

    # 3. 标准化
    logger.info("Scaling features using StandardScaler...")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    df_scaled = scaler.fit(df_vector).transform(df_vector)

    # 4. 肘部法 (寻找最佳 K 值)
    # 为了演示，我们测试 K=2 到 K=6
    logger.info("Starting Elbow Method to find optimal K...")
    evaluator = ClusteringEvaluator(predictionCol="prediction", featuresCol="scaledFeatures", \
                                    metricName="silhouette", distanceMeasure="squaredEuclidean")
    
    cost_list = []
    for k in range(2, 7):
        kmeans = KMeans().setK(k).setSeed(42).setFeaturesCol("scaledFeatures")
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)
        silhouette = evaluator.evaluate(predictions)
        logger.info(f"K={k}, Silhouette Score = {silhouette}")
        # 注意：Spark 3.x 移除了 computeCost，建议参考 silhouette 或手动计算 WSSSE

    # 5. 训练最终模型 (假设 K=4 为最佳值，通常电商用户分群 3-5 较为合适)
    best_k = 4
    logger.info(f"Training final KMeans model with K={best_k}...")
    kmeans_final = KMeans().setK(best_k).setSeed(42).setFeaturesCol("scaledFeatures")
    model_final = kmeans_final.fit(df_scaled)
    
    # 6. 产出结果
    df_final = model_final.transform(df_scaled)
    
    output_path = (project_root / "data/ads/ads_user_clusters").as_posix()
    logger.info(f"Writing clustering results to {output_path}...")
    df_final.select("user_id", "recency", "frequency", "monetary", "prediction").write.mode("overwrite").parquet(output_path)

    # 统计各聚类群体的均值特征
    logger.info("Cluster Centers (Mean R/F/M):")
    df_final.groupBy("prediction").mean("recency", "frequency", "monetary").orderBy("prediction").show()

    logger.info("KMeans Analysis completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
