"""
Enterprise ADS 计算: 用户 K-Means 聚类分析
基于 RFM 特征进行无监督分群，识别超级活跃、高价值、流失边缘等群体。
"""
import sys
from pathlib import Path
from pyspark.sql.functions import col, log1p
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# 添加项目根目录到 sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session

def main():
    spark = get_spark_session(app_name="EcoPulse_ADS_KMeans", enable_hive=True)
    
    print("[INFO] Loading RFM data from Hive...")
    try:
        # 1. 加载 ADS 层的 RFM 数据
        df_rfm = spark.table("ecop.ads_user_rfm")
        
        # 2. 特征预处理
        print("[INFO] Preprocessing: Log transformation and Vectorization...")
        df_prepped = df_rfm.withColumn("f_log", log1p(col("frequency"))) \
                           .withColumn("m_log", log1p(col("monetary"))) \
                           .withColumn("r_log", log1p(col("recency")))

        assembler = VectorAssembler(
            inputCols=["r_log", "f_log", "m_log"],
            outputCol="features"
        )
        df_vector = assembler.transform(df_prepped)

        # 3. 标准化
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
        df_scaled = scaler.fit(df_vector).transform(df_vector)

        # 4. 训练模型 (K=4)
        print("[INFO] Training KMeans model (K=4)...")
        kmeans = KMeans().setK(4).setSeed(42).setFeaturesCol("scaledFeatures")
        model = kmeans.fit(df_scaled)
        
        # 5. 产出结果并写入 Hive
        df_final = model.transform(df_scaled)
        
        print("[INFO] Writing results to Hive: ecop.ads_user_clusters")
        df_final.select("user_id", "recency", "frequency", "monetary", "prediction") \
            .write \
            .mode("overwrite") \
            .format("hive") \
            .saveAsTable("ecop.ads_user_clusters")
            
        print(f"[SUCCESS] Clustering completed for {df_final.count()} users.")
        
    except Exception as e:
        print(f"[ERROR] KMeans failed: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()
