"""
ADS 计算: 用户留存分析 (Cohort Retention)
按用户首次活跃日 (cohort_date) 分群，计算 period=0..N 的留存人数与留存率。
"""
import sys
from pathlib import Path
from pyspark.sql.functions import col, min as spark_min, datediff, countDistinct

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session

def main():
    spark = get_spark_session(app_name="EcoPulse_ADS_Retention", enable_hive=True)
    
    print("[INFO] Calculating User Retention...")
    try:
        base = spark.table("ecop.dwd_user_behavior").select("user_id", "dt").distinct()

        user_first_date = base.groupBy("user_id").agg(spark_min("dt").alias("cohort_date"))
        with_cohort = base.join(user_first_date, on="user_id", how="inner")

        retention = (
            with_cohort.select("user_id", "cohort_date", "dt")
            .distinct()
            .withColumn("period", datediff(col("dt"), col("cohort_date")))
            .filter(col("period").between(0, 14))
            .groupBy("cohort_date", "period")
            .agg(countDistinct("user_id").alias("retention_count"))
        )

        cohort_size = retention.filter(col("period") == 0).select(
            "cohort_date", col("retention_count").alias("cohort_size")
        )

        final_df = (
            retention.join(cohort_size, on="cohort_date", how="inner")
            .withColumn("retention_rate", col("retention_count") / col("cohort_size"))
            .orderBy("cohort_date", "period")
        )

        print("[INFO] Writing to ADS: ecop.ads_user_retention")
        final_df.write \
            .mode("overwrite") \
            .format("hive") \
            .saveAsTable("ecop.ads_user_retention")
            
        print("[SUCCESS] Retention calculated.")
        
    except Exception as e:
        print(f"[ERROR] Retention Calculation failed: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()
