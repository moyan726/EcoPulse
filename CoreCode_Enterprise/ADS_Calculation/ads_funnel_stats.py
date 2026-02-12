"""
ADS 计算: 漏斗转化分析
计算 View -> Cart -> Purchase 的各阶段转化率。
"""
import sys
from pathlib import Path
from pyspark.sql.functions import col, countDistinct, lit

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session

def main():
    spark = get_spark_session(app_name="EcoPulse_ADS_Funnel", enable_hive=True)
    
    print("[INFO] Calculating Funnel metrics...")
    try:
        base = spark.table("ecop.dwd_user_behavior").filter(
            col("event_type").isin(["view", "cart", "purchase"])
        )

        funnel_daily = base.groupBy("dt", "event_type").agg(
            countDistinct("user_session").alias("session_count"),
            countDistinct("user_id").alias("user_count"),
        ).withColumn("dimension", lit("daily")).withColumn("brand", lit(None).cast("string"))

        top_brands = (
            base.filter(col("brand").isNotNull())
            .groupBy("brand")
            .count()
            .orderBy(col("count").desc())
            .limit(20)
            .select("brand")
            .collect()
        )
        top_brand_list = [row["brand"] for row in top_brands]

        funnel_brand = base.filter(col("brand").isin(top_brand_list)).groupBy(
            "brand", "event_type"
        ).agg(
            countDistinct("user_session").alias("session_count"),
            countDistinct("user_id").alias("user_count"),
        ).withColumn("dimension", lit("brand")).withColumn("dt", lit(None).cast("date"))

        funnel_global = base.groupBy("event_type").agg(
            countDistinct("user_session").alias("session_count"),
            countDistinct("user_id").alias("user_count"),
        ).withColumn("dimension", lit("global")).withColumn("dt", lit(None).cast("date")).withColumn(
            "brand", lit(None).cast("string")
        )

        result_df = (
            funnel_global.select("event_type", "session_count", "user_count", "dimension", "dt", "brand")
            .unionByName(funnel_daily.select("event_type", "session_count", "user_count", "dimension", "dt", "brand"))
            .unionByName(funnel_brand.select("event_type", "session_count", "user_count", "dimension", "dt", "brand"))
        )

        print("[INFO] Writing to ADS: ecop.ads_funnel_stats")
        result_df.write \
            .mode("overwrite") \
            .format("hive") \
            .saveAsTable("ecop.ads_funnel_stats")
        result_df.filter(col("dimension") == "daily").groupBy("event_type").sum("session_count").show()
        
    except Exception as e:
        print(f"[ERROR] Funnel Calculation failed: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()
