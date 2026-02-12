"""
ADS 计算: 用户 RFM 模型
计算最近一次购买时间 (Recency), 购买频率 (Frequency), 消费金额 (Monetary)。
"""
import sys
from pathlib import Path
from pyspark.sql.functions import max, count, sum, datediff, lit, current_date

# 添加项目根目录到 sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session

def main():
    spark = get_spark_session(app_name="EcoPulse_ADS_RFM", enable_hive=True)
    
    print("[INFO] Calculating RFM metrics...")
    try:
        # 1. 从 DWD 读取购买行为 (event_type = 'purchase')
        df_purchase = spark.sql("""
            SELECT user_id, price, dt
            FROM ecop.dwd_user_behavior
            WHERE event_type = 'purchase'
        """)
        
        # 2. 聚合计算 R, F, M
        # 截止日期设为数据集的最后一天 (2020-04-30)，或者使用 current_date()
        # 这里为了演示效果，假设当前时间是数据集末尾
        analysis_date = lit("2020-05-01")
        
        rfm_df = df_purchase.groupBy("user_id").agg(
            datediff(analysis_date, max("dt")).alias("recency"),
            count("*").alias("frequency"),
            sum("price").alias("monetary")
        )
        
        # 3. 写入 ADS 表
        print("[INFO] Writing to ADS: ecop.ads_user_rfm")
        rfm_df.write \
            .mode("overwrite") \
            .format("hive") \
            .saveAsTable("ecop.ads_user_rfm")
            
        print(f"[SUCCESS] RFM calculated for {rfm_df.count()} users.")
        
    except Exception as e:
        print(f"[ERROR] RFM Calculation failed: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()
