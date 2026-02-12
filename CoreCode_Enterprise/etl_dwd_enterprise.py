"""
DWD 层 ETL 脚本 (Enterprise Version)
从 Hive ODS 表读取全量数据，清洗后写入 Hive DWD 表。
"""
import sys
from pathlib import Path
import argparse
from pyspark.sql.functions import count as spark_count, col, lit, to_timestamp, to_date

# 添加项目根目录到 sys.path，以便导入 common 模块
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dt-month",
        action="append",
        dest="dt_months",
        default=[],
        help="ODS 月分区过滤（可重复指定），如：--dt-month 2019-10",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    spark = get_spark_session(
        app_name="EcoPulse_ETL_DWD",
        enable_hive=True
    )
    
    print("[INFO] Reading from ODS: ecop.ods_user_behavior")
    try:
        # 1. 读取 ODS 全量数据
        if args.dt_months:
            dt_month_list = ", ".join([f"'{m}'" for m in args.dt_months])
            df_raw = spark.sql(
                f"SELECT * FROM ecop.ods_user_behavior WHERE dt_month IN ({dt_month_list})"
            )
            print(f"[INFO] ODS filter dt_month IN ({dt_month_list})")
        else:
            df_raw = spark.sql("SELECT * FROM ecop.ods_user_behavior")
        
        # 2. 数据清洗
        # 2.1 剔除 Bot 流量 (Session 频率异常检测)
        # 统计每个 user_session 的事件数
        session_counts = df_raw.groupBy("user_session").agg(
            spark_count("*").alias("event_cnt")
        )
        # 阈值: 单 Session 超过 500 次事件视为 Bot
        bot_sessions = session_counts.filter(col("event_cnt") > 500)
        print("[INFO] Identified bot sessions (>500 events)")
        
        # 使用 left_anti join 剔除 Bot
        df_clean = df_raw.join(bot_sessions, on="user_session", how="left_anti")
        
        # 2.2 异常价格标记
        # 价格 <= 0 标记为 1 (illegal), 否则 0
        df_final = df_clean.withColumn("price_is_illegal", 
                                     (col("price") <= 0).cast("int"))
        
        # 2.3 类型转换
        # event_time string -> timestamp
        # dt 分区字段 -> date
        df_final = df_final \
            .withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss z")) \
            .withColumn("dt", to_date("event_time")) \
            .repartition(200) # 优化：降低写入并发，避免小文件和 DataNode 过载
            
        # 3. 写入 DWD 层
        print("[INFO] Writing to DWD: ecop.dwd_user_behavior")
        
        # 开启动态分区
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        
        # 调整写入顺序以匹配 Hive 表定义 (最后是分区字段 dt)
        # Hive DDL: event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session, price_is_illegal, dt
        cols = [
            "event_time", "event_type", "product_id", "category_id", "category_code", 
            "brand", "price", "user_id", "user_session", "price_is_illegal", "dt"
        ]
        
        df_final.select(cols).write \
            .mode("overwrite") \
            .insertInto("ecop.dwd_user_behavior")
            
        print("[SUCCESS] DWD ETL Completed.")
        
    except Exception as e:
        print(f"[ERROR] ETL Failed: {e}")
        # 如果是 Metastore 连接问题，尝试打印更详细信息
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    spark.stop()

if __name__ == "__main__":
    main()
