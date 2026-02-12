"""
验证数据可读性 (临时方案)
绕过 Hive Metastore，直接读取 HDFS 上的 CSV 并注册为临时视图进行查询。
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def verify_data():
    os.environ["HADOOP_USER_NAME"] = "root"
    spark = SparkSession.builder \
        .appName("EcoPulse_Verify_Data") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.121.160:9000") \
        .getOrCreate()

    # 定义 Schema (与 Hive DDL 一致)
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True)
    ])

    path = "/ecop/ods/user_behavior/dt_month=2019-10/data.csv"
    print(f"Reading from {path}...")
    
    try:
        df = spark.read.csv(path, schema=schema, header=True)
        print("Data loaded successfully.")
        
        # 简单统计
        count = df.count()
        print(f"Total rows in 2019-10: {count}")
        
        print("Sample data:")
        df.show(5)
        
    except Exception as e:
        print(f"Error reading data: {e}")
        
    spark.stop()

if __name__ == "__main__":
    verify_data()
