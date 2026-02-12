
import os
import subprocess
from pyspark.sql import SparkSession

def check_hdfs_files():
    # Setup Spark
    spark = SparkSession.builder \
        .appName("Debug_HDFS") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.121.160:9000") \
        .getOrCreate()
    
    path = "/user/hive/warehouse/ecop.db/dwd_user_behavior"
    print(f"Checking path: {path}")
    
    try:
        # Try to read directly from path ignoring Metastore
        df = spark.read.parquet(path)
        count = df.count()
        print(f"Direct Parquet Read Count: {count}")
        
        # Show some partitions
        df.select("dt").distinct().show()
        
    except Exception as e:
        print(f"Error reading path: {e}")

    spark.stop()

if __name__ == "__main__":
    check_hdfs_files()
