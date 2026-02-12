"""
验证 HDFS 文件上传脚本
"""
import os
from pyspark.sql import SparkSession

def verify_upload():
    os.environ["HADOOP_USER_NAME"] = "root"
    spark = SparkSession.builder \
        .appName("EcoPulse_Verify_Upload") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.121.160:9000") \
        .getOrCreate()

    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    
    path_str = "/ecop/ods/user_behavior/dt_month=2019-10/data.csv"
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path_str)
    
    try:
        if fs.exists(path):
            status = fs.getFileStatus(path)
            size_mb = status.getLen() / 1024 / 1024
            print(f"✅ Success: File exists at {path_str}")
            print(f"   Size: {size_mb:.2f} MB")
        else:
            print(f"❌ Failed: File not found at {path_str}")
    except Exception as e:
        print(f"Error checking file: {e}")
        
    spark.stop()

if __name__ == "__main__":
    verify_upload()
