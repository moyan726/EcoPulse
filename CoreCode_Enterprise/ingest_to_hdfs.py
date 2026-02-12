"""
ODS 全量接入脚本
将本地 7 个月度 CSV 文件上传至 HDFS，并自动注册为 Hive 分区。
"""
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "row"

# 月份映射 (文件名 -> 分区值)
# 仅保留 2019-Oct 进行验证
MONTH_MAP = {
    "2019-Oct": "2019-10",
    # "2019-Nov": "2019-11",
    # "2019-Dec": "2019-12",
    # "2020-Jan": "2020-01",
    # "2020-Feb": "2020-02",
    # "2020-Mar": "2020-03",
    # "2020-Apr": "2020-04"
}

def get_spark():
    """构建支持 Hive 的 SparkSession"""
    # 解决 HDFS 权限问题: 设置 HADOOP_USER_NAME=root
    os.environ["HADOOP_USER_NAME"] = "root"
    
    return SparkSession.builder \
        .appName("EcoPulse_Ingest_ODS") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.121.160:9000") \
        .enableHiveSupport() \
        .getOrCreate()

def ingest_data(spark):
    """上传数据并修复分区"""
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    
    # 1. 遍历本地文件
    csv_files = list(DATA_DIR.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in {DATA_DIR}")
    
    for csv_file in csv_files:
        file_stem = csv_file.stem  # e.g., "2019-Oct"
        if file_stem not in MONTH_MAP:
            print(f"Skipping unknown file: {csv_file.name}")
            continue
            
        dt_month = MONTH_MAP[file_stem]
        hdfs_path = f"/ecop/ods/user_behavior/dt_month={dt_month}"
        
        print(f"\nProcessing {csv_file.name} -> {hdfs_path}...")
        
        # 2. 上传到 HDFS (使用 Hadoop API)
        # 先检查目录是否存在，不存在则创建
        target_dir = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path)
        if not fs.exists(target_dir):
            fs.mkdirs(target_dir)
            print(f"  Created directory: {hdfs_path}")
        
        # 上传文件
        local_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(str(csv_file))
        target_file = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"{hdfs_path}/data.csv")
        
        if fs.exists(target_file):
             print(f"  File already exists, skipping upload.")
        else:
             print(f"  Uploading {csv_file.stat().st_size / 1024 / 1024:.2f} MB...")
             fs.copyFromLocalFile(False, True, local_path, target_file) # delSrc=False, overwrite=True
             print(f"  Upload complete.")

    # 3. 修复 Hive 分区
    print("\nRunning MSCK REPAIR TABLE...")
    spark.sql("CREATE DATABASE IF NOT EXISTS ecop")
    
    # 确保表存在 (如果还没跑 DDL)
    # 这里简单起见，假设用户已经跑了 hive_ddl.sql 或者我们可以在这里通过 SparkSQL 建表
    # 但为了规范，建议先在 Hive/Beeline 执行 DDL。
    # 这里只做 Repair
    try:
        spark.sql("MSCK REPAIR TABLE ecop.ods_user_behavior")
        print("Partition repair command sent.")
        
        # 验证
        count_df = spark.sql("SELECT dt_month, count(*) as cnt FROM ecop.ods_user_behavior GROUP BY dt_month ORDER BY dt_month")
        count_df.show()
        
    except Exception as e:
        print(f"Error repairing table: {e}")
        print("Please ensure table 'ecop.ods_user_behavior' is created using 'hive_ddl.sql'.")

if __name__ == "__main__":
    spark = get_spark()
    ingest_data(spark)
    spark.stop()
