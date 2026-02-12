"""
修复 Hive 分区脚本
在 Metastore 重建后，扫描 HDFS 目录并恢复 ODS 表的分区元数据。
"""
import sys
from pathlib import Path

# 添加项目根目录到 sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session

def main():
    spark = get_spark_session(
        app_name="EcoPulse_Repair_Partitions",
        enable_hive=True
    )
    
    print("[INFO] Repairing partitions for ecop.ods_user_behavior...")
    try:
        # 执行 MSCK REPAIR
        spark.sql("MSCK REPAIR TABLE ecop.ods_user_behavior")
        print("✅ Partition repair command sent.")
        
        # 验证分区数
        df = spark.sql("SHOW PARTITIONS ecop.ods_user_behavior")
        count = df.count()
        print(f"✅ Found {count} partitions.")
        df.show(10, truncate=False)
        
    except Exception as e:
        print(f"❌ Error repairing table: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()
