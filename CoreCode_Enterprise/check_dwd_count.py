import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session

def check_dwd():
    spark = get_spark_session(app_name="Check_DWD", enable_hive=True)
    try:
        print("Checking DWD Table count...")
        spark.sql("USE ecop")
        # Check if table exists
        tables = spark.sql("SHOW TABLES").filter("tableName = 'dwd_user_behavior'").count()
        if tables == 0:
            print("Table dwd_user_behavior not found in Metastore.")
            return

        spark.sql("DESCRIBE FORMATTED dwd_user_behavior").show(100, False)
        count = spark.sql("SELECT count(*) FROM dwd_user_behavior").collect()[0][0]
        print(f"DWD Row Count: {count}")
    except Exception as e:
        print(f"Error checking DWD: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    check_dwd()
