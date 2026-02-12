"""
执行 Hive DDL 脚本
在 Spark 中运行 hive_ddl.sql，创建 ODS/DWD/ADS 表结构。
"""
from pathlib import Path
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("EcoPulse_Init_Schema") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://192.168.121.160:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    ddl_file = Path(__file__).parent / "hive_ddl.sql"
    
    print(f"Reading DDL from: {ddl_file}")
    with open(ddl_file, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # 简单拆分 SQL 语句 (以分号分隔)
    statements = [s.strip() for s in sql_content.split(";") if s.strip()]
    
    for i, sql in enumerate(statements):
        print(f"\nExecuting statement {i+1}/{len(statements)}:")
        print(f"{sql[:100]}...") # 打印前100字符
        try:
            spark.sql(sql)
            print("✅ Success")
        except Exception as e:
            print(f"❌ Failed: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
