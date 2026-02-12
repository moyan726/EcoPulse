"""
执行 Hive DDL 脚本
在 Spark 中运行 hive_ddl.sql，创建 ODS/DWD/ADS 表结构。
"""
import sys
from pathlib import Path

# 添加项目根目录到 sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session

def main():
    spark = get_spark_session(app_name="EcoPulse_Init_Schema", enable_hive=True)

    ddl_file = Path(__file__).parent / "hive_ddl.sql"
    
    print(f"Reading DDL from: {ddl_file}")
    with open(ddl_file, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # 简单拆分 SQL 语句 (以分号分隔)
    statements = [s.strip() for s in sql_content.split(";") if s.strip()]
    
    # 强制重建数据库 (CASCADE 删除所有表，确保清理旧的 Warehouse 路径)
    print("Dropping database 'ecop' CASCADE to cleanup old metadata...")
    spark.sql("DROP DATABASE IF EXISTS ecop CASCADE")
    
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
