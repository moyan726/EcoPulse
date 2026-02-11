"""
模块名称：Serving 层导出脚本 (Export to Serving)
作者：moyan726
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    1. 读取 ADS 层的所有分析结果。
    2. 导出为 CSV 格式供 Power BI 直接读取。
    3. 提供 MySQL 导出代码模版。

输入：
    - data/ads/
输出：
    - data/serving/*.csv
"""

import os
import sys
import shutil
import logging
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession

# 确保日志和输出目录存在
os.makedirs("logs", exist_ok=True)
os.makedirs("data/serving", exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/export_serving_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_spark_session():
    # 动态获取项目根目录和虚拟环境 Python 路径
    project_root = Path(__file__).resolve().parents[1]
    venv_python = (project_root / ".venv" / "Scripts" / "python.exe").as_posix()
    
    os.environ.setdefault("PYSPARK_PYTHON", venv_python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
    os.environ.setdefault("JAVA_HOME", r"E:\Java\jdk1.8.0_291")
    
    return SparkSession.builder \
        .appName("Export_To_Serving") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def export_to_csv(df, name):
    project_root = Path(__file__).resolve().parents[1]
    # Spark 输出的是一个目录
    output_dir = (project_root / f"data/serving/{name}_temp").as_posix()
    final_file = (project_root / f"data/serving/{name}.csv").as_posix()
    
    logger.info(f"Exporting {name} to CSV...")
    
    # 1. Spark 导出 (合并为一个 partition)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)
    
    # 2. 将 Spark 输出目录下的 part-xxxx.csv 移动并重命名为最终文件
    try:
        temp_path = Path(output_dir)
        csv_file = next(temp_path.glob("part-*.csv"))
        shutil.move(str(csv_file), final_file)
        # 清理临时目录
        shutil.rmtree(output_dir)
        logger.info(f"Successfully exported {name} to {final_file}")
    except Exception as e:
        logger.error(f"Failed to rename CSV for {name}: {str(e)}")

def main():
    spark = get_spark_session()
    project_root = Path(__file__).resolve().parents[1]
    
    # 定义需要导出的表
    tables = {
        "funnel_stats": "data/ads/ads_funnel_stats",
        "user_retention": "data/ads/ads_user_retention",
        "user_rfm": "data/ads/ads_user_rfm",
        "user_clusters": "data/ads/ads_user_clusters"
    }

    for name, path_rel in tables.items():
        path = (project_root / path_rel).as_posix()
        if os.path.exists(path):
            df = spark.read.parquet(path)
            
            # 1. 导出为 CSV (Power BI 常用)
            export_to_csv(df, name)
            
            # 2. MySQL 导出模版 (如果需要使用，请取消注释并配置参数)
            """
            logger.info(f"Exporting {name} to MySQL...")
            df.write.format("jdbc") \
                .option("url", "jdbc:mysql://localhost:3306/graduation_db?useSSL=false") \
                .option("dbtable", f"ads_{name}") \
                .option("user", "root") \
                .option("password", "your_password") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("overwrite") \
                .save()
            """
        else:
            logger.warning(f"Path not found, skipping: {path}")

    logger.info("All serving data exported.")
    spark.stop()

if __name__ == "__main__":
    main()
