"""
EcoPulse 公共 Spark 配置模块。
所有 CoreCode 脚本通过此模块获取 SparkSession，消除重复代码。
"""
import os
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def get_spark_session(
    app_name: str = "EcoPulse",
    driver_memory: str = "8g",
    enable_hive: bool = False,
    hdfs_uri: str = "hdfs://192.168.121.160:9000",
) -> SparkSession:
    """
    构建统一的 SparkSession。
    
    Args:
        app_name:      Spark 应用名
        driver_memory:  Driver 内存
        enable_hive:    是否启用 Hive 支持
        hdfs_uri:       HDFS 地址 (设为空字符串则使用本地模式)
    """
    # 从环境变量读取，不再硬编码
    venv_python = (PROJECT_ROOT / ".venv" / "Scripts" / "python.exe").as_posix()
    os.environ.setdefault("PYSPARK_PYTHON", venv_python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
    
    # JAVA_HOME 必须通过 dev_shell.ps1 或 .env 设置，此处仅做 fallback
    os.environ.setdefault("JAVA_HOME", os.getenv("JAVA_HOME", ""))
    
    # 解决 HDFS 权限问题
    os.environ.setdefault("HADOOP_USER_NAME", "root")

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    
    if hdfs_uri:
        builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_uri)
    
    if enable_hive:
        # 指向集群的 Hive Metastore
        # 注意: 如果本地没有 hive-site.xml，可能需要显式指定 uris
        # 这里为了稳健，显式指定 Thrift URI
        builder = builder \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://192.168.121.160:9083") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://192.168.121.160:9083") \
            .enableHiveSupport()
    
    return builder.getOrCreate()
