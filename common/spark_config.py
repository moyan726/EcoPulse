"""
EcoPulse 公共 Spark 配置模块。
所有 CoreCode 脚本通过此模块获取 SparkSession，消除重复代码。
"""
import os
import shutil
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def _pick_spark_local_dir() -> str:
    override = os.getenv("ECOPULSE_SPARK_LOCAL_DIR", "").strip()
    candidates: list[Path] = []
    if override:
        candidates.append(Path(override))

    temp_root = os.getenv("TEMP", "").strip()
    if temp_root:
        candidates.append(Path(temp_root) / "ecopulse_spark_local")

    candidates.append(PROJECT_ROOT / ".spark_local")

    best_path: Path | None = None
    best_free = -1
    for p in candidates:
        probe = p if p.exists() else p.parent
        if not probe.exists():
            continue
        try:
            free = shutil.disk_usage(str(probe)).free
        except Exception:
            continue
        if free > best_free:
            best_free = free
            best_path = p

    chosen = best_path if best_path is not None else (PROJECT_ROOT / ".spark_local")
    os.makedirs(chosen, exist_ok=True)
    return str(chosen)

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

    spark_local_dir = _pick_spark_local_dir()

    # 优化参数：降低并发，增加超时容忍
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.local.dir", spark_local_dir) \
        .config("spark.hadoop.dfs.replication", "1") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.default.parallelism", "100") \
        .config("spark.network.timeout", "1200s") \
        .config("spark.executor.heartbeatInterval", "120s")
    
    if hdfs_uri:
        builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_uri)
    
    if enable_hive:
        # 指向集群的 Hive Metastore (hadoop3)
        # 注意: 如果本地没有 hive-site.xml，可能需要显式指定 uris
        # 这里为了稳健，显式指定 Thrift URI
        builder = builder \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://192.168.121.162:9083") \
            .config("spark.sql.warehouse.dir", f"{hdfs_uri}/user/hive/warehouse") \
            .enableHiveSupport()
    
    return builder.getOrCreate()
