# Spark 统一配置模块 (spark_config.py)

## 1. 脚本定位
*   **路径**: `common/spark_config.py`
*   **作用**: 作为项目级的公共配置模块，统一管理 SparkSession 的构建逻辑，消除各业务脚本中的重复配置代码。

## 2. 核心功能
### 2.1 统一环境适配
*   **Python 环境**: 自动从项目 `.venv` 目录解析 `python.exe` 路径，并设置 `PYSPARK_PYTHON` 和 `PYSPARK_DRIVER_PYTHON` 环境变量，确保 Spark Worker 使用正确的 Python 版本。
*   **Java 环境**: 尝试从系统环境变量读取 `JAVA_HOME`，并提供默认回退机制。
*   **HDFS 权限**: 强制设置 `HADOOP_USER_NAME=root`，解决 Windows 开发环境与 Linux HDFS 的权限冲突。

### 2.2 统一连接参数
*   **HDFS**: 默认指向 `hdfs://192.168.121.160:9000`。
*   **Hive Metastore**: 默认通过 Thrift 协议连接 `thrift://192.168.121.160:9083`。
*   **Spark 调优**: 
    *   `spark.driver.memory`: 默认为 **8g**，适应全量数据处理需求。
    *   `spark.sql.parquet.compression.codec`: 统一使用 **snappy** 压缩。
    *   `spark.sql.session.timeZone`: 统一时区为 **UTC**。

## 3. 使用示例
```python
from common.spark_config import get_spark_session

# 获取启用 Hive 支持的 SparkSession
spark = get_spark_session(
    app_name="My_ETL_Job",
    enable_hive=True
)
```

## 4. 变更记录
*   **2026-02-12**: 初始版本，支持 HDFS/Hive 开关配置及 Driver 内存调整。
