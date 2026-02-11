"""
模块名称：Spark 环境探测
作者：[待填]
创建日期：2026-01-29
最后修改：2026-01-29

功能描述：
    验证 PySpark 是否可正常启动（方案 A：Conda 隔离 + 会话级 JAVA_HOME）。

输出：
    - logs/test_spark_<YYYYMMDD>.log
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession


def setup_logger(script_name: str) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/{script_name}_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )
    return logging.getLogger(script_name)


def main() -> None:
    logger = setup_logger("test_spark")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("JAVA_HOME", r"E:\Java\jdk1.8.0_291")

    try:
        spark = SparkSession.builder.appName("Test").getOrCreate()
        logger.info("SparkSession created successfully")
        logger.info("Spark version: %s", spark.version)
        spark.stop()
    except Exception:
        logger.exception("Spark startup failed")
        raise


if __name__ == "__main__":
    main()
