"""
模块名称：EDA 探索性分析 - 2019年10月数据
作者：moyan726
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    对 2019-10 月电商行为数据进行探索性分析，包括：
    - 事件类型分布（view/cart/purchase）
    - 缺失率统计（重点：brand/category_code）
    - 价格分位数与分布图
    - 会话事件数分布（用于识别异常高频会话）

输入：
    - data/dwd/sample_oct_2019（Parquet 目录）

输出：
    - outputs/eda/summary.json
    - outputs/eda/event_type_counts.csv
    - outputs/eda/missing_rates.csv
    - outputs/eda/price_hist.png
    - outputs/eda/session_event_count_stats.json
    - logs/eda_oct_2019_<YYYYMMDD>.log
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, col, count, isnan, lit, sum as spark_sum, when


def setup_logger(script_name: str) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/{script_name}_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )
    return logging.getLogger(script_name)


def ensure_dirs() -> None:
    os.makedirs("outputs/eda", exist_ok=True)


def save_json(path: str, payload: dict[str, Any]) -> None:
    if "generated_at" not in payload:
        payload["generated_at"] = datetime.now().isoformat(timespec="seconds")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    logger = setup_logger("eda_oct_2019")

    # 环境配置
    project_root = Path(__file__).resolve().parents[1]
    venv_python = (project_root / ".venv" / "Scripts" / "python.exe").as_posix()
    os.environ.setdefault("PYSPARK_PYTHON", venv_python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", venv_python)
    os.environ.setdefault("JAVA_HOME", r"E:\Java\jdk1.8.0_291")

    ensure_dirs()

    parquet_path = (project_root / "data" / "dwd" / "sample_oct_2019").as_posix()
    if not Path(parquet_path).exists():
        raise FileNotFoundError(f"Parquet 不存在：{parquet_path}，请先运行 sample_data.py")

    logger.info("Input Parquet=%s", parquet_path)

    spark = (
        SparkSession.builder.appName("EDA_Oct_2019")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(parquet_path).cache()

        # 1. 基础规模
        row_count = df.count()
        logger.info("Row count=%d", row_count)

        approx_uv = df.select(approx_count_distinct(col("user_id")).alias("uv")).collect()[0]["uv"]
        approx_sessions = df.select(approx_count_distinct(col("user_session")).alias("sessions")).collect()[0]["sessions"]
        logger.info("Approx distinct user_id=%d", int(approx_uv))
        logger.info("Approx distinct user_session=%d", int(approx_sessions))

        # 2. 事件类型分布
        event_type_counts = (
            df.groupBy("event_type").agg(count(lit(1)).alias("cnt")).orderBy(col("cnt").desc())
        )
        event_type_pdf = event_type_counts.toPandas()
        event_type_pdf.to_csv("outputs/eda/event_type_counts.csv", index=False, encoding="utf-8-sig")

        # 3. 缺失率统计
        cols = df.columns
        missing_exprs = []
        for c in cols:
            missing_exprs.append(
                spark_sum(
                    when(col(c).isNull() | (isnan(col(c)) if c == "price" else lit(False)), 1).otherwise(0)
                ).alias(c)
            )
        missing_counts = df.agg(*missing_exprs).collect()[0].asDict()
        missing_rates = [
            {
                "column": c,
                "missing_count": int(missing_counts.get(c, 0)),
                "missing_rate": float(missing_counts.get(c, 0)) / float(row_count),
            }
            for c in cols
        ]
        missing_rates_df = pd.DataFrame(missing_rates).sort_values(["missing_rate", "missing_count"], ascending=False)
        missing_rates_df.to_csv("outputs/eda/missing_rates.csv", index=False, encoding="utf-8-sig")

        # 4. 价格分位数
        price_q = df.stat.approxQuantile("price", [0.01, 0.5, 0.99, 0.999], 0.01)

        summary: dict[str, Any] = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "script": "eda_oct_2019.py",
            "results": {
                "approx_distinct": {"user_id": int(approx_uv), "user_session": int(approx_sessions)},
                "price_quantiles": {"p01": price_q[0], "p50": price_q[1], "p99": price_q[2], "p999": price_q[3]},
            },
        }
        save_json("outputs/eda/summary.json", summary)

        # 5. 价格分布图 (采样)
        sample_n = 500_000
        price_sample = (
            df.select(col("price").cast("double").alias("price"))
            .where(col("price").isNotNull() & (col("price") > 0))
            .sample(withReplacement=False, fraction=min(1.0, sample_n / max(1, row_count)))
            .limit(sample_n)
            .toPandas()
        )
        plt.rcParams["savefig.dpi"] = 300
        plt.figure(figsize=(10, 5))
        sns.histplot(price_sample["price"], bins=120, kde=False)
        plt.title("Price Distribution (sample, price > 0)")
        plt.xlabel("price")
        plt.ylabel("count")
        plt.tight_layout()
        plt.savefig("outputs/eda/price_hist.png", dpi=300, bbox_inches="tight")
        plt.close()

        # 6. 会话事件数分布
        session_counts = df.groupBy("user_session").agg(count(lit(1)).alias("events_in_session"))
        sess_desc = (
            session_counts.selectExpr(
                "min(events_in_session) as min",
                "percentile_approx(events_in_session, 0.5) as p50",
                "percentile_approx(events_in_session, 0.9) as p90",
                "percentile_approx(events_in_session, 0.99) as p99",
                "max(events_in_session) as max",
                "avg(events_in_session) as avg",
            )
            .collect()[0]
            .asDict()
        )
        save_json(
            "outputs/eda/session_event_count_stats.json",
            {"results": sess_desc},
        )

        logger.info("EDA completed successfully.")

    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
    finally:
        df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
