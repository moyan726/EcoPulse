"""
模块名称：数据质量审计 - 2019年10月数据
作者：moyan726
创建日期：2026-02-11
最后修改：2026-02-11

功能描述：
    基于 Track A（2019-10）样本数据，输出第一阶段必须完成的质量审计：
    - 价格异常值审计
    - TopN 品牌/类目占比（倾斜初步分析）

输入：
    - data/dwd/sample_oct_2019（Parquet 目录）

输出：
    - outputs/data_quality/price_outliers_summary.csv
    - outputs/data_quality/skew_top_brands.csv
    - outputs/data_quality/skew_top_categories.csv
    - outputs/data_quality/data_quality_report.json
    - logs/data_quality_oct_2019_<YYYYMMDD>.log
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit


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
    os.makedirs("outputs/data_quality", exist_ok=True)


def save_json(path: str, payload: dict[str, Any]) -> None:
    if "generated_at" not in payload:
        payload["generated_at"] = datetime.now().isoformat(timespec="seconds")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    logger = setup_logger("data_quality_oct_2019")

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

    spark = (
        SparkSession.builder.appName("DATA_QUALITY_Oct_2019")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(parquet_path).cache()
        total = df.count()
        logger.info("Total rows=%d", total)

        # 1. 价格异常审计
        price_lt0 = df.filter(col("price") < 0).count()
        price_eq0 = df.filter(col("price") == 0).count()
        price_le0 = df.filter(col("price") <= 0).count()
        p999 = df.stat.approxQuantile("price", [0.999], 0.01)[0]
        price_gt_p999 = df.filter(col("price") > lit(p999)).count()

        outlier_rows = [
            {"rule": "price < 0", "count": price_lt0, "rate": price_lt0 / total if total else 0.0, "threshold": None},
            {"rule": "price = 0", "count": price_eq0, "rate": price_eq0 / total if total else 0.0, "threshold": None},
            {"rule": "price <= 0", "count": price_le0, "rate": price_le0 / total if total else 0.0, "threshold": None},
            {"rule": "price > P99.9", "count": price_gt_p999, "rate": price_gt_p999 / total if total else 0.0, "threshold": float(p999)},
        ]
        outlier_df = pd.DataFrame(outlier_rows)
        outlier_df.to_csv("outputs/data_quality/price_outliers_summary.csv", index=False, encoding="utf-8-sig", float_format="%.6f")

        # 2. 品牌分布倾斜分析
        top_n = 30
        brand_top = (
            df.groupBy("brand")
            .agg(count(lit(1)).alias("cnt"))
            .orderBy(col("cnt").desc())
            .limit(top_n)
            .toPandas()
        )
        brand_top["share"] = brand_top["cnt"] / float(total)
        brand_top["cum_share"] = brand_top["share"].cumsum()
        brand_top.to_csv("outputs/data_quality/skew_top_brands.csv", index=False, encoding="utf-8-sig", float_format="%.6f")

        # 3. 类目分布倾斜分析
        category_top = (
            df.groupBy("category_code")
            .agg(count(lit(1)).alias("cnt"))
            .orderBy(col("cnt").desc())
            .limit(top_n)
            .toPandas()
        )
        category_top["share"] = category_top["cnt"] / float(total)
        category_top["cum_share"] = category_top["share"].cumsum()
        category_top.to_csv("outputs/data_quality/skew_top_categories.csv", index=False, encoding="utf-8-sig", float_format="%.6f")

        report: dict[str, Any] = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "results": {
                "price_p999": float(p999),
                "price_outliers": outlier_rows,
                "top_brands": brand_top.head(10).to_dict(orient="records"),
                "top_categories": category_top.head(10).to_dict(orient="records"),
            },
        }
        save_json("outputs/data_quality/data_quality_report.json", report)
        logger.info("Data Quality Audit completed successfully.")

    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
    finally:
        df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
