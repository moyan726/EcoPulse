"""
Hive → Serving CSV 导出脚本 (含 RFM 自动分群)
================================================
从 Hadoop 集群的 Hive ADS 表导出数据到 data/serving/*.csv，
供 CoreCode6 看板直接使用。

RFM 表导出后会自动调用 prepare_serving_data 中的分群逻辑，
确保 rfm_segment 列始终存在。

用法:
    .venv\Scripts\python.exe scripts/export_serving_from_hive.py --force
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session
from scripts.prepare_serving_data import _resegment_rfm


TABLE_MAP: dict[str, str] = {
    "ecop.ads_funnel_stats": "funnel_stats.csv",
    "ecop.ads_user_retention": "user_retention.csv",
    "ecop.ads_user_rfm": "user_rfm.csv",
    "ecop.ads_user_clusters": "user_clusters.csv",
}


def _export_table_to_single_csv(spark, table_name: str, csv_path: Path) -> None:
    """将 Hive 表导出为单个 CSV 文件。"""
    df = spark.table(table_name)
    tmp_dir = csv_path.parent / f".tmp_{csv_path.stem}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    tmp_dir_uri = tmp_dir.resolve().as_uri()
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir_uri)
    part_files = list(tmp_dir.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"CSV part file not found under {tmp_dir}")

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        csv_path.unlink()
    shutil.move(str(part_files[0]), str(csv_path))
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _post_process_rfm(csv_path: Path) -> None:
    """
    对导出的 RFM CSV 进行后处理：
    若缺少 rfm_segment 列，自动基于原始 RFM 值打分并分群。
    """
    print(f"[POST] 对 {csv_path.name} 执行 RFM 分群后处理...")
    df = pd.read_csv(csv_path)
    if "rfm_segment" in df.columns:
        print(f"[POST] rfm_segment 已存在，跳过后处理。")
        return

    df = _resegment_rfm(df)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"[POST] 已写回 {csv_path.name}（{len(df):,} 行，含 rfm_segment）")


def main(force: bool) -> None:
    spark = get_spark_session(app_name="Export_Serving_From_Hive", enable_hive=True)
    serving_dir = PROJECT_ROOT / "data" / "serving"
    serving_dir.mkdir(parents=True, exist_ok=True)

    for table_name, filename in TABLE_MAP.items():
        out_path = serving_dir / filename
        if out_path.exists() and not force:
            print(f"[SKIP] {out_path} exists (use --force to overwrite)")
            continue

        if not spark.catalog.tableExists(table_name):
            print(f"[SKIP] Hive table not found: {table_name}")
            continue

        print(f"[EXPORT] {table_name} -> {out_path.name}")
        _export_table_to_single_csv(spark, table_name, out_path)

        # RFM 表需要额外后处理：补上 rfm_segment
        if "rfm" in table_name:
            _post_process_rfm(out_path)

    spark.stop()
    print("[DONE] serving CSV updated")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="从 Hive ADS 表导出 Serving CSV（含 RFM 自动分群）"
    )
    parser.add_argument("--force", action="store_true", help="强制覆盖已存在的 CSV")
    args = parser.parse_args()
    main(force=args.force)
