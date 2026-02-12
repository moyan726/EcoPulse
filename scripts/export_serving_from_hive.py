from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session


TABLE_MAP: dict[str, str] = {
    "ecop.ads_funnel_stats": "funnel_stats.csv",
    "ecop.ads_user_retention": "user_retention.csv",
    "ecop.ads_user_rfm": "user_rfm.csv",
    "ecop.ads_user_clusters": "user_clusters.csv",
}


def _export_table_to_single_csv(spark, table_name: str, csv_path: Path) -> None:
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

    spark.stop()
    print("[DONE] serving CSV updated")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    main(force=args.force)
