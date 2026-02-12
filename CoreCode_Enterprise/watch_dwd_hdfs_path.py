import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from common.spark_config import get_spark_session


def main() -> None:
    spark = get_spark_session(app_name="Watch_DWD_HDFS_Path", enable_hive=False)
    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)

    base = jvm.org.apache.hadoop.fs.Path("/user/hive/warehouse/ecop.db/dwd_user_behavior")
    deadline = time.time() + 15 * 60

    while True:
        if not fs.exists(base):
            print("NOT_FOUND /user/hive/warehouse/ecop.db/dwd_user_behavior")
        else:
            statuses = fs.listStatus(base)
            children = [s.getPath().getName() for s in statuses]
            print(f"FOUND {len(children)} children under dwd_user_behavior")
            if children:
                print("SAMPLE:", ", ".join(children[:10]))
                break

        if time.time() >= deadline:
            print("TIMEOUT waiting for DWD files")
            break
        time.sleep(30)

    spark.stop()


if __name__ == "__main__":
    main()

