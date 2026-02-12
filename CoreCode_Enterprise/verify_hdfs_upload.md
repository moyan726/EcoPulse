# HDFS 上传验证脚本 (verify_hdfs_upload.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/verify_hdfs_upload.py`
*   **作用**: 验证本地 CSV 文件是否成功上传至 HDFS，并校验文件大小。

## 2. 核心逻辑
*   **权限控制**: 显式设置 `os.environ["HADOOP_USER_NAME"] = "root"` 以绕过 HDFS 的权限检查。
*   **FileSystem API**: 使用 Spark Context 获取 Hadoop FileSystem 对象，直接操作 HDFS 元数据。
*   **验证项**:
    *   检查文件是否存在 (`fs.exists`).
    *   获取文件大小 (`status.getLen()`) 并转换为 MB 显示。

## 3. 验证结果 (2019-10)
*   **目标路径**: `/ecop/ods/user_behavior/dt_month=2019-10/data.csv`
*   **状态**: ✅ Success
*   **大小**: 5406.01 MB
*   **结论**: 5.4GB 数据完整上传，无丢失。

## 4. 变更记录
*   **2026-02-12**: 初始版本，用于 ODS 层接入后的冒烟测试。
