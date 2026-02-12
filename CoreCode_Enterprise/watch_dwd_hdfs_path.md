# watch_dwd_hdfs_path.py

## 用途

用于“轮询等待”DWD 写入落盘的辅助脚本：周期性检查 HDFS 上 `ecop.dwd_user_behavior` 的底层目录，直到看到分区目录/文件出现，或超时退出。

适用场景：

- `etl_dwd_enterprise.py` 正在跑，你想在另一个终端持续观察是否已经开始产生分区目录
- HDFS 写入不稳定时，用来判断是“还在跑”还是“根本没写出来”

## 检查目标

- HDFS 目录：`/user/hive/warehouse/ecop.db/dwd_user_behavior`
- 默认轮询间隔：30 秒
- 默认最长等待：15 分钟

## 运行方式

```bash
python CoreCode_Enterprise/watch_dwd_hdfs_path.py
```

## 输出说明

- `FOUND 0 children under dwd_user_behavior`：目录存在但还没有子项（常见于写入尚未提交）
- `FOUND N children ...` + `SAMPLE: dt=...`：检测到分区/文件，脚本立即退出
- `TIMEOUT waiting for DWD files`：等待超时，通常意味着写入失败或持续阻塞

## 注意事项

- 该脚本只用于观测，不修改任何数据
- 若 Spark UI 端口被占用，可能出现 `Attempting port 4041` 等 WARN，不影响检查逻辑

