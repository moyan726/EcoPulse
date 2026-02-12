# 启动 Hive Metastore 服务指南

您的检查结果显示 **Hive Metastore 服务未运行**（只有 grep 进程）。这是导致 ETL 连接拒绝的根本原因。

请在 `hadoop1` 终端执行以下命令启动服务：

```bash
# 1. 启动 Metastore (后台运行)
nohup hive --service metastore > /dev/null 2>&1 &

# 2. 等待约 10-20 秒，确认端口监听
# 直到看到类似 "tcp ... 0.0.0.0:9083 ... LISTEN" 的输出
netstat -anp | grep 9083
```

启动成功后，请回复“已启动”，我将立即重新运行数据清洗脚本。
