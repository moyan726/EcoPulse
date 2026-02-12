# HBase 集群启动前的最后冲刺

看到您已经成功配置了环境变量并验证了 HDFS (`hdfs://hadoop1:9000`)，非常棒！

在启动之前，还有 **2 个关键动作** 必须执行，否则集群会启动失败或 Python 无法连接。

### 1. 补全 core-site.xml 软链接 (必须执行)
您刚才只链接了 `hdfs-site.xml`，但 HBase 还需要 `core-site.xml` 来知道 HDFS 的端口是 9000。

请在 `hadoop1` 执行：
```bash
ln -sf /export/servers/hadoop-3.3.0/etc/hadoop/core-site.xml /export/servers/hbase-2.4.9/conf/core-site.xml
```

### 2. 分发 HBase 程序包 (最耗时的一步)
您刚才只分发了 `profile`，现在必须把整个 HBase 安装包发给 `hadoop2` 和 `hadoop3`。

请在 `hadoop1` 执行：
```bash
# 发送给 hadoop2
scp -r /export/servers/hbase-2.4.9 root@hadoop2:/export/servers/

# 发送给 hadoop3
scp -r /export/servers/hbase-2.4.9 root@hadoop3:/export/servers/
```

---

### 3. 启动时刻！🚀

当上述分发完成后，请按顺序启动：

1.  **启动 HBase 集群**:
    ```bash
    /export/servers/hbase-2.4.9/bin/start-hbase.sh
    ```

2.  **检查进程 (JPS)**:
    ```bash
    jps
    # hadoop1 应显示: HMaster
    # hadoop2/3 应显示: HRegionServer
    ```

3.  **启动 Thrift Server (Python 连接专用)**:
    ```bash
    /export/servers/hbase-2.4.9/bin/hbase-daemon.sh start thrift
    ```

**等待您的启动捷报！** 启动成功后（JPS 看到 HMaster 和 ThriftServer），请告诉我，我们在 Windows 端一键测试连接。
