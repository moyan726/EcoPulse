# 启动 Thrift Server 并进行最终测试

**状态分析**:
- ✅ `HMaster` (13863): HBase 主服务已启动，状态良好。
- ✅ `QuorumPeerMain` (13692): Zookeeper 正常。
- ❌ `ThriftServer`: **尚未启动**。这是 Python 连接 HBase 的桥梁，必须启动。

---

## 1. 启动 Thrift Server (在 hadoop1 执行)

请执行以下命令：

```bash
/export/servers/hbase-2.4.9/bin/hbase-daemon.sh start thrift
```

启动后，再次运行 `jps`，确保能看到 `ThriftServer` 进程。

---

## 2. Windows 端连接测试

一旦您在 `jps` 中看到了 `ThriftServer`，请立即告诉我。

我将运行以下脚本来验证 Windows 开发环境能否成功读写 HBase：

```python
# 测试脚本: CoreCode_Enterprise/test_connectivity.py
# 目标: 
# 1. 连接 192.168.121.160:9090
# 2. 列出当前表
```

期待您的好消息！
