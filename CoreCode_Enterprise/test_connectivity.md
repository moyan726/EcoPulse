# HBase 连接性测试脚本 (test_connectivity.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/test_connectivity.py`
*   **作用**: 验证 Windows 开发环境与 Linux Hadoop 集群之间的网络连通性及服务可用性。

## 2. 测试项
### 2.1 HDFS 连接测试
*   **目标**: `hdfs://hadoop1:9000`
*   **方法**: 使用 Socket 直接连接 NameNode RPC 端口。
*   **意义**: 确保 Spark 作业可以读取底层数据。

### 2.2 HBase Thrift 连接测试
*   **目标**: `192.168.121.160:9090`
*   **方法**: 使用 `happybase` 库尝试建立连接并列出所有表。
*   **意义**: 确保 Python 后端及可视化应用可以读写 HBase 数据。

## 3. 故障排查指南
如果测试失败，脚本会输出以下建议：
1.  检查防火墙是否放行 9090 端口。
2.  确认 `hbase-daemon.sh start thrift` 已在 Master 节点执行。
3.  确认 `hbase-site.xml` 中已配置 `hbase.thrift.support.proxyuser=true`。

## 4. 变更记录
*   **2026-02-12**: 初始版本，集成 HDFS 与 HBase 双重检测逻辑。
