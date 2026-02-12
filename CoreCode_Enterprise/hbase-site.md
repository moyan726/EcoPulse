# HBase 核心配置文件说明 (hbase-site.xml)

## 1. 文件定位
*   **路径**: `/export/servers/hbase-2.4.9/conf/hbase-site.xml`
*   **作用**: 定义 HBase 集群的拓扑结构、存储路径及关键服务参数。

## 2. 核心配置项
### 2.1 HDFS 存储路径
```xml
<property>
    <name>hbase.rootdir</name>
    <value>hdfs://hadoop1:9000/hbase</value>
</property>
```
*   **说明**: 指定 HBase 数据在 HDFS 上的物理存储位置。
*   **注意**: 端口 `9000` 必须与 `core-site.xml` 中的 `fs.defaultFS` 保持一致。

### 2.2 Zookeeper 集群地址
```xml
<property>
    <name>hbase.zookeeper.quorum</name>
    <value>hadoop1,hadoop2,hadoop3</value>
</property>
<property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
</property>
```
*   **说明**: 指定外部 Zookeeper 集群的节点列表和端口。

### 2.3 Thrift Server 支持 (Python 连接)
```xml
<property>
    <name>hbase.thrift.support.proxyuser</name>
    <value>true</value>
</property>
```
*   **说明**: 允许 Thrift Server 使用代理用户模式。
*   **用途**: 这是 Python (`happybase`) 客户端能够成功连接并操作 HBase 的前提条件。

### 2.4 分布式模式
```xml
<property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
</property>
```
*   **说明**: 开启完全分布式模式。

## 3. 变更记录
*   **2026-02-12**: 修正了 `hbase.rootdir` 端口，添加了 Thrift 代理支持，完成了生产环境配置。
