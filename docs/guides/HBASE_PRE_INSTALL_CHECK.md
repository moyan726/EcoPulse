为了确保 HBase 2.5.8 在您的环境中**一次性安装成功**，我需要您在 `hadoop1` 节点上执行以下命令，并将结果截图或复制给我。这些信息将决定配置文件的具体写法。

请按顺序执行：

### 1. 确认 Zookeeper 状态与端口
我们需要确认 ZK 是否正常运行，以及它使用的端口（默认是 2181，但需要核实）。

```bash
# 检查 Zookeeper 进程
jps | grep QuorumPeerMain

# 检查 Zookeeper 状态 (如果没配置环境变量，可能需要进 bin 目录)
/export/servers/zookeeper-3.7.0/bin/zkServer.sh status

# 查看 Zookeeper 配置文件，确认 clientPort
cat /export/servers/zookeeper-3.7.0/conf/zoo.cfg | grep clientPort
```

### 2. 确认 Hadoop 版本与 HDFS 路径
HBase 需要知道 HDFS 的具体地址。

```bash
# 检查 Hadoop 版本
hadoop version

# 查看 core-site.xml 中的 fs.defaultFS 配置 (关键！)
cat /export/servers/hadoop-3.3.0/etc/hadoop/core-site.xml | grep -A 1 fs.defaultFS
```

### 3. 确认 Hosts 映射
HBase 对主机名解析非常敏感，必须确保 `hadoop1`、`hadoop2`、`hadoop3` 互相能解析。

```bash
# 查看 hosts 文件
cat /etc/hosts

# 测试是否能 ping 通自己和其他节点
ping -c 2 hadoop1
ping -c 2 hadoop2
ping -c 2 hadoop3
```

### 4. 确认时间同步 (非常重要)
HBase 集群要求节点间时间误差不能超过 30 秒，否则 RegionServer 会自动挂掉。

```bash
# 查看所有节点时间 (如果配置了 ssh 免密)
date
ssh hadoop2 date
ssh hadoop3 date
```

### 5. 确认 JDK 路径
虽然我们看到了 `jdk1.8.0_241`，但为了稳妥，确认一下环境变量里的设置。

```bash
echo $JAVA_HOME
```

---

**请将以上 5 步的执行结果反馈给我，我将为您生成最精确的 `hbase-site.xml` 和 `hbase-env.sh` 内容。**
