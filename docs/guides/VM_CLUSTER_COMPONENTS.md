# 虚拟机集群与组件清单

> 最后核验时间：2026-03-07（UTC+8）  
> 适用范围：EcoPulse Hadoop/Hive 三节点环境

---

## 1. 集群节点信息

| 节点 | IP | 角色 | 已确认组件 |
|---|---|---|---|
| `hadoop1` | `192.168.121.160` | Master | NameNode, ResourceManager |
| `hadoop2` | `192.168.121.161` | Worker | DataNode, SecondaryNameNode, NodeManager |
| `hadoop3` | `192.168.121.162` | Worker + Hive | DataNode, NodeManager, Hive Metastore |

---

## 2. 关键服务与端口

| 服务 | 节点 | 端口 | 说明 |
|---|---|---|---|
| HDFS RPC | `hadoop1` | `9000` | `fs.defaultFS` 指向入口 |
| NameNode Web UI | `hadoop1` | `9870` | HDFS Web 管理页面 |
| Hive Metastore | `hadoop3` | `9083` | Spark SQL/Hive catalog 元数据入口 |

---

## 3. 当前项目配置映射

项目默认连接配置位于 [spark_config.py](/E:/a_VibeCoding/EcoPulse/common/spark_config.py)：

- `hdfs_uri`: `hdfs://192.168.121.160:9000`
- `spark.hadoop.hive.metastore.uris`: `thrift://192.168.121.162:9083`

只要集群端口和上述配置一致，Windows 侧脚本即可直接运行。

---

## 4. Hive 在 hadoop3 的启动要求

`hadoop3` 上 Hive 安装目录已确认：`/export/servers/hive-3.1.3`

每次新会话建议先设置：

```bash
export HIVE_HOME=/export/servers/hive-3.1.3
export PATH=$PATH:$HIVE_HOME/bin
```

验证 Hive 命令：

```bash
hive --version
```

启动 Metastore：

```bash
nohup hive --service metastore -p 9083 >/tmp/hive-metastore.log 2>&1 &
```

确认启动结果：

```bash
jps | grep -i Hive
ss -lntp | grep 9083
tail -n 50 /tmp/hive-metastore.log
```

---

## 5. 快速健康检查命令

### 5.1 在 hadoop1 检查 HDFS

```bash
ss -lntp | egrep ':9000|:9870'
hdfs getconf -confKey fs.defaultFS
```

预期示例：

- `192.168.121.160:9000` 处于 `LISTEN`
- `fs.defaultFS` 返回 `hdfs://hadoop1:9000`

### 5.2 在 hadoop3 检查 Hive Metastore

```bash
jps | grep -i Hive
ss -lntp | grep 9083
```

预期示例：

- 存在 `HiveMetaStore` 进程
- `*:9083` 处于 `LISTEN`

---

## 6. 常见故障与处理

### 问题 A：`hive: command not found`

原因：`HIVE_HOME`/`PATH` 未配置。  
处理：按第 4 节导出环境变量后重试。

### 问题 B：Windows 侧报 `Connection refused`（9000 或 9083）

原因：

- 服务未启动，或
- 启动在错误节点，或
- 端口未监听/被防火墙阻断。

处理：

1. 在对应节点执行 `ss -lntp` 检查端口监听。
2. 用 `jps` 确认 Java 进程存在。
3. 修正节点/端口后再执行本地 Spark 任务。

### 问题 C：Spark 能启动但 `SHOW DATABASES` 失败

原因：Metastore URI 不可达或 Hive 服务异常。  
处理：优先检查 `192.168.121.162:9083` 连通性和 `/tmp/hive-metastore.log`。

---

## 7. 已核验的数据覆盖（Hive 表）

按 2026-03-07 实测：

- `ecop.dwd_user_behavior`：覆盖 `2019-10` 和 `2019-11`
- `ecop.ads_funnel_stats`（`dimension='daily'`）：`2019-10-01` 到 `2019-11-30`（61 天）
- `ecop.ads_user_retention`：`cohort_date` 覆盖 `2019-10-01` 到 `2019-11-30`
- `ecop.ads_user_rfm`：`697,470` 行
- `ecop.ads_user_clusters`：`697,470` 行，簇为 `[0,1,2,3]`

此结果可作为“当前 Hive 正式结果”口径基线。
