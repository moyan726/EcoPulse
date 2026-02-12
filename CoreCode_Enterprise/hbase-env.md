# HBase 环境配置文件说明 (hbase-env.sh)

## 1. 文件定位
*   **路径**: `/export/servers/hbase-2.4.9/conf/hbase-env.sh`
*   **作用**: 配置 HBase 运行所需的系统环境变量，特别是 Java 路径和 Zookeeper 管理模式。

## 2. 核心配置项
### 2.1 JDK 路径
```bash
export JAVA_HOME=/export/servers/jdk1.8.0_241
```
*   **说明**: 必须指向通过 `echo $JAVA_HOME` 确认的真实 JDK 安装路径。

### 2.2 Zookeeper 管理模式
```bash
export HBASE_MANAGES_ZK=false
```
*   **说明**: **关键配置**。
    *   `true`: HBase 启动自带的 Zookeeper（开发环境常用）。
    *   `false`: HBase 使用外部独立的 Zookeeper 集群（生产环境标准）。
    *   **本项目**: 复用了已有的 Zookeeper 3.7.0 集群，故设为 `false`。

### 2.3 Classpath 隔离
```bash
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
```
*   **说明**: 禁止 HBase 自动查找 Hadoop 的 Jar 包，避免版本冲突（Jar Hell）。

## 3. 变更记录
*   **2026-02-12**: 完成生产环境适配，指定了 JDK 1.8 并关闭了内置 ZK 管理。
