# HBase 2.5.8 分布式集群安装保姆级教程

> **适用环境**：
> - **操作节点**: 主要在 `hadoop1` (Master) 操作，然后分发给 `hadoop2`, `hadoop3`
> - **用户**: `root` (根据您提供的截图，您是 root 用户)
> - **前提**: Hadoop 和 Zookeeper 已经启动

---

## 第一阶段：下载与解压

**操作节点**: `hadoop1`

1.  **进入软件下载目录**
    ```bash
    [root@hadoop1 ~]# cd /export/software/
    ```

2.  **下载 HBase 2.5.8 安装包**
    ```bash
    # 如果服务器能联网，直接下载
    [root@hadoop1 software]# wget https://archive.apache.org/dist/hbase/2.5.8/hbase-2.5.8-bin.tar.gz
    
    # 如果不能联网，请先下载到 Windows，然后用 Xftp 上传到 /export/software/ 目录下
    ```

3.  **解压到运行目录**
    ```bash
    [root@hadoop1 software]# tar -zxvf hbase-2.5.8-bin.tar.gz -C /export/servers/
    ```

4.  **重命名文件夹 (方便后续配置)**
    ```bash
    [root@hadoop1 software]# cd /export/servers/
    [root@hadoop1 servers]# mv hbase-2.5.8 hbase
    ```

---

## 第二阶段：修改配置文件

**操作节点**: `hadoop1`
**工作目录**: `/export/servers/hbase/conf/`

1.  **进入配置目录**
    ```bash
    [root@hadoop1 servers]# cd /export/servers/hbase/conf/
    ```

2.  **修改 `hbase-env.sh` (环境配置)**
    
    使用 vi 编辑：
    ```bash
    [root@hadoop1 conf]# vi hbase-env.sh
    ```
    
    **操作方法**:
    - 按 `G` (大写) 跳到文件末尾
    - 按 `o` (小写) 在末尾插入新行
    - **复制并粘贴以下内容**:
    ```bash
    # ================= EcoPulse Config =================
    export JAVA_HOME=/export/servers/jdk1.8.0_241
    export HBASE_MANAGES_ZK=false
    export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
    export HBASE_PID_DIR=/var/run/hbase
    export HBASE_LOG_DIR=/export/servers/hbase/logs
    ```
    - 按 `Esc`，输入 `:wq` 保存退出。

3.  **修改 `hbase-site.xml` (核心配置)**

    使用 vi 编辑：
    ```bash
    [root@hadoop1 conf]# vi hbase-site.xml
    ```
    
    **操作方法**:
    - 找到 `<configuration>` 和 `</configuration>` 标签。
    - 删除这中间的所有内容 (如果有的话)。
    - **复制并粘贴以下完整内容**:
    ```xml
    <configuration>
        <!-- 1. HDFS 存储路径 (必须与 core-site.xml 保持一致) -->
        <property>
            <name>hbase.rootdir</name>
            <value>hdfs://hadoop1:9000/hbase</value>
        </property>
    
        <!-- 2. 分布式模式开关 -->
        <property>
            <name>hbase.cluster.distributed</name>
            <value>true</value>
        </property>
    
        <!-- 3. Zookeeper 集群地址 (复用现有 ZK) -->
        <property>
            <name>hbase.zookeeper.quorum</name>
            <value>hadoop1,hadoop2,hadoop3</value>
        </property>
        <property>
            <name>hbase.zookeeper.property.clientPort</name>
            <value>2181</value>
        </property>
        <property>
            <name>hbase.zookeeper.property.dataDir</name>
            <value>/export/data/zookeeper</value>
        </property>
    
        <!-- 4. Python/Thrift 连接支持 -->
        <property>
            <name>hbase.thrift.support.proxyuser</name>
            <value>true</value>
        </property>
        <property>
            <name>hbase.regionserver.thrift.http</name>
            <value>false</value>
        </property>
    
        <!-- 5. 稳定性配置 -->
        <property>
            <name>hbase.unsafe.stream.capability.enforce</name>
            <value>false</value>
        </property>
    </configuration>
    ```
    - 按 `Esc`，输入 `:wq` 保存退出。

4.  **修改 `regionservers` (节点列表)**

    使用 vi 编辑：
    ```bash
    [root@hadoop1 conf]# vi regionservers
    ```
    
    **操作方法**:
    - 删除里面的 `localhost`
    - **填入以下内容**:
    ```text
    hadoop1
    hadoop2
    hadoop3
    ```
    - 按 `Esc`，输入 `:wq` 保存退出。

5.  **建立 Hadoop 配置文件软链接 (关键！)**
    
    执行以下命令 (直接复制执行)：
    ```bash
    [root@hadoop1 conf]# ln -s /export/servers/hadoop-3.3.0/etc/hadoop/core-site.xml /export/servers/hbase/conf/core-site.xml
    [root@hadoop1 conf]# ln -s /export/servers/hadoop-3.3.0/etc/hadoop/hdfs-site.xml /export/servers/hbase/conf/hdfs-site.xml
    ```

---

## 第三阶段：分发与环境变量

**操作节点**: `hadoop1`

1.  **配置环境变量 (仅在 hadoop1)**
    
    ```bash
    [root@hadoop1 conf]# vi /etc/profile
    ```
    在文件末尾添加：
    ```bash
    # HBASE
    export HBASE_HOME=/export/servers/hbase
    export PATH=$PATH:$HBASE_HOME/bin
    ```
    保存退出后，使其生效：
    ```bash
    [root@hadoop1 conf]# source /etc/profile
    ```

2.  **分发 HBase 到其他节点**
    
    ```bash
    # 分发给 hadoop2
    [root@hadoop1 conf]# scp -r /export/servers/hbase root@hadoop2:/export/servers/
    
    # 分发给 hadoop3
    [root@hadoop1 conf]# scp -r /export/servers/hbase root@hadoop3:/export/servers/
    ```

3.  **分发环境变量**
    
    ```bash
    [root@hadoop1 conf]# scp /etc/profile root@hadoop2:/etc/profile
    [root@hadoop1 conf]# scp /etc/profile root@hadoop3:/etc/profile
    ```

4.  **在其他节点生效环境变量**
    
    **你需要分别登录 hadoop2 和 hadoop3 执行 source 命令！**
    
    ```bash
    [root@hadoop1 conf]# ssh hadoop2 "source /etc/profile"
    [root@hadoop1 conf]# ssh hadoop3 "source /etc/profile"
    ```

---

## 第四阶段：启动与验证

**操作节点**: `hadoop1`

1.  **启动顺序检查**
    
    确保 Hadoop 和 Zookeeper 都在运行：
    ```bash
    [root@hadoop1 ~]# jps
    # 必须看到: NameNode, ResourceManager, QuorumPeerMain
    ```

2.  **启动 HBase 集群**
    
    ```bash
    [root@hadoop1 ~]# start-hbase.sh
    ```

3.  **验证启动结果**
    
    ```bash
    [root@hadoop1 ~]# jps
    # 应该看到: HMaster, HRegionServer
    ```

4.  **启动 Thrift Server (供 Python 连接)**
    
    ```bash
    [root@hadoop1 ~]# hbase-daemon.sh start thrift
    ```
    
    再查一次 jps，应该多一个 `ThriftServer`。

5.  **进入 HBase Shell 测试**
    
    ```bash
    [root@hadoop1 ~]# hbase shell
    
    # 在 hbase shell 中输入:
    hbase:001:0> list
    TABLE
    0 row(s)
    ```
    
    如果显示 `0 row(s)`，恭喜你，安装成功！

---

## 第五阶段：回到 Windows

安装成功后，请在 Windows 告诉我，我们将运行 Python 脚本连接集群。
