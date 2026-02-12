# HBase 节点配置文件说明 (regionservers)

## 1. 文件定位
*   **路径**: `/export/servers/hbase-2.4.9/conf/regionservers`
*   **作用**: 定义哪些节点将启动 `HRegionServer` 进程（数据存储与计算节点）。

## 2. 配置内容
```text
hadoop1
hadoop2
hadoop3
```

## 3. 配置策略
*   **全节点部署**: 将 `hadoop1` (Master) 也加入到 RegionServer 列表中。
*   **原因**:
    1.  **资源最大化**: 当前集群规模较小（3台），让 Master 节点分担部分存储压力可以提升整体容量。
    2.  **性能提升**: 增加一个计算节点，并行处理能力理论提升 50%。

## 4. 变更记录
*   **2026-02-12**: 从默认的 `localhost` 修改为 `hadoop1, hadoop2, hadoop3` 全量集群模式。
