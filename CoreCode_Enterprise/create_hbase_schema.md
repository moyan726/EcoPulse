# HBase 表结构初始化脚本 (create_hbase_schema.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/create_hbase_schema.py`
*   **作用**: 基于 HappyBase 库，幂等地初始化 EcoPulse 项目所需的 HBase 表结构。

## 2. 核心逻辑
### 2.1 连接配置
*   连接到 HBase Thrift Server (`192.168.121.160:9090`)。
*   设置超时时间为 5000ms。

### 2.2 表结构定义
脚本自动创建以下三张核心业务表（如果表已存在则跳过）：

| 表名 | 列族 (Column Family) | 业务用途 |
| :--- | :--- | :--- |
| `ecop:user_profile` | `rfm`, `model` | 存储用户画像（RFM 分数）及 K-Means 聚类结果 |
| `ecop:funnel_stats` | `stats` | 存储漏斗分析统计数据（PV/UV/转化率） |
| `ecop:user_retention` | `retention` | 存储用户留存率数据（次日/7日留存） |

## 3. 执行方式
```bash
python CoreCode_Enterprise/create_hbase_schema.py
```

## 4. 变更记录
*   **2026-02-12**: 初始版本，实现了自动化 Schema 部署，支持幂等执行。
