# Hive DDL 执行脚本 (execute_ddl.py)

## 1. 脚本定位
*   **路径**: `CoreCode_Enterprise/execute_ddl.py`
*   **作用**: 通过 PySpark 连接 Hive Metastore，批量执行 `hive_ddl.sql` 中的建表语句，构建 ODS/DWD/ADS 数仓分层结构。

## 2. 核心逻辑
*   **SQL 解析**: 自动读取 `hive_ddl.sql` 文件，按分号 `;` 拆分为独立的 SQL 语句。
*   **执行引擎**: 使用 SparkSQL 引擎提交任务，相比 Beeline 更便于在 Python 环境中集成和错误处理。
*   **Metastore 连接**:
    *   `hive.metastore.uris`: 指向 `thrift://192.168.121.160:9083`。
    *   **注意**: 脚本目前配置为使用 Spark 内置 Derby 或远程 Hive，具体取决于 `hive-site.xml` 是否存在于 Classpath 中。

## 3. 执行结果
*   **2026-02-12**: 成功解析并执行了建表语句。由于 Hive Metastore 连接问题，暂时在 Spark 内部 Catalog 中创建了表结构，完成了逻辑验证。

## 4. 变更记录
*   **2026-02-12**: 初始版本，支持 SQL 文件自动解析与批量执行。
