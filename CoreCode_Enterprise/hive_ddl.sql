-- 数据库
CREATE DATABASE IF NOT EXISTS ecop;

-- ═══════════════════════════════════════════════════════
-- ODS 层：原始数据 (外部表，指向 HDFS 上的 CSV)
-- ═══════════════════════════════════════════════════════
CREATE EXTERNAL TABLE IF NOT EXISTS ecop.ods_user_behavior (
    event_time      STRING,
    event_type      STRING,
    product_id      BIGINT,
    category_id     BIGINT,
    category_code   STRING,
    brand           STRING,
    price           DOUBLE,
    user_id         BIGINT,
    user_session    STRING
)
PARTITIONED BY (dt_month STRING)          -- 按月分区: '2019-10', '2019-11', ...
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/ecop/ods/user_behavior'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ═══════════════════════════════════════════════════════
-- DWD 层：清洗后明细 (管理表，Parquet + Snappy)
-- ═══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ecop.dwd_user_behavior (
    event_time        TIMESTAMP,
    event_type        STRING,
    product_id        BIGINT,
    category_id       BIGINT,
    category_code     STRING,
    brand             STRING,
    price             DOUBLE,
    user_id           BIGINT,
    user_session      STRING,
    price_is_illegal  INT               -- 异常价格标记
)
PARTITIONED BY (dt DATE)                 -- 按日分区
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ═══════════════════════════════════════════════════════
-- ADS 层：业务指标 (管理表，Parquet)
-- ═══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ecop.ads_user_rfm (
    user_id       BIGINT,
    recency       INT,
    frequency     INT,
    monetary      DOUBLE,
    r_score       INT,
    f_score       INT,
    m_score       INT,
    rfm_segment   STRING              -- 8 段分群，在 Spark 端直接计算
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ecop.ads_funnel_stats (
    event_type      STRING,
    session_count   BIGINT,
    user_count      BIGINT,
    dimension       STRING,
    dt              DATE,
    brand           STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ecop.ads_user_retention (
    cohort_date      DATE,
    period           INT,
    retention_count  BIGINT,
    cohort_size      BIGINT,
    retention_rate   DOUBLE
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ecop.ads_user_clusters (
    user_id     BIGINT,
    recency     INT,
    frequency   INT,
    monetary    DOUBLE,
    prediction  INT                   -- K-Means 簇 ID
)
STORED AS PARQUET;
