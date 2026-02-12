"""
HBase Schema 初始化脚本
根据 ENTERPRISE_DESIGN_V2.md 创建必要的 HBase 表结构。

需要先启动 HBase Thrift Server:
$ hbase thrift start
"""
import happybase
import sys

# HBase Thrift Server 配置
HBASE_HOST = "192.168.121.160"
HBASE_PORT = 9090

def create_schema():
    try:
        print(f"Connecting to HBase Thrift Server at {HBASE_HOST}:{HBASE_PORT}...")
        conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=5000)
        conn.open()
        
        existing_tables = [t.decode() for t in conn.tables()]
        print(f"Existing tables: {existing_tables}")
        
        # 定义表结构
        # 1. 用户画像表 (RFM + K-Means)
        # RowKey: MD5(user_id)[0:4] + user_id
        # Column Family: rfm, model
        if 'ecop:user_profile' not in existing_tables:
            print("Creating table 'ecop:user_profile'...")
            conn.create_table(
                'ecop:user_profile',
                {
                    'rfm': dict(),  # rfm:recency, rfm:frequency, rfm:monetary
                    'model': dict() # model:cluster_id
                }
            )
        else:
            print("Table 'ecop:user_profile' already exists.")

        # 2. 漏斗统计表
        # RowKey: dt + brand
        # Column Family: stats
        if 'ecop:funnel_stats' not in existing_tables:
            print("Creating table 'ecop:funnel_stats'...")
            conn.create_table(
                'ecop:funnel_stats',
                {
                    'stats': dict() # stats:pv, stats:uv, stats:conversion_rate
                }
            )
        else:
            print("Table 'ecop:funnel_stats' already exists.")

        # 3. 用户留存表
        # RowKey: cohort_date
        # Column Family: retention
        if 'ecop:user_retention' not in existing_tables:
            print("Creating table 'ecop:user_retention'...")
            conn.create_table(
                'ecop:user_retention',
                {
                    'retention': dict() # retention:day1, retention:day7, etc.
                }
            )
        else:
            print("Table 'ecop:user_retention' already exists.")
            
        conn.close()
        print("Schema initialization completed successfully.")
        return True
        
    except Exception as e:
        print(f"Error creating schema: {e}")
        return False

if __name__ == "__main__":
    create_schema()
