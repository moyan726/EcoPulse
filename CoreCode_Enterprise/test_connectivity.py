"""
验证 Windows -> Hadoop 集群连通性。
测试项: HDFS 读写 / HBase 连接。
"""
import os
import sys
import time

def test_hdfs():
    """测试 HDFS 连通性 (模拟)"""
    print("=" * 50)
    print("[Test 1] HDFS Connectivity")
    print("=" * 50)
    
    # 既然我们还没有配置 core-site.xml，这里先只测试网络层
    # 真正的 HDFS 写入需要 PySpark 或 hdfs 库支持
    import socket
    host = "192.168.121.160" # hadoop1
    port = 9000 # NameNode RPC
    
    try:
        sock = socket.create_connection((host, port), timeout=3)
        print(f"  HDFS NameNode ({host}:{port}): ✅ Connected")
        sock.close()
        return True
    except Exception as e:
        print(f"  HDFS NameNode ({host}:{port}): ❌ Connection Failed ({e})")
        return False

def test_hbase():
    """测试 HBase Thrift 连通性"""
    print("\n" + "=" * 50)
    print("[Test 2] HBase Connectivity")
    print("=" * 50)
    
    try:
        import happybase
        # HBase Thrift Server 默认端口 9090
        # 即使 hosts 没配，直接用 IP 也能连
        host = "192.168.121.160"
        print(f"  Connecting to HBase Thrift Server at {host}:9090 ...")
        
        conn = happybase.Connection(host, port=9090, timeout=5000)
        conn.open()
        
        tables = conn.tables()
        print(f"  HBase tables: {[t.decode() for t in tables]}")
        print(f"  HBase connection: ✅ OK")
        
        conn.close()
        return True
    except Exception as e:
        print(f"  HBase connection: ❌ FAIL")
        print(f"  Error details: {e}")
        print(f"  Possible causes:")
        print(f"    1. HBase Thrift Server not started (run 'hbase thrift start' on hadoop1)")
        print(f"    2. Firewall blocking port 9090")
        return False

if __name__ == "__main__":
    print(f"Starting Connectivity Test... (Time: {time.strftime('%H:%M:%S')})")
    
    r1 = test_hdfs()
    r2 = test_hbase()
    
    print("\n" + "=" * 50)
    print(f"Summary Results:")
    print(f"  HDFS (Port 9000): {'✅ PASS' if r1 else '❌ FAIL'}")
    print(f"  HBase (Port 9090): {'✅ PASS' if r2 else '❌ FAIL'}")
    print("=" * 50)
