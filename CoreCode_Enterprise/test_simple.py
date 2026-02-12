import happybase
import sys

def test_simple_connection():
    host = "192.168.121.160"
    port = 9090
    print(f"Connecting to {host}:{port}...")
    try:
        connection = happybase.Connection(host, port=port, timeout=5000)
        connection.open()
        print("Connected successfully!")
        print("Existing tables:", connection.tables())
        connection.close()
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_simple_connection()
