"""
全链路每日调度脚本
顺序执行: ODS分区修复 -> DWD ETL -> ADS 指标计算
"""
import sys
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENTERPRISE_DIR = PROJECT_ROOT / "CoreCode_Enterprise"
ADS_DIR = ENTERPRISE_DIR / "ADS_Calculation"

TASKS = [
    # 1. ODS Metadata Repair
    {
        "name": "ODS Partition Repair",
        "script": ENTERPRISE_DIR / "repair_partitions.py",
        "critical": True
    },
    # 2. DWD ETL
    {
        "name": "DWD ETL (Cleaning & Partitioning)",
        "script": ENTERPRISE_DIR / "etl_dwd_enterprise.py",
        "critical": True
    },
    # 3. ADS Calculations
    {
        "name": "ADS: User RFM",
        "script": ADS_DIR / "ads_user_rfm.py",
        "critical": False
    },
    {
        "name": "ADS: Funnel Stats",
        "script": ADS_DIR / "ads_funnel_stats.py",
        "critical": False
    },
    {
        "name": "ADS: User Retention",
        "script": ADS_DIR / "ads_user_retention.py",
        "critical": False
    }
]

def run_task(task):
    print(f"\n{'='*60}")
    print(f"🚀 Starting Task: {task['name']}")
    print(f"   Script: {task['script'].name}")
    print(f"{'='*60}")
    
    try:
        cmd = [sys.executable, str(task['script'])]
        # Stream output to console
        result = subprocess.run(cmd, check=True)
        print(f"✅ Task '{task['name']}' completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Task '{task['name']}' FAILED with exit code {e.returncode}.")
        if task['critical']:
            print("⛔ Critical task failed. Aborting pipeline.")
            sys.exit(1)
        return False
    except Exception as e:
        print(f"❌ Unexpected error in '{task['name']}': {e}")
        if task['critical']:
            sys.exit(1)
        return False

if __name__ == "__main__":
    print("🌟 Starting EcoPulse Daily Batch Pipeline 🌟")
    for task in TASKS:
        run_task(task)
    print("\n🎉 All Tasks Finished.")
