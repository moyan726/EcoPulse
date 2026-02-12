"""
ADS 任务调度器
一键运行所有 ADS 层计算任务。
"""
import os
import sys
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
ADS_DIR = PROJECT_ROOT / "CoreCode_Enterprise" / "ADS_Calculation"

SCRIPTS = [
    "ads_user_rfm.py",
    "ads_funnel_stats.py",
    "ads_user_retention.py"
]

def run_script(script_name):
    script_path = ADS_DIR / script_name
    print(f"\n{'='*50}")
    print(f"🚀 Running {script_name}...")
    print(f"{'='*50}")
    
    try:
        # 使用当前环境的 python 解释器
        cmd = [sys.executable, str(script_path)]
        result = subprocess.run(cmd, check=True)
        if result.returncode == 0:
            print(f"✅ {script_name} completed successfully.")
        else:
            print(f"❌ {script_name} failed with code {result.returncode}.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_name}: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    print(f"Starting ADS Batch Execution (Total: {len(SCRIPTS)} jobs)")
    for script in SCRIPTS:
        run_script(script)
    print("\n🎉 All ADS jobs finished.")
