"""
CoreCode6 Benchmark - Data & Module Validation
"""
from __future__ import annotations

import os
import sys
import time

_BENCHMARK_DIR = os.path.dirname(os.path.abspath(__file__))
_CORECODE6_DIR = os.path.dirname(_BENCHMARK_DIR)
_PROJECT_ROOT = os.path.dirname(_CORECODE6_DIR)

if _CORECODE6_DIR not in sys.path:
    sys.path.insert(0, _CORECODE6_DIR)

SERVING_DIR = os.path.join(_PROJECT_ROOT, "data", "serving")
REQUIRED_FILES = [
    "funnel_stats.csv",
    "user_retention.csv",
    "user_rfm.csv",
    "user_clusters.csv",
]


def check_serving_data():
    print("=" * 60)
    print("  1. Serving Data Integrity")
    print("=" * 60)
    all_ok = True
    for fname in REQUIRED_FILES:
        path = os.path.join(SERVING_DIR, fname)
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 * 1024)
            print(f"  [OK] {fname:<25s}  {size_mb:>8.2f} MB")
        else:
            print(f"  [!!] {fname:<25s}  MISSING")
            all_ok = False
    print(f"\n  => {'All ready' if all_ok else 'Some missing'}")
    return all_ok


def benchmark_csv_load():
    import pandas as pd
    print("\n" + "=" * 60)
    print("  2. CSV Load Performance")
    print("=" * 60)
    total_rows, total_time = 0, 0.0
    for fname in REQUIRED_FILES:
        path = os.path.join(SERVING_DIR, fname)
        if not os.path.exists(path):
            print(f"  [skip] {fname}")
            continue
        t0 = time.perf_counter()
        df = pd.read_csv(path)
        elapsed = time.perf_counter() - t0
        total_rows += len(df)
        total_time += elapsed
        print(f"  {fname:<25s}  {len(df):>10,} rows  {elapsed:.3f}s")
    print(f"\n  => Total: {total_rows:,} rows in {total_time:.3f}s")


def check_module_imports():
    print("\n" + "=" * 60)
    print("  3. Module Import Check")
    print("=" * 60)
    pairs = [
        ("src.dashboard.animations", "AnimationConfig"),
        ("src.dashboard.utils", "COLOR_MAP"),
    ]
    for mod_name, attr in pairs:
        try:
            mod = __import__(mod_name, fromlist=[attr])
            obj = getattr(mod, attr, None)
            st = "OK" if obj is not None else "WARN"
            print(f"  [{st}] {mod_name}.{attr}")
        except Exception as e:
            print(f"  [FAIL] {mod_name} -- {e}")


def check_animation_config():
    print("\n" + "=" * 60)
    print("  4. Animation Config Self-test")
    print("=" * 60)
    try:
        from src.dashboard.animations import AnimationConfig, _normalize_config
        cfg = AnimationConfig()
        norm = _normalize_config(cfg)
        print(f"  Default: enabled={norm.enabled} speed={norm.global_speed_factor} "
              f"delay={norm.global_delay_ms}ms fps={norm.fps_target}")
        extreme = AnimationConfig(enabled=True, global_speed_factor=999.0,
                                  global_delay_ms=-100, fps_target=0)
        n2 = _normalize_config(extreme)
        tests = [
            ("speed clamped 3.0", n2.global_speed_factor == 3.0),
            ("delay clamped 0", n2.global_delay_ms == 0),
            ("fps clamped 1", n2.fps_target == 1),
        ]
        for desc, ok in tests:
            print(f"  [{'OK' if ok else 'FAIL'}] {desc}")
    except Exception as e:
        print(f"  [FAIL] {e}")


def main():
    print()
    print("=" * 60)
    print("  EcoPulse CoreCode6 Benchmark")
    print("=" * 60)
    print(f"  Project root: {_PROJECT_ROOT}")
    print(f"  Serving dir:  {SERVING_DIR}")
    print()
    ok = check_serving_data()
    if ok:
        benchmark_csv_load()
    check_module_imports()
    check_animation_config()
    print("\n" + "=" * 60)
    print("  Done")
    print("=" * 60)


if __name__ == "__main__":
    main()
