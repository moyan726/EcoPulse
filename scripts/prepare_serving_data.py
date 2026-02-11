"""
模块名称：Serving 层数据准备脚本 (Prepare Serving Data)
创建日期：2026-02-11

功能描述：
    纯 Python (pandas + pyarrow) 实现，不依赖 PySpark / Java。
    读取 data/ads/ 下的 Parquet 分片文件，合并后导出为
    data/serving/*.csv，供 CoreCode6 看板直接使用。

输入：data/ads/ads_funnel_stats, ads_user_retention, ads_user_rfm, ads_user_clusters
输出：data/serving/funnel_stats.csv, user_retention.csv, user_rfm.csv, user_clusters.csv
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

# ── 项目根目录 ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ── Parquet 源 → CSV 目标映射 ──────────────────────────────────────
TABLE_MAP: dict[str, str] = {
    "ads_funnel_stats":   "funnel_stats.csv",
    "ads_user_retention": "user_retention.csv",
    "ads_user_rfm":       "user_rfm.csv",
    "ads_user_clusters":  "user_clusters.csv",
}

ADS_DIR    = PROJECT_ROOT / "data" / "ads"
SERVING_DIR = PROJECT_ROOT / "data" / "serving"


def _is_serving_ready() -> bool:
    """检查 data/serving/ 下 4 个 CSV 是否都已存在。"""
    if not SERVING_DIR.exists():
        return False
    return all((SERVING_DIR / csv_name).exists() for csv_name in TABLE_MAP.values())


def _read_parquet_dir(parquet_dir: Path) -> pd.DataFrame:
    """读取 Spark 风格的 Parquet 目录（多分片）并合并为一个 DataFrame。"""
    parts = sorted(parquet_dir.glob("*.parquet"))
    if not parts:
        raise FileNotFoundError(f"未找到 Parquet 文件：{parquet_dir}")
    frames = [pd.read_parquet(p) for p in parts]
    df = pd.concat(frames, ignore_index=True)
    return df


def _resegment_rfm(df: pd.DataFrame) -> pd.DataFrame:
    """
    基于 r_score / f_score / m_score 重新计算 8 段 RFM 分群。

    分群逻辑（适配实际数据分布）:
        fm_score = f_score + m_score  (综合购买力)
        fm_mid   = fm_score 的中位数（动态计算，通常为 5）

        ┌────────────────────────┬──────────────────────────────┬─────────────────────────────┐
        │ r_score \ fm           │ fm_score ≥ fm_hi (高购买力)   │ fm_score < fm_hi (低购买力)  │
        ├────────────────────────┼──────────────────────────────┼─────────────────────────────┤
        │ r_score ≥ 4 (近期活跃)  │ fm≥fm_hi+1 → Champions      │ Promising (成长客户)         │
        │                        │ else → Potential Loyalist     │                             │
        │ r_score == 3 (中等)     │ Loyal (一般价值客户)           │ freq≤1 → New Customers     │
        │                        │                              │ else → Need Attention       │
        │ r_score ≤ 2 (久未活跃)  │ At Risk (流失预警)            │ Hibernating (沉睡客户)       │
        └────────────────────────┴──────────────────────────────┴─────────────────────────────┘
    """
    required = {"r_score", "f_score", "m_score"}
    if not required.issubset(df.columns):
        print(f"  [警告] RFM 数据缺少必要列 {required - set(df.columns)}，跳过重分群。")
        return df

    import numpy as np

    df = df.copy()
    df["fm_score"] = df["f_score"] + df["m_score"]

    # 动态计算分位阈值，适配实际数据分布
    fm_mid = int(df["fm_score"].median())       # 中位数作为高低分界
    fm_hi = fm_mid + 1                          # 高购买力门槛

    print(f"  [RFM 阈值] fm_score 中位数={fm_mid}，高购买力阈值={fm_hi}")

    has_freq = "frequency" in df.columns
    freq_cond = (df["frequency"].fillna(0) <= 1) if has_freq else pd.Series(False, index=df.index)

    conditions = [
        # Champions: 近期活跃 + 高购买力（超过 fm_hi）
        (df["r_score"] >= 4) & (df["fm_score"] >= fm_hi + 1),
        # Potential Loyalist: 近期活跃 + 中等购买力
        (df["r_score"] >= 4) & (df["fm_score"] >= fm_hi) & (df["fm_score"] < fm_hi + 1),
        # Promising: 近期活跃 + 低购买力
        (df["r_score"] >= 4) & (df["fm_score"] < fm_hi),
        # New Customers: 中等活跃 + 低购买力 + 仅购买 1 次
        (df["r_score"] == 3) & (df["fm_score"] < fm_hi) & freq_cond,
        # Loyal: 中等活跃 + 高购买力
        (df["r_score"] == 3) & (df["fm_score"] >= fm_hi),
        # Need Attention: 中等活跃 + 低购买力 (非新客)
        (df["r_score"] == 3) & (df["fm_score"] < fm_hi) & (~freq_cond),
        # At Risk: 久未活跃 + 高购买力
        (df["r_score"] <= 2) & (df["fm_score"] >= fm_hi),
        # Hibernating: 久未活跃 + 低购买力
        (df["r_score"] <= 2) & (df["fm_score"] < fm_hi),
    ]
    labels = [
        "Champions (重要价值客户)",
        "Potential Loyalist (潜力客户)",
        "Promising (成长客户)",
        "New Customers (新客户)",
        "Loyal (一般价值客户)",
        "Need Attention (需要关注)",
        "At Risk (流失预警)",
        "Hibernating (沉睡客户)",
    ]

    df["rfm_segment"] = np.select(conditions, labels, default="Hibernating (沉睡客户)")

    seg_counts = df["rfm_segment"].value_counts()
    print(f"  [RFM 重分群] 共 {seg_counts.nunique()} 个分群：")
    for seg_name, cnt in seg_counts.items():
        print(f"    {seg_name}: {cnt:,}")

    return df


def prepare(force: bool = False) -> None:
    """主入口：从 Parquet 导出 CSV。"""
    if not ADS_DIR.exists():
        print(f"[错误] ADS 数据目录不存在：{ADS_DIR}")
        sys.exit(1)

    if not force and _is_serving_ready():
        print("[跳过] data/serving/ 已包含全部 CSV 文件。如需强制重新生成请使用 --force 参数。")
        return

    SERVING_DIR.mkdir(parents=True, exist_ok=True)

    total_start = time.perf_counter()
    for ads_name, csv_name in TABLE_MAP.items():
        parquet_dir = ADS_DIR / ads_name
        csv_path    = SERVING_DIR / csv_name

        if not parquet_dir.exists():
            print(f"[警告] {parquet_dir} 不存在，跳过。")
            continue

        t0 = time.perf_counter()
        print(f"  读取 {ads_name} ...", end=" ", flush=True)
        df = _read_parquet_dir(parquet_dir)
        print(f"{len(df):,} 行", end=" → ", flush=True)

        # ── RFM 数据特殊处理：重新计算 8 段分群 ──────────
        if ads_name == "ads_user_rfm":
            df = _resegment_rfm(df)

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        elapsed = time.perf_counter() - t0
        print(f"已导出 {csv_name} ({elapsed:.1f}s)")

    total_elapsed = time.perf_counter() - total_start
    print(f"\n[完成] 全部 CSV 已生成至 {SERVING_DIR}（总耗时 {total_elapsed:.1f}s）")


# ── CLI ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="将 ADS Parquet 数据导出为 Serving CSV")
    parser.add_argument("--force", action="store_true", help="强制重新生成，即使 CSV 已存在")
    args = parser.parse_args()
    prepare(force=args.force)
