"""
EcoPulse Dashboard · 工具模块
==============================
职责:
1. 常量 & 配色方案
2. CSS 全局样式注入 (深色模式 + 动画关键帧 + 高对比可访问性)
3. Plotly 深色主题注册
4. 统一数据加载 (st.cache_data + 文件 mtime 缓存键)

改进点 (vs VIEW 旧版):
- load_data 合并为单一入口，消除重复定义
- 使用项目根目录 data/serving/ 作为数据源，不再自带冗余副本
- CSS 中移除冗余的 JS countUp（由 animations.py 的 animated_number 取代）
- 更明确的 Plotly 模板字体 fallback
"""

from __future__ import annotations

import logging
import os
from typing import Tuple

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

logger = logging.getLogger(__name__)

# =====================================================================
#  常量 & 配色
# =====================================================================
COLOR_MAP = {
    "view": "#00d4ff",     # 青蓝 — 浏览
    "cart": "#00ff88",     # 绿色 — 加购
    "purchase": "#ff9500", # 橙色 — 购买
}

CLUSTER_COLORS = ["#00d4ff", "#a855f7", "#ff6b6b", "#ffd93d"]

RETENTION_HEATMAP_SCALE = [
    [0.0, "#edf3fb"],
    [0.25, "#dce9f8"],
    [0.5, "#c6dcf3"],
    [0.75, "#a5c8e8"],
    [1.0, "#7fb1db"],
]

DARK_TEMPLATE = {
    "layout": {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(15, 39, 68, 0.2)",
        "font": {
            "color": "#d7e7ff",
            "family": "Microsoft YaHei, Segoe UI, sans-serif",
            "size": 14,
        },
        "title": {"font": {"color": "#ffffff", "size": 20}},
        "xaxis": {
            "gridcolor": "rgba(45, 74, 111, 0.4)",
            "linecolor": "#2d4a6f",
            "tickfont": {"color": "#cfe2ff", "size": 12},
            "title": {"font": {"color": "#cfe2ff", "size": 14}},
        },
        "yaxis": {
            "gridcolor": "rgba(45, 74, 111, 0.4)",
            "linecolor": "#2d4a6f",
            "tickfont": {"color": "#cfe2ff", "size": 12},
            "title": {"font": {"color": "#cfe2ff", "size": 14}},
        },
        "legend": {
            "font": {"color": "#d7e7ff", "size": 12},
            "bgcolor": "rgba(0,0,0,0)",
        },
        "colorway": list(COLOR_MAP.values()) + CLUSTER_COLORS,
        "transition": {"duration": 800, "easing": "cubic-in-out"},
    }
}


# =====================================================================
#  Plotly 主题注册
# =====================================================================
def init_plotly_theme() -> None:
    """注册并激活 EcoPulse 深色 Plotly 主题。"""
    pio.templates["dark_dashboard"] = go.layout.Template(DARK_TEMPLATE)
    pio.templates.default = "dark_dashboard"


# =====================================================================
#  全局 CSS 注入
# =====================================================================
def inject_custom_css() -> None:
    """注入全局深色模式样式、动画关键帧及可访问性优化。"""
    st.markdown(
        """
<style>
/* ── 基础设置 ─────────────────────────────────────── */
html,body,[class*="css"]{
  font-family:'Microsoft YaHei','Segoe UI',sans-serif;
  font-size:16px!important;
  color:#d7e7ff;
  line-height:1.6;
}
section[data-testid="stMain"] .stMarkdown p,
section[data-testid="stMain"] .stMarkdown li,
section[data-testid="stMain"] .stMarkdown label,
section[data-testid="stMain"] .stCaption{color:#d7e7ff!important;}

/* ── 可访问性 ─────────────────────────────────────── */
@media(prefers-reduced-motion:reduce){
  *,*::before,*::after{
    animation-duration:.01ms!important;
    animation-iteration-count:1!important;
    transition-duration:.01ms!important;
  }
}

/* ── 深色背景 ─────────────────────────────────────── */
.stApp{background:linear-gradient(135deg,#0c1929 0%,#1a2b47 50%,#0d1f36 100%);overflow-x:hidden;}

/* ── 侧边栏 ──────────────────────────────────────── */
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#0f2744 0%,#1a3a5c 100%);border-right:1px solid #3a5a8a;}
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] span{color:#fff!important;font-size:16px!important;text-shadow:0 1px 2px rgba(0,0,0,.5);}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"]{margin-top:6px;}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] p{
  color:#d8ebff!important;
  font-size:1.5rem!important;
  font-weight:700!important;
  letter-spacing:.4px!important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a{
  color:#eaf3ff!important;
  min-height:40px!important;
  padding:8px 10px!important;
  border-radius:10px;
  font-size:1.5rem!important;
  font-weight:700!important;
  line-height:1.35!important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a span{
  color:#eaf3ff!important;
  font-size:1.5rem!important;
  font-weight:700!important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover{background:rgba(0,212,255,.12);border-radius:10px;}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-current="page"]{
  background:rgba(0,212,255,.2)!important;
  border:1px solid rgba(0,212,255,.35)!important;
}

/* ── 动画关键帧 ──────────────────────────────────── */
@keyframes fadeInUp{from{opacity:0;transform:translateY(30px)}to{opacity:1;transform:translateY(0)}}
@keyframes countUp{from{opacity:0;transform:scale(.5)}to{opacity:1;transform:scale(1)}}
@keyframes shimmer{0%{background-position:-200% 0}100%{background-position:200% 0}}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(0,212,255,.4)}50%{box-shadow:0 0 0 15px rgba(0,212,255,0)}}
@keyframes growUp{from{transform:scaleY(0);transform-origin:bottom}to{transform:scaleY(1);transform-origin:bottom}}

/* ── 指标卡片 ─────────────────────────────────────── */
.metric-card{
  background:linear-gradient(135deg,rgba(30,58,95,.95) 0%,rgba(45,90,135,.9) 100%);
  border-radius:16px;padding:28px 24px;text-align:center;
  border:1px solid rgba(80,130,180,.4);
  box-shadow:0 8px 32px rgba(0,0,0,.4);
  backdrop-filter:blur(12px);position:relative;overflow:hidden;
  animation:fadeInUp .6s ease-out forwards;opacity:0;
}
.stHorizontalBlock>div:nth-child(1) .metric-card{animation-delay:0s}
.stHorizontalBlock>div:nth-child(2) .metric-card{animation-delay:.1s}
.stHorizontalBlock>div:nth-child(3) .metric-card{animation-delay:.2s}
.stHorizontalBlock>div:nth-child(4) .metric-card{animation-delay:.3s}

.metric-card::before{
  content:'';position:absolute;top:0;left:0;right:0;bottom:0;
  background:linear-gradient(90deg,transparent 0%,rgba(255,255,255,.05) 50%,transparent 100%);
  background-size:200% 100%;animation:shimmer 3s ease-in-out infinite;pointer-events:none;
}
.metric-card:hover{transform:translateY(-6px) scale(1.02);border-color:#00d4ff;box-shadow:0 12px 48px rgba(0,212,255,.25);transition:all .4s cubic-bezier(.25,.46,.45,.94);}
.metric-card.blue{border-top:4px solid #00d4ff}
.metric-card.green{border-top:4px solid #00ff88}
.metric-card.orange{border-top:4px solid #ff9500}
.metric-card.purple{border-top:4px solid #a855f7}

.metric-value{font-size:2.8rem;font-weight:800;color:#fff;margin:14px 0;text-shadow:0 2px 12px rgba(0,0,0,.6),0 0 30px rgba(255,255,255,.1);animation:countUp .8s cubic-bezier(.34,1.56,.64,1) forwards;animation-delay:.4s;opacity:0;}
.metric-label{font-size:1.1rem;color:#e0ecff;text-transform:uppercase;letter-spacing:1.5px;font-weight:600;text-shadow:0 1px 4px rgba(0,0,0,.7);}

.trend-up{color:#00ff88;font-weight:bold;font-size:1rem;text-shadow:0 0 8px rgba(0,255,136,.5)}
.trend-down{color:#ff6b6b;font-weight:bold;font-size:1rem;text-shadow:0 0 8px rgba(255,107,107,.5)}
.trend-neutral{color:#a6c1ee;font-weight:bold;font-size:1rem;}

/* ── 标题 ─────────────────────────────────────────── */
.section-title{font-size:1.5rem;color:#fff;font-weight:700;margin-bottom:20px;padding-left:15px;border-left:5px solid #00d4ff;text-shadow:0 2px 8px rgba(0,212,255,.5);animation:fadeInUp .5s ease-out forwards;}
.big-title{
  font-size:clamp(1.9rem,2.5vw,2.5rem);color:#fff!important;font-weight:800;letter-spacing:.4px;
  margin:6px 0 18px;padding:6px 12px;border-left:5px solid #00d4ff;
  background:linear-gradient(90deg,rgba(8,26,47,.75),rgba(8,26,47,0));
  text-shadow:0 2px 10px rgba(0,0,0,.65),0 0 24px rgba(0,212,255,.35);
  animation:fadeInUp .5s ease-out forwards;
}
h1{font-size:2.2rem!important;color:#fff!important;text-shadow:0 0 20px rgba(0,212,255,.6),0 0 40px rgba(0,212,255,.3)!important;}

/* ── Streamlit UI 组件高对比 ──────────────────────── */
.stTabs [data-baseweb="tab-list"]{background:rgba(15,39,68,.6);border-radius:8px;padding:4px;}
.stTabs [data-baseweb="tab"]{color:#fff!important;font-size:16px!important;font-weight:600!important;text-shadow:0 1px 3px rgba(0,0,0,.5);padding:12px 20px!important;}
.stTabs [aria-selected="true"]{background:linear-gradient(135deg,#00d4ff33,#00ff8833)!important;border-radius:6px;}

.streamlit-expanderHeader,div[data-testid="stExpander"] summary{color:#f3f8ff!important;font-size:18px!important;font-weight:700!important;text-shadow:0 1px 4px rgba(0,0,0,.65);background:rgba(30,58,95,.72)!important;border-radius:8px!important;}
div[data-testid="stExpander"] summary *{color:inherit!important;}
div[data-testid="stExpander"] summary svg{color:#f3f8ff!important;fill:#f3f8ff!important;}
div[data-testid="stExpander"] summary:hover,.streamlit-expanderHeader:hover{background:rgba(45,90,135,.82)!important;}
.streamlit-expanderContent,div[data-testid="stExpander"] [data-testid="stExpanderDetails"]{background:rgba(15,39,68,.5)!important;border:1px solid #3a5a8a!important;border-radius:0 0 8px 8px!important;}

.stRadio>label,.stRadio [data-baseweb="radio"]>div{color:#fff!important;font-size:16px!important;font-weight:500!important;}
.stSelectbox label,.stMultiSelect label{color:#fff!important;font-size:16px!important;font-weight:600!important;text-shadow:0 1px 3px rgba(0,0,0,.5);}
.stSelectbox [data-baseweb="select"]>div,.stMultiSelect [data-baseweb="select"]>div{background:rgba(30,58,95,.8)!important;border-color:#3a5a8a!important;color:#fff!important;}
.stCheckbox label span{color:#fff!important;font-size:16px!important;}
.stTextInput label,.stNumberInput label{color:#fff!important;font-size:16px!important;font-weight:600!important;}
.stTextInput input,.stNumberInput input{background:rgba(30,58,95,.8)!important;border-color:#3a5a8a!important;color:#fff!important;}
.stTextInput input::placeholder,.stNumberInput input::placeholder{color:#c9dcf8!important;opacity:1;}
.stDateInput label{color:#fff!important;font-size:16px!important;font-weight:600!important;}
.stSubheader,h2,h3{color:#fff!important;text-shadow:0 1px 4px rgba(0,0,0,.5)!important;}
.stCaption,.stMarkdown p{color:#d7e7ff!important;font-size:15px!important;}
div[data-testid="stAlert"]{color:#f5f9ff!important;}
div[data-testid="stAlert"] *{color:inherit!important;}
.stButton>button{background:linear-gradient(135deg,#1e3a5f,#2d5a87)!important;color:#fff!important;border:1px solid #3a5a8a!important;font-weight:600!important;}
.stButton>button:hover{background:linear-gradient(135deg,#2d5a87,#3d7ab7)!important;border-color:#00d4ff!important;}
.stDownloadButton>button{background:linear-gradient(135deg,#0f5132,#198754)!important;color:#fff!important;border:none!important;}

/* ── 图表容器动画 ─────────────────────────────────── */
.stPlotlyChart{animation:fadeInUp .7s ease-out forwards;opacity:0;}
.stHorizontalBlock:nth-child(odd) .stPlotlyChart{animation-delay:.2s}
.stHorizontalBlock:nth-child(even) .stPlotlyChart{animation-delay:.4s}

/* ── Plotly 工具栏位置修正：右上改左上，避免与图例重叠 ───────── */
.stPlotlyChart .modebar-container{
  right:auto!important;
  left:8px!important;
}
.stPlotlyChart .modebar{
  right:auto!important;
  left:0!important;
}

/* ── 分隔线 & 隐藏默认 ───────────────────────────── */
hr{border:none;height:1px;background:linear-gradient(90deg,transparent,#3a5a8a,transparent);margin:30px 0;}
#MainMenu{visibility:hidden}
footer{visibility:hidden}
header[data-testid="stHeader"]{background:transparent;}

/* ── 性能优化 ─────────────────────────────────────── */
.metric-card,.stPlotlyChart,.section-title{will-change:transform,opacity;}
.metric-card{transform:translateZ(0);backface-visibility:hidden;}
</style>
""",
        unsafe_allow_html=True,
    )


# =====================================================================
#  统一数据加载
# =====================================================================
def _serving_dir() -> str:
    """数据目录：优先环境变量，否则使用项目根目录 data/serving。"""
    env = os.environ.get("SERVING_PATH", "")
    if env:
        return env
    # __file__ = CoreCode6/src/dashboard/utils.py
    # 向上 4 层: dashboard -> src -> CoreCode6 -> EcoPulse (项目根)
    project_root = os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
    )
    return os.path.join(project_root, "data", "serving")


def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def _mtime(path: str) -> float:
    return os.path.getmtime(path) if os.path.exists(path) else 0.0


@st.cache_data
def _load_data_cached(
    serving_dir: str, mtimes: Tuple[float, float, float, float]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """带缓存的数据加载（mtime 变化时自动刷新）。"""
    df_funnel = _read_csv(os.path.join(serving_dir, "funnel_stats.csv"))
    df_retention = _read_csv(os.path.join(serving_dir, "user_retention.csv"))
    df_rfm = _read_csv(os.path.join(serving_dir, "user_rfm.csv"))
    df_clusters = _read_csv(os.path.join(serving_dir, "user_clusters.csv"))

    # 类型转换
    if not df_funnel.empty and "dt" in df_funnel.columns:
        df_funnel["dt"] = pd.to_datetime(df_funnel["dt"], errors="coerce")
    if not df_retention.empty and "cohort_date" in df_retention.columns:
        df_retention["cohort_date"] = pd.to_datetime(
            df_retention["cohort_date"], errors="coerce"
        ).dt.date

    return df_funnel, df_retention, df_rfm, df_clusters


def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """加载所有 Serving 层数据，返回 (funnel, retention, rfm, clusters)。"""
    sd = _serving_dir()
    paths = [
        os.path.join(sd, f)
        for f in ("funnel_stats.csv", "user_retention.csv", "user_rfm.csv", "user_clusters.csv")
    ]
    missing = [p for p in paths if not os.path.exists(p)]
    if missing:
        logger.warning("缺少 Serving CSV: %s", missing)

    mtimes = tuple(_mtime(p) for p in paths)
    return _load_data_cached(sd, mtimes)
