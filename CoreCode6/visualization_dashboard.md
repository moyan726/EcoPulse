# CoreCode6 全栈可视化看板架构分析

---

### 1. 分析来源 (Analysis Source)
*   **应用入口**: [app.py](file:///e:/a_VibeCoding/EcoPulse/CoreCode6/app.py)
*   **技术栈**: Streamlit (v1.54+) + Custom HTML/JS/CSS + Plotly + Pandas
*   **架构模式**: 前后端分离 (Python Wrapper + Client-side Rendering)
*   **数据源**: `data/serving/*.csv` (支持自动回退与缓存)

### 2. 架构升级 (Architecture Evolution)
相较于 CoreCode5 的单文件脚本模式，CoreCode6 引入了工程化的分层架构，显著提升了可维护性与性能：

*   **资源解耦**: 将原本硬编码在 Python 中的 CSS/JS 提取为独立的静态资源文件 (`assets/`)，实现了**关注点分离**。
*   **组件化封装**: 通过 `components/` 目录封装了前端动画组件，Python 端仅需调用接口传递配置，无需关心底层 DOM 操作。
*   **健壮的数据层**: `utils/data_loader.py` 引入了环境变量配置、路径自动解析、异常捕获及 `st.cache_data` 缓存机制。
*   **性能优化**: 动画逻辑移交客户端 JS 引擎 (`requestAnimationFrame`)，并开启 GPU 硬件加速 (`will-change`)，彻底解决了 Streamlit 频繁重绘导致的卡顿问题。

### 3. 模块详解 (Module Breakdown)

#### 3.1 核心代码 (`CoreCode6/`)
*   **[app.py](file:///e:/a_VibeCoding/EcoPulse/CoreCode6/app.py)**: 主应用程序入口。负责页面布局、Tab 导航、数据上下文注入及组件组装。采用了 "Executive Dashboard" 布局风格。

#### 3.2 静态资源 (`CoreCode6/assets/`)
*   **[style.css](file:///e:/a_VibeCoding/EcoPulse/CoreCode6/assets/style.css)**: 全局样式表。
    *   定义了深色模式 (Dark Mode) 的配色方案。
    *   实现了 `.metric-card` 的玻璃拟态 (Glassmorphism) 效果与悬浮交互。
    *   配置了 `fadeInUp`, `shimmer` 等 CSS 关键帧动画。
*   **[motion-runtime.js](file:///e:/a_VibeCoding/EcoPulse/CoreCode6/assets/motion-runtime.js)**: 客户端动画运行时。
    *   实现了基于 `requestAnimationFrame` 的高性能动画循环。
    *   内置 `Easing` 缓动函数库 (easeOutCubic, easeOutBack)。
    *   提供无障碍支持 (Respect Reduced Motion)。
*   **[charts.js](file:///e:/a_VibeCoding/EcoPulse/CoreCode6/assets/charts.js)**: 自定义图表渲染器。
    *   包含 `renderCountUp` (数字递增), `renderRadar` (雷达图), `renderBars` (动态柱状图) 的底层 SVG/DOM 操作逻辑。

#### 3.3 组件层 (`CoreCode6/components/`)
*   **[animations.py](file:///e:/a_VibeCoding/EcoPulse/CoreCode6/components/animations.py)**: Python-JS 桥接层。
    *   负责读取 `assets` 资源并注入 Streamlit 前端。
    *   定义了 `AnimationConfig` 数据类，管理全局动画配置。
    *   提供了 `animated_number`, `animated_radar` 等 Python 函数接口，将参数序列化为 JSON 传递给前端。

#### 3.4 工具层 (`CoreCode6/utils/`)
*   **[data_loader.py](file:///e:/a_VibeCoding/EcoPulse/CoreCode6/utils/data_loader.py)**: 数据加载工具。
    *   封装了 Pandas 读取逻辑，统一处理日期格式转换。
    *   实现了数据缺失时的容错处理与日志记录。
*   **[theme.py](file:///e:/a_VibeCoding/EcoPulse/CoreCode6/utils/theme.py)**: 主题配置。
    *   统一管理 Plotly 图表的颜色序列与布局模板，确保图表风格与 UI 一致。

### 4. 核心功能与交互 (Features & Interactions)

#### 4.1 沉浸式总览 (Overview)
*   **动态 KPI 卡片**: 支持前缀/后缀、精度控制的数字递增动画，带有 Shimmer 扫光效果。
*   **交互式趋势图**: Plotly 折线图，支持缩放与 Tooltip 悬停。
*   **Top Brands 排行**: 带有生长动画的横向柱状图。

#### 4.2 转化漏斗 (Funnel)
*   **全链路漏斗**: 展示从 View 到 Purchase 的转化留存。
*   **核心指标雷达**: 动态展开的雷达图，多维展示转化率、留存率、复购率等关键指标。

#### 4.3 用户洞察 (User Insights)
*   **3D 聚类视图**: 可交互旋转缩放的 3D 散点图，直观展示 K-Means 用户分群。
*   **RFM 分布**: 环形图展示各价值分层用户占比。

### 5. 性能与工程化 (Performance & Engineering)
*   **GPU 加速**: 关键动画元素应用 `will-change: transform, opacity`，提升渲染帧率。
*   **缓存策略**: 数据加载层设置了 `ttl=300` 秒的缓存，避免重复 IO。
*   **类型安全**: Python 代码全面采用 Type Hints，JS 代码结构清晰，易于迁移至 TypeScript。
*   **热更新**: 修改 `assets` 目录下的 CSS/JS 无需重启 Streamlit，刷新浏览器即可生效。

---
*记录日期: 2026/2/11*
