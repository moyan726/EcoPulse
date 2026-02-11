# EcoPulse 动态交互式数据看板设计方案

## 1. 设计概述 (Design Overview)

本方案旨在构建一个**动静结合、深度交互**的电商数据决策中心。区别于传统的静态报表，EcoPulse 看板引入了**“数据体验”**的设计理念，通过平滑的数值增长、图表生长动画和深度钻取交互，提升数据汇报的观赏性与决策效率。

### 1.1 核心设计理念
*   **Motion First**: 关键指标（KPI）与核心图表（雷达图、柱状图）均支持 60fps 的流畅入场动画，强化视觉冲击力。
*   **Interactive Deep Dive**: 从“总览”到“品牌”再到“用户”，提供层层递进的钻取分析路径。
*   **Configurable Experience**: 提供全局动画控制面板，适配不同性能设备（如会议室大屏 vs 移动端）及演示场景。

### 1.2 技术架构 (Tech Stack)
*   **Presentation Layer**: `Streamlit` (多页面应用框架) + `Plotly` (交互式图表库)。
*   **Animation Engine**: `Custom JS Runtime` (基于 requestAnimationFrame 的轻量级动画引擎)，不依赖 Python 低频刷新。
*   **Data Layer**: `Pandas` (数据处理) + `Local CSV Serving` (离线宽表) + `st.cache_data` (多级缓存)。

---

## 2. 系统架构设计 (System Architecture)

```mermaid
graph TD
    User[用户 / 决策者] -->|访问| App[Streamlit 主应用]
    
    subgraph "前端交互层 (View)"
        Nav[侧边栏导航 & 筛选]
        Panel[动画控制面板 (Popover)]
        
        subgraph "页面模块"
            P1[核心指标总览 (Overview)]
            P2[品牌深度分析 (Brand)]
            P3[用户画像洞察 (User)]
            P4[动画演示实验室 (Demo)]
        end
        
        Components[UI 组件库]
        Components -->|调用| AnimatedNum[数值递增动画]
        Components -->|调用| AnimatedRadar[雷达图辐射动画]
        Components -->|调用| AnimatedBar[柱状图生长动画]
    end
    
    subgraph "核心引擎 (Core)"
        Motion[Motion Runtime (JS)]
        State[Session State 管理]
        Theme[深色模式/样式注入]
    end
    
    subgraph "数据服务层 (Model)"
        Loader[Data Loader]
        Cache[st.cache_data]
        CSV[Serving CSV Files]
    end
    
    App --> Nav
    App --> Panel
    Nav --> P1 & P2 & P3 & P4
    P1 & P2 & P3 & P4 --> Components
    Components --> Motion
    Loader --> Cache --> CSV
```

---

## 3. 动画控制系统 (Animation Control System)

为了满足不同演示场景的需求，系统内置了一个悬浮式**动画控制面板**（对应 UI 右下角的设置按钮）。

### 3.1 详细配置参数
该面板以 `Popover` 形式存在，支持实时调整以下参数，即时生效：

| 参数项 | 类型 | 范围/选项 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- | :--- |
| **启用动画** (Enabled) | Toggle | On / Off | **On** | 全局总开关。关闭后所有图表直接显示最终状态，适合快速查阅数据。 |
| **速度倍率** (Speed) | Slider | 0.5x - 2.0x | **1.0x** | 控制动画播放速度。**0.5x** 适合演示细节，**2.0x** 适合快速汇报。 |
| **全局延迟** (Delay) | Slider | 0ms - 1200ms | **0ms** | 所有动画的起始等待时间，用于错峰展示。 |
| **帧率目标** (FPS) | Select | 30 / 45 / 60 | **60** | 性能调优选项。低性能设备可降至 30fps 以减少 CPU 占用。 |
| **Respect Reduced Motion** | Checkbox | True / False | **True** | 自动检测操作系统的“减少动态效果”设置，增强无障碍支持 (A11y)。 |

### 3.2 交互逻辑
*   用户调整参数后，通过 `st.session_state` 广播至所有组件。
*   组件重新渲染时，JS 运行时会自动读取最新配置并应用新的动画曲线。

---

## 4. 页面功能详细设计 (Page Specifications)

### 4.1 页面一：核心指标总览 (Overview)
*   **定位**: CEO / 高管视角的“驾驶舱”。
*   **关键组件**:
    1.  **动态 KPI 卡片组**:
        *   **PV / UV / 订单数**: 使用 `animated_number` 组件，数字从 0 滚动至千万级数值（如 `9,218,342`），带千分位。
        *   **转化率**: 百分比滚动（如 `0% -> 3.01%`），配色区分（紫色代表核心转化）。
    2.  **每日行为趋势图**:
        *   Plotly 折线图，展示 Session Count 随时间变化。
        *   支持 View / Cart / Purchase 多线条对比，Tooltip 显示具体数值。
    3.  **数据下载**: 支持导出当前筛选范围内的趋势数据 (CSV)。

### 4.2 页面二：品牌深度分析 (Brand Analysis)
*   **定位**: 市场 / 品牌运营视角的竞品分析工具。
*   **关键组件**:
    1.  **品牌筛选器**: 侧边栏下拉框，支持搜索特定品牌（如 Samsung, Apple）。
    2.  **品牌专属 KPI**: 选中品牌的 PV、销量、转化率，并与**大盘均值**进行对比（显示 ↑/↓ 趋势箭头）。
    3.  **品牌转化漏斗 (Funnel)**:
        *   View -> Cart -> Purchase 标准漏斗。
        *   **流失率标注**: 在漏斗层级间自动计算并标注流失率（如 “流失 88.6%”）。
    4.  **竞品销量对比 (动画柱状图)**:
        *   使用 `animated_bar_chart`。
        *   **生长动画**: 柱子从底部向上生长，带有轻微的 **Bounce (回弹)** 效果。
        *   **高亮**: 自动高亮当前选中的品牌颜色，其余品牌为灰色或辅助色。

### 4.3 页面三：用户画像洞察 (User Insights)
*   **定位**: CRM / 数据分析师视角的深度挖掘。
*   **关键组件**:
    1.  **RFM 价值分层**:
        *   Plotly 环形图，展示 Loyal, Champions, Hibernating 等 8 类人群的占比。
    2.  **K-Means 聚类画像 (动画雷达图)**:
        *   使用 `animated_radar` 组件。
        *   **辐射动画**: 雷达图背景网格先行，数据多边形从中心向外辐射展开。
        *   **维度**: 活跃度、购买频次、消费能力、忠诚度、价值潜力。
    3.  **留存热力图 (Retention Heatmap)**:
        *   展示用户在第 1-30 天的留存率衰减。
        *   颜色越深代表留存越好，直观发现留存断崖点。
    4.  **明细数据查询**:
        *   Data Grid 表格，支持按 User ID 搜索或按 RFM 分群筛选。
        *   显示 `recency`, `frequency`, `monetary` 具体数值。

### 4.4 页面四：动画演示实验室 (Animations Demo)
*   **定位**: 开发 / 演示用途，用于展示系统动画能力。
*   **功能**:
    *   独立展示所有动画组件（数值、雷达、柱状图）。
    *   提供“重播”按钮，方便反复调试动画效果。

---

## 5. 视觉与样式规范 (Visual & UI Standards)

### 5.1 配色方案 (Color Palette)
采用深色模式 (Dark Mode) 以突显数据图表的荧光色效：
*   **背景色**: `#0e1117` (Streamlit 默认深色)
*   **主色调 (Primary)**: `#00d4ff` (青蓝色，用于强调)
*   **辅助色 (Secondary)**: `#ff9500` (橙色，用于订单/警告), `#00ff88` (绿色，用于增长/成功), `#a855f7` (紫色，用于转化率)
*   **文本色**: `#fafafa` (主标题), `#bcbcbc` (次级文本)

### 5.2 字体与排版
*   **数字字体**: 使用等宽或无衬线字体（如 `Roboto Mono`, `Inter`），确保数字滚动时不抖动。
*   **卡片样式**: 统一使用圆角卡片 + 轻微阴影 (`box-shadow: 0 4px 6px rgba(0,0,0,0.1)`)，营造悬浮感。

---

## 6. 数据流设计 (Data Flow)

1.  **数据源**: `data/serving/*.csv` (由 ETL 脚本生成)。
2.  **加载层**: `utils/data_loader.py`
    *   使用 `st.cache_data` 读取 CSV。
    *   自动进行类型转换（如 `dt` 转 datetime）。
    *   **缓存策略**: 检查文件 `mtime`，若文件未更新则直接返回缓存，实现毫秒级响应。
3.  **计算层**:
    *   页面加载时基于 `Pandas` 进行即时聚合（Group By）。
    *   过滤逻辑（日期、品牌）在内存中完成。
4.  **渲染层**:
    *   将聚合后的数据转换为 JSON Payload。
    *   注入 HTML/JS 组件进行渲染。

---

## 7. 总结

本设计方案完全摒弃了静态报表的沉闷感，通过引入**前端动画引擎**和**全局交互配置**，打造了一个既专业又具观赏性的数据产品。它不仅能满足日常的数据监控需求，更能在汇报演示场景下提供极佳的视觉体验。
