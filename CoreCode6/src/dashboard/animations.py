"""
EcoPulse · 动态数据可视化动画系统
=================================
核心能力:
1. 百分比 / 数值递增动画 (requestAnimationFrame)
2. 雷达图中心辐射展开 + 数据点依次出现 (SVG)
3. 柱状图自底向上生长 (可选轻微弹跳 Bounce)
4. 全局动画配置 (开关 / 速度 / 延迟 / fps / reduced-motion)

改进点 (vs VIEW 旧版):
- 统一使用 _COMMON_JS，避免每次组件重复注入 MotionRuntime
- 组件 key 生成策略更稳定，减少 Streamlit 不必要的 rerun
- 更严格的参数边界校验
- 帧率限流从 frameBudget 改为 requestAnimationFrame 原生节流
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from html import escape
from typing import Literal, Optional, Sequence
from uuid import uuid4

import streamlit as st
import streamlit.components.v1 as components

NumberFormat = Literal["percent", "integer", "float"]


# =====================================================================
#  全局动画配置
# =====================================================================
@dataclass
class AnimationConfig:
    """全局动画配置 — 所有动画组件均从此读取参数。"""

    enabled: bool = True
    respect_reduced_motion: bool = True
    global_speed_factor: float = 1.0
    global_delay_ms: int = 0
    fps_target: int = 60


def _normalize_config(config: AnimationConfig) -> AnimationConfig:
    return AnimationConfig(
        enabled=bool(config.enabled),
        respect_reduced_motion=bool(config.respect_reduced_motion),
        global_speed_factor=max(0.1, min(3.0, float(config.global_speed_factor))),
        global_delay_ms=max(0, int(config.global_delay_ms)),
        fps_target=max(1, min(120, int(config.fps_target))),
    )


def get_animation_config() -> AnimationConfig:
    """从 session_state 获取全局动画配置。"""
    raw = st.session_state.get("animation_config")
    if isinstance(raw, AnimationConfig):
        config = raw
    elif isinstance(raw, dict):
        defaults = AnimationConfig()
        config = AnimationConfig(
            enabled=raw.get("enabled", defaults.enabled),
            respect_reduced_motion=raw.get(
                "respect_reduced_motion", defaults.respect_reduced_motion
            ),
            global_speed_factor=raw.get(
                "global_speed_factor", defaults.global_speed_factor
            ),
            global_delay_ms=raw.get("global_delay_ms", defaults.global_delay_ms),
            fps_target=raw.get("fps_target", defaults.fps_target),
        )
    else:
        config = AnimationConfig()
    config = _normalize_config(config)
    st.session_state.animation_config = config
    return config


def set_animation_config(config: AnimationConfig) -> AnimationConfig:
    """更新并返回规范化后的全局动画配置。"""
    normalized = _normalize_config(config)
    st.session_state.animation_config = normalized
    return normalized


# =====================================================================
#  动画控制面板 (页面右侧集成 Popover)
# =====================================================================
_PANEL_CSS = """
<style>
.ecopulse-animation-panel [data-testid="stPopover"]{
  display:flex;
  justify-content:flex-start;
}
.ecopulse-animation-panel [data-testid="stPopover"] > button{
  min-height:40px!important;
  padding:0.36rem 0.9rem!important;
  border-radius:999px!important;
  border:1px solid rgba(0,212,255,.45)!important;
  background:linear-gradient(135deg,rgba(10,35,66,.88),rgba(16,64,104,.92))!important;
  color:#eaf6ff!important;
  font-size:.94rem!important;
  font-weight:700!important;
  letter-spacing:.2px!important;
  box-shadow:0 8px 22px rgba(0,0,0,.28),0 0 16px rgba(0,212,255,.16)!important;
  transition:transform .2s ease,box-shadow .2s ease,border-color .2s ease!important;
}
.ecopulse-animation-panel [data-testid="stPopover"] > button:hover{
  transform:translateY(-1px)!important;
  border-color:rgba(0,212,255,.82)!important;
  box-shadow:0 12px 26px rgba(0,0,0,.32),0 0 22px rgba(0,212,255,.28)!important;
}
.ecopulse-animation-panel [data-testid="stPopover"] > button svg{
  color:#93e8ff!important;
  fill:#93e8ff!important;
}
.ecopulse-animation-panel [data-testid="stPopover"] > button:focus-visible{
  outline:2px solid rgba(0,212,255,.6)!important;
  outline-offset:2px!important;
}
.ecopulse-animation-panel .stCaption{
  text-align:left;
  color:#b9d0ea!important;
  font-size:12px!important;
}
@media(max-width:900px){
  .ecopulse-animation-panel [data-testid="stPopover"]{
    justify-content:flex-start;
  }
  .ecopulse-animation-panel [data-testid="stPopover"] > button{
    width:100%!important;
    border-radius:12px!important;
  }
  .ecopulse-animation-panel .stCaption{
    text-align:left;
  }
}
</style>
"""


def animation_control_panel() -> AnimationConfig:
    """渲染左上集成式动画设置面板，返回当前配置。"""
    st.markdown(_PANEL_CSS, unsafe_allow_html=True)

    config = get_animation_config()
    speed_val = min(2.0, max(0.5, float(config.global_speed_factor)))
    delay_val = min(1200, max(0, int(config.global_delay_ms)))
    fps_opts = [30, 45, 60]
    fps_val = min(fps_opts, key=lambda v: abs(v - int(config.fps_target)))

    left_col, _ = st.columns([0.24, 0.76])
    with left_col:
        st.markdown('<div class="ecopulse-animation-panel">', unsafe_allow_html=True)
        with st.popover("🎛 动效设置", help="调整全局动画节奏", width="content"):
            st.markdown("##### 动画设置")
            enabled = st.toggle(
                "启用动画", value=config.enabled, help="关闭后所有动画直接显示最终状态。"
            )
            global_speed = st.slider(
                "速度倍率",
                0.5, 2.0, speed_val, 0.1,
                help="1.0=默认；0.5 更快；2.0 更慢。",
                disabled=not enabled,
            )
            global_delay = st.slider(
                "全局延迟 (ms)",
                0, 1200, delay_val, 50,
                help="所有动画统一附加的起始延迟。",
                disabled=not enabled,
            )
            fps_target = st.select_slider(
                "帧率目标 (fps)",
                options=fps_opts, value=fps_val,
                help="requestAnimationFrame 节流。",
                disabled=not enabled,
            )
            reduced = st.checkbox(
                "尊重系统 reduced-motion",
                value=config.respect_reduced_motion,
                help="若系统偏好「减少动态效果」则自动禁用动画。",
            )

            updated = set_animation_config(
                AnimationConfig(
                    enabled=enabled,
                    respect_reduced_motion=reduced,
                    global_speed_factor=global_speed,
                    global_delay_ms=global_delay,
                    fps_target=fps_target,
                )
            )
            st.caption(
                f"enabled={updated.enabled}  speed={updated.global_speed_factor:.1f}  "
                f"delay={updated.global_delay_ms}ms  fps={updated.fps_target}"
            )
        st.markdown("</div>", unsafe_allow_html=True)
    return updated


# =====================================================================
#  内部工具
# =====================================================================
def _component_id(prefix: str, key: Optional[str]) -> str:
    k = (key or "").strip()
    if k:
        norm = "".join(c if c.isalnum() else "-" for c in k).strip("-")
        if norm:
            return f"{prefix}-{norm}-{uuid4().hex[:6]}"
    return f"{prefix}-{uuid4().hex[:10]}"


def _render(markup: str, *, height: int) -> None:
    components.html(markup, height=height, scrolling=False)


# ── Motion Runtime (浏览器端 JS) ──────────────────────────
_COMMON_JS = """
<script>
const MotionRuntime=(()=>{
  const clamp=(v,lo,hi)=>Math.min(hi,Math.max(lo,v));
  const Easing={
    linear:t=>t,
    easeOutQuad:t=>t*(2-t),
    easeOutCubic:t=>1-Math.pow(1-t,3),
    easeOutQuart:t=>1-Math.pow(1-t,4),
    easeOutBack:t=>{const c1=1.2,c3=c1+1;return 1+c3*Math.pow(t-1,3)+c1*Math.pow(t-1,2);}
  };
  const prefersReduced=()=>window.matchMedia&&window.matchMedia('(prefers-reduced-motion:reduce)').matches;
  const shouldAnimate=c=>{if(!c||!c.enabled)return false;if(c.respect_reduced_motion&&prefersReduced())return false;return true;};
  const resolveDuration=(ms,c,cap)=>{if(!shouldAnimate(c))return 0;const s=clamp(Number(c.global_speed_factor||1),.1,3);let d=Math.max(0,Number(ms||0)*s);if(cap!=null)d=Math.min(d,cap);return d;};
  const resolveDelay=(ms,c)=>Math.max(0,Number(ms||0)+Number(c&&c.global_delay_ms||0));
  const frameInterval=c=>{const fps=clamp(Number(c&&c.fps_target||60),1,120);return 1000/fps;};
  const getEasing=n=>Easing[n]||Easing.easeOutCubic;
  const formatNumber=(v,fmt,prec,suf)=>{
    const p=Math.max(0,Number(prec||0)),s=suf||'',n=Number(v||0);
    if(fmt==='integer')return Math.round(n).toLocaleString()+s;
    if(fmt==='float')return n.toLocaleString(undefined,{minimumFractionDigits:p,maximumFractionDigits:p})+s;
    return n.toFixed(p)+s;
  };
  return{shouldAnimate,resolveDuration,resolveDelay,frameInterval,getEasing,formatNumber};
})();
</script>
"""


# =====================================================================
#  百分比 / 数值递增动画
# =====================================================================
def animated_number(
    value: float,
    label: str = "",
    format: NumberFormat = "integer",
    precision: int = 0,
    suffix: str = "",
    duration_ms: int = 1200,
    delay_ms: int = 0,
    easing: str = "easeOutCubic",
    font_size: str = "3rem",
    color: str = "#ffffff",
    height: int = 140,
    component_key: Optional[str] = None,
) -> None:
    """
    数值从 0 滚动至目标值的递增动画。

    - format: percent / integer / float
    - duration_ms: 动画持续时间（受全局速度倍率影响）
    - delay_ms: 本地延迟（叠加全局延迟）
    """
    config = get_animation_config()
    eid = _component_id("countup", component_key)

    payload = {
        "element_id": eid,
        "target": float(value),
        "format": format,
        "precision": max(0, int(precision)),
        "suffix": str(suffix) if suffix else ("%" if format == "percent" else ""),
        "duration_ms": max(0, int(duration_ms)),
        "delay_ms": max(0, int(delay_ms)),
        "easing": easing,
        "motion": asdict(config),
    }

    label_html = f'<div class="countup-label">{escape(label)}</div>' if label else ""

    markup = _COMMON_JS + f"""
<style>
.countup-wrap{{display:flex;flex-direction:column;justify-content:center;align-items:center;width:100%;min-height:120px;padding:10px 8px;box-sizing:border-box;contain:content;}}
.countup-label{{font-size:.95rem;color:#bfd3f1;margin-bottom:6px;letter-spacing:1px;text-transform:uppercase;text-align:center;}}
.countup-value{{font-weight:800;line-height:1;opacity:0;transform:translateY(8px);transition:opacity 180ms ease-out,transform 220ms ease-out;text-shadow:0 2px 10px rgba(0,0,0,.35);will-change:contents;}}
.countup-value.is-visible{{opacity:1;transform:translateY(0);}}
</style>
<div class="countup-wrap">
  {label_html}
  <div class="countup-value" id="{eid}" style="font-size:{escape(font_size,quote=True)};color:{escape(color,quote=True)};">0</div>
</div>
<script>
(()=>{{
  const cfg={json.dumps(payload,ensure_ascii=False)};
  const el=document.getElementById(cfg.element_id);if(!el)return;
  const m=cfg.motion||{{}};
  const doAnim=MotionRuntime.shouldAnimate(m);
  const dur=MotionRuntime.resolveDuration(cfg.duration_ms,m);
  const dly=MotionRuntime.resolveDelay(cfg.delay_ms,m);
  const ease=MotionRuntime.getEasing(cfg.easing);
  const fb=MotionRuntime.frameInterval(m);
  const target=Number(cfg.target||0),delta=target;
  let last=null;
  const render=v=>{{let o=v;if(cfg.format==='integer')o=delta>=0?Math.floor(v):Math.ceil(v);const t=MotionRuntime.formatNumber(o,cfg.format,cfg.precision,cfg.suffix);if(t!==last){{el.textContent=t;last=t;}}}};
  const final_=()=>{{el.classList.add('is-visible');const t=MotionRuntime.formatNumber(target,cfg.format,cfg.precision,cfg.suffix);if(t!==last){{el.textContent=t;last=t;}}}};
  if(!doAnim||dur<=0){{final_();return;}}
  const go=()=>{{el.classList.add('is-visible');let s=null,lp=-Infinity;
    const tick=ts=>{{if(s===null)s=ts;if(ts-lp<fb){{requestAnimationFrame(tick);return;}}lp=ts;const e=ts-s,p=Math.min(e/dur,1),cur=delta*ease(p);render(cur);if(p<1)requestAnimationFrame(tick);else final_();}};
    requestAnimationFrame(tick);}};
  if(dly>0)setTimeout(go,dly);else go();
}})();
</script>
"""
    _render(markup, height=height)


# =====================================================================
#  雷达图辐射展开动画
# =====================================================================
def animated_radar(
    categories: Sequence[str],
    values: Sequence[float],
    max_value: float = 100,
    title: str = "",
    total_duration_ms: int = 1400,
    stagger_ms: int = 100,
    delay_ms: int = 0,
    easing: str = "easeOutCubic",
    fill_color: str = "rgba(0, 212, 255, 0.28)",
    stroke_color: str = "#00d4ff",
    height: int = 430,
    component_key: Optional[str] = None,
) -> None:
    """雷达图中心辐射展开动画（SVG 绘制，JS 驱动）。"""

    n = min(len(categories), len(values))
    if n == 0:
        st.info("雷达图暂无数据")
        return

    cats = [str(categories[i]) for i in range(n)]
    vals = [float(values[i]) for i in range(n)]
    safe_max = max(1e-6, float(max_value))

    config = get_animation_config()
    eid = _component_id("radar", component_key)

    payload = {
        "element_id": eid,
        "title": str(title),
        "categories": cats,
        "values": vals,
        "max_value": safe_max,
        "total_duration_ms": max(0, int(total_duration_ms)),
        "stagger_ms": max(0, int(stagger_ms)),
        "delay_ms": max(0, int(delay_ms)),
        "easing": easing,
        "fill_color": str(fill_color),
        "stroke_color": str(stroke_color),
        "motion": asdict(config),
    }

    title_html = f'<div class="radar-title">{escape(title)}</div>' if title else ""

    markup = _COMMON_JS + f"""
<style>
.radar-wrap{{width:100%;padding:8px 6px 24px;box-sizing:border-box;contain:content;}}
.radar-title{{text-align:center;color:#ecf4ff;font-size:1.05rem;font-weight:600;margin-bottom:8px;}}
.radar-shell{{width:100%;max-width:500px;margin:0 auto;}}
.radar-svg{{width:100%;height:auto;overflow:visible;}}
.radar-grid{{fill:none;stroke:rgba(140,175,220,.22);stroke-width:1;}}
.radar-axis{{stroke:rgba(140,175,220,.32);stroke-width:1;}}
.radar-label{{font-size:12px;fill:#d4e3fb;text-anchor:middle;dominant-baseline:middle;}}
.radar-polygon{{stroke-width:2;filter:drop-shadow(0 2px 8px rgba(0,0,0,.24));}}
.radar-point{{opacity:0;transform-origin:center;transition:opacity 150ms ease-out;}}
.radar-point.is-visible{{opacity:1;}}
</style>
<div class="radar-wrap">
  {title_html}
  <div class="radar-shell">
    <svg class="radar-svg" id="{eid}" viewBox="-220 -200 440 400" aria-label="animated-radar"></svg>
  </div>
</div>
<script>
(()=>{{
  const cfg={json.dumps(payload,ensure_ascii=False)};
  const svg=document.getElementById(cfg.element_id);if(!svg)return;
  const NS='http://www.w3.org/2000/svg';
  const cats=cfg.categories||[],vals=cfg.values||[],cnt=Math.min(cats.length,vals.length);if(!cnt)return;
  const m=cfg.motion||{{}};
  const doAnim=MotionRuntime.shouldAnimate(m);
  const dur=MotionRuntime.resolveDuration(cfg.total_duration_ms,m,1500);
  const dly=MotionRuntime.resolveDelay(cfg.delay_ms,m);
  const ease=MotionRuntime.getEasing(cfg.easing);
  const fb=MotionRuntime.frameInterval(m);
  const R=120,slice=Math.PI*2/cnt,mx=Math.max(1e-6,Number(cfg.max_value||100));
  const mk=t=>document.createElementNS(NS,t);
  for(let l=1;l<=5;l++){{const r=R/5*l,pts=[];for(let i=0;i<cnt;i++){{const a=i*slice-Math.PI/2;pts.push(`${{(r*Math.cos(a)).toFixed(2)}},${{(r*Math.sin(a)).toFixed(2)}}`);}}const g=mk('polygon');g.setAttribute('points',pts.join(' '));g.setAttribute('class','radar-grid');svg.appendChild(g);}}
  for(let i=0;i<cnt;i++){{const a=i*slice-Math.PI/2,x=R*Math.cos(a),y=R*Math.sin(a);const ax=mk('line');ax.setAttribute('x1','0');ax.setAttribute('y1','0');ax.setAttribute('x2',x.toFixed(2));ax.setAttribute('y2',y.toFixed(2));ax.setAttribute('class','radar-axis');svg.appendChild(ax);const lb=mk('text');lb.setAttribute('x',(x*1.22).toFixed(2));lb.setAttribute('y',(y*1.22).toFixed(2));lb.setAttribute('class','radar-label');lb.textContent=cats[i];svg.appendChild(lb);}}
  const poly=mk('polygon');poly.setAttribute('class','radar-polygon');poly.setAttribute('fill',cfg.fill_color);poly.setAttribute('stroke',cfg.stroke_color);svg.appendChild(poly);
  const tgtPts=[],dots=[];
  for(let i=0;i<cnt;i++){{const a=i*slice-Math.PI/2,nr=Math.max(0,Number(vals[i]||0)/mx),r2=nr*R,tx=r2*Math.cos(a),ty=r2*Math.sin(a);tgtPts.push({{x:tx,y:ty}});const d=mk('circle');d.setAttribute('class','radar-point');d.setAttribute('r','4.5');d.setAttribute('cx','0');d.setAttribute('cy','0');d.setAttribute('fill',cfg.stroke_color);d.setAttribute('stroke','#fff');d.setAttribute('stroke-width','1.5');const tip=mk('title');tip.textContent=`${{cats[i]}}: ${{Number(vals[i]||0).toLocaleString()}}`;d.appendChild(tip);svg.appendChild(d);dots.push(d);}}
  const setF=(p,el)=>{{const e=ease(Math.max(0,Math.min(1,p)));const pp=[];const ss=cnt>1?Math.min(Number(cfg.stagger_ms||100),dur/(cnt-1)):0;for(let i=0;i<cnt;i++){{const cx=tgtPts[i].x*e,cy=tgtPts[i].y*e;pp.push(`${{cx.toFixed(2)}},${{cy.toFixed(2)}}`);dots[i].setAttribute('cx',cx.toFixed(2));dots[i].setAttribute('cy',cy.toFixed(2));if(el>=i*ss||p>=1)dots[i].classList.add('is-visible');}}poly.setAttribute('points',pp.join(' '));}};
  const final_=()=>setF(1,1e9);
  if(!doAnim||dur<=0){{final_();return;}}
  const go=()=>{{let s=null,lp=-Infinity;const tick=ts=>{{if(s===null)s=ts;if(ts-lp<fb){{requestAnimationFrame(tick);return;}}lp=ts;const el=ts-s,p=Math.min(el/dur,1);setF(p,el);if(p<1)requestAnimationFrame(tick);else final_();}};requestAnimationFrame(tick);}};
  if(dly>0)setTimeout(go,dly);else go();
}})();
</script>
"""
    _render(markup, height=height)


# =====================================================================
#  柱状图生长动画
# =====================================================================
def animated_bar_chart(
    categories: Sequence[str],
    values: Sequence[float],
    title: str = "",
    duration_ms: int = 900,
    stagger_ms: int = 80,
    delay_ms: int = 0,
    bounce: bool = True,
    bar_color: str = "#00d4ff",
    highlight_index: int = -1,
    highlight_color: str = "#ff9500",
    show_values: bool = True,
    height: int = 430,
    component_key: Optional[str] = None,
) -> None:
    """
    柱状图从底部向上生长动画（可选 bounce 回弹）。

    改进: 新增 highlight_index / highlight_color 用于品牌对比页面高亮选中品牌。
    """
    n = min(len(categories), len(values))
    if n == 0:
        st.info("柱状图暂无数据")
        return

    cats = [str(categories[i]) for i in range(n)]
    vals = [float(values[i]) for i in range(n)]
    max_val = max(max(vals), 1e-6)

    config = get_animation_config()
    eid = _component_id("bars", component_key)

    # 为每根柱子计算颜色
    colors = []
    for i in range(n):
        if highlight_index >= 0 and i == highlight_index:
            colors.append(highlight_color)
        elif highlight_index >= 0:
            colors.append("rgba(100,140,180,0.35)")  # 其余灰色
        else:
            colors.append(bar_color)

    payload = {
        "element_id": eid,
        "title": str(title),
        "categories": cats,
        "values": vals,
        "max_value": max_val,
        "duration_ms": max(0, int(duration_ms)),
        "stagger_ms": max(0, int(stagger_ms)),
        "delay_ms": max(0, int(delay_ms)),
        "bounce": bool(bounce),
        "colors": colors,
        "show_values": bool(show_values),
        "motion": asdict(config),
    }

    inner_h = max(220, height - 90)
    title_html = f'<div class="bars-title">{escape(title)}</div>' if title else ""

    markup = _COMMON_JS + f"""
<style>
.bars-wrap{{width:100%;padding:8px 8px 14px;box-sizing:border-box;contain:content;}}
.bars-title{{text-align:center;color:#ecf4ff;font-size:1.05rem;font-weight:600;margin-bottom:8px;}}
.bars-shell{{width:100%;max-width:680px;margin:0 auto;}}
.bars-svg{{width:100%;height:auto;overflow:visible;}}
.bars-axis{{stroke:rgba(145,178,220,.36);stroke-width:1;}}
.bars-rect{{rx:6;ry:6;filter:drop-shadow(0 3px 8px rgba(0,0,0,.22));}}
.bars-label{{font-size:11px;fill:#d4e3fb;text-anchor:middle;}}
.bars-value{{font-size:11px;fill:#fff;text-anchor:middle;font-weight:600;}}
</style>
<div class="bars-wrap">
  {title_html}
  <div class="bars-shell">
    <svg class="bars-svg" id="{eid}" viewBox="0 0 680 {inner_h}" aria-label="animated-bars"></svg>
  </div>
</div>
<script>
(()=>{{
  const cfg={json.dumps(payload,ensure_ascii=False)};
  const svg=document.getElementById(cfg.element_id);if(!svg)return;
  const NS='http://www.w3.org/2000/svg';
  const cats=cfg.categories||[],vals=cfg.values||[],cnt=Math.min(cats.length,vals.length);if(!cnt)return;
  const m=cfg.motion||{{}};
  const doAnim=MotionRuntime.shouldAnimate(m);
  const perDur=MotionRuntime.resolveDuration(cfg.duration_ms,m);
  const dly=MotionRuntime.resolveDelay(cfg.delay_ms,m);
  const fb=MotionRuntime.frameInterval(m);
  const ease=MotionRuntime.getEasing(cfg.bounce?'easeOutBack':'easeOutCubic');
  const cW=680,cH={inner_h},lP=36,rP=20,tP=16,bP=40;
  const uW=cW-lP-rP,uH=cH-tP-bP;
  const mx=Math.max(1e-6,Number(cfg.max_value||1));
  const gap=Math.max(8,Math.min(22,uW/(cnt*4)));
  const bW=Math.max(14,(uW-gap*(cnt-1))/cnt);
  const total=bW*cnt+gap*(cnt-1);
  const ox=lP+(uW-total)/2;
  const baseY=tP+uH;
  const mk=t=>document.createElementNS(NS,t);
  const ax=mk('line');ax.setAttribute('x1',String(lP-8));ax.setAttribute('y1',String(baseY));ax.setAttribute('x2',String(cW-rP+4));ax.setAttribute('y2',String(baseY));ax.setAttribute('class','bars-axis');svg.appendChild(ax);
  const bars=[];
  const colors=cfg.colors||[];
  for(let i=0;i<cnt;i++){{
    const x=ox+i*(bW+gap),tH=Math.max(0,(Number(vals[i]||0)/mx)*uH);
    const rect=mk('rect');rect.setAttribute('class','bars-rect');rect.setAttribute('x',x.toFixed(2));rect.setAttribute('y',baseY.toFixed(2));rect.setAttribute('width',bW.toFixed(2));rect.setAttribute('height','0');rect.setAttribute('fill',colors[i]||'#00d4ff');svg.appendChild(rect);
    const lb=mk('text');lb.setAttribute('class','bars-label');lb.setAttribute('x',(x+bW/2).toFixed(2));lb.setAttribute('y',(baseY+20).toFixed(2));lb.textContent=cats[i].length>10?cats[i].slice(0,10)+'…':cats[i];svg.appendChild(lb);
    let vl=null;if(cfg.show_values){{vl=mk('text');vl.setAttribute('class','bars-value');vl.setAttribute('x',(x+bW/2).toFixed(2));vl.setAttribute('y',(baseY-6).toFixed(2));vl.textContent=Number(vals[i]||0).toLocaleString();vl.style.opacity='0';svg.appendChild(vl);}}
    bars.push({{rect,vl,tH,off:i*Number(cfg.stagger_ms||0)}});
  }}
  const rbar=(b,p)=>{{const e=ease(Math.max(0,Math.min(1,p)));const h=Math.max(0,b.tH*e),y=baseY-h;b.rect.setAttribute('y',y.toFixed(2));b.rect.setAttribute('height',h.toFixed(2));if(b.vl){{b.vl.setAttribute('y',(y-6).toFixed(2));b.vl.style.opacity=String(Math.min(1,p*1.4));}}}};
  const final_=()=>{{for(const b of bars)rbar(b,1);}};
  if(!doAnim||perDur<=0){{final_();return;}}
  const go=()=>{{let s=null,lp=-Infinity;const tick=ts=>{{if(s===null)s=ts;if(ts-lp<fb){{requestAnimationFrame(tick);return;}}lp=ts;const el=ts-s;let done=true;for(const b of bars){{const le=el-b.off;if(le<=0){{done=false;continue;}}const p=Math.min(le/perDur,1);if(p<1)done=false;rbar(b,p);}}if(!done)requestAnimationFrame(tick);else final_();}};requestAnimationFrame(tick);}};
  if(dly>0)setTimeout(go,dly);else go();
}})();
</script>
"""
    _render(markup, height=height)


# =====================================================================
#  导出
# =====================================================================
__all__ = [
    "AnimationConfig",
    "animation_control_panel",
    "animated_bar_chart",
    "animated_number",
    "animated_radar",
    "get_animation_config",
    "set_animation_config",
]
