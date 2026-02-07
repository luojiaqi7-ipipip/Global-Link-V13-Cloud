import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime
import time

# 🎨 UI 全面升级：V14.1 PRO 机构级量化决策看板
st.set_page_config(page_title="Global-Link V14.1 PRO", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    html, body, [class*="css"], .stMarkdown {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    .main { background-color: #0d1117; color: #c9d1d9; }
    .cyber-title {
        font-weight: 700; color: #00f2ff; text-shadow: 0 0 10px rgba(0, 242, 255, 0.4);
        margin-bottom: 25px; text-align: center; border-bottom: 2px solid #00f2ff;
        padding-bottom: 10px; letter-spacing: 2px;
    }
    .decision-card {
        padding: 25px; border-radius: 15px; background: linear-gradient(145deg, #161b22 0%, #0d1117 100%);
        border: 2px solid #00f2ff; box-shadow: 0 0 20px rgba(0, 242, 255, 0.2);
        margin-bottom: 20px; transition: all 0.3s ease;
    }
    .decision-buy { border-color: #00ff88; box-shadow: 0 0 20px rgba(0, 255, 136, 0.2); }
    .decision-sell { border-color: #ff3366; box-shadow: 0 0 20px rgba(255, 51, 102, 0.2); }
    .decision-wait { border-color: #8b949e; box-shadow: 0 0 20px rgba(139, 148, 158, 0.1); }
    .highlight-value { font-weight: 800; letter-spacing: -1px; }
    .macro-card {
        background: #161b22; padding: 12px; border-radius: 8px; border-left: 3px solid #00f2ff;
        margin-bottom: 10px; min-height: 110px; display: flex; flex-direction: column;
        justify-content: center; position: relative;
    }
    .macro-label { font-size: 0.85rem; color: #8b949e; margin-bottom: 4px; display: flex; align-items: center; }
    .macro-value { font-size: 1.1rem; color: #00f2ff; font-weight: 700; }
    .macro-intel { font-size: 0.72rem; color: #8b949e; margin-top: 5px; border-top: 1px solid #30363d; padding-top: 5px; }
    .status-light { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
    .sys-log {
        background-color: #05070a; color: #00ff88; padding: 15px; border-radius: 8px;
        font-family: 'Consolas', monospace; border: 1px solid #30363d; height: 320px;
        overflow-y: auto; font-size: 0.9rem; line-height: 1.5;
    }
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f: return json.load(f)
        except: return None
    return None

def format_beijing_time(ts_str):
    """统一格式化为：2026-02-10 12:00（北京时间）"""
    if not ts_str or ts_str == "unknown": return "N/A"
    try:
        # 兼容 YYYYMMDD_HHMM 和 YYYY-MM-DD HH:mm
        if "_" in ts_str:
            dt = datetime.strptime(ts_str, "%Y%m%d_%H%M")
        else:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
        return dt.strftime("%Y-%m-%d %H:%M（北京时间）")
    except: return ts_str

base_dir = os.path.dirname(os.path.abspath(__file__))
audit_data = load_json(os.path.join(base_dir, 'data', 'audit_result.json'))
metrics_data = load_json(os.path.join(base_dir, 'data', 'processed', 'latest_metrics.json'))

# --- 侧边栏 ---
with st.sidebar:
    st.markdown("<h2 style='text-align: center; color: #00f2ff; font-weight:700;'>配置中心</h2>", unsafe_allow_html=True)
    st.markdown("---")
    st.subheader("📡 数据链路状态")
    sources = [{"name": "实时行情 (Sina)", "s": "在线"}, {"name": "机构级历史 (V14)", "s": "就绪"}]
    for s in sources: st.markdown(f"🟢 **{s['name']}**: `{s['s']}`")
    st.markdown("---")
    st.subheader("🧠 策略引擎")
    st.code("模型: Gemini 3 Flash\n架构: V14.1 PRO\n分位回溯: 5年 (1250D)", language="yaml")
    if st.button("🚀 强制刷新"): st.rerun()

# --- 主界面 ---
st.markdown("<h1 class='cyber-title'>GLOBAL-LINK V14.1 PRO 宏观特征全貌决策系统</h1>", unsafe_allow_html=True)

if audit_data:
    c1, c2 = st.columns([3, 1])
    decision = audit_data.get('decision', 'WAIT')
    factor = audit_data.get('attack_factor', 0.0)
    
    card_class = "decision-wait"
    d_color = "#8b949e"
    if "BUY" in decision.upper() or "买入" in decision: card_class = "decision-buy"; d_color = "#00ff88"
    elif "SELL" in decision.upper() or "卖出" in decision: card_class = "decision-sell"; d_color = "#ff3366"
    
    display_decision = decision
    if "WAIT" in decision.upper() or "等待" in decision or "观望" in decision: display_decision = "⏳ 观望等待"
    elif "BUY" in decision.upper() or "买入" in decision: display_decision = "⚔️ 策略买入"
    elif "SELL" in decision.upper() or "卖出" in decision: display_decision = "🛡️ 策略卖出"

    with c1:
        st.markdown(f"""
            <div class="decision-card {card_class}">
                <div style='display: flex; justify-content: space-between; align-items: center;'>
                    <div><span style='font-size: 1rem; color: #8b949e;'>当前策略指令</span><h1 style='color: {d_color}; margin:5px 0 0 0; font-size: 3.5rem;' class='highlight-value'>{display_decision}</h1></div>
                    <div style='text-align: right;'><span style='font-size: 1rem; color: #8b949e;'>配置目标</span><h2 style='color: #00f2ff; margin:5px 0 0 0; font-size: 2rem;'>{audit_data.get('target', 'CASH_NEUTRAL')}</h2></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
            <div class="decision-card" style='text-align: center; height: 100%; border-color: #f1e05a;'>
                <span style='font-size: 1rem; color: #8b949e;'>风险敞口系数</span>
                <h1 style='color: #f1e05a; margin:15px 0; font-size: 3.5rem;' class='highlight-value'>{factor}</h1>
                <div style='font-size: 0.8rem; color: #8b949e;'>Risk Exposure</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>🌐 全球宏观态势矩阵 (20D | 1Y | 5Y)</h3>", unsafe_allow_html=True)
    
    macro = metrics_data.get('macro_matrix', {}) if metrics_data else {}
    health = metrics_data.get('macro_health', {}) if metrics_data else {}
    ref_time = metrics_data.get('timestamp') if metrics_data else "unknown"

    def get_status_color(key):
        h_key = "CSI300_Vol" if key == "A_Share_Vol" else key
        if not health or h_key not in health: return "#ff3366"
        if health[h_key].get('status') == 'FAILED': return "#ff3366"
        return "#00ff88"

    def render_macro_cell(label, key):
        data = macro.get(key, {})
        color = get_status_color(key)
        if not data or not isinstance(data, dict):
            return f'<div class="macro-card"><div class="macro-label"><span class="status-light" style="background-color: {color};"></span>{label}</div><div class="macro-value">等待同步</div></div>'
        
        val = data.get('value', 'N/A')
        if key == 'A_Share_Vol': val = f"{data.get('amplitude', 'N/A')}%"
        elif key == 'Southbound': val = f"{val}亿"
        elif key == 'Margin_Debt': val = f"{val}亿"
        elif key in ['CN10Y', 'US10Y', 'SHIBOR'] and val != 'N/A': val = f"{val}%"
        
        change = data.get('change_pct')
        change_str = f" <span style='font-size:0.8rem; color:{'#00ff88' if (change or 0) >=0 else '#ff3366'}'>({change}%)</span>" if change is not None else ""
        
        p20 = round(data.get('p_20d', 50.0), 1)
        p1y = round(data.get('p_250d', 50.0), 1)
        p5y = round(data.get('p_1250d', 50.0), 1)
        slope = data.get('slope', 0.0)
        arrow = "↑" if slope > 0.0001 else ("↓" if slope < -0.0001 else "→")
        
        return f"""
            <div class="macro-card">
                <div class="macro-label"><span class="status-light" style="background-color: {color}; box-shadow: 0 0 5px {color};"></span>{label}</div>
                <div class="macro-value">{val}{change_str}</div>
                <div class="macro-intel">
                    <span style="color:#8b949e">分位:</span> {p20}% | {p1y}% | {p5y}%<br>
                    <span style="color:#8b949e">趋势:</span> {arrow} ({slope})
                </div>
            </div>
        """

    macro_items = [
        {"l": "离岸人民币", "k": "CNH"}, {"l": "纳斯达克", "k": "Nasdaq"}, {"l": "恒生指数", "k": "HangSeng"},
        {"l": "A50 指数", "k": "A50_Futures"}, {"l": "VIX 风险指数", "k": "VIX"}, {"l": "沪深300振幅", "k": "A_Share_Vol"},
        {"l": "中债10Y收益", "k": "CN10Y"}, {"l": "美债10Y收益", "k": "US10Y"}, {"l": "国内流动性", "k": "SHIBOR"},
        {"l": "港股通流入", "k": "Southbound"}, {"l": "两融变动", "k": "Margin_Debt"}, {"l": "黄金价格", "k": "Gold"}
    ]
    
    cols = st.columns(6)
    for i, item in enumerate(macro_items):
        with cols[i % 6]: st.markdown(render_macro_cell(item['l'], item['k']), unsafe_allow_html=True)

    st.markdown("---")
    t_col, l_col = st.columns([2, 1])

    with t_col:
        st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>📊 标的量化监测矩阵</h3>", unsafe_allow_html=True)
        tech = metrics_data.get('technical_matrix', []) if metrics_data else []
        if tech:
            df = pd.DataFrame(tech).rename(columns={"code":"证券代码","name":"证券名称","price":"现价","bias":"乖离率 %","vol_ratio":"量比"})
            st.dataframe(df, use_container_width=True, hide_index=True)
        else: st.info("数据链路同步中...")

    with l_col:
        st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>📜 策略决策审计摘要 (CSO Summary)</h3>", unsafe_allow_html=True)
        rationale = audit_data.get('rationale', "正在初始化链路...")
        log_content = f"[运行日志]<br>[决策引擎已连接: GEMINI-3-FLASH]<br>[执行多维特征深度审计]<br>---------------------------------<br>{rationale}<br>---------------------------------<br>[审计闭环]<br>[系统待命]"
        st.markdown(f"<div class='sys-log'>{log_content}</div>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(f"<p style='text-align: center; color: #8b949e; font-size: 0.8rem;'>V14.1 PRO 机构级决策引擎 | 最后同步时间: {format_beijing_time(ref_time)}</p>", unsafe_allow_html=True)
else:
    st.error("❌ 数据链路异常")
