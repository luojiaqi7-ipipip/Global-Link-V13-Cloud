import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime
import time

# 🎨 UI 全面升级：全球量化策略决策看板 - V14 宏观特征驱动版
st.set_page_config(page_title="Global-Link V14 PRO", layout="wide", initial_sidebar_state="expanded")

# 自定义风格：专业量化风格
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');

    /* 全局字体统一 */
    html, body, [class*="css"], .stMarkdown {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif;
    }

    .main { 
        background-color: #0d1117; 
        color: #c9d1d9; 
    }
    
    .title-banner {
        font-weight: 700;
        color: #00f2ff;
        text-shadow: 0 0 10px rgba(0, 242, 255, 0.4);
        margin-bottom: 25px;
        text-align: center;
        border-bottom: 2px solid #00f2ff;
        padding-bottom: 10px;
        letter-spacing: 2px;
    }

    .decision-card {
        padding: 25px;
        border-radius: 15px;
        background: linear-gradient(145deg, #161b22 0%, #0d1117 100%);
        border: 2px solid #00f2ff;
        box-shadow: 0 0 20px rgba(0, 242, 255, 0.2);
        margin-bottom: 20px;
        transition: all 0.3s ease;
    }
    
    .decision-buy { border-color: #00ff88; box-shadow: 0 0 20px rgba(0, 255, 136, 0.2); }
    .decision-sell { border-color: #ff3366; box-shadow: 0 0 20px rgba(255, 51, 102, 0.2); }
    .decision-wait { border-color: #8b949e; box-shadow: 0 0 20px rgba(139, 148, 158, 0.1); }

    .highlight-value {
        font-weight: 800;
        letter-spacing: -1px;
    }

    .macro-card {
        background: #161b22;
        padding: 12px;
        border-radius: 8px;
        border-left: 3px solid #00f2ff;
        margin-bottom: 10px;
        min-height: 90px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        position: relative;
    }
    .macro-label { font-size: 0.85rem; color: #8b949e; margin-bottom: 4px; display: flex; align-items: center; }
    .macro-value { font-size: 1.1rem; color: #00f2ff; font-weight: 700; }
    
    .status-light {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 6px;
    }

    .sys-log {
        background-color: #05070a;
        color: #00ff88;
        padding: 15px;
        border-radius: 8px;
        font-family: 'Consolas', 'Monaco', monospace;
        border: 1px solid #30363d;
        height: 350px;
        overflow-y: auto;
        font-size: 0.9rem;
        line-height: 1.5;
    }

    [data-testid="stSidebar"] {
        background-color: #0d1117;
        border-right: 1px solid #30363d;
    }
    
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

def load_data(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return None
    return None

def format_beijing_time(ts_str):
    if not ts_str or ts_str == 'N/A' or ts_str == 'unknown':
        return 'N/A'
    try:
        # 统一处理格式
        if '_' in ts_str:
            dt = datetime.strptime(ts_str, "%Y%m%d_%H%M")
        elif ' ' in ts_str and '-' in ts_str:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
        else:
            return ts_str
        return dt.strftime("%Y-%m-%d %H:%M（北京时间）")
    except:
        return ts_str

# 路径定位
base_dir = os.path.dirname(os.path.abspath(__file__))
audit_file = os.path.join(base_dir, 'data', 'audit_result.json')
metrics_file = os.path.join(base_dir, 'data', 'processed', 'latest_metrics.json')

audit_data = load_data(audit_file)
metrics_data = load_data(metrics_file)

# --- 侧边栏：配置中心 ---
with st.sidebar:
    st.markdown("<h2 style='text-align: center; color: #00f2ff; font-weight:700;'>配置中心</h2>", unsafe_allow_html=True)
    
    st.markdown("---")
    st.subheader("📡 数据链路状态")
    
    sources = [
        {"name": "实时行情 (Sina)", "status": "在线", "icon": "🟢"},
        {"name": "宏观特征引擎 (Intel)", "status": "运行中", "icon": "🟢"},
        {"name": "策略审计模块 (CSO)", "status": "待命", "icon": "🟢"}
    ]
    
    for s in sources:
        st.markdown(f"{s['icon']} **{s['name']}**: `{s['status']}`")
    
    st.markdown("---")
    st.subheader("🧠 决策审计引擎")
    st.code("模型: Gemini 3 Flash\n架构: V14 Intelligence\n状态: 已激活", language="yaml")
    
    st.markdown("---")
    st.subheader("💡 核心特征维度说明")
    st.markdown("""
    - **历史分位 (20D | 1Y | 5Y)**: 指标在 20天 / 1年 / 5年 窗口内的百分比排名。
    - **趋势斜率 (Slope)**: 5日线性回归趋势向量。
    - **偏离度 (Z-Score)**: 价格/指标偏离均值的标准差倍数。
    """, unsafe_allow_html=True)
    
    if st.button("🚀 强制刷新决策数据"):
        st.rerun()

# --- 主界面 ---
st.markdown("<h1 class='title-banner'>GLOBAL-LINK V14 全球量化策略决策系统</h1>", unsafe_allow_html=True)

if audit_data:
    c1, c2 = st.columns([3, 1])
    
    decision = audit_data.get('decision', 'WAIT')
    target = audit_data.get('target', 'CASH_NEUTRAL')
    factor = audit_data.get('attack_factor', 0.0)
    
    card_class = "decision-wait"
    d_color = "#8b949e"
    if "BUY" in decision.upper() or "买入" in decision:
        card_class = "decision-buy"
        d_color = "#00ff88"
    elif "SELL" in decision.upper() or "卖出" in decision:
        card_class = "decision-sell"
        d_color = "#ff3366"
    
    display_decision = decision
    if "WAIT" in decision.upper() or "等待" in decision: display_decision = "⏳ 观望等待"
    elif "BUY" in decision.upper() or "买入" in decision: display_decision = "⚔️ 策略买入"
    elif "SELL" in decision.upper() or "卖出" in decision: display_decision = "🛡️ 策略卖出"
    elif "HOLD" in decision.upper() or "持有" in decision: display_decision = "💎 坚定持有"

    display_target = target
    if target == "CASH_NEUTRAL": display_target = "🛡️ 现金中性"
    
    with c1:
        st.markdown(f"""
            <div class="decision-card {card_class}">
                <div style='display: flex; justify-content: space-between; align-items: center;'>
                    <div>
                        <span style='font-size: 1rem; color: #8b949e;'>当前策略指令</span>
                        <h1 style='color: {d_color}; margin:5px 0 0 0; font-size: 3.8rem;' class='highlight-value'>{display_decision}</h1>
                    </div>
                    <div style='text-align: right;'>
                        <span style='font-size: 1rem; color: #8b949e;'>当前配置目标</span>
                        <h2 style='color: #00f2ff; margin:5px 0 0 0; font-size: 2rem;'>{display_target}</h2>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
            <div class="decision-card" style='text-align: center; height: 100%; border-color: #f1e05a; box-shadow: 0 0 20px rgba(241, 224, 90, 0.2);'>
                <span style='font-size: 1rem; color: #8b949e;'>策略风险敞口</span>
                <h1 style='color: #f1e05a; margin:15px 0; font-size: 3.8rem;' class='highlight-value'>{factor}</h1>
                <div style='font-size: 0.8rem; color: #8b949e;'>风险敞口系数</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # 🌐 宏观特征态势 (Macro Features)
    st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>🌐 全球宏观特征态势 (V14 Intelligence)</h3>", unsafe_allow_html=True)
    
    macro = metrics_data.get('macro_matrix', {}) if metrics_data else {}
    health = metrics_data.get('macro_health', {}) if metrics_data else {}
    ref_time = metrics_data.get('timestamp', audit_data.get('timestamp', 'N/A'))

    def get_status_color(key):
        h_key = key
        if key == 'A_Share_Vol': h_key = 'CSI300_Vol'
        if not health or h_key not in health: return "#ff3366" 
        h = health[h_key]
        if h.get('status') == 'FAILED': return "#ff3366"
        return "#00ff88"

    def render_macro_cell(label, key, color):
        data = macro.get(key, {})
        if not data or not isinstance(data, dict):
            return f'<div class="macro-card"><div class="macro-label"><span class="status-light" style="background-color: {color};"></span>{label}</div><div class="macro-value">等待同步</div></div>'
        
        val = data.get('value')
        if val is None:
            if key == 'A_Share_Vol': val = f"{data.get('amplitude', 'N/A')}%"
            elif key == 'Southbound': val = f"{data.get('value_billion', 'N/A')}亿"
            else: val = "N/A"
        else:
            if key in ['CN10Y', 'US10Y', 'SHIBOR', 'Margin_Debt']: val = f"{val}%"
        
        change = data.get('change_pct')
        change_str = f"<span style='font-size:0.8rem; color:{'#00ff88' if (change or 0) >=0 else '#ff3366'}'>({change}%)</span>" if change is not None else ""
        
        p20 = data.get('p_20d', 50.0)
        p250 = data.get('p_250d', 50.0)
        p1250 = data.get('p_1250d', 50.0)
        slope = data.get('slope', 0.0)
        arrow = "→"
        if slope > 0.0001: arrow = "↑"
        elif slope < -0.0001: arrow = "↓"
        
        return f"""
            <div class="macro-card">
                <div class="macro-label">
                    <span class="status-light" style="background-color: {color};"></span>{label}
                </div>
                <div class="macro-value">{val} {change_str}</div>
                <div style="font-size: 0.75rem; color: #8b949e; margin-top: 4px;">
                    20D|1Y|5Y: {p20}|{p250}|{p1250}
                </div>
                <div style="font-size: 0.7rem; color: #8b949e;">
                    趋势: {arrow} ({slope})
                </div>
            </div>
        """

    macro_items = [
        {"label": "离岸人民币", "key": "CNH"},
        {"label": "纳斯达克", "key": "Nasdaq"},
        {"label": "恒生指数", "key": "HangSeng"},
        {"label": "A50 指数", "key": "A50_Futures"},
        {"label": "VIX 风险指数", "key": "VIX"},
        {"label": "沪深300振幅", "key": "A_Share_Vol"},
        {"label": "中债10Y收益率", "key": "CN10Y"},
        {"label": "美债10Y收益率", "key": "US10Y"},
        {"label": "国内流动性", "key": "SHIBOR"},
        {"label": "港股通流入", "key": "Southbound"},
        {"label": "两融变动", "key": "Margin_Debt"},
        {"label": "黄金价格", "key": "Gold"},
    ]
    
    cols = st.columns(6)
    for i, item in enumerate(macro_items):
        color = get_status_color(item['key'])
        with cols[i % 6]:
            st.markdown(render_macro_cell(item['label'], item['key'], color), unsafe_allow_html=True)

    st.markdown("---")

    # ⚔️ 标的监测矩阵
    t_col, l_col = st.columns([2, 1])

    with t_col:
        st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>📊 标的量化监测矩阵</h3>", unsafe_allow_html=True)
        technical = metrics_data.get('technical_matrix', []) if metrics_data else []
        if technical:
            df = pd.DataFrame(technical)
            df = df.rename(columns={"code": "证券代码", "name": "证券名称", "price": "现价", "bias": "乖离率 %", "vol_ratio": "量比"})
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("正在等待数据流计算...")

    with l_col:
        st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>📜 策略决策审计摘要 (CSO Summary)</h3>", unsafe_allow_html=True)
        rationale = audit_data.get('rationale', "正在执行策略审计...")
        log_content = f"""[执行日志]<br>[决策引擎已连接: GEMINI-3-FLASH]<br>[执行全貌特征审计]<br>---------------------------------<br>{rationale}<br>---------------------------------<br>[审计闭环]<br>[系统待命]"""
        st.markdown(f"<div class='sys-log'>{log_content}</div>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(f"<p style='text-align: center; color: #8b949e; font-size: 0.8rem;'>V14-Intelligence 策略引擎 | 数据最后同步: {format_beijing_time(ref_time)} | 亚太/上海</p>", unsafe_allow_html=True)

else:
    st.error("❌ 数据链路连接异常")
