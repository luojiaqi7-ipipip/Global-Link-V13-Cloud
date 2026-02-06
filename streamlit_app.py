import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime
import time

# 🎨 UI 全面升级：赛博朋克量化研判看板 - V13 Cloud 汉化精修版
st.set_page_config(page_title="Global-Link V13 PRO", layout="wide", initial_sidebar_state="expanded")

# 自定义风格：深空/赛博朋克风格
# 霓虹蓝: #00f2ff, 霓虹绿: #00ff88, 警戒红: #ff3366, 背景: #0d1117, 金色: #f1e05a
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
    
    /* 标题样式 - 移除 Orbitron，改用现代黑体 */
    .cyber-title {
        font-weight: 700;
        color: #00f2ff;
        text-shadow: 0 0 10px rgba(0, 242, 255, 0.4);
        margin-bottom: 25px;
        text-align: center;
        border-bottom: 2px solid #00f2ff;
        padding-bottom: 10px;
        letter-spacing: 2px;
    }

    /* 决策卡片 - 发光效果 */
    .decision-card {
        padding: 25px;
        border-radius: 15px;
        background: linear-gradient(145deg, #161b22 0%, #0d1117 100%);
        border: 2px solid #00f2ff;
        box-shadow: 0 0 20px rgba(0, 242, 255, 0.2);
        margin-bottom: 20px;
        transition: all 0.3s ease;
    }
    .decision-card:hover {
        box-shadow: 0 0 30px rgba(0, 242, 255, 0.4);
        transform: translateY(-2px);
    }
    
    .decision-buy { border-color: #00ff88; box-shadow: 0 0 20px rgba(0, 255, 136, 0.2); }
    .decision-sell { border-color: #ff3366; box-shadow: 0 0 20px rgba(255, 51, 102, 0.2); }
    .decision-wait { border-color: #8b949e; box-shadow: 0 0 20px rgba(139, 148, 158, 0.1); }

    /* 核心指标数值增强 */
    .highlight-value {
        font-weight: 800;
        letter-spacing: -1px;
    }

    /* 宏观矩阵网格卡片 */
    .macro-card {
        background: #161b22;
        padding: 12px;
        border-radius: 8px;
        border-left: 3px solid #00f2ff;
        margin-bottom: 10px;
        min-height: 80px;
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

    /* 系统日志样式 */
    .sys-log {
        background-color: #05070a;
        color: #00ff88;
        padding: 15px;
        border-radius: 8px;
        font-family: 'Consolas', 'Monaco', monospace;
        border: 1px solid #30363d;
        height: 300px;
        overflow-y: auto;
        font-size: 0.9rem;
        line-height: 1.5;
    }

    /* 侧边栏样式定制 */
    [data-testid="stSidebar"] {
        background-color: #0d1117;
        border-right: 1px solid #30363d;
    }
    
    /* 数据表格美化 */
    .stDataFrame {
        border: 1px solid #30363d;
        border-radius: 8px;
    }
    
    /* 隐藏 Streamlit 默认页脚 */
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

# 路径定位
base_dir = os.path.dirname(os.path.abspath(__file__))
audit_file = os.path.join(base_dir, 'data', 'audit_result.json')
metrics_file = os.path.join(base_dir, 'data', 'processed', 'latest_metrics.json')

audit_data = load_data(audit_file)
metrics_data = load_data(metrics_file)

# --- 侧边栏：系统控制中心 ---
with st.sidebar:
    st.markdown("<h2 style='text-align: center; color: #00f2ff; font-weight:700;'>系统控制中心</h2>", unsafe_allow_html=True)
    st.markdown("<div style='text-align:center; margin-bottom:20px;'><img src='https://img.icons8.com/nolan/96/cyber-security.png' width='80'></div>", unsafe_allow_html=True)
    
    st.markdown("---")
    st.subheader("📡 系统健康度")
    
    # 数据源状态
    sources = [
        {"name": "腾讯数据源 (Tencent)", "status": "在线", "icon": "🟢"},
        {"name": "雅虎财经 (Yahoo)", "status": "在线", "icon": "🟢"},
        {"name": "东方财富 (Eastmoney)", "status": "已同步", "icon": "🟢"}
    ]
    
    for s in sources:
        st.markdown(f"{s['icon']} **{s['name']}**: `{s['status']}`")
    
    st.markdown("---")
    st.subheader("🧠 审计大脑")
    st.code("模型: Gemini 3 Flash\n潜意识推理: 开启\n逻辑审计: 激活", language="yaml")
    
    st.markdown("---")
    st.subheader("💡 狙击核心指标说明")
    st.markdown("""
    - <span style='color:#ff3366'>乖离率 < -2.5%</span>: 极度超跌
    - <span style='color:#00ff88'>量比 > 1.2</span>: 动能确认
    - <span style='color:#00f2ff'>进攻系数</span>: 仓位进攻激进程度
    """, unsafe_allow_html=True)
    
    if st.button("🚀 强制重载云端数据"):
        st.rerun()

# --- 主界面 ---
st.markdown("<h1 class='cyber-title'>GLOBAL-LINK V13 量化研判系统</h1>", unsafe_allow_html=True)

if audit_data:
    # 顶部核心：当前审计指令 & 进攻系数
    c1, c2 = st.columns([3, 1])
    
    decision = audit_data.get('decision', 'WAIT')
    target = audit_data.get('target', 'CASH_NEUTRAL')
    factor = audit_data.get('attack_factor', 0.0)
    
    # 决策颜色逻辑
    card_class = "decision-wait"
    d_color = "#8b949e"
    if "BUY" in decision.upper() or "开火" in decision or "买入" in decision:
        card_class = "decision-buy"
        d_color = "#00ff88"
    elif "SELL" in decision.upper() or "撤退" in decision or "卖出" in decision:
        card_class = "decision-sell"
        d_color = "#ff3366"
    
    # 汉化指令显示
    display_decision = decision
    if "WAIT" in decision.upper() or "等待" in decision: display_decision = "⏳ 观望等待"
    elif "BUY" in decision.upper() or "买入" in decision: display_decision = "⚔️ 执行进攻"
    elif "SELL" in decision.upper() or "卖出" in decision: display_decision = "🛡️ 执行防御"
    elif "HOLD" in decision.upper() or "持有" in decision: display_decision = "💎 坚定持有"

    display_target = target
    if target == "CASH_NEUTRAL": display_target = "🛡️ 现金中性"
    
    with c1:
        st.markdown(f"""
            <div class="decision-card {card_class}">
                <div style='display: flex; justify-content: space-between; align-items: center;'>
                    <div>
                        <span style='font-size: 1rem; color: #8b949e;'>当前审计指令</span>
                        <h1 style='color: {d_color}; margin:5px 0 0 0; font-size: 3.8rem;' class='highlight-value'>{display_decision}</h1>
                    </div>
                    <div style='text-align: right;'>
                        <span style='font-size: 1rem; color: #8b949e;'>狙击目标</span>
                        <h2 style='color: #00f2ff; margin:5px 0 0 0; font-size: 2rem;'>{display_target}</h2>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
            <div class="decision-card" style='text-align: center; height: 100%; border-color: #f1e05a; box-shadow: 0 0 20px rgba(241, 224, 90, 0.2);'>
                <span style='font-size: 1rem; color: #8b949e;'>进攻系数</span>
                <h1 style='color: #f1e05a; margin:15px 0; font-size: 3.8rem;' class='highlight-value'>{factor}</h1>
                <div style='font-size: 0.8rem; color: #8b949e;'>风险/倍率系数</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # 🌐 全球宏观脉搏 (The Macro Pulse)
    st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>🌐 全球宏观脉搏</h3>", unsafe_allow_html=True)
    
    macro = metrics_data.get('macro_matrix', {}) if metrics_data else audit_data.get('macro_snapshot', {})
    health = metrics_data.get('macro_health', {}) if metrics_data else {}
    ref_time = metrics_data.get('timestamp') if metrics_data else audit_data.get('timestamp')

    def get_status_color(key):
        if not health or key not in health: return "#ff3366" # 红色 (缺失)
        h = health[key]
        if h.get('status') == 'FAILED': return "#ff3366"
        
        try:
            up_dt = datetime.strptime(h.get('last_update', '20000101_0000'), "%Y%m%d_%H%M")
            ref_dt = datetime.strptime(ref_time, "%Y%m%d_%H%M")
            if (ref_dt - up_dt).total_seconds() / 60 > 15: return "#f1e05a" # 黄色 (延迟)
            return "#00ff88" # 绿色 (实时)
        except: return "#f1e05a"

    def format_val(val, unit="", suffix=""):
        if val is None or val == "N/A" or val == "...":
            return "等待同步"
        # 修复：如果是 0，也可能是尚未抓取到有效值，或者真的是 0
        # 这里根据用户需求，如果不想要 N/A，可以显示具体数值 0 或 "等待同步"
        if val == 0 or val == 0.0:
            return f"0{unit}{suffix}"
        return f"{val}{unit}{suffix}"

    # 映射宏观指标到 raw 里的 key
    macro_items = [
        {"label": "离岸人民币", "value": f"{format_val(macro.get('CNH_Price'))} ({format_val(macro.get('CNH_Change'), unit='%')})", "key": "CNH"},
        {"label": "纳斯达克", "value": format_val(macro.get('Nasdaq_Price')), "key": "Nasdaq"},
        {"label": "恒生指数", "value": format_val(macro.get('HangSeng_Price')), "key": "HangSeng"},
        {"label": "A50 期货", "value": format_val(macro.get('A50_Futures_Price')), "key": "A50_Futures"},
        {"label": "VIX 恐慌指数", "value": format_val(macro.get('VIX')), "key": "VIX"},
        {"label": "中债10Y收益率", "value": format_val(macro.get('CN10Y_Yield'), unit="%"), "key": "CN10Y"},
        {"label": "美债10Y收益率", "value": format_val(macro.get('US10Y_Yield'), unit="%"), "key": "US10Y"},
        {"label": "纽约黄金", "value": format_val(macro.get('Gold_Price')), "key": "Gold"},
        {"label": "原油价格", "value": format_val(macro.get('CrudeOil_Price')), "key": "CrudeOil"},
        {"label": "两融变动 %", "value": format_val(macro.get('Margin_Change_Pct'), unit="%"), "key": "Margin_Debt"},
        {"label": "北向资金 (亿)", "value": format_val(macro.get('Northbound_Flow_Billion')), "key": "Northbound"},
        {"label": "流入行业", "value": ", ".join(macro.get('Inflow_Sectors', [])) if isinstance(macro.get('Inflow_Sectors'), list) and macro.get('Inflow_Sectors') else "等待同步", "key": "Sector_Flow"},
    ]
    
    # 每行 6 个指标，共两行
    cols = st.columns(6)
    for i, item in enumerate(macro_items):
        color = get_status_color(item['key'])
        with cols[i % 6]:
            st.markdown(f"""
                <div class="macro-card">
                    <div class="macro-label">
                        <span class="status-light" style="background-color: {color}; box-shadow: 0 0 5px {color};"></span>
                        {item['label']}
                    </div>
                    <div class="macro-value">{item['value']}</div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("---")

    # ⚔️ 狙击监测池 (Target Scanner) 与 AI 审计逻辑
    t_col, l_col = st.columns([2, 1])

    with t_col:
        st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>⚔️ 狙击监测池</h3>", unsafe_allow_html=True)
        technical = metrics_data.get('technical_matrix', []) if metrics_data else []
        if technical:
            df = pd.DataFrame(technical)
            # 列名汉化
            df = df.rename(columns={
                "code": "代码", 
                "name": "名称", 
                "price": "价格", 
                "bias": "乖离率 %", 
                "vol_ratio": "量比"
            })
            
            # 高亮逻辑
            def highlight_cells(s):
                styles = ['' for _ in s]
                if s.name == '乖离率 %':
                    for i, v in enumerate(s):
                        try:
                            if float(v) < -2.5: styles[i] = 'background-color: rgba(255, 51, 102, 0.2); color: #ff3366; font-weight: bold;'
                        except: pass
                elif s.name == '量比':
                    for i, v in enumerate(s):
                        try:
                            if float(v) > 1.2: styles[i] = 'background-color: rgba(0, 255, 136, 0.2); color: #00ff88; font-weight: bold;'
                        except: pass
                return styles

            st.dataframe(
                df.style.apply(highlight_cells),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("监测池休眠中。等待市场脉搏触发...")

    with l_col:
        st.markdown("<h3 style='color: #00f2ff; font-weight:600;'>📜 AI 审计报告 (CIO 执行综述)</h3>", unsafe_allow_html=True)
        rationale = audit_data.get('rationale', "正在连接神经链路...")
        
        # 汉化系统日志
        log_content = f"""
[系统初始化完成...]
[AI 核心已连接: GEMINI-3-FLASH]
[正在审计数据集]
---------------------------------
{rationale}
---------------------------------
[审计任务执行完毕]
[系统状态: 待命]
        """
        st.markdown(f"<div class='sys-log'>{log_content.replace('\n', '<br>')}</div>", unsafe_allow_html=True)

    # 底部页脚
    st.markdown("---")
    st.markdown(f"<p style='text-align: center; color: #8b949e; font-size: 0.8rem;'>V13-Cloud 云端引擎 | 最后同步: {audit_data.get('timestamp', 'N/A')} | 亚洲/上海</p>", unsafe_allow_html=True)

else:
    st.error("❌ 致命错误: 任务数据缺失")
    st.info("请检查 GitHub Actions 运行状态及 JSON 完整性。")
