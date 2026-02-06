import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime

# 🎨 UI 全面升级：专业量化研判看板
st.set_page_config(page_title="Global-Link V13 量化研判系统", layout="wide", initial_sidebar_state="expanded")

# 自定义风格：深空灰 + 极光蓝
st.markdown("""
    <style>
    .main { background-color: #0d1117; color: #c9d1d9; }
    .stMetric { background-color: #161b22; padding: 15px; border-radius: 10px; border: 1px solid #30363d; }
    .decision-card {
        padding: 30px;
        border-radius: 15px;
        background: linear-gradient(135deg, #1f2937 0%, #111827 100%);
        border: 1px solid #3b82f6;
        margin-bottom: 20px;
    }
    .status-active { color: #10b981; font-weight: bold; }
    h1, h2, h3 { color: #58a6ff; }
    </style>
    """, unsafe_allow_html=True)

def load_data():
    path = os.path.join(os.path.dirname(__file__), 'data', 'audit_result.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return None
    return None

data = load_data()

# --- 侧边栏：系统状态与使用指南 ---
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/radar.png", width=80)
    st.title("V13 审计中枢")
    st.markdown("---")
    st.markdown("**系统状态**: <span class='status-active'>● 云端在线</span>", unsafe_allow_html=True)
    st.markdown(f"**审计大脑**: `Gemini 3 Flash` (Google)")
    st.markdown(f"**更新频率**: 15:15 收盘终评")
    st.markdown("---")
    st.subheader("💡 狙击指南")
    st.info("""
    - **乖离率 (Bias)**: 反映超跌程度。<-2.5% 为机会区。
    - **量比 (Vol Ratio)**: 反映承接力度。>1.2 为确认信号。
    - **系数 (Factor)**: AI 根据宏观与政策面计算的进攻倍数。
    """)
    if st.button("🔄 刷新看板数据"):
        st.rerun()

# --- 主界面 ---
st.title("🛡️ GLOBAL-LINK V13 量化研判看板")
st.markdown(f"最后同步时间: `{data.get('timestamp', '正在初始化...') if data else '正在初始化...'}`")
st.markdown("---")

if data:
    # 1. 核心审计区域
    col1, col2 = st.columns([2, 1])
    
    with col1:
        decision = data.get('decision', 'WAIT')
        target = data.get('target', 'N/A')
        rationale = data.get('rationale', '数据审计中，请稍候。')
        
        # 决策色块逻辑
        decision_color = "#8b949e"
        if "BUY" in decision.upper(): decision_color = "#238636"
        elif "SELL" in decision.upper(): decision_color = "#da3633"
        elif "HOLD" in decision.upper(): decision_color = "#1f6feb"
        
        st.markdown(f"""
            <div class="decision-card">
                <h1 style='color: {decision_color}; margin:0;'>{decision}</h1>
                <h3 style='color: #8b949e; margin-top:5px;'>最优目标: {target}</h3>
                <p style='font-size: 1.15rem; margin-top: 20px; line-height: 1.6; border-top: 1px solid #30363d; padding-top: 15px;'>
                    {rationale}
                </p>
            </div>
            """, unsafe_allow_html=True)

    with col2:
        st.subheader("📋 铁血执行参数")
        params = data.get('parameters', {})
        st.metric("风险调节系数 (Factor)", data.get('attack_factor', 1.0))
        st.metric("止损红线 (Factor加权)", f"{params.get('stop_loss', 0.0)} %", delta_color="inverse")
        st.metric("目标止盈位", f"{params.get('stop_profit', 0.0)} %")
        st.markdown(f"**建议持仓上限**: `{params.get('time_limit', '4天')}`")

    st.markdown("---")

    # 2. 全景宏观与技术看板
    st.header("🌐 全景战术看板 (Market Pulse)")
    m_col, t_col = st.columns([1, 2])
    
    with m_col:
        st.subheader("📊 宏观指标")
        macro = data.get('macro', {})
        if macro:
            for k, v in macro.items():
                st.markdown(f"**{k}**: `{v}`")
        else:
            st.warning("正在同步全量宏观情报...")

    with t_col:
        st.subheader("⚔️ ETF 监测池实时态势")
        candidates = data.get('top_candidates', [])
        if candidates:
            df = pd.DataFrame(candidates)
            # 重命名列名显示
            display_df = df.copy()
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.info("当前监测池内无触发预警的标的。")

else:
    st.error("❌ 无法加载审计结果 JSON。可能是云端 Action 尚未完成首轮同步。")
    st.info("请检查 GitHub Actions 的运行状态。")
