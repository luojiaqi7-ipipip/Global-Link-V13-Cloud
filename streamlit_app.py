import streamlit as st
import json
import os
import pandas as pd

# 🎨 页面配置：黑色科技风，宽屏显示
st.set_page_config(page_title="Global-Link V13 量化狙击看板", layout="wide")

# 加载自定义 CSS 提升视觉效果
st.markdown("""
    <style>
    .main { background-color: #0a0b10; color: #e0e0e0; }
    .decision-card {
        padding: 30px;
        border-radius: 20px;
        background: linear-gradient(135deg, rgba(0, 242, 255, 0.1), rgba(112, 0, 255, 0.1));
        border: 1px solid rgba(255, 255, 255, 0.1);
        margin-bottom: 25px;
    }
    .metric-container {
        background: rgba(255, 255, 255, 0.03);
        padding: 15px;
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
    h1, h2, h3 { font-family: 'Inter', sans-serif; }
    </style>
    """, unsafe_allow_html=True)

def load_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, 'data', 'audit_result.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return None
    return None

data = load_data()

# --- 头部标题 ---
st.title("🛰️ GLOBAL-LINK V13 量化狙击系统")
st.write(f"**当前大脑**: `Gemini 3 Flash (顶级审计模式)` | **运行环境**: `GitHub Actions 全云端`")
st.markdown("---")

if data:
    # --- 第一行：核心决策区 ---
    decision = data.get('decision', '等待')
    target = data.get('target', '无')
    
    # 颜色逻辑
    color = "#888888"
    if decision == "BUY": color = "#00ff88"; decision_zh = "🎯 建议开火 (BUY)"
    elif decision == "SELL": color = "#ff3366"; decision_zh = "🏳️ 建议平仓 (SELL)"
    elif decision == "HOLD": color = "#00f2ff"; decision_zh = "🛡️ 继续持仓 (HOLD)"
    else: decision_zh = "🔭 观望 (WAIT)"

    col_main, col_params = st.columns([2, 1])
    
    with col_main:
        st.markdown(f"""
            <div class="decision-card">
                <h1 style='color: {color}; margin: 0; font-size: 3rem;'>{decision_zh}</h1>
                <h2 style='color: #888888; margin-top: 10px;'>目标标的: {target}</h2>
                <p style='font-size: 1.2rem; margin-top: 20px; line-height: 1.6; color: #ffffff;'>
                    <b>AI 审计核心逻辑:</b><br>{data.get('rationale', '正在搜集情报...') }
                </p>
            </div>
            """, unsafe_allow_html=True)

    with col_params:
        st.subheader("🛡️ 铁血执行参数")
        params = data.get('parameters', {})
        st.markdown(f"""
            <div class="metric-container">
                <p style='color: #888888;'>进攻系数 (Factor)</p>
                <h2 style='color: #ffcc00;'>{data.get('attack_factor', 1.0)}</h2>
                <hr style='opacity: 0.1'>
                <p style='color: #888888;'>铁血止损线</p>
                <h3 style='color: #ff3366;'>{params.get('stop_loss', '0.0')} %</h3>
                <p style='color: #888888; margin-top: 10px;'>目标止盈线</p>
                <h3 style='color: #00ff88;'>{params.get('stop_profit', '0.0')} %</h3>
                <p style='color: #888888; margin-top: 10px;'>时间熔断</p>
                <h3 style='color: #ffffff;'>{params.get('time_limit', '4 天')}</h3>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # --- 第二行：全景数据区 ---
    st.header("🌐 全景战术地图")
    col_macro, col_full = st.columns([1, 2])

    with col_macro:
        st.subheader("📊 宏观脉冲")
        macro = data.get('macro', {})
        if macro:
            for k, v in macro.items():
                st.info(f"**{k}**: {v}")
        else:
            st.warning("正在同步全球宏观指标...")

    with col_full:
        st.subheader("⚔️ 16罗汉实时战况")
        candidates = data.get('top_candidates', [])
        if candidates:
            # 转换成中文表头展示
            df = pd.DataFrame(candidates)
            df.columns = ["代码", "乖离率(Bias)", "量比"]
            st.dataframe(df, use_container_width=True)
        else:
            st.write("当前无标的进入狙击区间")

    st.write("---")
    st.caption(f"最后云端同步时间: {data.get('timestamp', '未知')}")
else:
    st.warning("⚠️ 正在等待云端第一次审计完成...")
    st.info("系统正在 GitHub Actions 中抓取全量数据并由 Gemini 3 Flash 进行评审，请在 60 秒后刷新。")

# 侧边栏：新手指南
with st.sidebar:
    st.header("📖 狙击手手册")
    st.markdown("""
    **1. 乖离率 (Bias)**
    反映跌幅是否过载。低于 -2.5% 意味着进入“黄金坑”。
    
    **2. 量比**
    反映成交热度。大于 1.2 意味着有大资金入场承接。
    
    **3. 进攻系数 (Factor)**
    由 AI 根据政策权重计算。1.2 代表全力进攻，0.8 代表轻仓试探。
    
    **4. 4天熔断**
    超跌反弹的时效性极强。4天内不反弹，逻辑即失效，必须撤离。
    """)
    if st.button("🔄 强制云端同步"):
        st.rerun()
