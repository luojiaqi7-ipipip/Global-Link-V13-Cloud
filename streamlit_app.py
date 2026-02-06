import streamlit as st
import json
import os
import pandas as pd

# 🎨 页面配置：黑色科技风，宽屏显示
st.set_page_config(page_title="Global-Link V13 量化研判系统", layout="wide")

# 自定义 CSS 优化视觉，去除中二风格
st.markdown("""
    <style>
    .main { background-color: #0a0b10; color: #e0e0e0; }
    .decision-card {
        padding: 30px;
        border-radius: 16px;
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid rgba(255, 255, 255, 0.1);
        margin-bottom: 25px;
    }
    .metric-container {
        background: rgba(255, 255, 255, 0.01);
        padding: 20px;
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
    [data-testid="stMetricValue"] { font-family: 'JetBrains Mono', monospace; font-size: 1.8rem; }
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

# --- 头部 ---
st.title("🛡️ Global-Link V13 量化研判系统")
st.write(f"模型：`Gemini 3 Flash` | 运行环境：`云端全自动集群` | 状态：`监测中`")
st.markdown("---")

if data:
    # --- 核心决策区 ---
    decision = data.get('decision', 'WAIT')
    target = data.get('target', 'N/A')
    
    # 颜色与翻译
    color_map = {"BUY": "#00ff88", "SELL": "#ff3366", "HOLD": "#00f2ff", "WAIT": "#888888"}
    decision_map = {"BUY": "建议买入", "SELL": "建议卖出", "HOLD": "继续持有", "WAIT": "持币观望"}
    
    color = color_map.get(decision, "#888888")
    decision_zh = decision_map.get(decision, "等待数据")

    col_main, col_params = st.columns([2, 1])
    
    with col_main:
        st.markdown(f"""
            <div class="decision-card">
                <h1 style='color: {color}; margin: 0;'>{decision_zh} ({decision})</h1>
                <h3 style='color: #888888; margin-top: 10px;'>核心标的: {target}</h3>
                <div style='height: 1px; background: rgba(255,255,255,0.1); margin: 20px 0;'></div>
                <p style='font-size: 1.1rem; line-height: 1.6;'>
                    <b>审计逻辑摘要：</b><br>{data.get('rationale', '数据同步中...') }
                </p>
            </div>
            """, unsafe_allow_html=True)

    with col_params:
        st.subheader("执行参考参数")
        params = data.get('parameters', {})
        st.markdown(f"""
            <div class="metric-container">
                <small style='color: #888888;'>风险调节系数 (Factor)</small>
                <h2 style='color: #ffcc00; margin-bottom: 15px;'>{data.get('attack_factor', 1.0)}</h2>
                <small style='color: #888888;'>铁血止损线 (Factor已加权)</small>
                <h3 style='color: #ff3366;'>{params.get('stop_loss', '0.0')} %</h3>
                <small style='color: #888888;'>目标止盈线</small>
                <h3 style='color: #00ff88;'>{params.get('stop_profit', '0.0')} %</h3>
                <small style='color: #888888;'>建议持仓时长</small>
                <h3 style='color: #ffffff;'>{params.get('time_limit', '4个交易日')}</h3>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # --- 数据全景区 ---
    st.header("实时市场快照")
    col_macro, col_full = st.columns([1, 2])

    with col_macro:
        st.subheader("宏观环境指标")
        macro = data.get('macro', {})
        if macro:
            for k, v in macro.items():
                st.write(f"**{k}**: `{v}`")
        else:
            st.info("正在获取实时宏观指标...")

    with col_full:
        st.subheader("ETF 监测池实时数据")
        candidates = data.get('top_candidates', [])
        if candidates:
            # 使用更专业的表格展示，包含中文名称
            df = pd.DataFrame(candidates)
            df.columns = ["代码", "名称", "乖离率(%)", "量比"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning("当前监测池中未发现符合超跌条件的标的")

    st.write("---")
    st.caption(f"数据更新时间: {data.get('timestamp', 'N/A')} (每 4 小时刷新一次)")
else:
    st.error("无法加载审计结果数据。请确认 GitHub Actions 是否正常运行。")

# 侧边栏：功能解释与说明
with st.sidebar:
    st.header("系统说明")
    st.info("本系统由 V13 全云端架构驱动，每日 09:15, 13:30, 15:15 自动执行全量数据抓取与 AI 审计。")
    st.markdown("""
    - **乖离率 (Bias)**: 反映价格偏离 5 日均线的程度。
    - **量比**: 今日成交量与过去 5 日均量的比值。
    - **系数 (Factor)**: 根据宏观和政策面调整的风险杠杆。
    """)
    if st.button("🔄 刷新页面视图"):
        st.rerun()
