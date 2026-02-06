import streamlit as st
import json
import os
import pandas as pd

st.set_page_config(page_title="Global-Link V13 Dashboard", layout="wide")

# 加载数据逻辑
def load_data():
    path = os.path.join(os.path.dirname(__file__), 'data', 'audit_result.json')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

data = load_data()

st.title("🛰️ GLOBAL-LINK V13 QUANTUM CLOUD")
st.markdown(f"**审计大脑**: `Gemini 3 Flash` | **全景状态**: `云端闭环模式`")
st.write("---")

if data:
    c1, c2 = st.columns([2, 1])
    with c1:
        st.info(f"### 当前决策: {data['decision']}")
        st.success(f"**目标标的**: {data['target']}")
        st.write(f"**审计理由**: {data['rationale']}")
        
    with c2:
        st.subheader("📊 宏观监控")
        macro = data.get('macro_snapshot', {})
        for k, v in macro.items():
            st.metric(k, v)

    st.subheader("⚔️ ETF 监测池矩阵 (实时)")
    if 'top_candidates' in data:
        st.dataframe(pd.DataFrame(data['top_candidates']), use_container_width=True)
    
    st.write("---")
    st.caption(f"最后同步: {data['timestamp']} (Asia/Shanghai)")
else:
    st.warning("正在等待云端首轮数据持久化...")
