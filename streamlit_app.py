import streamlit as st
import json
import os
import pandas as pd

# 🎨 UI Configuration
st.set_page_config(page_title="Global-Link V13 Dashboard", layout="wide")

# 📂 Data Loading Logic
def get_data_path():
    # Attempt to locate the JSON relative to this script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, 'data', 'audit_result.json')

def load_data():
    path = get_data_path()
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"JSON Parsing Error: {e}")
            return None
    return None

# 🚀 Main View
st.title("🚀 GLOBAL-LINK V13 CLOUD")
st.write("---")

data = load_data()

if data:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        decision = data.get('decision', 'WAIT')
        color = "#00f2ff"
        if decision == "BUY": color = "#00ff88"
        elif decision == "SELL": color = "#ff3366"
        
        st.markdown(f"""
            <div style="padding: 30px; border-radius: 20px; background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1);">
                <h1 style='color: {color}; margin: 0;'>{decision}</h1>
                <h2 style='color: #888888;'>Target: {data.get('target', 'N/A')}</h2>
                <p style='font-size: 1.1rem; margin-top: 15px;'>{data.get('rationale', 'No rationale provided.')}</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.write("### 🛡️ Ironclad Parameters")
        params = data.get('parameters', {})
        p1, p2, p3 = st.columns(3)
        p1.metric("Factor", data.get('attack_factor', 1.0))
        p2.metric("Stop Loss", params.get('stop_loss', 0.0))
        p3.metric("Stop Profit", params.get('stop_profit', 0.0))

    with col2:
        st.subheader("📊 Macro Pulse")
        macro = data.get('macro', {})
        if macro:
            for k, v in macro.items():
                st.write(f"**{k}**: `{v}`")
        else:
            st.info("Macro data empty")

        st.subheader("⚔️ Candidates")
        candidates = data.get('top_candidates', [])
        if candidates:
            st.dataframe(pd.DataFrame(candidates))
        else:
            st.write("No candidates met criteria")

    st.write("---")
    st.caption(f"Sync Timestamp: {data.get('timestamp', 'N/A')}")
else:
    st.warning("⚠️ CRITICAL: Data file `data/audit_result.json` not found or empty.")
    st.info("The Cloud Action may still be running for the first time.")
    
    # Debug info
    if st.checkbox("Show System Debug"):
        st.write("Current Dir:", os.getcwd())
        st.write("Dir Contents:", os.listdir('.'))
        if os.path.exists('data'):
            st.write("Data Dir Contents:", os.listdir('data'))
