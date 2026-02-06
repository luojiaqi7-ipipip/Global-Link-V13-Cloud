import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Global-Link V13 Dashboard", layout="wide")

# Custom CSS for Premium Look
st.markdown("""
    <style>
    .main {
        background-color: #0a0b10;
        color: #e0e0e0;
    }
    [data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace;
    }
    .stMetric {
        background-color: rgba(255, 255, 255, 0.05);
        padding: 20px;
        border-radius: 15px;
    }
    .decision-card {
        padding: 40px;
        border-radius: 24px;
        background: linear-gradient(135deg, rgba(0, 242, 255, 0.1), rgba(112, 0, 255, 0.1));
        border: 1px solid rgba(255, 255, 255, 0.1);
        margin-bottom: 30px;
    }
    </style>
    """, unsafe_allow_html=True)

def load_data():
    data_path = "data/audit_result.json"
    if os.path.exists(data_path):
        try:
            with open(data_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading JSON: {e}")
    return None

data = load_data()

st.title("🚀 GLOBAL-LINK V13 QUANTUM CLOUD")
st.markdown(f"**STATUS**: `ACTIVE AUDIT (GEMINI 3 FLASH)`")
st.markdown("---")

if data:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        decision = data.get('decision', 'WAIT')
        target = data.get('target', 'N/A')
        rationale = data.get('rationale', 'No rationale provided.')
        
        st.markdown(f"""
            <div class="decision-card">
                <h1 style='color: #00f2ff; margin-bottom: 0;'>{decision}</h1>
                <h3 style='color: #888888;'>Target: {target}</h3>
                <p style='font-size: 1.1rem; margin-top: 20px; line-height: 1.6;'>{rationale}</p>
            </div>
            """, unsafe_allow_html=True)
        
        params = data.get('parameters', {})
        c1, c2, c3 = st.columns(3)
        c1.metric("Attack Factor", data.get('attack_factor', 1.0))
        c2.metric("Stop Loss", params.get('stop_loss', 0.0))
        c3.metric("Stop Profit", params.get('stop_profit', 0.0))

    with col2:
        st.subheader("📊 Macro Indicators")
        macro = data.get('macro', {})
        if macro:
            for k, v in macro.items():
                st.write(f"**{k}**: `{v}`")
        else:
            st.info("Macro data temporarily unavailable")

        st.markdown("---")
        st.subheader("⚔️ Top Candidates")
        candidates = data.get('top_candidates', [])
        if candidates:
            df = pd.DataFrame(candidates)
            st.table(df)
        else:
            st.write("No candidates met criteria")

    st.markdown("---")
    st.caption(f"Last Cloud Sync: {data.get('timestamp', 'Unknown')}")
else:
    st.warning("Waiting for the first Cloud Audit to complete or JSON data missing.")

if st.sidebar.button("Force Cloud Refresh"):
    st.info("Triggering GitHub Action...")
    # Triggering action via CLI from my side
    st.rerun()
