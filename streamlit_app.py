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
        with open(data_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

data = load_data()

st.title("🚀 GLOBAL-LINK V13 QUANTUM CLOUD")
st.markdown("---")

if data:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown(f"""
            <div class="decision-card">
                <h1 style='color: #00f2ff; margin-bottom: 0;'>{data['decision']}</h1>
                <h3 style='color: #888888;'>Target: {data['target']}</h3>
                <p style='font-size: 1.2rem; margin-top: 20px;'>{data['rationale']}</p>
            </div>
            """, unsafe_allow_html=True)
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Attack Factor", data['attack_factor'])
        c2.metric("Stop Loss", data['parameters']['stop_loss'], delta_color="inverse")
        c3.metric("Stop Profit", data['parameters']['stop_profit'])

    with col2:
        st.subheader("📊 Top Candidates")
        df = pd.DataFrame(data['top_candidates'])
        st.table(df)

    st.markdown("---")
    st.caption(f"Last Cloud Audit: {data['timestamp']}")
else:
    st.warning("Waiting for the first Cloud Audit to complete...")

if st.button("Manual Refresh (Cloud Sync)"):
    st.info("Triggering GitHub Action...")
    # This would normally trigger a GH Action via API
    st.rerun()
