from core.data_engine import DataEngine
from core.audit_engine import AuditEngine
import json
import os

def main():
    print("🚀 Initializing V13 Cloud Audit...")
    
    # 1. Fetch Data
    data_engine = DataEngine()
    market_data = data_engine.sync_all()
    
    # 2. Perform AI Audit
    audit_engine = AuditEngine()
    
    # Debug: List available models
    import google.generativeai as genai
    print("Available Models:")
    for m in genai.list_models():
        print(f" - {m.name}")
    
    result = audit_engine.perform_audit(market_data)
    
    # 3. Save Results
    os.makedirs("data", exist_ok=True)
    with open("data/audit_result.json", "w", encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print("✅ Audit Complete. Data saved for Streamlit.")

if __name__ == "__main__":
    main()
