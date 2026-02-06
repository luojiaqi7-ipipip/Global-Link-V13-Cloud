from core.data_engine import DataEngine
from core.audit_engine import AuditEngine
import json
import os
import time

def main():
    print("🚀 Initializing V13 Cloud Audit...")
    
    # 1. Fetch Data
    data_engine = DataEngine()
    
    # Simple retry logic for Cloud environment
    market_data = None
    for attempt in range(3):
        try:
            print(f"📡 Syncing Market Data (Attempt {attempt+1})...")
            market_data = data_engine.sync_all()
            if market_data['technical']:
                print("✅ Data Sync Successful.")
                break
        except Exception as e:
            print(f"⚠️ Sync Error: {e}")
            time.sleep(10)
    
    if not market_data:
        print("❌ Critical: Data Sync failed after retries.")
        return

    # 2. Perform AI Audit
    print("🤖 Executing AI Quantum Audit (Gemini 3 Flash)...")
    audit_engine = AuditEngine()
    
    try:
        result = audit_engine.perform_audit(market_data)
        
        # 3. Save Results
        os.makedirs("data", exist_ok=True)
        with open("data/audit_result.json", "w", encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print("✅ Audit Complete. Data saved for Streamlit.")
    except Exception as e:
        print(f"❌ Audit Error: {e}")

if __name__ == "__main__":
    main()
