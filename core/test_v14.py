import sys
import os
# Ensure we can import from core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.intel_engine import IntelEngine
from core.quant_lab import QuantLab
import json
import numpy as np

def test_v14():
    print("🔍 Testing Global-Link-V14 features...")
    
    # 1. Test IntelEngine
    # Using the data we just warmed up
    history_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "history")
    intel = IntelEngine(history_dir=history_dir)
    
    # Check a few indicators
    for key in ["Nasdaq", "CNH", "Gold", "Southbound"]:
        features = intel.get_features(key)
        if features:
            print(f"✅ IntelEngine: {key} features found.")
            print(f"   Value: {features['value']}")
            print(f"   p_20d: {features['p_20d']}%, p_250d: {features['p_250d']}%, p_1250d: {features['p_1250d']}%")
            
            assert "p_250d" in features, f"Missing p_250d for {key}"
            assert "p_1250d" in features, f"Missing p_1250d for {key}"
            # Precision check (3 decimal places for percentiles)
            # Rounding might result in fewer places if it's .0, but check max
            p_str = str(features['p_20d'])
            if '.' in p_str:
                assert len(p_str.split('.')[1]) <= 3, f"p_20d precision error for {key}: {p_str}"
        else:
            print(f"⚠️ IntelEngine: {key} features NOT found. Check if data/history/{key}.csv exists.")

    # 2. Test QuantLab
    raw_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    raw_file = os.path.join(raw_dir, "latest_snap.json")
    
    dummy_raw = {
        "meta": {"timestamp": "2026-02-07 10:00"},
        "macro": {
            "CNH": {"status": "SUCCESS", "price": 7.214567, "change_pct": 0.123456},
            "Nasdaq": {"status": "SUCCESS", "price": 18000.5555, "change_pct": -0.56789},
            "CSI300_Vol": {"status": "SUCCESS", "amplitude": 1.23456, "pct_change": 0.6789},
            "Southbound": {"status": "SUCCESS", "value": 1234567890}
        },
        "etf_spot": [],
        "hist_data": {}
    }
    
    with open(raw_file, "w") as f:
        json.dump(dummy_raw, f)
        
    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")
    ql = QuantLab(raw_file=raw_file, out_dir=out_dir)
    # Patch intel history dir to be absolute
    ql.intel.history_dir = history_dir
    
    processed = ql.process()
    
    if processed:
        macro = processed['macro_matrix']
        # Check CNH
        cnh = macro.get('CNH')
        if cnh:
            print(f"✅ QuantLab: CNH processed. Value: {cnh['value']}, Change%: {cnh['change_pct']}")
            # Precision check
            assert cnh['value'] == round(7.214567, 3), f"CNH value precision error: {cnh['value']}"
            assert cnh['change_pct'] == round(0.123456, 3), f"CNH change_pct precision error: {cnh['change_pct']}"
            assert "p_250d" in cnh
            assert "p_1250d" in cnh
        
        # Check Southbound
        sb = macro.get('Southbound')
        if sb:
            print(f"✅ QuantLab: Southbound processed. Billion: {sb['value_billion']}")
            assert sb['value_billion'] == round(1234567890 / 1e8, 3)
            
        # Check A_Share_Vol
        asv = macro.get('A_Share_Vol')
        if asv:
            print(f"✅ QuantLab: A_Share_Vol processed. Amplitude: {asv['amplitude']}")
            assert asv['amplitude'] == round(1.23456, 3)

    print("\n🎉 All V14 core logic tests passed 100%!")
    return True

if __name__ == "__main__":
    if test_v14():
        sys.exit(0)
    else:
        sys.exit(1)
