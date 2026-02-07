import os
import json
import glob
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.intel_engine import IntelEngine

def backfill():
    raw_dir = "data/raw"
    files = glob.glob(f"{raw_dir}/market_snap_*.json")
    
    # Sort files by timestamp in filename
    # market_snap_20260206_1519.json
    files.sort()
    
    print(f"Found {len(files)} snap files for backfilling...")
    
    engine = IntelEngine(history_dir="data/history")
    
    for f_path in files:
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                engine.update_history(raw_data)
                print(f"Processed {os.path.basename(f_path)}")
        except Exception as e:
            print(f"Error processing {f_path}: {e}")

    print("Backfill complete.")

if __name__ == "__main__":
    backfill()
