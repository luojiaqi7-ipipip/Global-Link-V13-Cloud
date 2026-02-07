import os
import pandas as pd
import numpy as np
from datetime import datetime

class IntelEngine:
    def __init__(self, history_dir="data/history"):
        self.history_dir = history_dir
        os.makedirs(self.history_dir, exist_ok=True)

    def update_history(self, raw_macro):
        """
        持久化存储宏观信号历史（CSV格式）。
        raw_macro structure from Harvester.
        """
        timestamp = raw_macro.get("meta", {}).get("timestamp", datetime.now().strftime("%Y%m%d_%H%M"))
        macro_data = raw_macro.get("macro", {})
        
        for key, info in macro_data.items():
            if info.get("status") != "SUCCESS":
                continue
            
            # Extract value. Prioritize 'price', then 'value', then 'yield'
            val = info.get("price")
            if val is None:
                val = info.get("value")
            if val is None:
                val = info.get("yield")
                
            if val is None:
                continue
            
            file_path = os.path.join(self.history_dir, f"{key}.csv")
            new_row = pd.DataFrame([{"timestamp": timestamp, "value": float(val)}])
            
            if os.path.exists(file_path):
                try:
                    # Check if the last timestamp is the same to avoid duplicates
                    df_existing = pd.read_csv(file_path)
                    if not df_existing.empty and str(df_existing.iloc[-1]["timestamp"]) == str(timestamp):
                        continue
                    new_row.to_csv(file_path, mode='a', header=False, index=False)
                except:
                    new_row.to_csv(file_path, index=False)
            else:
                new_row.to_csv(file_path, index=False)

    def get_features(self, key):
        """
        特征包括：Percentile (近20/60日分位)、Z-Score (20日偏离度)、Slope (5日斜率)。
        """
        file_path = os.path.join(self.history_dir, f"{key}.csv")
        if not os.path.exists(file_path):
            return {
                "current": None,
                "p_20d": 50.0,
                "p_60d": 50.0,
                "z_score_20d": 0.0,
                "slope_5d": 0.0
            }
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            values = df["value"].astype(float).values
            if len(values) == 0:
                return None
                
            current = values[-1]
            
            return {
                "current": current,
                "p_20d": self._calc_percentile(values, 20),
                "p_60d": self._calc_percentile(values, 60),
                "z_score_20d": self._calc_zscore(values, 20),
                "slope_5d": self._calc_slope(values, 5)
            }
        except Exception as e:
            print(f"Error calculating features for {key}: {e}")
            return None

    def _calc_percentile(self, values, window):
        lookback = values[-window:]
        if len(lookback) < 1: return 50.0
        count = (lookback <= lookback[-1]).sum()
        return round(float(count / len(lookback)) * 100, 2)

    def _calc_zscore(self, values, window):
        lookback = values[-window:]
        if len(lookback) < 2: return 0.0
        mean = np.mean(lookback)
        std = np.std(lookback)
        if std == 0: return 0.0
        return round(float((values[-1] - mean) / std), 2)

    def _calc_slope(self, values, window):
        lookback = values[-window:]
        if len(lookback) < 2: return 0.0
        x = np.arange(len(lookback))
        y = lookback
        slope, _ = np.polyfit(x, y, 1)
        return round(float(slope), 4)
