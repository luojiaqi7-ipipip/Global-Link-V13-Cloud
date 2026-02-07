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
        """
        timestamp = raw_macro.get("meta", {}).get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M"))
        macro_data = raw_macro.get("macro", {})
        
        for key, info in macro_data.items():
            if info.get("status") != "SUCCESS":
                continue
            
            # 提取值，优先取 price, 然后是 value, 最后是 yield
            val = info.get("price")
            if val is None:
                val = info.get("value")
            if val is None:
                val = info.get("yield")
                
            if val is None:
                continue
            
            # 【重要】单位对齐：将大额数值（亿级）转为“亿”
            final_val = float(val)
            if key in ['Southbound', 'Margin_Debt', 'Northbound'] and abs(final_val) > 1e6:
                final_val = final_val / 1e8
            
            file_path = os.path.join(self.history_dir, f"{key}.csv")
            new_row = pd.DataFrame([{"timestamp": timestamp, "value": round(final_val, 3)}])
            
            if os.path.exists(file_path):
                try:
                    df_existing = pd.read_csv(file_path)
                    # 避免重复写入相同时间戳的数据
                    if not df_existing.empty and str(df_existing.iloc[-1]["timestamp"]) == str(timestamp):
                        continue
                    new_row.to_csv(file_path, mode='a', header=False, index=False)
                except:
                    new_row.to_csv(file_path, index=False)
            else:
                new_row.to_csv(file_path, index=False)

    def get_features(self, key):
        """
        计算特征：Percentile (分位)、Z-Score (偏离度)、Slope (斜率)。
        """
        file_path = os.path.join(self.history_dir, f"{key}.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty or len(df) < 1:
                return None
            
            values = df["value"].astype(float).values
            current = values[-1]
            
            return {
                "value": current,
                "p_20d": self._calc_percentile(values, 20),
                "p_250d": self._calc_percentile(values, 250),
                "p_1250d": self._calc_percentile(values, 1250),
                "z_score": self._calc_zscore(values, 20),
                "slope": self._calc_slope(values, 5)
            }
        except Exception as e:
            print(f"Error calculating features for {key}: {e}")
            return None

    def _calc_percentile(self, values, window):
        lookback = values[-window:]
        if len(lookback) < 1: return 50.0
        # 优化大样本下的计算效率
        current_val = lookback[-1]
        count = np.count_nonzero(lookback <= current_val)
        return round(float(count / len(lookback)) * 100, 3)

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
