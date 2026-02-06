import json
import os
import pandas as pd

class QuantLab:
    """
    模块 B: 逻辑计算引擎
    负责将原始 JSON 转化为结构化的量化指标矩阵。
    """
    def __init__(self, raw_file="data/raw/latest_snap.json", out_dir="data/processed"):
        self.raw_file = raw_file
        self.out_dir = out_dir
        os.makedirs(self.out_dir, exist_ok=True)

    def process(self):
        if not os.path.exists(self.raw_file):
            return None
            
        with open(self.raw_file, 'r') as f:
            raw = json.load(f)

        processed = {
            "timestamp": raw['meta']['timestamp'],
            "macro_matrix": self._calc_macro(raw['macro']),
            "technical_matrix": self._calc_tech(raw['etf_spot'], raw['hist_data'])
        }

        out_path = f"{self.out_dir}/metrics_{raw['meta']['timestamp']}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(processed, f, ensure_ascii=False, indent=2)
        
        with open(f"{self.out_dir}/latest_metrics.json", 'w', encoding='utf-8') as f:
            json.dump(processed, f, ensure_ascii=False, indent=2)

        print(f"量化矩阵已生成: {out_path}")
        return processed

    def _calc_macro(self, raw_macro):
        # 提取核心变化率和数值
        m = {}
        if 'CNH' in raw_macro:
            m['CNH_Price'] = raw_macro['CNH']['【最新价】']
            m['CNH_Change'] = raw_macro['CNH']['【涨跌幅】']
        if 'SHIBOR' in raw_macro:
            m['Liquidity_Rate'] = raw_macro['SHIBOR']['利率']
            m['Liquidity_Change'] = raw_macro['SHIBOR']['涨跌']
        return m

    def _calc_tech(self, spot, hist_map):
        matrix = []
        for s in spot:
            code = s['代码']
            # 计算乖离率
            if code in hist_map:
                df_hist = pd.DataFrame(hist_map[code])
                closes = df_hist['收盘'].tolist()
                ma5 = sum(closes[-5:]) / 5
                bias = ((s['最新价'] - ma5) / ma5) * 100
                vol_avg = df_hist['成交量'].iloc[-5:].mean()
                vol_ratio = s['成交量'] / vol_avg if vol_avg > 0 else 0
                
                matrix.append({
                    "code": code,
                    "name": s['名称'],
                    "price": s['最新价'],
                    "bias": round(bias, 2),
                    "vol_ratio": round(vol_ratio, 2)
                })
        return sorted(matrix, key=lambda x: x['bias'])

if __name__ == "__main__":
    lab = QuantLab()
    lab.process()
