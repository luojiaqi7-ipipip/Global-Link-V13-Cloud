import json
import os
import pandas as pd

class QuantLab:
    """
    模块 B: 逻辑计算引擎 - V5 (Unit Normalization)
    负责统一成交量度量衡，确保量比计算准确。
    """
    def __init__(self, raw_file="data/raw/latest_snap.json", out_dir="data/processed"):
        self.raw_file = raw_file
        self.out_dir = out_dir
        os.makedirs(self.out_dir, exist_ok=True)

    def process(self):
        if not os.path.exists(self.raw_file):
            print(f"❌ 错误: 找不到原始文件 {self.raw_file}")
            return None
            
        with open(self.raw_file, 'r') as f:
            raw = json.load(f)

        processed = {
            "timestamp": raw.get('meta', {}).get('timestamp', 'unknown'),
            "macro_matrix": self._calc_macro(raw.get('macro', {})),
            "technical_matrix": self._calc_tech(raw.get('etf_spot', []), raw.get('hist_data', {}))
        }

        out_path = f"{self.out_dir}/metrics_{processed['timestamp']}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(processed, f, ensure_ascii=False, indent=2)
        
        with open(f"{self.out_dir}/latest_metrics.json", 'w', encoding='utf-8') as f:
            json.dump(processed, f, ensure_ascii=False, indent=2)

        print(f"📈 量化矩阵已生成: {out_path}")
        return processed

    def _calc_macro(self, raw_macro):
        """处理宏观矩阵"""
        m = {}
        # 1. 汇率
        if 'CNH' in raw_macro:
            p = raw_macro['CNH'].get('price', 0)
            pc = raw_macro['CNH'].get('prev_close', 0)
            m['CNH_Price'] = p
            m['CNH_Change'] = round((p/pc - 1)*100, 3) if pc != 0 else 0
        
        # 2. 流动性 (SHIBOR)
        if 'SHIBOR' in raw_macro:
            m['Liquidity_Rate'] = raw_macro['SHIBOR'].get('利率', 'N/A')
            m['Liquidity_Change'] = raw_macro['SHIBOR'].get('涨跌', 0)
        
        # 3. 资金流 (北向)
        if 'Northbound' in raw_macro:
            m['Northbound_Flow_Billion'] = round(raw_macro['Northbound'].get('value', 0) / 1e8, 2)
        
        # 4. 全球指数
        for key in ['Nasdaq', 'HangSeng', 'A50_Futures', 'US_10Y_Yield']:
            if key in raw_macro:
                m[f'{key}_Price'] = raw_macro[key].get('price', 'N/A')
            
        return m

    def _calc_tech(self, spot, hist_map):
        """统一单位：所有成交量转换为‘股’"""
        matrix = []
        if not spot: return []
            
        for s in spot:
            try:
                code = s.get('代码')
                if not code or code not in hist_map: continue
                
                df_hist = pd.DataFrame(hist_map[code])
                if len(df_hist) < 5: continue
                
                # 价格计算
                closes = df_hist['收盘'].tolist() if '收盘' in df_hist else df_hist['收盘价'].tolist()
                current_price = float(s.get('最新价', 0))
                ma5 = sum(closes[-5:]) / 5
                bias = ((current_price - ma5) / ma5) * 100
                
                # 成交量单位归一化：
                # 1. 历史数据 (EM hist) 通常为“手”
                # 2. 实时行情 (EM spot 或 Sina) 通常为“股”
                # 我们将历史成交量乘 100 统一到“股”
                vols_hist = (df_hist['成交量'] * 100).tolist()
                current_vol_shares = float(s.get('成交量', 0))
                
                vol_avg_shares = sum(vols_hist[-5:]) / 5
                vol_ratio = current_vol_shares / vol_avg_shares if vol_avg_shares > 0 else 0
                
                matrix.append({
                    "code": code,
                    "name": s.get('名称', 'N/A'),
                    "price": current_price,
                    "bias": round(bias, 2),
                    "vol_ratio": round(vol_ratio, 2)
                })
            except Exception as e:
                print(f"    [!] 指标计算失败 {s.get('代码')}: {e}")
                
        return sorted(matrix, key=lambda x: x['bias'])

if __name__ == "__main__":
    lab = QuantLab()
    lab.process()
