import json
import os
import pandas as pd

class QuantLab:
    """
    模块 B: 逻辑计算引擎 - V6 (Consistent Units)
    假设原始数据成交量单位已由 Harvester 统一为“股”。
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
        """处理宏观矩阵 - V13 Cloud 增强版"""
        m = {}
        # 1. 核心汇率 (人民币情绪)
        if 'CNH' in raw_macro:
            p = raw_macro['CNH'].get('price', 0)
            pc = raw_macro['CNH'].get('prev_close', 0)
            m['CNH_Price'] = p
            m['CNH_Change'] = round((p/pc - 1)*100, 3) if pc != 0 else 0
        
        # 2. 流动性深度 (国内 SHIBOR + 中美利差背景)
        if 'SHIBOR' in raw_macro:
            m['Liquidity_Rate'] = raw_macro['SHIBOR'].get('利率', 'N/A')
            m['Liquidity_Change'] = raw_macro['SHIBOR'].get('涨跌', 0)
        
        if 'CN10Y' in raw_macro:
            m['CN10Y_Yield'] = raw_macro['CN10Y'].get('yield')
        if 'US10Y' in raw_macro:
            m['US10Y_Yield'] = raw_macro['US10Y'].get('price')
            
        # 3. 风险偏好 (VIX + A股波动率 + 杠杆情绪)
        if 'VIX' in raw_macro:
            m['VIX'] = raw_macro['VIX'].get('price')
        if 'CSI300_Vol' in raw_macro:
            m['A_Share_Amplitude'] = raw_macro['CSI300_Vol'].get('amplitude')
        if 'Margin_Debt' in raw_macro:
            m['Margin_Change_Pct'] = raw_macro['Margin_Debt'].get('change_pct')

        # 4. 资金流向 (北向 + 行业热点)
        if 'Northbound' in raw_macro:
            m['Northbound_Flow_Billion'] = round(raw_macro['Northbound'].get('value', 0) / 1e8, 2)
        
        if 'Sector_Flow' in raw_macro:
            m['Inflow_Sectors'] = [s['名称'] for s in raw_macro['Sector_Flow'].get('top_inflow', [])]
            m['Outflow_Sectors'] = [s['名称'] for s in raw_macro['Sector_Flow'].get('top_outflow', [])]

        # 5. 另类数据 (避险与通胀)
        if 'Gold' in raw_macro:
            m['Gold_Price'] = raw_macro['Gold'].get('price')
        if 'CrudeOil' in raw_macro:
            m['CrudeOil_Price'] = raw_macro['CrudeOil'].get('price')

        # 6. 全球指数
        for key in ['Nasdaq', 'HangSeng', 'A50_Futures']:
            if key in raw_macro:
                m[f'{key}_Price'] = raw_macro[key].get('price', 'N/A')
            
        return m

    def _calc_tech(self, spot, hist_map):
        """单位已在 Harvester 统一为股"""
        matrix = []
        if not spot: return []
            
        for s in spot:
            try:
                code = s.get('代码')
                if not code or code not in hist_map: continue
                
                df_hist = pd.DataFrame(hist_map[code])
                if len(df_hist) < 5: continue
                
                # 价格计算
                closes = df_hist['收盘'].tolist()
                current_price = float(s.get('最新价', 0))
                ma5 = sum(closes[-5:]) / 5
                bias = ((current_price - ma5) / ma5) * 100
                
                # 成交量计算 (单位均为股)
                vols_hist = df_hist['成交量'].tolist()
                current_vol = float(s.get('成交量', 0))
                vol_avg = sum(vols_hist[-5:]) / 5
                vol_ratio = current_vol / vol_avg if vol_avg > 0 else 0
                
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
