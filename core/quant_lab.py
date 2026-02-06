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
        """处理宏观矩阵，提取核心变化率"""
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
        
        # 4. 全球指数 (纳指、恒指、富时A50)
        if 'Nasdaq' in raw_macro:
            m['Nasdaq_Price'] = raw_macro['Nasdaq'].get('price', 'N/A')
        if 'HangSeng' in raw_macro:
            m['HangSeng_Price'] = raw_macro['HangSeng'].get('price', 'N/A')
        if 'A50_Futures' in raw_macro:
            m['A50_Futures_Price'] = raw_macro['A50_Futures'].get('price', 'N/A')
            
        # 5. 美债收益率
        if 'US_10Y_Yield' in raw_macro:
            m['US_10Y_Yield'] = raw_macro['US_10Y_Yield'].get('price', 'N/A')
            
        return m

    def _calc_tech(self, spot, hist_map):
        """计算核心技术指标：Bias, Vol_Ratio"""
        matrix = []
        if not spot:
            return []
            
        for s in spot:
            try:
                code = s.get('代码')
                if not code: continue
                
                # 计算乖离率与量比
                if code in hist_map and hist_map[code]:
                    df_hist = pd.DataFrame(hist_map[code])
                    if len(df_hist) < 5: continue
                    
                    # 统一列名（支持 EM 和 Sina 两种格式）
                    closes = df_hist['收盘'].tolist() if '收盘' in df_hist else df_hist['收盘价'].tolist()
                    vols = df_hist['成交量'].tolist()
                    
                    ma5 = sum(closes[-5:]) / 5
                    current_price = float(s.get('最新价', 0))
                    
                    bias = ((current_price - ma5) / ma5) * 100
                    
                    vol_avg = sum(vols[-5:]) / 5
                    current_vol = float(s.get('成交量', 0))
                    vol_ratio = current_vol / vol_avg if vol_avg > 0 else 0
                    
                    matrix.append({
                        "code": code,
                        "name": s.get('名称', 'N/A'),
                        "price": current_price,
                        "bias": round(bias, 2),
                        "vol_ratio": round(vol_ratio, 2)
                    })
            except Exception as e:
                print(f"    [!] 计算指标失败 {s.get('代码')}: {e}")
                
        return sorted(matrix, key=lambda x: x['bias'])

if __name__ == "__main__":
    lab = QuantLab()
    lab.process()
