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
            "macro_health": {k: {"status": v.get('status', 'FAILED'), "last_update": v.get('last_update', 'unknown')} for k, v in raw.get('macro', {}).items()},
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
        """处理宏观矩阵 - V13 Cloud 增强版 (强类型安全)"""
        m = {}
        
        def get_val(key, subkey, default=None):
            ind = raw_macro.get(key, {})
            if ind.get('status') == 'SUCCESS':
                val = ind.get(subkey)
                return val if val is not None else default
            return default

        # 1. 核心汇率 (人民币情绪)
        m['CNH_Price'] = get_val('CNH', 'price') or get_val('CNH', 'value')
        m['CNH_Change'] = get_val('CNH', 'change_pct')
        
        # 2. 流动性深度 (国内 SHIBOR + 中美利差背景)
        shibor = raw_macro.get('SHIBOR', {})
        if shibor.get('status') == 'SUCCESS':
            # 适配 AkShare 或 NetEase 的 SHIBOR 结构
            m['Liquidity_Rate'] = shibor.get('ON')
            m['Liquidity_Change'] = None # 确保不再硬编码为 0.0
        else:
            m['Liquidity_Rate'] = None
            m['Liquidity_Change'] = None
        
        m['CN10Y_Yield'] = get_val('CN10Y', 'yield')
        # US10Y 可能在 price 或 yield 字段
        m['US10Y_Yield'] = get_val('US10Y', 'price') or get_val('US10Y', 'yield')
            
        # 3. 风险偏好 (VIX + A股波动率 + 杠杆情绪)
        m['VIX'] = get_val('VIX', 'price')
        # 优先取 amplitude，没有则取 pct_change
        amp = get_val('CSI300_Vol', 'amplitude')
        if amp is None: amp = get_val('CSI300_Vol', 'pct_change')
        m['A_Share_Amplitude'] = amp
        
        m['Margin_Change_Pct'] = get_val('Margin_Debt', 'change_pct')

        # 4. 资金流向 (北向 + 行业热点)
        nb = raw_macro.get('Northbound', {})
        if nb.get('status') == 'SUCCESS':
            nb_val = nb.get('value')
            if nb_val is not None:
                m['Northbound_Flow_Billion'] = round(float(nb_val) / 1e8, 2)
            else:
                m['Northbound_Flow_Billion'] = 0.0 # 默认为 0
        else:
            m['Northbound_Flow_Billion'] = None
        
        sf = raw_macro.get('Sector_Flow', {})
        if sf.get('status') == 'SUCCESS':
            m['Inflow_Sectors'] = [s['名称'] for s in sf.get('top_inflow', [])]
            m['Outflow_Sectors'] = [s['名称'] for s in sf.get('top_outflow', [])]
        else:
            m['Inflow_Sectors'] = None
            m['Outflow_Sectors'] = None

        # 5. 另类数据 (避险与通胀)
        m['Gold_Price'] = get_val('Gold', 'price')
        m['CrudeOil_Price'] = get_val('CrudeOil', 'price')

        # 6. 全球指数
        for key in ['Nasdaq', 'HangSeng', 'A50_Futures']:
            m[f'{key}_Price'] = get_val(key, 'price')
            
        return m
            
        # A50 特殊映射
        m['A50_Futures_Price'] = get_val('A50_Futures', 'price') or get_val('A50', 'price')
            
        return m

    def _calc_tech(self, spot, hist_map):
        """
        V13 Cloud 增强版：
        1. 实时 MA5 重构 (过去 4 日 + 今日当前价)
        2. 成交量单位强校验 (LOT -> SHARE)
        3. 强类型安全 (None 检查)
        """
        matrix = []
        if not spot: return []
            
        for s in spot:
            try:
                code = s.get('代码')
                if not code or code not in hist_map: continue
                
                df_hist = pd.DataFrame(hist_map[code])
                if len(df_hist) < 4: continue
                
                # 1. 价格与实时乖离率 (Bias) 重构
                curr_price_raw = s.get('最新价')
                if curr_price_raw is None: continue
                current_price = float(curr_price_raw)
                
                closes_hist = df_hist['收盘'].dropna().astype(float).tolist()
                if len(closes_hist) < 4: continue
                
                real_time_ma5 = (sum(closes_hist[-4:]) + current_price) / 5
                bias = (current_price / real_time_ma5 - 1) * 100 if real_time_ma5 != 0 else None
                
                # 2. 成交量单位强校验 (强制统一为“股”)
                hist_unit = df_hist.iloc[0].get('unit', 'SHARE')
                vols_hist = df_hist['成交量'].dropna().astype(float).tolist()
                if hist_unit == 'LOT':
                    vols_hist = [v * 100 for v in vols_hist]
                
                current_vol_raw = s.get('成交量')
                if current_vol_raw is None: continue
                current_vol = float(current_vol_raw)
                
                spot_unit = s.get('unit', 'SHARE')
                if spot_unit == 'LOT':
                    current_vol *= 100
                
                # 计算量比 (Vol Ratio)
                if len(vols_hist) >= 5:
                    vol_avg = sum(vols_hist[-5:]) / 5
                elif len(vols_hist) > 0:
                    vol_avg = sum(vols_hist) / len(vols_hist)
                else:
                    vol_avg = 0
                    
                vol_ratio = current_vol / vol_avg if vol_avg > 0 else None
                
                matrix.append({
                    "code": code,
                    "name": s.get('名称', '等待同步'),
                    "price": current_price,
                    "bias": round(bias, 2) if bias is not None else None,
                    "vol_ratio": round(vol_ratio, 2) if vol_ratio is not None else None,
                    "real_time_ma5": round(real_time_ma5, 3)
                })
            except Exception as e:
                print(f"    [!] 指标计算失败 {s.get('代码')}: {e}")
                
        return sorted([m for m in matrix if m['bias'] is not None], key=lambda x: x['bias'])

if __name__ == "__main__":
    lab = QuantLab()
    lab.process()
