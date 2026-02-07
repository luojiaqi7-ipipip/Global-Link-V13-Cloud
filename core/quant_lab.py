import json
import os
import pandas as pd
from core.intel_engine import IntelEngine

class QuantLab:
    """
    模块 B: 逻辑计算引擎 - V14.1 (Precision & Unit Robust)
    """
    def __init__(self, raw_file="data/raw/latest_snap.json", out_dir="data/processed"):
        self.raw_file = raw_file
        self.out_dir = out_dir
        self.intel = IntelEngine()
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

        out_path = f"{self.out_dir}/metrics_{processed['timestamp'].replace(' ', '_').replace(':', '')}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(processed, f, ensure_ascii=False, indent=2)
        
        with open(f"{self.out_dir}/latest_metrics.json", 'w', encoding='utf-8') as f:
            json.dump(processed, f, ensure_ascii=False, indent=2)

        print(f"📈 量化特征矩阵已生成: {out_path}")
        return processed

    def _calc_macro(self, raw_macro):
        """处理宏观矩阵 - V14 特征全貌版 (带单位鲁棒性)"""
        m = {}
        
        def get_full_signal(key, subkey='price'):
            ind = raw_macro.get(key, {})
            val = None
            if ind.get('status') == 'SUCCESS':
                val = ind.get(subkey)
                if val is None and subkey == 'price':
                    val = ind.get('value') or ind.get('yield')
            
            # 获取历史特征
            features = self.intel.get_features(key) or {}
            
            # 1. 实时值提取与单位对齐 (Billion Normalization)
            final_val = val if val is not None else features.get("value")
            if final_val is not None:
                final_val = float(final_val)
                if key in ['Southbound', 'Margin_Debt', 'Northbound'] and abs(final_val) > 1e6:
                    final_val = final_val / 1e8
                final_val = round(final_val, 3)
            
            # 2. 变动率精度控制
            change_pct = ind.get('change_pct')
            if change_pct is not None:
                change_pct = round(float(change_pct), 3)

            return {
                "value": final_val,
                "change_pct": change_pct,
                "p_20d": round(features.get("p_20d", 50.0), 3),
                "p_250d": round(features.get("p_250d", 50.0), 3),
                "p_1250d": round(features.get("p_1250d", 50.0), 3),
                "z_score": round(features.get("z_score", 0.0), 3),
                "slope": round(features.get("slope", 0.0), 3)
            }

        # 核心指标
        for k in ['CNH', 'Nasdaq', 'HangSeng', 'A50_Futures', 'VIX', 'Gold', 'CrudeOil']:
            m[k] = get_full_signal(k)
            
        m['CN10Y'] = get_full_signal('CN10Y', 'yield')
        m['US10Y'] = get_full_signal('US10Y')
        m['SHIBOR'] = get_full_signal('SHIBOR', 'value')
        
        # 特殊处理：沪深300振幅
        csi300_data = raw_macro.get('CSI300_Vol', {})
        csi300_features = self.intel.get_features('CSI300_Vol') or {}
        m['A_Share_Vol'] = {
            "amplitude": round(float(csi300_data.get('amplitude', 0)), 3) if csi300_data.get('amplitude') else 0.0,
            "pct_change": round(float(csi300_data.get('pct_change', 0)), 3) if csi300_data.get('pct_change') else 0.0,
            "p_20d": round(csi300_features.get("p_20d", 50.0), 3),
            "p_250d": round(csi300_features.get("p_250d", 50.0), 3),
            "p_1250d": round(csi300_features.get("p_1250d", 50.0), 3),
            "z_score": round(csi300_features.get("z_score", 0.0), 3),
            "slope": round(csi300_features.get("slope", 0.0), 3)
        }
        
        # 资金类
        m['Southbound'] = get_full_signal('Southbound', 'value')
        m['Margin_Debt'] = get_full_signal('Margin_Debt', 'change_pct')
            
        return m

    def _calc_tech(self, spot, hist_map):
        matrix = []
        if not spot: return []
            
        for s in spot:
            try:
                code = s.get('代码')
                if not code or code not in hist_map: continue
                df_hist = pd.DataFrame(hist_map[code])
                if len(df_hist) < 4: continue
                
                current_price = float(s.get('最新价', 0))
                closes_hist = df_hist['收盘'].dropna().astype(float).tolist()
                
                real_time_ma5 = (sum(closes_hist[-4:]) + current_price) / 5
                bias = (current_price / real_time_ma5 - 1) * 100 if real_time_ma5 != 0 else None
                
                vols_hist = df_hist['成交量'].dropna().astype(float).tolist()
                if df_hist.iloc[0].get('unit') == 'LOT':
                    vols_hist = [v * 100 for v in vols_hist]
                
                current_vol = float(s.get('成交量', 0))
                if s.get('unit') == 'LOT':
                    current_vol *= 100
                
                vol_avg = sum(vols_hist[-5:]) / 5 if len(vols_hist) >= 5 else (sum(vols_hist) / len(vols_hist) if vols_hist else 0)
                vol_ratio = current_vol / vol_avg if vol_avg > 0 else None
                
                matrix.append({
                    "code": code,
                    "name": s.get('名称', 'N/A'),
                    "price": current_price,
                    "bias": round(bias, 2) if bias is not None else None,
                    "vol_ratio": round(vol_ratio, 2) if vol_ratio is not None else None
                })
            except: pass
                
        return sorted([m for m in matrix if m['bias'] is not None], key=lambda x: x['bias'])

if __name__ == "__main__":
    QuantLab().process()
