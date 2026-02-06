import akshare as ak
import json
import os
from datetime import datetime, timedelta, date
import pytz
import time
import pandas as pd
import requests
import yfinance as yf

class Harvester:
    def __init__(self, data_dir="data/raw"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.beijing_tz = pytz.timezone('Asia/Shanghai')
        self.timestamp = datetime.now(self.beijing_tz).strftime("%Y%m%d_%H%M")
        self.watchlist = ["159995", "513050", "512760", "512480", "588000", "159915", "510500", "510300", "512660", "512880", "510880", "515080", "512010", "512800", "512690", "159928"]

    def harvest_all(self):
        print(f"🚀 [V13] 开始全量数据抓取 [{self.timestamp}]...")
        raw_data = {
            "meta": {"timestamp": self.timestamp, "timezone": "Asia/Shanghai", "version": "V13-Final-Robust"},
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        raw_data = self._serialize_clean(raw_data)
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        return raw_data

    def _serialize_clean(self, obj):
        if isinstance(obj, dict): return {k: self._serialize_clean(v) for k, v in obj.items()}
        elif isinstance(obj, list): return [self._serialize_clean(i) for i in obj]
        elif pd.isna(obj): return None
        return obj

    def _get_spot(self):
        try:
            symbols = [f"sh{c}" if c.startswith(('5', '6')) else f"sz{c}" for c in self.watchlist]
            # 添加 Headers 避免被屏蔽
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "http://finance.sina.com.cn"
            }
            r = requests.get(f"http://qt.gtimg.cn/q=s_{','.join(symbols)}", headers=headers, timeout=5)
            if r.status_code == 200:
                results = []
                for p in r.text.strip().split(';'):
                    if '~' not in p: continue
                    parts = p.split('~')
                    results.append({
                        "代码": parts[2], "名称": parts[1], "最新价": float(parts[3]),
                        "成交量": float(parts[6]), "涨跌幅": float(parts[5]), "unit": "LOT"
                    })
                return results
        except: pass
        return []

    def _get_macro(self):
        macro = {}
        def wrap(data): return {**(data if isinstance(data, dict) else {"value": data}), "status": "SUCCESS" if data is not None else "FAILED", "last_update": self.timestamp}

        # 1. 全球宏观 (Sina Global)
        sina_map = {
            "CNH": "fx_susdcnh", 
            "Nasdaq": "gb_ndx", 
            "HangSeng": "rt_hkHSI", 
            "A50_Futures": "hf_CHA50CFD", 
            "VIX": "hf_VX",
            "Gold": "hf_GC",
            "CrudeOil": "hf_CL"
        }
        try:
            url = f"http://hq.sinajs.cn/list={','.join(sina_map.values())}"
            r = requests.get(url, headers={"Referer": "http://finance.sina.com.cn"}, timeout=5)
            lines = r.text.strip().split('\n')
            inv_map = {v: k for k, v in sina_map.items()}
            for line in lines:
                try:
                    sym = line.split('=')[0].replace('var hq_str_', '').strip()
                    data = line.split('"')[1].split(',')
                    key = inv_map.get(sym)
                    if not key or not data or len(data) < 2: continue
                    
                    if sym.startswith("fx_"): 
                        price = float(data[1])
                        change_pct = float(data[11]) * 100 if len(data) > 11 and data[11] else None
                    elif sym.startswith("gb_"): 
                        price = float(data[1])
                        change_pct = float(data[2]) if len(data) > 2 and data[2] else None
                    elif sym.startswith("rt_"): 
                        price = float(data[2])
                        # rt_hkHSI: Current=data[2], PrevClose=data[3]
                        change_pct = (float(data[2]) / float(data[3]) - 1) * 100 if len(data) > 3 and data[3] else None
                    elif sym.startswith("hf_"): 
                        price = float(data[0])
                        # hf_: Current=data[0], PrevClose=data[7]
                        change_pct = (float(data[0]) / float(data[7]) - 1) * 100 if len(data) > 7 and data[7] and float(data[7]) != 0 else None
                    else: 
                        price = float(data[1])
                        change_pct = None
                    
                    macro[key] = wrap({"price": price, "change_pct": change_pct})
                except: pass
        except: pass

        # 2. 国债收益率 (CN & US) - 使用综合接口
        try:
            df_rates = ak.bond_zh_us_rate()
            if not df_rates.empty:
                # 寻找最新的有效数据
                cn_latest = df_rates.dropna(subset=['中国国债收益率10年']).iloc[-1]
                macro['CN10Y'] = wrap({"yield": float(cn_latest['中国国债收益率10年'])})
                
                us_latest = df_rates.dropna(subset=['美国国债收益率10年']).iloc[-1]
                macro['US10Y'] = wrap({"price": float(us_latest['美国国债收益率10年'])})
        except: pass

        # 3. 北向资金 (由于新规隐藏实时净流入，改用历史接口取最新有效值)
        try:
            df = ak.stock_hsgt_hist_em(symbol="北向资金")
            if not df.empty:
                # 过滤掉 NaN 所在的行，取最后一个有效值
                valid_df = df.dropna(subset=['当日成交净买额'])
                if not valid_df.empty:
                    latest = valid_df.iloc[-1]
                    val = float(latest['当日成交净买额']) * 1e8
                    macro['Northbound'] = wrap({"value": val, "date": str(latest['日期'])})
        except: pass

        # 4. 行业流入 (东财 Push2)
        try:
            r = requests.get("https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10&po=1&np=1&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50&fields=f14,f62", timeout=5).json()
            diff = r.get('data', {}).get('diff', [])
            if diff: macro['Sector_Flow'] = wrap({"top_inflow": [{"名称": d['f14'], "今日净额": d['f62']} for d in diff[:3]]})
        except: pass

        # 5. 两融 (AkShare 宏观两融接口)
        try:
            m_sh = ak.macro_china_market_margin_sh()
            m_sz = ak.macro_china_market_margin_sz()
            sh_val = float(m_sh.iloc[-1]['融资融券余额']) if '融资融券余额' in m_sh.columns else float(m_sh.iloc[-1].iloc[-1])
            sz_val = float(m_sz.iloc[-1]['融资融券余额']) if '融资融券余额' in m_sz.columns else float(m_sz.iloc[-1].iloc[-1])
            macro['Margin_Debt'] = wrap({"value": sh_val + sz_val})
        except: pass

        return macro

        return macro

    def _get_hist_context(self):
        ctx = {}
        for c in self.watchlist:
            try:
                # 使用新浪 K 线接口，比 AkShare 更稳定
                prefix = "sh" if c.startswith(('5', '6')) else "sz"
                url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={prefix}{c}&scale=240&ma=no&datalen=45"
                r = requests.get(url, timeout=5).json()
                if r:
                    ctx[c] = [{
                        "日期": item['day'],
                        "开盘": float(item['open']),
                        "最高": float(item['high']),
                        "最低": float(item['low']),
                        "收盘": float(item['close']),
                        "成交量": float(item['volume']),
                        "unit": "SHARE"
                    } for item in r]
            except: pass
        return ctx

if __name__ == "__main__":
    Harvester().harvest_all()
