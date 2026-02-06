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
            "meta": {"timestamp": self.timestamp, "timezone": "Asia/Shanghai", "version": "V13-Full-Restore"},
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
        print(" -> 正在抓取实时报价 (Tencent)...")
        try:
            symbols = [f"sh{c}" if c.startswith(('5', '6')) else f"sz{c}" for c in self.watchlist]
            r = requests.get(f"http://qt.gtimg.cn/q=s_{','.join(symbols)}", timeout=5)
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
        except Exception as e: print(f"Spot Error: {e}")
        return []

    def _get_macro(self):
        macro = {}
        def wrap(data): return {**(data if isinstance(data, dict) else {"value": data}), "status": "SUCCESS" if data is not None else "FAILED", "last_update": self.timestamp}

        # 1. 全球宏观 (Sina Global - 最快)
        print(" -> 正在抓取 Sina 全球宏观...")
        sina_map = {
            "CNH": "fx_susdcnh", "Nasdaq": "gb_ixic", "HangSeng": "rt_hkHSI",
            "A50": "hf_CHA50CFD", "VIX": "gb_$vix", "Gold": "hf_GC", "CrudeOil": "hf_CL", "US10Y": "fx_sus10y"
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
                    
                    price = 0.0
                    if key == "A50": price = float(data[0])
                    elif key == "CNH": price = float(data[1])
                    elif key == "Nasdaq": price = float(data[1])
                    elif key == "HangSeng": price = float(data[6])
                    elif key == "VIX": price = float(data[1])
                    elif key == "US10Y": price = float(data[1])
                    elif "hf_" in sym: price = float(data[0])
                    macro[key] = wrap({"price": price})
                except: continue
        except: pass

        # 2. 北向资金
        print(" -> 正在抓取北向资金...")
        try:
            r = requests.get("https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1&fields2=f51,f52", timeout=5).json().get('data', {})
            val = (float(r.get('hk2sh', {}).get('dayNetAmtIn', 0)) + float(r.get('hk2sz', {}).get('dayNetAmtIn', 0))) * 10000
            if val == 0:
                hist = requests.get("https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY&columns=NET_DEAL_AMT&filter=(MUTUAL_TYPE%3D%22001%22)&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=5", timeout=5).json().get('result', {}).get('data', [])
                for i in hist:
                    if i.get('NET_DEAL_AMT'): val = float(i['NET_DEAL_AMT']) * 1e8; break
            macro['Northbound'] = wrap({"value": val})
        except: pass

        # 3. 行业流入
        print(" -> 正在抓取行业流入...")
        try:
            r = requests.get("https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10&po=1&np=1&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50&fields=f14,f62", timeout=5).json()
            diff = r.get('data', {}).get('diff', [])
            if diff: macro['Sector_Flow'] = wrap({"top_inflow": [{"名称": d['f14'], "今日净额": d['f62']} for d in diff[:3]]})
        except: pass

        # 4. 两融 & 国债 (AkShare)
        print(" -> 正在抓取 AkShare 指标...")
        try:
            m = ak.stock_margin_sh()
            if not m.empty: macro['Margin_Debt'] = wrap({"value": float(m.iloc[-1].iloc[-1]), "change_pct": round((m.iloc[-1].iloc[-1]/m.iloc[-2].iloc[-1]-1)*100, 3)})
        except: pass
        try:
            y = ak.bond_china_yield(start_date=(datetime.now()-timedelta(days=5)).strftime("%Y%m%d"))
            if not y.empty: macro['CN10Y'] = wrap({"yield": float(y['10年'].iloc[-1])})
        except: pass

        return macro

    def _get_hist_context(self):
        print(" -> 正在抓取历史背景...")
        ctx = {}
        for c in self.watchlist:
            try:
                df = ak.fund_etf_hist_em(symbol=c, period="daily", start_date=(datetime.now()-timedelta(days=45)).strftime("%Y%m%d"), adjust="qfq")
                if not df.empty: ctx[c] = df.to_dict(orient='records')
            except: pass
        return ctx

if __name__ == "__main__":
    Harvester().harvest_all()
