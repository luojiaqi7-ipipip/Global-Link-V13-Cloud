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
    """
    模块 A: 情报获取引擎 - V13 Cloud (Ultra Robust)
    """
    def __init__(self, data_dir="data/raw"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.beijing_tz = pytz.timezone('Asia/Shanghai')
        self.timestamp = datetime.now(self.beijing_tz).strftime("%Y%m%d_%H%M")
        self.watchlist = [
            "159995", "513050", "512760", "512480", "588000",
            "159915", "510500", "510300", "512660", "512880",
            "510880", "515080", "512010", "512800", "512690", "159928"
        ]

    def harvest_all(self):
        print(f"🚀 [V13] 开始全量数据抓取 [{self.timestamp}]...")
        raw_data = {
            "meta": {"timestamp": self.timestamp, "timezone": "Asia/Shanghai", "version": "V13-Cloud-Ultra-Robust"},
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
        elif isinstance(obj, (datetime, date)): return obj.isoformat()
        elif pd.isna(obj): return None
        return obj

    def _get_spot(self):
        try:
            symbols = [f"sh{c}" if c.startswith(('5', '6')) else f"sz{c}" for c in self.watchlist]
            r = requests.get(f"http://qt.gtimg.cn/q=s_{','.join(symbols)}", timeout=5)
            if r.status_code == 200:
                return [{"代码": p.split('~')[2], "名称": p.split('~')[1], "最新价": float(p.split('~')[3]), "成交量": float(p.split('~')[6]), "涨跌幅": float(p.split('~')[5]), "unit": "LOT"} for p in r.text.strip().split(';') if '~' in p]
        except: pass
        return []

    def _get_macro(self):
        macro = {}
        def wrap(data): return {**(data if isinstance(data, dict) else {"value": data}), "status": "SUCCESS" if data is not None else "FAILED", "last_update": self.timestamp}

        # 1. 全球宏观 (Yahoo Finance - 云端最稳)
        print(" -> 抓取 Yahoo 宏观...")
        try:
            tk = {"Nasdaq": "^IXIC", "HangSeng": "^HSI", "VIX": "^VIX", "US10Y": "^TNX", "Gold": "GC=F", "CrudeOil": "CL=F", "A50": "XIN9.F"}
            df = yf.download(list(tk.values()), period="2d", interval="1d", progress=False)['Close']
            for k, t in tk.items():
                if t in df: macro[k] = wrap({"price": float(df[t].iloc[-1])})
        except: pass

        # 2. 北向资金 (Hybrid)
        print(" -> 抓取北向资金...")
        try:
            r = requests.get("https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1&fields2=f51,f52", timeout=5).json().get('data', {})
            val = (float(r.get('hk2sh', {}).get('dayNetAmtIn', 0)) + float(r.get('hk2sz', {}).get('dayNetAmtIn', 0))) * 10000
            if val == 0:
                hist = requests.get("https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY&columns=NET_DEAL_AMT&filter=(MUTUAL_TYPE%3D%22001%22)&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=5", timeout=5).json().get('result', {}).get('data', [])
                for i in hist:
                    if i.get('NET_DEAL_AMT'): val = float(i['NET_DEAL_AMT']) * 1e8; break
            macro['Northbound'] = wrap({"value": val})
        except: pass

        # 3. 行业流入 (Direct Push2)
        print(" -> 抓取行业流入...")
        try:
            headers = {"Referer": "https://data.eastmoney.com/zjlx/dpzjlx.html", "User-Agent": "Mozilla/5.0"}
            r = requests.get("https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10&po=1&np=1&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50&fields=f14,f62", headers=headers, timeout=5).json()
            diff = r.get('data', {}).get('diff', [])
            if diff: macro['Sector_Flow'] = wrap({"top_inflow": [{"名称": d['f14'], "今日净额": d['f62']} for d in diff[:3]]})
        except: pass

        # 4. 两融 & 国债 (AkShare)
        print(" -> 抓取 AkShare 指标...")
        try:
            m = ak.stock_margin_sh()
            if not m.empty: macro['Margin_Debt'] = wrap({"value": float(m.iloc[-1].iloc[-1]), "change_pct": round((m.iloc[-1].iloc[-1]/m.iloc[-2].iloc[-1]-1)*100, 3)})
        except: pass
        try:
            y = ak.bond_china_yield(start_date=(datetime.now()-timedelta(days=5)).strftime("%Y%m%d"))
            if not y.empty: macro['CN10Y'] = wrap({"yield": float(y['10年'].iloc[-1])})
        except: pass

        for k in ["CNH", "Nasdaq", "HangSeng", "A50_Futures", "VIX", "US10Y", "Gold", "CrudeOil", "Northbound", "CN10Y", "Margin_Debt", "Sector_Flow"]:
            if k not in macro: macro[k] = wrap(None)
        return macro

    def _get_hist_context(self):
        ctx = {}
        for c in self.watchlist:
            try:
                df = ak.fund_etf_hist_em(symbol=c, period="daily", start_date=(datetime.now()-timedelta(days=45)).strftime("%Y%m%d"), adjust="qfq")
                if not df.empty: ctx[c] = df.to_dict(orient='records')
            except: pass
        return ctx

if __name__ == "__main__":
    Harvester().harvest_all()
