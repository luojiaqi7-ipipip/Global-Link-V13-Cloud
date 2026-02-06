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
    模块 A: 情报获取引擎 - V13 Cloud (Final Hardening)
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
            "meta": {
                "timestamp": self.timestamp, 
                "timezone": "Asia/Shanghai",
                "version": "V13-Cloud-Final-Hardening"
            },
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        
        raw_data = self._serialize_clean(raw_data)
        print(f"📊 [结果统计] ETF行情: {len(raw_data['etf_spot'])} | 宏观指标: {len(raw_data['macro'])} | 历史背景: {len(raw_data['hist_data'])}")
        
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
        return raw_data

    def _serialize_clean(self, obj):
        if isinstance(obj, dict):
            return {k: self._serialize_clean(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_clean(i) for i in obj]
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif pd.isna(obj):
            return None
        return obj

    def _get_spot(self):
        """抓取实时行情 - 腾讯 API"""
        try:
            symbols = [f"sh{c}" if c.startswith(('5', '6')) else f"sz{c}" for c in self.watchlist]
            url = f"http://qt.gtimg.cn/q=s_{','.join(symbols)}"
            r = requests.get(url, timeout=5)
            results = []
            if r.status_code == 200:
                lines = r.text.strip().split(';')
                for line in lines:
                    if '~' not in line: continue
                    parts = line.split('~')
                    results.append({
                        "代码": parts[2], "名称": parts[1], "最新价": float(parts[3]),
                        "成交量": float(parts[6]), "涨跌幅": float(parts[5]),
                        "unit": "LOT", "source": "tencent"
                    })
                return results
        except: pass
        return []

    def _get_macro(self):
        macro = {}
        def wrap_indicator(data, status="SUCCESS"):
            if data is None: status = "FAILED"
            return {"value": data, "status": status, "last_update": self.timestamp} if not isinstance(data, dict) else {**data, "status": status, "last_update": self.timestamp}

        def safe_change_pct(curr, prev):
            return round((float(curr) / float(prev) - 1) * 100, 3) if curr and prev and prev != 0 else None

        # 1. 全球宏观 (Sina 全球接口 - 最稳)
        print(" -> 抓取 Sina 全球宏观...")
        sina_map = {
            "CNH": "fx_susdcnh", "Nasdaq": "gb_ixic", "HangSeng": "rt_hkHSI",
            "A50_Futures": "hf_CHA50CFD", "VIX": "gb_$vix", "Gold": "hf_GC", "CrudeOil": "hf_CL"
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
                    if not key: continue
                    if key == "A50_Futures": price = float(data[0])
                    elif key == "CNH": price = float(data[1])
                    elif key == "Nasdaq": price = float(data[1])
                    elif key == "HangSeng": price = float(data[6])
                    elif key == "VIX": price = float(data[1])
                    elif "hf_" in sym: price = float(data[0])
                    macro[key] = wrap_indicator({"price": price})
                except: pass
        except: pass

        # 2. 北向资金 (修复版)
        print(" -> 抓取北向资金...")
        try:
            url_rt = "https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54"
            data_rt = requests.get(url_rt, timeout=5).json().get('data', {})
            total_val = (float(data_rt.get('hk2sh', {}).get('dayNetAmtIn', 0)) + float(data_rt.get('hk2sz', {}).get('dayNetAmtIn', 0))) * 10000
            if total_val == 0:
                url_hist = "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY&columns=ALL&filter=(MUTUAL_TYPE%3D%22001%22)&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=5"
                items = requests.get(url_hist, timeout=5).json().get('result', {}).get('data', [])
                for item in items:
                    if item.get('NET_DEAL_AMT'):
                        total_val = float(item['NET_DEAL_AMT']) * 1e8
                        break
            if total_val: macro['Northbound'] = wrap_indicator({"value": total_val})
        except: pass

        # 3. 行业流入
        print(" -> 抓取行业流入...")
        try:
            url = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10&po=1&np=1&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50&fields=f14,f62"
            r = requests.get(url, timeout=5)
            diff = r.json().get('data', {}).get('diff', [])
            if diff:
                macro['Sector_Flow'] = wrap_indicator({"top_inflow": [{"名称": d['f14'], "今日净额": d['f62']} for d in diff[:3]]})
        except: pass

        # 4. 两融 (AkShare 兜底)
        print(" -> 抓取两融数据...")
        try:
            margin = ak.stock_margin_sh()
            if not margin.empty:
                macro['Margin_Debt'] = wrap_indicator({"value": float(margin.iloc[-1].iloc[-1]), "change_pct": safe_change_pct(margin.iloc[-1].iloc[-1], margin.iloc[-2].iloc[-1])})
        except: pass

        # 5. 国债 10Y
        try:
            df = ak.bond_china_yield(start_date=(datetime.now() - timedelta(days=5)).strftime("%Y%m%d"))
            macro['CN10Y'] = wrap_indicator({"yield": float(df['10年'].iloc[-1])})
            # 美债 10Y 从 Sina 补
            url_us10y = "http://hq.sinajs.cn/list=fx_sus10y"
            r = requests.get(url_us10y, timeout=5)
            if '="' in r.text: macro['US10Y'] = wrap_indicator({"price": float(r.text.split('="')[1].split(',')[1])})
        except: pass

        required = ["CNH", "Nasdaq", "HangSeng", "A50_Futures", "VIX", "US10Y", "Gold", "CrudeOil", "Northbound", "CN10Y", "Margin_Debt", "Sector_Flow"]
        for k in required:
            if k not in macro: macro[k] = wrap_indicator(None)
        return macro

    def _get_hist_context(self):
        context = {}
        for code in self.watchlist:
            try:
                df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=(datetime.now() - timedelta(days=45)).strftime("%Y%m%d"), adjust="qfq")
                if not df.empty:
                    df['unit'] = 'LOT'
                    context[code] = df.to_dict(orient='records')
            except: pass
        return context

if __name__ == "__main__":
    Harvester().harvest_all()
