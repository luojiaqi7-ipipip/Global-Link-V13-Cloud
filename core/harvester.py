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
        except: pass
        return []

    def _get_macro(self):
        macro = {}
        def wrap(data): return {**(data if isinstance(data, dict) else {"value": data}), "status": "SUCCESS" if data is not None else "FAILED", "last_update": self.timestamp}

        # 1. 全球宏观 (Sina Global)
        sina_map = {"CNH": "fx_susdcnh", "Nasdaq": "gb_ixic", "HangSeng": "rt_hkHSI", "A50_Futures": "hf_CHA50CFD", "Gold": "hf_GC", "CrudeOil": "hf_CL"}
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
                    price = float(data[0]) if "hf_" in sym else float(data[1]) if key != "HangSeng" else float(data[6])
                    macro[key] = wrap({"price": price})
                except: pass
        except: pass

        # 2. VIX & US10Y (Yahoo Finance)
        try:
            for k, t in {"VIX": "^VIX", "US10Y": "^TNX"}.items():
                val = yf.Ticker(t).history(period="1d")['Close'].iloc[-1]
                macro[k] = wrap({"price": float(val)})
        except: pass

        # 3. 北向资金 (由于新规，每日净流入已停止实时披露，改用成交总额作为活跃度指标)
        try:
            r = requests.get("https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54", timeout=5).json().get('data', {})
            # 这里的 dayNetAmtIn 在盘后通常为 0，我们优先取历史结算终值
            url_hist = "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY&columns=ALL&filter=(MUTUAL_TYPE%3D%22005%22)&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=5"
            hist = requests.get(url_hist, timeout=5).json().get('result', {}).get('data', [])
            val = None
            for i in hist:
                if i.get('NET_DEAL_AMT') and i.get('NET_DEAL_AMT') != 0:
                    val = float(i['NET_DEAL_AMT']) * 1e8
                    break
            if val is None: # 如果历史也没数，取成交额作为替代参考
                val = (float(r.get('hk2sh', {}).get('dayAmtRemain', 0)) + float(r.get('hk2sz', {}).get('dayAmtRemain', 0))) # 这不是净额，仅作占位
            macro['Northbound'] = wrap({"value": val})
        except: pass

        # 4. 行业流入 (东财 Push2)
        try:
            r = requests.get("https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10&po=1&np=1&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50&fields=f14,f62", timeout=5).json()
            diff = r.get('data', {}).get('diff', [])
            if diff: macro['Sector_Flow'] = wrap({"top_inflow": [{"名称": d['f14'], "今日净额": d['f62']} for d in diff[:3]]})
        except: pass

        # 5. 两融 & 国债 (AkShare)
        try:
            m = ak.stock_margin_sh()
            if not m.empty: macro['Margin_Debt'] = wrap({"value": float(m.iloc[-1].iloc[-1]), "change_pct": round((m.iloc[-1].iloc[-1]/m.iloc[-2].iloc[-1]-1)*100, 3)})
        except: pass
        try:
            y = ak.bond_china_yield(start_date=(datetime.now()-timedelta(days=10)).strftime("%Y%m%d"))
            if not y.empty: macro['CN10Y'] = wrap({"yield": float(y['10年'].iloc[-1])})
        except: pass

        return macro

    def _get_hist_context(self):
        ctx = {}
        for c in self.watchlist:
            try:
                df = ak.fund_etf_hist_em(symbol=c, period="daily", start_date=(datetime.now()-timedelta(days=45)).strftime("%Y%m%d"), adjust="qfq")
                if not df.empty:
                    df['unit'] = 'LOT'
                    ctx[c] = df.to_dict(orient='records')
            except: pass
        return ctx

if __name__ == "__main__":
    Harvester().harvest_all()
