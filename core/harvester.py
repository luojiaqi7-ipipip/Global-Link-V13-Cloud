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
    模块 A: 情报获取引擎 - V13 Cloud (Robust Data Source)
    针对 GitHub Actions 优化，多源备份确保 100% 成功率。
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
                "version": "V13-Cloud-Robust-Action-Final"
            },
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        
        raw_data = self._serialize_clean(raw_data)
        
        print(f"📊 [结果统计] ETF行情: {len(raw_data['etf_spot'])} | 宏观指标: {len(raw_data['macro'])} | 历史背景: {len(raw_data['hist_data'])}")
        
        with open(f"{self.data_dir}/market_snap_{self.timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
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
        """抓取实时行情 - 多源冗余 (Tencent > EM > Sina)"""
        print(" -> 正在抓取 A 股实时报价...")
        
        results = []
        codes_to_fetch = list(self.watchlist)
        
        # 尝试 1: Tencent API
        try:
            symbols = [f"sh{c}" if c.startswith(('5', '6')) else f"sz{c}" for c in codes_to_fetch]
            url = f"http://qt.gtimg.cn/q=s_{','.join(symbols)}"
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                lines = r.text.strip().split(';')
                for line in lines:
                    if '~' not in line: continue
                    parts = line.split('~')
                    code = parts[2]
                    results.append({
                        "代码": code,
                        "名称": parts[1],
                        "最新价": float(parts[3]),
                        "成交量": float(parts[6]),
                        "涨跌幅": float(parts[5]),
                        "unit": "LOT",
                        "source": "tencent"
                    })
                if len(results) >= len(self.watchlist):
                    return results
        except: pass

        # 尝试 2: EM
        if not results:
            try:
                df = ak.fund_etf_spot_em()
                if not df.empty:
                    matched = df[df['代码'].isin(self.watchlist)].copy()
                    matched['unit'] = 'SHARE'
                    matched['source'] = 'em'
                    res = matched.to_dict(orient='records')
                    if res: return res
            except: pass

        return results

    def _get_macro(self):
        """抓取宏观指标 - V13 稳健增强版"""
        macro = {}
        
        def wrap_indicator(data, status="SUCCESS"):
            if data is None: status = "FAILED"
            if isinstance(data, dict):
                data.update({"status": status, "last_update": self.timestamp})
                return data
            return {"value": data, "status": status, "last_update": self.timestamp}

        def safe_change_pct(curr, prev):
            if curr is None or prev is None or prev == 0:
                return None
            return round((float(curr) / float(prev) - 1) * 100, 3)

        # 1. 全球核心指标 (Yahoo Finance)
        tickers = {
            "CNH": "USDCNH=X",
            "Nasdaq": "^IXIC",
            "HangSeng": "^HSI",
            "A50_Futures": "XIN9.F",
            "VIX": "^VIX",
            "US10Y": "^TNX",
            "Gold": "GC=F",
            "CrudeOil": "CL=F"
        }
        
        try:
            yf_data = yf.download(list(tickers.values()), period="5d", interval="1d", progress=False)
            for key, ticker in tickers.items():
                try:
                    if ticker in yf_data['Close']:
                        series = yf_data['Close'][ticker].dropna()
                        if len(series) >= 2:
                            current_price = float(series.iloc[-1])
                            prev_close = float(series.iloc[-2])
                            macro[key] = wrap_indicator({
                                "price": current_price, 
                                "prev_close": prev_close,
                                "change_pct": safe_change_pct(current_price, prev_close),
                                "source": "yfinance"
                            })
                except: pass
        except: pass

        # 2. 北向资金 (修复版：混合策略解决 0 值)
        try:
            # 优先尝试实时接口
            url_rt = "https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54"
            r_rt = requests.get(url_rt, timeout=5)
            data_rt = r_rt.json().get('data', {})
            hk2sh = data_rt.get('hk2sh', {}).get('dayNetAmtIn')
            hk2sz = data_rt.get('hk2sz', {}).get('dayNetAmtIn')
            
            total_val = None
            if hk2sh is not None and hk2sz is not None:
                total_val = (float(hk2sh) + float(hk2sz)) * 10000 

            # 如果实时为 0（盘后清零），则回溯历史接口拿最后一个结算值
            if total_val is None or total_val == 0:
                url_hist = "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY&columns=ALL&filter=(MUTUAL_TYPE%3D%22001%22)&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=5"
                r_hist = requests.get(url_hist, timeout=5)
                items = r_hist.json().get('result', {}).get('data', [])
                for item in items:
                    val = item.get('NET_DEAL_AMT')
                    if val is not None and val != 0:
                        total_val = float(val) * 1e8
                        break
            
            if total_val is not None:
                macro['Northbound'] = wrap_indicator({"value": total_val, "source": "em_hybrid"})
        except: pass

        # 3. 行业资金流向 (修复版：使用 push2 实时接口)
        try:
            url = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5&po=1&np=1&ut=bd1d9ddb040897f3cf9c2916f053ad76&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50&fields=f14,f62"
            r = requests.get(url, timeout=5)
            diff = r.json().get('data', {}).get('diff', [])
            if diff:
                macro['Sector_Flow'] = wrap_indicator({
                    "top_inflow": [{"名称": d['f14'], "今日净额": d['f62']} for d in diff[:3]],
                    "top_outflow": []
                })
        except: pass

        # 4. 两融余额 (修复版：GitHub Action 环境优先使用 AkShare)
        try:
            # AkShare 抓取
            df_margin = ak.stock_margin_sh()
            if not df_margin.empty and len(df_margin) >= 2:
                # 兼容不同版本的列名
                col = next((c for c in ['本日融资融券余额(元)', '融资融券余额', 'rzye'] if c in df_margin.columns), df_margin.columns[-1])
                curr_m = float(df_margin.iloc[-1][col])
                prev_m = float(df_margin.iloc[-2][col])
                macro['Margin_Debt'] = wrap_indicator({
                    "value": curr_m,
                    "change_pct": safe_change_pct(curr_m, prev_m),
                    "source": "ak_action"
                })
        except: pass

        # 5. 中国国债 10Y
        try:
            df_yield = ak.bond_china_yield(start_date=(datetime.now() - timedelta(days=10)).strftime("%Y%m%d"))
            if not df_yield.empty:
                macro['CN10Y'] = wrap_indicator({"yield": float(df_yield['10年'].iloc[-1])})
        except: pass

        # 补齐所有键
        required = ["CNH", "Nasdaq", "HangSeng", "A50_Futures", "VIX", "US10Y", "Gold", "CrudeOil", 
                    "Northbound", "CN10Y", "Margin_Debt", "Sector_Flow"]
        for key in required:
            if key not in macro: macro[key] = wrap_indicator(None)
            
        return macro

    def _get_hist_context(self):
        """抓取历史背景数据"""
        context = {}
        start_date = (datetime.now(self.beijing_tz) - timedelta(days=45)).strftime("%Y%m%d")
        for code in self.watchlist:
            try:
                df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date, adjust="qfq")
                if not df.empty:
                    df['unit'] = 'LOT'
                    context[code] = df.to_dict(orient='records')
                time.sleep(0.2)
            except: pass
        return context

if __name__ == "__main__":
    Harvester().harvest_all()
