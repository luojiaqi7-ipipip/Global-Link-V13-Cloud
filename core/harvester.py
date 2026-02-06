import akshare as ak
import json
import os
from datetime import datetime, timedelta, date
import pytz
import time
import pandas as pd
import requests

class Harvester:
    """
    模块 A: 情报获取引擎 - V4 (Indicator Expansion)
    确保 100% 覆盖各类价格、指标，具备极强的容错与备选源切换能力。
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
        print(f"🚀 [V4] 开始全量指标抓取 [{self.timestamp}]...")
        
        raw_data = {
            "meta": {
                "timestamp": self.timestamp, 
                "timezone": "Asia/Shanghai",
                "version": "V13-Cloud-Robust-V4"
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
            
        print(f"✅ 数据抓取阶段任务完成")
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
        """抓取实时行情 - 核心标的 100% 捕获"""
        print(" -> 正在抓取 ETF 实时报价...")
        try:
            df = ak.fund_etf_spot_em()
            if not df.empty:
                res = df[df['代码'].isin(self.watchlist)].to_dict(orient='records')
                if res: return res
        except: pass

        # 新浪备份流 (更稳)
        sina_results = []
        for code in self.watchlist:
            try:
                symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
                url = f"http://hq.sinajs.cn/list={symbol}"
                r = requests.get(url, headers={'Referer': 'http://finance.sina.com.cn'}, timeout=5)
                if r.status_code == 200 and '="' in r.text:
                    data = r.text.split('="')[1].split(',')
                    if len(data) > 1:
                        sina_results.append({
                            "代码": code,
                            "名称": data[0],
                            "最新价": float(data[3]),
                            "成交量": float(data[8]),
                            "昨收": float(data[2])
                        })
            except: pass
        return sina_results

    def _get_macro(self):
        """宏观核心矩阵：汇率、流动性、外资、全球指数、美债"""
        print(" -> 正在抓取宏观多维矩阵...")
        macro = {}
        
        # 1. 离岸人民币 (CNH)
        try:
            # Sina 极速源
            url = "http://hq.sinajs.cn/list=fx_susdcnh"
            r = requests.get(url, headers={'Referer': 'http://finance.sina.com.cn'}, timeout=5)
            if r.status_code == 200 and '="' in r.text:
                data = r.text.split('="')[1].split(',')
                macro['CNH'] = {"price": float(data[1]), "prev_close": float(data[3]), "source": "sina"}
        except: pass
        
        # 2. SHIBOR (中国市场流动性)
        for _ in range(3): # 增加重试
            try:
                shibor = ak.rate_shibor_em()
                if not shibor.empty:
                    macro['SHIBOR'] = shibor.iloc[-1].to_dict()
                    break
                time.sleep(1)
            except: pass
        
        # 3. 北向资金 (外资/国家队动向)
        for _ in range(3):
            try:
                north = ak.stock_hsgt_north_net_flow_em()
                if not north.empty:
                    macro['Northbound'] = north.iloc[-1].to_dict()
                    break
                time.sleep(1)
            except: pass

        # 4. 全球指数 (纳指、恒指、富时A50)
        global_map = {".IXIC": "Nasdaq", "HSI": "HangSeng", "SIN0": "A50_Futures"}
        for sym, key in global_map.items():
            try:
                # 尝试 Sina 实时接口
                url = f"http://hq.sinajs.cn/list=gb_{sym.lower().replace('.','')}" if sym.startswith('.') else f"http://hq.sinajs.cn/list={sym}"
                if sym == ".IXIC": url = "http://hq.sinajs.cn/list=gb_ixic"
                
                r = requests.get(url, headers={'Referer': 'http://finance.sina.com.cn'}, timeout=5)
                if r.status_code == 200 and '="' in r.text:
                    data = r.text.split('="')[1].split(',')
                    macro[key] = {"price": float(data[1]) if len(data)>1 else 0}
                
                # 若 Sina 失败，尝试 akshare 历史补登
                if key not in macro or macro[key]['price'] == 0:
                    if sym == ".IXIC":
                        df = ak.index_us_stock_sina(symbol=".IXIC")
                        if not df.empty: macro[key] = {"price": float(df.iloc[-1]['close'])}
            except: pass

        # 5. 美债收益率 (US 10Y)
        try:
            url = "http://hq.sinajs.cn/list=gb_znb_10y"
            r = requests.get(url, headers={'Referer': 'http://finance.sina.com.cn'}, timeout=5)
            if r.status_code == 200 and '="' in r.text:
                data = r.text.split('="')[1].split(',')
                macro['US_10Y_Yield'] = {"price": float(data[1]) if len(data)>1 else 0}
        except: pass

        return macro

    def _get_hist_context(self):
        """抓取历史数据用于 Bias 计算 - 增加 Sina 强制备份"""
        print(f" -> 正在建立审计背景 (Watchlist: {len(self.watchlist)} 只)...")
        context = {}
        start_date = (datetime.now(self.beijing_tz) - timedelta(days=45)).strftime("%Y%m%d")
        
        for code in self.watchlist:
            # 优先 EM 
            try:
                hist = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date, adjust="qfq")
                if not hist.empty and len(hist) >= 5:
                    context[code] = hist.to_dict(orient='records')
                    continue
            except: pass
            
            # Sina 历史源备份
            try:
                symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
                hist = ak.fund_etf_hist_sina(symbol=symbol)
                if not hist.empty:
                    hist = hist.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
                    context[code] = hist.to_dict(orient='records')
            except: pass
            time.sleep(0.2)
            
        return context

if __name__ == "__main__":
    harvester = Harvester()
    harvester.harvest_all()
