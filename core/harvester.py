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
    模块 A: 情报获取引擎 - V6 (Unit & Recency Logic)
    确保 100% 覆盖，且所有成交量统一为“股”，且历史数据必须是最近的。
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
        print(f"🚀 [V6] 开始全量高精指标抓取 [{self.timestamp}]...")
        
        raw_data = {
            "meta": {
                "timestamp": self.timestamp, 
                "timezone": "Asia/Shanghai",
                "version": "V13-Cloud-Robust-V6"
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
        """抓取实时行情 - 统一单位为‘股’"""
        print(" -> 正在抓取 A 股实时报价...")
        # 尝试 1: EM
        try:
            df = ak.fund_etf_spot_em()
            if not df.empty:
                res = df[df['代码'].isin(self.watchlist)].to_dict(orient='records')
                if res: 
                    # fund_etf_spot_em 的成交量单位已经是股
                    return res
        except: pass

        # 尝试 2: Sina
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
                            "成交量": float(data[8]), # Sina 也是股
                            "昨收": float(data[2]),
                            "source": "sina"
                        })
            except: pass
        return sina_results

    def _get_macro(self):
        """抓取宏观指标"""
        macro = {}
        # 1. CNH
        try:
            url = "http://hq.sinajs.cn/list=fx_susdcnh"
            r = requests.get(url, headers={'Referer': 'http://finance.sina.com.cn'}, timeout=5)
            if r.status_code == 200 and '="' in r.text:
                data = r.text.split('="')[1].split(',')
                macro['CNH'] = {"price": float(data[1]), "prev_close": float(data[3])}
        except: pass
        
        # 2. SHIBOR
        try:
            shibor = ak.rate_shibor_em()
            if not shibor.empty:
                macro['SHIBOR'] = shibor.iloc[-1].to_dict()
        except: pass
        
        # 3. 北向资金
        try:
            north = ak.stock_hsgt_north_net_flow_em()
            if not north.empty:
                macro['Northbound'] = north.iloc[-1].to_dict()
        except: pass

        # 4. 全球指数
        global_map = {"gb_ixic": "Nasdaq", "rt_hkHSI": "HangSeng", "nf_CHA50CFD": "A50_Futures"}
        for sym, key in global_map.items():
            try:
                url = f"http://hq.sinajs.cn/list={sym}"
                r = requests.get(url, headers={'Referer': 'http://finance.sina.com.cn'}, timeout=5)
                if r.status_code == 200 and '="' in r.text:
                    data = r.text.split('="')[1].split(',')
                    if key == "Nasdaq": macro[key] = {"price": float(data[1])}
                    elif key == "HangSeng": macro[key] = {"price": float(data[6])}
                    elif key == "A50_Futures": macro[key] = {"price": float(data[1])}
            except: pass

        return macro

    def _get_hist_context(self):
        """抓取历史数据 - 核心：强制单位统一为‘股’"""
        print(f" -> 正在建立审计背景 (Watchlist: {len(self.watchlist)} 只)...")
        context = {}
        # 抓取 45 天确保有足够的交易日
        start_date = (datetime.now(self.beijing_tz) - timedelta(days=45)).strftime("%Y%m%d")
        
        for code in self.watchlist:
            hist_df = pd.DataFrame()
            # 尝试 1: EM (单位：手)
            try:
                df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date, adjust="qfq")
                if not df.empty and len(df) >= 5:
                    # 转换单位：手 -> 股
                    df['成交量'] = df['成交量'] * 100
                    hist_df = df
            except: pass
            
            # 尝试 2: Sina (单位：股)
            if hist_df.empty:
                try:
                    symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
                    df = ak.fund_etf_hist_sina(symbol=symbol)
                    if not df.empty:
                        df = df.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
                        # Sina 接口返回的历史数据可能很旧，需过滤
                        df['日期'] = pd.to_datetime(df['日期'])
                        cutoff = datetime.now() - timedelta(days=60)
                        df = df[df['日期'] > cutoff]
                        if not df.empty:
                            hist_df = df
                except: pass
            
            if not hist_df.empty:
                # 统一字段名并保存
                context[code] = hist_df.to_dict(orient='records')
            
            time.sleep(0.2)
            
        return context

if __name__ == "__main__":
    harvester = Harvester()
    harvester.harvest_all()
