import akshare as ak
import json
import os
from datetime import datetime
import pytz
import time
import pandas as pd

class Harvester:
    """
    模块 A: 情报获取引擎
    负责 100% 真实的原始数据抓取。
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
        print(f"开始抓取原始情报 [{self.timestamp}]...")
        raw_data = {
            "meta": {"timestamp": self.timestamp, "timezone": "Asia/Shanghai"},
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        
        file_path = f"{self.data_dir}/market_snap_{self.timestamp}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
        print(f"原始数据已持久化至: {file_path}")
        return raw_data

    def _get_spot(self):
        """抓取实时行情快照"""
        try:
            # 增加重试
            for _ in range(3):
                df = ak.stock_zh_index_spot_em()
                if not df.empty:
                    return df[df['代码'].isin(self.watchlist)].to_dict(orient='records')
                time.sleep(2)
            return []
        except Exception as e:
            print(f"Spot 抓取异常: {e}")
            return []

    def _get_macro(self):
        """抓取宏观指标"""
        macro = {}
        # 1. 离岸人民币 (CNH)
        try:
            fx = ak.fx_spot_quote()
            if not fx.empty:
                match = fx[fx['【名称】'].str.contains('美元/人民币', na=False)]
                if not match.empty:
                    macro['CNH'] = match.iloc[0].to_dict()
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

        # 4. 纳指 (隔夜)
        try:
            nasdaq = ak.index_investing_global(country="美国", index_name="纳斯达克综合指数", period="每日", start_date="20250101")
            if not nasdaq.empty:
                macro['Nasdaq'] = nasdaq.iloc[-1].to_dict()
        except: pass

        return macro

    def _get_hist_context(self):
        """抓取背景数据用于计算 MA5 Bias"""
        context = {}
        # 为了稳定，只抓取 Watchlist 的数据
        for code in self.watchlist:
            try:
                hist = ak.fund_etf_hist_em(symbol=code, period="daily", 
                                          start_date=(datetime.now(self.beijing_tz) - timedelta(days=20)).strftime("%Y%m%d"))
                if not hist.empty:
                    context[code] = hist.to_dict(orient='records')
                time.sleep(0.5) # 避开流控
            except: pass
        return context

if __name__ == "__main__":
    from datetime import timedelta
    harvester = Harvester()
    harvester.harvest_all()
