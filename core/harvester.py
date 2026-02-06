import akshare as ak
import json
import os
from datetime import datetime
import pytz
import time

class Harvester:
    """
    模块 A: 情报获取引擎
    负责 100% 真实的原始数据抓取，不进行任何复杂计算。
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
        
        # 同时更新一个 latest 指向，方便模块 B 读取
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
        print(f"原始数据已持久化至: {file_path}")
        return raw_data

    def _get_spot(self):
        """抓取实时行情快照"""
        try:
            df = ak.stock_zh_index_spot_em()
            return df[df['代码'].isin(self.watchlist)].to_dict(orient='records')
        except Exception as e:
            print(f"Spot 抓取失败: {e}")
            return []

    def _get_macro(self):
        """抓取宏观全景指标"""
        macro = {}
        # 1. 离岸人民币
        try:
            fx = ak.fx_spot_quote()
            cnh = fx[fx['【名称】'].str.contains('美元/人民币', na=False)].iloc[0]
            macro['CNH'] = cnh.to_dict()
        except: pass
        # 2. 市场流动性
        try:
            shibor = ak.rate_shibor_em()
            macro['SHIBOR'] = shibor.iloc[-1].to_dict()
        except: pass
        # 3. 资金流向
        try:
            north = ak.stock_hsgt_north_net_flow_em()
            macro['Northbound'] = north.iloc[-1].to_dict()
        except: pass
        return macro

    def _get_hist_context(self):
        """抓取有限的历史背景数据用于指标计算"""
        context = {}
        for code in self.watchlist[:5]: # 优先抓取 Top 候选的历史
            try:
                hist = ak.fund_etf_hist_em(symbol=code, period="daily", 
                                          start_date=(datetime.now() - pd.Timedelta(days=15)).strftime("%Y%m%d"))
                context[code] = hist.to_dict(orient='records')
            except: pass
        return context

if __name__ == "__main__":
    import pandas as pd # 临时引用
    harvester = Harvester()
    harvester.harvest_all()
