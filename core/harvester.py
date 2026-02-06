import akshare as ak
import json
import os
from datetime import datetime, timedelta, date
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
        print(f"🚀 开始抓取全量原始情报 [{self.timestamp}]...")
        
        raw_data = {
            "meta": {
                "timestamp": self.timestamp, 
                "timezone": "Asia/Shanghai",
                "version": "V13-Cloud-Ultra-Robust"
            },
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        
        # 处理非 JSON 序列化对象 (处理 TypeError: Object of type date is not JSON serializable)
        raw_data = self._serialize_clean(raw_data)
        
        if not raw_data["etf_spot"]:
            print("⚠️ 警告: ETF 实时行情抓取为空")
        
        file_path = f"{self.data_dir}/market_snap_{self.timestamp}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
        print(f"✅ 原始数据已持久化至: {file_path}")
        return raw_data

    def _serialize_clean(self, obj):
        """递归清理对象中的非序列化项"""
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
        """抓取实时行情快照 - 采用备选方案应对 EM 封锁"""
        print(" -> 正在抓取 A 股 ETF 实时行情...")
        # 尝试 1: ETF 专用接口 (EM)
        try:
            df = ak.fund_etf_spot_em()
            if not df.empty:
                return df[df['代码'].isin(self.watchlist)].to_dict(orient='records')
        except Exception as e:
            print(f"    [!] EM ETF 接口失效: {e}")

        # 尝试 2: 指数快照接口 (EM)
        try:
            df = ak.stock_zh_index_spot_em()
            if not df.empty:
                return df[df['代码'].isin(self.watchlist)].to_dict(orient='records')
        except Exception as e:
            print(f"    [!] EM Index 接口失效: {e}")

        # 尝试 3: 新浪接口 (Sina) - 最后的防线
        try:
            # 新浪接口需要循环或一次性拉取，此处尝试拉取主流指数作为补偿
            # 对于 watchlist，尝试逐个获取最新的 sina 价格 (虽然慢点但稳)
            sina_results = []
            for code in self.watchlist:
                try:
                    # 简化逻辑，仅抓取关键字段
                    symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
                    df = ak.fund_etf_category_sina(symbol=symbol)
                    if not df.empty:
                        latest = df.iloc[-1]
                        sina_results.append({
                            "代码": code,
                            "名称": "Sina_Backup",
                            "最新价": latest['close'],
                            "成交量": latest['volume']
                        })
                    time.sleep(0.2)
                except: pass
            if sina_results: return sina_results
        except: pass

        return []

    def _get_macro(self):
        """抓取宏观指标"""
        print(" -> 正在抓取全球宏观矩阵...")
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

        # 4. 纳指 (隔夜) - 使用更稳的新浪美股接口
        try:
            nasdaq = ak.index_us_stock_sina(symbol=".IXIC")
            if not nasdaq.empty:
                macro['Nasdaq'] = nasdaq.iloc[-1].to_dict()
        except: pass

        return macro

    def _get_hist_context(self):
        """抓取背景数据用于计算 MA5 Bias"""
        print(f" -> 正在抓取 {len(self.watchlist)} 只标的的审计背景数据...")
        context = {}
        for code in self.watchlist:
            # 增加重试和间隔
            for _ in range(2):
                try:
                    start_date = (datetime.now(self.beijing_tz) - timedelta(days=30)).strftime("%Y%m%d")
                    hist = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date, adjust="qfq")
                    if not hist.empty:
                        context[code] = hist.to_dict(orient='records')
                        break
                except:
                    time.sleep(1)
            time.sleep(0.5) 
        return context

if __name__ == "__main__":
    harvester = Harvester()
    harvester.harvest_all()
