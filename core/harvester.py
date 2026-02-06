import akshare as ak
import json
import os
from datetime import datetime, timedelta
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
        # 扩展监测池
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
                "version": "V13-Cloud-Robust"
            },
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        
        # 数据完整性校验
        if not raw_data["etf_spot"]:
            print("⚠️ 警告: ETF 实时行情抓取为空")
        
        file_path = f"{self.data_dir}/market_snap_{self.timestamp}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        
        # 符号链接/快捷访问
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
        print(f"✅ 原始数据已持久化至: {file_path}")
        return raw_data

    def _get_spot(self):
        """抓取实时行情快照 - 优化接口稳定性"""
        print(" -> 正在抓取 A 股 ETF 实时行情...")
        try:
            # 优先使用 fund_etf_spot_em，这是专门针对 ETF 的接口，更稳定
            for i in range(3):
                try:
                    df = ak.fund_etf_spot_em()
                    if not df.empty:
                        # 转换列名以适配后续 QuantLab (AkShare 接口列名可能变动)
                        # 标准化处理
                        filtered = df[df['代码'].isin(self.watchlist)]
                        return filtered.to_dict(orient='records')
                except Exception as e:
                    print(f"    [尝试 {i+1}] 接口报错: {e}")
                    time.sleep(2)
            return []
        except Exception as e:
            print(f"❌ Spot 抓取异常: {e}")
            return []

    def _get_macro(self):
        """全量宏观指标抓取：汇率、利率、资金流、全球指数"""
        print(" -> 正在抓取全球宏观矩阵...")
        macro = {}
        
        # 1. 离岸人民币 (CNH) - 多接口备份
        try:
            fx = ak.fx_spot_quote()
            if not fx.empty:
                match = fx[fx['【名称】'].str.contains('美元/人民币', na=False)]
                if not match.empty:
                    macro['CNH'] = match.iloc[0].to_dict()
        except: pass
        
        # 2. SHIBOR (流动性价格)
        try:
            shibor = ak.rate_shibor_em()
            if not shibor.empty:
                macro['SHIBOR'] = shibor.iloc[-1].to_dict()
        except: pass
        
        # 3. 北向资金 (国家队/外资风向标)
        try:
            north = ak.stock_hsgt_north_net_flow_em()
            if not north.empty:
                macro['Northbound'] = north.iloc[-1].to_dict()
        except: pass

        # 4. 美股核心指数 (实时/隔夜) - 使用更稳定的 Sina 接口
        try:
            us_index = ak.index_us_stock_sina(symbol=".IXIC") # 纳斯达克
            if not us_index.empty:
                macro['Nasdaq'] = us_index.iloc[-1].to_dict()
        except: 
            # 备选接口
            try:
                us_spot = ak.stock_us_spot_em()
                if not us_spot.empty:
                    macro['Nasdaq_EM'] = us_spot[us_spot['名称'].str.contains('纳斯达克', na=False)].iloc[0].to_dict()
            except: pass

        # 5. 国债收益率 (10年期)
        try:
            bond = ak.bond_china_yield(start_date=datetime.now().strftime("%Y%m%d"))
            if not bond.empty:
                macro['China_10Y_Bond'] = bond.iloc[-1].to_dict()
        except: pass

        return macro

    def _get_hist_context(self):
        """抓取背景数据用于计算 MA5 Bias - 增加容错"""
        print(f" -> 正在抓取 {len(self.watchlist)} 只标的的审计背景数据...")
        context = {}
        for code in self.watchlist:
            try:
                # 抓取最近 30 天数据确保 MA5 计算准确
                start_date = (datetime.now(self.beijing_tz) - timedelta(days=30)).strftime("%Y%m%d")
                hist = ak.fund_etf_hist_em(symbol=code, period="daily", 
                                          start_date=start_date, adjust="qfq")
                if not hist.empty:
                    context[code] = hist.to_dict(orient='records')
                # 适当延时防止被封
                time.sleep(0.3)
            except Exception as e:
                print(f"    [!] 抓取历史数据失败 {code}: {e}")
        return context

if __name__ == "__main__":
    harvester = Harvester()
    harvester.harvest_all()
