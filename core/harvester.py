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
                "version": "V13-Cloud-Robust-V2"
            },
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        
        # 处理非 JSON 序列化对象
        raw_data = self._serialize_clean(raw_data)
        
        # 统计抓取情况
        print(f"📊 抓取统计: ETF行情={len(raw_data['etf_spot'])}, 宏观指标={len(raw_data['macro'])}, 历史背景={len(raw_data['hist_data'])}")
        
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
        """抓取实时行情快照 - 采用多源备份"""
        print(" -> 正在抓取 A 股 ETF 实时行情...")
        
        # 尝试 1: 东财全量接口 (EM) - 增加 User-Agent 伪装
        try:
            df = ak.fund_etf_spot_em()
            if not df.empty:
                res = df[df['代码'].isin(self.watchlist)].to_dict(orient='records')
                if res:
                    print("    [✓] 东财 ETF 接口抓取成功")
                    return res
        except Exception as e:
            print(f"    [!] 东财 ETF 接口异常: {e}")

        # 尝试 2: 新浪接口 (Sina) - 最后的稳定屏障
        print("    [!] 尝试切换至新浪备份源...")
        sina_results = []
        for code in self.watchlist:
            try:
                symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
                # 使用 sina 实时行情
                url = f"http://hq.sinajs.cn/list={symbol}"
                # 注意：新浪接口现在可能需要 Referer
                headers = {'Referer': 'http://finance.sina.com.cn'}
                r = requests.get(url, headers=headers, timeout=5)
                if r.status_code == 200 and '="' in r.text:
                    data = r.text.split('="')[1].split(',')
                    if len(data) > 1:
                        sina_results.append({
                            "代码": code,
                            "名称": data[0],
                            "最新价": float(data[3]),
                            "成交量": float(data[8]),
                            "成交额": float(data[9]),
                            "昨收": float(data[2]),
                            "涨跌幅": round((float(data[3])/float(data[2]) - 1)*100, 2) if float(data[2]) != 0 else 0
                        })
                time.sleep(0.1)
            except: pass
        
        if sina_results:
            print(f"    [✓] 新浪备份源抓取成功 ({len(sina_results)} 只)")
            return sina_results

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
                    print("    [✓] CNH 汇率抓取成功")
        except: pass
        
        # 2. SHIBOR
        try:
            shibor = ak.rate_shibor_em()
            if not shibor.empty:
                macro['SHIBOR'] = shibor.iloc[-1].to_dict()
                print("    [✓] SHIBOR 利率抓取成功")
        except: pass
        
        # 3. 北向资金
        try:
            north = ak.stock_hsgt_north_net_flow_em()
            if not north.empty:
                macro['Northbound'] = north.iloc[-1].to_dict()
                print("    [✓] 北向资金抓取成功")
        except: pass

        # 4. 纳指 (隔夜)
        try:
            nasdaq = ak.index_us_stock_sina(symbol=".IXIC")
            if not nasdaq.empty:
                macro['Nasdaq'] = nasdaq.iloc[-1].to_dict()
                print("    [✓] 纳指数据抓取成功")
        except: pass

        return macro

    def _get_hist_context(self):
        """抓取背景数据用于计算 MA5 Bias"""
        print(f" -> 正在抓取 {len(self.watchlist)} 只标的的审计背景数据...")
        context = {}
        success_count = 0
        for code in self.watchlist:
            # 增加重试和间隔
            for _ in range(3):
                try:
                    start_date = (datetime.now(self.beijing_tz) - timedelta(days=40)).strftime("%Y%m%d")
                    hist = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date, adjust="qfq")
                    if not hist.empty:
                        context[code] = hist.to_dict(orient='records')
                        success_count += 1
                        break
                except:
                    time.sleep(1)
            time.sleep(0.3) 
        print(f"    [✓] 历史背景数据抓取完成 ({success_count}/{len(self.watchlist)})")
        return context

if __name__ == "__main__":
    harvester = Harvester()
    harvester.harvest_all()
