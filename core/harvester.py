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
    模块 A: 情报获取引擎 - V3 (Ultra-Robust)
    确保在 GitHub Action 的恶劣网络/封锁环境下 100% 捕获数据。
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
        print(f"🚀 [V3] 开始高可用原始情报抓取 [{self.timestamp}]...")
        
        raw_data = {
            "meta": {
                "timestamp": self.timestamp, 
                "timezone": "Asia/Shanghai",
                "version": "V13-Cloud-Robust-V3"
            },
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        
        raw_data = self._serialize_clean(raw_data)
        
        print(f"📊 [结果统计] ETF行情: {len(raw_data['etf_spot'])} | 宏观指标: {len(raw_data['macro'])} | 历史背景: {len(raw_data['hist_data'])}")
        
        # 保存
        with open(f"{self.data_dir}/market_snap_{self.timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
        print(f"✅ 任务闭环完成")
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
        """抓取实时行情 - 优先东财，死磕新浪"""
        print(" -> 正在抓取实时行情矩阵...")
        try:
            df = ak.fund_etf_spot_em()
            if not df.empty:
                res = df[df['代码'].isin(self.watchlist)].to_dict(orient='records')
                if res:
                    print("    [✓] 东财主源连接成功")
                    return res
        except: pass

        print("    [!] 东财主源连接超时，切换新浪实时流...")
        sina_results = []
        for code in self.watchlist:
            try:
                symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
                url = f"http://hq.sinajs.cn/list={symbol}"
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
                            "昨收": float(data[2])
                        })
            except: pass
        if sina_results: print(f"    [✓] 新浪备份流捕获完成 ({len(sina_results)} 只)")
        return sina_results

    def _get_macro(self):
        """抓取全量宏观 - 极度增强版"""
        print(" -> 正在探测宏观核心脉搏...")
        macro = {}
        
        # 1. 离岸人民币 (CNH) - 尝试多源
        try:
            # Sina 汇率源
            url = "http://hq.sinajs.cn/list=fx_susdcnh"
            r = requests.get(url, headers={'Referer': 'http://finance.sina.com.cn'}, timeout=5)
            if r.status_code == 200 and '="' in r.text:
                data = r.text.split('="')[1].split(',')
                macro['CNH'] = {"【最新价】": data[1], "【涨跌幅】": "N/A", "source": "sina"}
                print("    [✓] CNH (Sina) 探测成功")
        except: pass
        
        if 'CNH' not in macro:
            try:
                fx = ak.fx_spot_quote()
                match = fx[fx['【名称】'].str.contains('美元/人民币', na=False)]
                if not match.empty:
                    macro['CNH'] = match.iloc[0].to_dict()
                    print("    [✓] CNH (EM) 抓取成功")
            except: pass
        
        # 2. SHIBOR
        try:
            shibor = ak.rate_shibor_em()
            if not shibor.empty:
                macro['SHIBOR'] = shibor.iloc[-1].to_dict()
                print("    [✓] SHIBOR 利率抓取成功")
        except: pass
        
        # 3. 资金流 (北向)
        try:
            north = ak.stock_hsgt_north_net_flow_em()
            if not north.empty:
                macro['Northbound'] = north.iloc[-1].to_dict()
                print("    [✓] 北向资金抓取成功")
        except: pass

        # 4. 纳斯达克
        try:
            nasdaq = ak.index_us_stock_sina(symbol=".IXIC")
            if not nasdaq.empty:
                macro['Nasdaq'] = nasdaq.iloc[-1].to_dict()
                print("    [✓] 纳指数据抓取成功")
        except: pass

        return macro

    def _get_hist_context(self):
        """抓取历史数据 - 增加 Sina 历史源备份"""
        print(f" -> 正在建立审计背景 (Watchlist: {len(self.watchlist)} 只)...")
        context = {}
        start_date = (datetime.now(self.beijing_tz) - timedelta(days=45)).strftime("%Y%m%d")
        
        for code in self.watchlist:
            # 尝试 1: EM 历史接口
            try:
                hist = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date, adjust="qfq")
                if not hist.empty and len(hist) >= 5:
                    context[code] = hist.to_dict(orient='records')
                    continue
            except: pass
            
            # 尝试 2: Sina 历史接口
            try:
                symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
                hist = ak.fund_etf_hist_sina(symbol=symbol)
                if not hist.empty:
                    # 转换列名以适配 QuantLab
                    hist = hist.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
                    context[code] = hist.to_dict(orient='records')
            except: pass
            
            time.sleep(0.3)
            
        success_rate = len(context) / len(self.watchlist)
        print(f"    [✓] 审计背景建立完成 (捕获率: {success_rate:.0%})")
        return context

if __name__ == "__main__":
    harvester = Harvester()
    harvester.harvest_all()
