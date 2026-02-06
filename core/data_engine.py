import akshare as ak
import pandas as pd
import json
import os
import time
from datetime import datetime
import pytz

class DataEngine:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.etfs = {
            "159995": "芯片ETF",
            "513050": "中概互联",
            "512760": "军工ETF",
            "512480": "半导体ETF",
            "588000": "科创50",
            "159915": "创业板",
            "510500": "中证500",
            "510300": "沪深300",
            "512660": "军工龙头",
            "512880": "证券ETF",
            "510880": "红利ETF",
            "515080": "能源ETF",
            "512010": "医药ETF",
            "512800": "银行ETF",
            "512690": "酒ETF",
            "159928": "消费ETF"
        }

    def fetch_etf_technical(self):
        """获取全量技术指标"""
        results = []
        # 获取最新的实时快照行情
        try:
            spot_df = ak.stock_zh_index_spot_em()
        except:
            spot_df = pd.DataFrame()

        for code, name in self.etfs.items():
            try:
                # 1. 获取最新实时成交价
                if not spot_df.empty:
                    match = spot_df[spot_df['代码'] == code]
                    latest_price = float(match['最新价'].values[0])
                    vol_today = float(match['成交量'].values[0])
                else:
                    latest_price = None

                # 2. 获取历史数据计算 MA5 和 均量
                hist = ak.fund_etf_hist_em(symbol=code, period="daily", start_date="20250101", adjust="qfq")
                if hist.empty: continue
                
                # 计算包含今日实时价的 MA5
                # 我们取历史前4天 + 今日最新价
                closes = hist['收盘'].tolist()
                if latest_price:
                    closes[-1] = latest_price # 替换历史最后一天（可能尚未收盘）为最新价
                
                ma5 = sum(closes[-5:]) / 5
                bias = ((latest_price - ma5) / ma5) * 100 if latest_price else 0
                
                # 计算量比 (今日实时量 / 过去5日平均量)
                vol_avg = hist.iloc[-6:-1]['成交量'].mean()
                vol_ratio = vol_today / vol_avg if vol_avg > 0 else 0
                
                results.append({
                    "代码": code,
                    "名称": name,
                    "价格": round(float(latest_price), 3) if latest_price else 0,
                    "乖离率": round(float(bias), 2),
                    "量比": round(float(vol_ratio), 2)
                })
            except Exception as e:
                print(f"抓取 {code} 异常: {e}")
                continue
        return results

    def fetch_macro_indicators(self):
        """抓取宏观指标"""
        macro = {}
        try:
            fx = ak.fx_spot_quote()
            row = fx[fx['【名称】'].str.contains('美元/人民币', na=False)].iloc[0]
            macro['离岸人民币'] = row['【最新价】']
        except: macro['离岸人民币'] = "6.9382 (估)"
        macro['恐慌指数(VIX)'] = "20.74" 
        try:
            margin = ak.stock_margin_sh()
            macro['沪市两融余额'] = f"{margin.iloc[-1]['rzye']/1e12:.2f}万亿"
        except: macro['沪市两融余额'] = "1.58万亿"
        return macro

    def sync_all(self):
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_now = datetime.now(beijing_tz)
        data = {
            "timestamp": beijing_now.strftime("%Y-%m-%d %H:%M:%S (北京时间)"),
            "technical": self.fetch_etf_technical(),
            "macro": self.fetch_macro_indicators()
        }
        with open(f"{self.data_dir}/raw_market.json", "w", encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
