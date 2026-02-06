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
        try:
            spot_df = ak.stock_zh_index_spot_em()
        except:
            spot_df = pd.DataFrame()

        for code, name in self.etfs.items():
            try:
                if spot_df.empty: continue
                match = spot_df[spot_df['代码'] == code]
                if match.empty: continue
                
                latest_price = float(match['最新价'].values[0])
                vol_today = float(match['成交量'].values[0])

                hist = ak.fund_etf_hist_em(symbol=code, period="daily", start_date="20250101", adjust="qfq")
                if hist.empty: continue
                
                closes = hist['收盘'].tolist()
                closes[-1] = latest_price
                ma5 = sum(closes[-5:]) / 5
                bias = ((latest_price - ma5) / ma5) * 100
                
                vol_avg = hist.iloc[-6:-1]['成交量'].mean()
                vol_ratio = vol_today / vol_avg if vol_avg > 0 else 0
                
                results.append({
                    "代码": code,
                    "名称": name,
                    "价格": round(latest_price, 3),
                    "乖离率": round(float(bias), 2),
                    "量比": round(float(vol_ratio), 2)
                })
            except: continue
        return results

    def fetch_macro_indicators(self):
        """抓取真实宏观指标 - 严禁硬编码"""
        macro = {}
        # 1. 离岸人民币 (实时)
        try:
            fx = ak.fx_spot_quote()
            row = fx[fx['【名称】'].str.contains('美元/人民币', na=False)].iloc[0]
            macro['离岸人民币'] = row['【最新价】']
        except: macro['离岸人民币'] = "获取失败"

        # 2. A股实际波动率 (用沪深300近5日振幅均值代替 VIX)
        try:
            hs300 = ak.stock_zh_index_daily_em(symbol="sh000300")
            recent = hs300.tail(5)
            # 振幅 = (最高 - 最低) / 昨收
            volatility = ((recent['最高'] - recent['最低']) / recent['收盘'].shift(1)).mean() * 100
            macro['A股实际波动率'] = f"{round(volatility, 2)}%"
        except: macro['A股实际波动率'] = "获取失败"

        # 3. 两融余额 (最新)
        try:
            margin = ak.stock_margin_sh()
            macro['沪市两融余额'] = f"{margin.iloc[-1]['rzye']/1e12:.2f}万亿"
        except: macro['沪市两融余额'] = "获取失败"

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
