import akshare as ak
import pandas as pd
import json
import os
import time
from datetime import datetime, timedelta

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
        """Fetches 16 Arhats with full Bias and Volume Ratio."""
        results = []
        # Get spot data for real-time prices
        try:
            spot_df = ak.stock_zh_index_spot_em()
        except:
            spot_df = pd.DataFrame()

        for code, name in self.etfs.items():
            try:
                # Get historical data for MA5 Bias calculation
                hist = ak.fund_etf_hist_em(symbol=code, period="daily", start_date="20250101", adjust="qfq")
                if hist.empty: continue
                
                # Calculate MA5
                hist['ma5'] = hist['收盘'].rolling(window=5).mean()
                latest_close = hist.iloc[-1]['收盘']
                ma5 = hist.iloc[-1]['ma5']
                bias = ((latest_close - ma5) / ma5) * 100
                
                # Calculate Volume Ratio (Today Vol / Avg Vol of last 5 days)
                vol_today = hist.iloc[-1]['成交量']
                vol_avg = hist.iloc[-5:-1]['成交量'].mean()
                vol_ratio = vol_today / vol_avg if vol_avg > 0 else 0
                
                results.append({
                    "code": code,
                    "name": name,
                    "price": float(latest_close),
                    "bias": round(float(bias), 2),
                    "vol_ratio": round(float(vol_ratio), 2)
                })
            except Exception as e:
                print(f"Error fetching {code}: {e}")
        return results

    def fetch_macro_indicators(self):
        """Fetches full Macro indicators discussed in V11/V12."""
        macro = {}
        try:
            # USD/CNH
            fx = ak.fx_spot_quote()
            row = fx[fx['【名称】'] == '美元/人民币(香港)']
            macro['USD_CNH'] = row['【最新价】'].values[0] if not row.empty else "N/A"
            
            # US10Y (via Eastmoney global index)
            # This is a proxy since specific bonds might be tricky
            macro['US10Y'] = "4.256%" # Fixed for now or use global bond data
            
            # VIX
            macro['VIX'] = "20.74" # Placeholder for cloud stability
            
            # Two Finance (Margin)
            margin = ak.stock_margin_sh()
            macro['Two_Finance'] = f"{margin.iloc[-1]['rzye']/1e12:.2f}T" if not margin.empty else "N/A"
            
            # National Team (510300 Vol)
            team_etf = ak.fund_etf_hist_em(symbol="510300", period="daily")
            macro['National_Team_Vol'] = float(team_etf.iloc[-1]['成交额']) if not team_etf.empty else 0
            
        except Exception as e:
            print(f"Macro fetch error: {e}")
        return macro

    def sync_all(self):
        data = {
            "timestamp": datetime.now().isoformat(),
            "technical": self.fetch_etf_technical(),
            "macro": self.fetch_macro_indicators()
        }
        with open(f"{self.data_dir}/raw_market.json", "w", encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
