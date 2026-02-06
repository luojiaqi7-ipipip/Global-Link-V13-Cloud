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

    def _safe_fetch_spot(self):
        """尝试获取全量实时行情，带重试和更长的间隔"""
        for i in range(5):
            try:
                df = ak.stock_zh_index_spot_em()
                if not df.empty:
                    return df
            except Exception as e:
                print(f"实时行情获取失败 (尝试 {i+1}/5): {e}")
                time.sleep(5)
        return pd.DataFrame()

    def fetch_etf_technical(self):
        """获取ETF全量技术面数据 - 增加重试逻辑以应对GitHub环境"""
        results = []
        spot_df = self._safe_fetch_spot()
        
        if spot_df.empty:
            print("❌ 警告：全量实时行情抓取失败，ETF技术面审计将受限。")
            return []

        for code, name in self.etfs.items():
            try:
                match = spot_df[spot_df['代码'] == code]
                if match.empty: continue
                
                latest_price = float(match['最新价'].values[0])
                vol_today = float(match['成交量'].values[0])
                # 涨跌幅
                pct_chg = float(match['涨跌幅'].values[0])

                # 为了减少 API 调用（防止GitHub IP被封），我们在这里做个折中：
                # 如果是交易时间内，MA5 暂时用上一交易日收盘和今日实时估算
                # 或者尝试抓取极简历史
                hist = None
                for _ in range(2):
                    try:
                        hist = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=(datetime.now() - pd.Timedelta(days=10)).strftime("%Y%m%d"), adjust="qfq")
                        if not hist.empty: break
                    except:
                        time.sleep(2)
                
                if hist is not None and not hist.empty:
                    closes = hist['收盘'].tolist()
                    # 如果今日未在历史中，添加进去
                    if len(closes) >= 4:
                        ma5 = (sum(closes[-4:]) + latest_price) / 5
                        bias = ((latest_price - ma5) / ma5) * 100
                        vol_avg = hist['成交量'].iloc[-5:].mean()
                        vol_ratio = vol_today / vol_avg if vol_avg > 0 else 0
                    else:
                        bias, vol_ratio = 0, 0
                else:
                    bias, vol_ratio = 0, 0
                
                results.append({
                    "代码": code,
                    "名称": name,
                    "价格": round(latest_price, 3),
                    "涨跌幅": f"{round(pct_chg, 2)}%",
                    "乖离率": round(float(bias), 2),
                    "量比": round(float(vol_ratio), 2)
                })
            except Exception as e:
                print(f"处理 {code} 时出错: {e}")
                continue
        return results

    def fetch_macro_indicators(self):
        """宏观参数全量扩容：国内+国际、实时+变化"""
        macro = {}
        
        # 1. 离岸人民币
        try:
            fx = ak.fx_spot_quote()
            if not fx.empty:
                cnh = fx[fx['【名称】'].str.contains('美元/人民币', na=False)].iloc[0]
                macro['离岸人民币'] = f"{cnh['【最新价】']} (变动: {cnh['【涨跌幅】']}%)"
        except: macro['离岸人民币'] = "同步中..."

        # 2. 市场流动性 (SHIBOR)
        try:
            shibor = ak.rate_shibor_em()
            if not shibor.empty:
                latest = shibor.iloc[-1]
                macro['SHIBOR隔夜'] = f"{latest['利率']}% ({latest['涨跌']}bp)"
        except: pass

        # 3. 北向资金 (实时流向)
        try:
            flow = ak.stock_hsgt_north_net_flow_em()
            if not flow.empty:
                macro['北向资金(日内)'] = f"{round(flow.iloc[-1]['value']/1e8, 2)}亿"
        except: pass

        # 4. 恐慌度 (用沪深300日内波幅代替)
        try:
            hs300 = ak.stock_zh_index_spot_em()
            row = hs300[hs300['代码'] == '000300']
            if not row.empty:
                # 振幅估算
                high = float(row['最高'].values[0])
                low = float(row['最低'].values[0])
                prev_close = float(row['昨收'].values[0])
                volatility = ((high - low) / prev_close) * 100
                macro['A股实时波动率'] = f"{round(volatility, 2)}%"
        except: pass

        # 5. 两融余额
        try:
            margin = ak.stock_margin_sh()
            if not margin.empty:
                macro['沪市两融余额'] = f"{margin.iloc[-1]['rzye']/1e12:.2f}万亿"
        except: pass

        return macro

    def sync_all(self):
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_now = datetime.now(beijing_tz)
        
        technical_data = self.fetch_etf_technical()
        macro_data = self.fetch_macro_indicators()
        
        data = {
            "timestamp": beijing_now.strftime("%Y-%m-%d %H:%M:%S (北京时间)"),
            "technical": technical_data,
            "macro": macro_data
        }
        
        with open(f"{self.data_dir}/raw_market.json", "w", encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
