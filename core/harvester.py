import akshare as ak
import json
import os
from datetime import datetime, timedelta, date
import pytz
import time
import pandas as pd
import requests
import yfinance as yf

class Harvester:
    """
    模块 A: 情报获取引擎 - V13 Cloud (Robust Data Source)
    针对 GitHub Actions 优化，多源备份确保 100% 成功率。
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
        print(f"🚀 [V13] 开始稳健性全量抓取 [{self.timestamp}]...")
        
        raw_data = {
            "meta": {
                "timestamp": self.timestamp, 
                "timezone": "Asia/Shanghai",
                "version": "V13-Cloud-Robust-Action-Optimized"
            },
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        
        raw_data = self._serialize_clean(raw_data)
        
        print(f"📊 [结果统计] ETF行情: {len(raw_data['etf_spot'])} | 宏观指标: {len(raw_data['macro'])} | 历史背景: {len(raw_data['hist_data'])}")
        
        with open(f"{self.data_dir}/market_snap_{self.timestamp}.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
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
        """抓取实时行情 - 多源冗余 (Tencent > EM > Sina)"""
        print(" -> 正在抓取 A 股实时报价 (优先腾讯 API)...")
        
        results = []
        codes_to_fetch = list(self.watchlist)
        
        # 尝试 1: Tencent API (极速且稳定)
        try:
            symbols = [f"sh{c}" if c.startswith(('5', '6')) else f"sz{c}" for c in codes_to_fetch]
            url = f"http://qt.gtimg.cn/q=s_{','.join(symbols)}"
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                lines = r.text.strip().split(';')
                for line in lines:
                    if '~' not in line: continue
                    parts = line.split('~')
                    code = parts[2]
                    results.append({
                        "代码": code,
                        "名称": parts[1],
                        "最新价": float(parts[3]),
                        "成交量": float(parts[6]), # 腾讯 s_ 接口返回的是手 (LOT)
                        "涨跌幅": float(parts[5]),
                        "unit": "LOT",
                        "source": "tencent"
                    })
                if len(results) >= len(self.watchlist):
                    return results
        except Exception as e:
            print(f"⚠️ 腾讯接口异常: {e}")

        # 尝试 2: EM
        if not results:
            try:
                df = ak.fund_etf_spot_em()
                if not df.empty:
                    matched = df[df['代码'].isin(self.watchlist)].copy()
                    matched['unit'] = 'SHARE'
                    matched['source'] = 'em'
                    res = matched.to_dict(orient='records')
                    if res: return res
            except: pass

        # 尝试 3: Sina
        if not results:
            sina_results = []
            for code in self.watchlist:
                try:
                    symbol = f"sh{code}" if code.startswith(('5', '6')) else f"sz{code}"
                    url = f"http://hq.sinajs.cn/list={symbol}"
                    r = requests.get(url, headers={'Referer': 'http://finance.sina.com.cn'}, timeout=5)
                    if r.status_code == 200 and '="' in r.text:
                        data = r.text.split('="')[1].split(',')
                        if len(data) > 1:
                            sina_results.append({
                                "代码": code,
                                "名称": data[0],
                                "最新价": float(data[3]),
                                "成交量": float(data[8]),
                                "昨收": float(data[2]),
                                "unit": "SHARE",
                                "source": "sina"
                            })
                except: pass
            return sina_results
            
        return results

    def _get_macro(self):
        """抓取宏观指标 - 全接口大换血版 (去 AkShare 化 + GitHub 鲁棒性优化)"""
        macro = {}
        
        def wrap_indicator(data, status="SUCCESS"):
            if data is None: status = "FAILED"
            if isinstance(data, dict):
                data.update({"status": status, "last_update": self.timestamp})
                return data
            return {"value": data, "status": status, "last_update": self.timestamp}

        # 1. 全球核心指标 (Yahoo Finance 为主)
        print(" -> 正在抓取核心宏观指标 (多源游击战模式)...")
        tickers = {
            "CNH": "USDCNH=X",
            "Nasdaq": "^IXIC",
            "HangSeng": "^HSI",
            "A50_Futures": "XIN9.F",
            "VIX": "^VIX",
            "US10Y": "^TNX",
            "Gold": "GC=F",
            "CrudeOil": "CL=F"
        }
        
        try:
            yf_data = yf.download(list(tickers.values()), period="5d", interval="1d", progress=False)
            for key, ticker in tickers.items():
                try:
                    if ticker in yf_data['Close']:
                        series = yf_data['Close'][ticker].dropna()
                        if len(series) >= 2:
                            current_price = float(series.iloc[-1])
                            prev_close = float(series.iloc[-2])
                            change_pct = round((current_price / prev_close - 1) * 100, 3) if prev_close != 0 else 0
                            macro[key] = wrap_indicator({
                                "price": current_price, 
                                "prev_close": prev_close,
                                "change_pct": change_pct,
                                "source": "yfinance"
                            })
                except: pass
        except Exception as e:
            print(f"⚠️ Yahoo Finance 抓取受限: {e}")

        # 2. 新浪全球实时源 (针对 CNH, A50, Nasdaq, HSI 的热备份)
        sina_map = {
            "A50_Futures": "hf_CHA50CFD",
            "CNH": "fx_susdcnh",
            "Nasdaq": "gb_ixic",
            "HangSeng": "rt_hkHSI"
        }
        
        for key, sym in sina_map.items():
            if macro.get(key, {}).get('status') == 'SUCCESS': continue
            try:
                url = f"http://hq.sinajs.cn/list={sym}"
                r = requests.get(url, headers={"Referer": "http://finance.sina.com.cn"}, timeout=5)
                if r.status_code == 200 and '="' in r.text:
                    raw_parts = r.text.split('="')[1].split(',')
                    if key == "A50_Futures":
                        price, prev_close = float(raw_parts[0]), float(raw_parts[7])
                        macro[key] = wrap_indicator({
                            "price": price, "prev_close": prev_close, 
                            "change_pct": round((price/prev_close - 1)*100, 3), "source": "sina_global"
                        })
                    elif key == "CNH":
                        price, prev_close = float(raw_parts[1]), float(raw_parts[3])
                        macro[key] = wrap_indicator({
                            "price": price, "prev_close": prev_close, 
                            "change_pct": round((price/prev_close - 1)*100, 3), "source": "sina_global"
                        })
                    elif key == "Nasdaq":
                        price, prev_close = float(raw_parts[1]), float(raw_parts[26])
                        macro[key] = wrap_indicator({
                            "price": price, "prev_close": prev_close, 
                            "change_pct": round((price/prev_close - 1)*100, 3), "source": "sina_global"
                        })
                    elif key == "HangSeng":
                        price, prev_close = float(raw_parts[6]), float(raw_parts[3])
                        macro[key] = wrap_indicator({
                            "price": price, "prev_close": prev_close, 
                            "change_pct": round((price/prev_close - 1)*100, 3), "source": "sina_global"
                        })
            except: pass

        # 3. 北向资金 (EastMoney Mobile)
        if macro.get('Northbound', {}).get('status') != 'SUCCESS':
            try:
                url = "https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54"
                r = requests.get(url, timeout=5)
                data = r.json()
                if data.get('data'):
                    hk2sh = data['data'].get('hk2sh', {}).get('dayNetAmtIn', 0)
                    hk2sz = data['data'].get('hk2sz', {}).get('dayNetAmtIn', 0)
                    macro['Northbound'] = wrap_indicator({"value": (hk2sh + hk2sz) * 10000, "source": "em_mobile"})
            except: pass

        # 4. 国内流动性 (SHIBOR)
        try:
            shibor = ak.rate_shibor_em()
            if not shibor.empty:
                macro['SHIBOR'] = wrap_indicator(shibor.iloc[-1].to_dict())
        except: pass
        
        # 5. A股波动率 (Tencent API)
        try:
            url = "http://qt.gtimg.cn/q=s_sh000300"
            r = requests.get(url, timeout=5)
            if r.status_code == 200 and '~' in r.text:
                parts = r.text.split('~')
                macro['CSI300_Vol'] = wrap_indicator({"pct_change": float(parts[5]), "source": "tencent"})
        except: pass

        # 6. 中国国债 10Y (AkShare)
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")
            df_yield = ak.bond_china_yield(start_date=start_date, end_date=end_date)
            if not df_yield.empty:
                macro['CN10Y'] = wrap_indicator({"yield": float(df_yield['10年'].iloc[-1])})
        except: pass

        # 7. 两融余额 (AkShare)
        try:
            margin = ak.stock_margin_sh()
            if not margin.empty and len(margin) >= 2:
                curr_m = float(margin.iloc[-1]['本日融资融券余额(元)'])
                prev_m = float(margin.iloc[-2]['本日融资融券余额(元)'])
                macro['Margin_Debt'] = wrap_indicator({
                    "value": curr_m,
                    "change_pct": round((curr_m / prev_m - 1) * 100, 3)
                })
        except: pass

        # 8. 行业资金流向 (AkShare)
        try:
            flow = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
            if not flow.empty:
                macro['Sector_Flow'] = wrap_indicator({
                    "top_inflow": flow.head(3)[['名称', '今日净额']].to_dict(orient='records'),
                    "top_outflow": flow.tail(3)[['名称', '今日净额']].to_dict(orient='records')
                })
        except: pass

        # 补齐缺项
        for key in ["CNH", "Nasdaq", "HangSeng", "A50_Futures", "VIX", "US10Y", "Gold", "CrudeOil", 
                    "Northbound", "SHIBOR", "CSI300_Vol", "CN10Y", "Margin_Debt", "Sector_Flow"]:
            if key not in macro: macro[key] = wrap_indicator(None)

        return macro

    def _get_hist_context(self):
        """抓取历史数据 - 核心：针对 GitHub Actions 增加重试和延时控制"""
        print(f" -> 正在建立审计背景 (Watchlist: {len(self.watchlist)} 只)...")
        context = {}
        start_date = (datetime.now(self.beijing_tz) - timedelta(days=45)).strftime("%Y%m%d")
        
        for code in self.watchlist:
            hist_df = pd.DataFrame()
            # 尝试 1: EM (单位：手)
            for _ in range(2): # 两次重试
                try:
                    df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date, adjust="qfq")
                    if not df.empty and len(df) >= 5:
                        df['unit'] = 'LOT'
                        hist_df = df
                        break
                except:
                    time.sleep(1)
            
            # 尝试 2: Sina (单位：股)
            if hist_df.empty:
                try:
                    symbol = f"sh{code}" if code.startswith(('5', '6')) else f"sz{code}"
                    df = ak.fund_etf_hist_sina(symbol=symbol)
                    if not df.empty:
                        df = df.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
                        df['日期'] = pd.to_datetime(df['日期'])
                        cutoff = datetime.now() - timedelta(days=60)
                        df = df[df['日期'] > cutoff]
                        if not df.empty:
                            df['unit'] = 'SHARE'
                            hist_df = df
                except: pass
            
            if not hist_df.empty:
                context[code] = hist_df.to_dict(orient='records')
            
            time.sleep(0.5)
            
        return context

if __name__ == "__main__":
    harvester = Harvester()
    harvester.harvest_all()
