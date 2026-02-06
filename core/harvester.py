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
    def __init__(self, data_dir="data/raw"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.beijing_tz = pytz.timezone('Asia/Shanghai')
        self.timestamp = datetime.now(self.beijing_tz).strftime("%Y%m%d_%H%M")
        self.watchlist = ["159995", "513050", "512760", "512480", "588000", "159915", "510500", "510300", "512660", "512880", "510880", "515080", "512010", "512800", "512690", "159928"]

    def harvest_all(self):
        print(f"🚀 [V13] 开始全量数据抓取 [{self.timestamp}]...")
        raw_data = {
            "meta": {"timestamp": self.timestamp, "timezone": "Asia/Shanghai", "version": "V13-Final-Robust"},
            "etf_spot": self._get_spot(),
            "macro": self._get_macro(),
            "hist_data": self._get_hist_context()
        }
        raw_data = self._serialize_clean(raw_data)
        with open(f"{self.data_dir}/latest_snap.json", 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        return raw_data

    def _serialize_clean(self, obj):
        if isinstance(obj, dict): return {k: self._serialize_clean(v) for k, v in obj.items()}
        elif isinstance(obj, list): return [self._serialize_clean(i) for i in obj]
        elif pd.isna(obj): return None
        return obj

    def _get_spot(self):
        try:
            symbols = [f"sh{c}" if c.startswith(('5', '6')) else f"sz{c}" for c in self.watchlist]
            # 添加 Headers 避免被屏蔽
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://finance.sina.com.cn"
            }
            r = requests.get(f"http://qt.gtimg.cn/q=s_{','.join(symbols)}", headers=headers, timeout=5)
            if r.status_code == 200:
                results = []
                for p in r.text.strip().split(';'):
                    if '~' not in p: continue
                    parts = p.split('~')
                    results.append({
                        "代码": parts[2], "名称": parts[1], "最新价": float(parts[3]),
                        "成交量": float(parts[6]), "涨跌幅": float(parts[5]), "unit": "LOT"
                    })
                return results
        except: pass
        return []

    def _get_macro(self):
        macro = {}
        def wrap(data): return {**(data if isinstance(data, dict) else {"value": data}), "status": "SUCCESS" if data is not None else "FAILED", "last_update": self.timestamp}

        # 1. 全球宏观 + 国内核心指数 (Sina Global & Domestic)
        sina_map = {
            "CNH": "fx_susdcnh", 
            "Nasdaq": "gb_ndx", 
            "HangSeng": "rt_hkHSI", 
            "A50_Futures": "hf_CHA50CFD", 
            "VIX": "hf_VX",
            "Gold": "hf_GC",
            "CrudeOil": "hf_CL",
            "CSI300_Spot": "sh000300"
        }
        try:
            url = f"http://hq.sinajs.cn/list={','.join(sina_map.values())}"
            headers = {
                "Referer": "https://finance.sina.com.cn",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            r = requests.get(url, headers=headers, timeout=5)
            lines = r.text.strip().split('\n')
            inv_map = {v: k for k, v in sina_map.items()}
            for line in lines:
                try:
                    sym = line.split('=')[0].replace('var hq_str_', '').strip()
                    content = line.split('"')[1]
                    if not content: 
                        print(f"⚠️ Sina {sym} returned empty")
                        continue
                    data = content.split(',')
                    key = inv_map.get(sym)
                    if not key: continue
                    
                    if sym.startswith("fx_"): 
                        price = float(data[1])
                        change_pct = float(data[11]) * 100 if len(data) > 11 and data[11] else None
                    elif sym.startswith("gb_"): 
                        price = float(data[1])
                        change_pct = float(data[2]) if len(data) > 2 and data[2] else None
                    elif sym.startswith("rt_"): 
                        price = float(data[2])
                        change_pct = (float(data[2]) / float(data[3]) - 1) * 100 if len(data) > 3 and data[3] else None
                    elif sym.startswith("hf_"): 
                        price = float(data[0])
                        prev_close = float(data[7]) if len(data) > 7 and data[7] else 0
                        change_pct = (float(data[0]) / prev_close - 1) * 100 if prev_close != 0 else None
                    elif sym.startswith("sh"): # 沪深指数 sh000300
                        price = float(data[3])
                        prev_close = float(data[2])
                        change_pct = (price / prev_close - 1) * 100 if prev_close != 0 else None
                        amp = (float(data[4]) - float(data[5])) / prev_close * 100 if prev_close != 0 else None
                        macro['CSI300_Vol'] = wrap({"amplitude": round(amp, 3), "pct_change": round(change_pct, 3)})
                        print(f"✅ CSI300 Captured: Amp {amp:.2f}%")
                        continue 
                    else: 
                        price = float(data[1])
                        change_pct = None
                    
                    macro[key] = wrap({"price": price, "change_pct": change_pct})
                except Exception as e:
                    print(f"❌ Error parsing Sina {sym}: {e}")
        except Exception as e:
            print(f"❌ Sina Request Failed: {e}")

        # 2. 国债收益率 (CN & US)
        try:
            df_rates = ak.bond_zh_us_rate()
            if not df_rates.empty:
                cn_latest = df_rates.dropna(subset=['中国国债收益率10年']).iloc[-1]
                macro['CN10Y'] = wrap({"yield": float(cn_latest['中国国债收益率10年'])})
                us_latest = df_rates.dropna(subset=['美国国债收益率10年']).iloc[-1]
                macro['US10Y'] = wrap({"price": float(us_latest['美国国债收益率10年'])})
        except: pass

        # 3. 跨境资金 (由于新规隐藏北向实时净流入，此处取南向净流入作为对冲情绪参考)
        try:
            df = ak.stock_hsgt_fund_flow_summary_em()
            south = df[df['资金方向'] == '南向']
            val = south['成交净买额'].astype(float).sum() * 1e8
            macro['Southbound'] = wrap({"value": val, "note": "Northbound hidden; using Southbound as proxy"})
        except: pass

        # 4. 行业流入 (东财 Push2 - 使用更稳健的 Token)
        try:
            ut = "bd1d9ddb04089700cf9c27f6f7426281"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "http://quote.eastmoney.com/center/gridlist.html"
            }
            url = f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10&po=1&np=1&ut={ut}&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50&fields=f14,f62"
            r = requests.get(url, headers=headers, timeout=5).json()
            diff = r.get('data', {}).get('diff', [])
            if diff: 
                macro['Sector_Flow'] = wrap({"top_inflow": [{"名称": d['f14'], "今日净额": d['f62']} for d in diff[:3]]})
                print(f"✅ Sector Flow Captured: {[d['f14'] for d in diff[:3]]}")
            else:
                print("⚠️ Sector Flow Empty")
        except Exception as e:
            print(f"⚠️ Sector Flow Error: {e}")

        # 5. 两融 (AkShare 宏观两融接口)
        try:
            m_sh = ak.macro_china_market_margin_sh()
            m_sz = ak.macro_china_market_margin_sz()
            if not m_sh.empty and not m_sz.empty:
                def get_total(df, idx):
                    col = '融资融券余额' if '融资融券余额' in df.columns else df.columns[-1]
                    return float(df.iloc[idx][col])
                latest_total = get_total(m_sh, -1) + get_total(m_sz, -1)
                prev_total = get_total(m_sh, -2) + get_total(m_sz, -2)
                change_pct = (latest_total / prev_total - 1) * 100 if prev_total != 0 else None
                macro['Margin_Debt'] = wrap({"value": latest_total, "change_pct": change_pct})
        except: pass

        # 6. 国内流动性 (SHIBOR)
        try:
            shibor = ak.rate_interbank(market="上海银行同业拆借市场", symbol="Shibor人民币", indicator="隔夜")
            if not shibor.empty:
                macro['SHIBOR'] = wrap({"value": float(shibor.iloc[-1]['利率'])})
                print(f"✅ SHIBOR Captured: {macro['SHIBOR']['value']}%")
        except Exception as e:
            print(f"⚠️ SHIBOR Error: {e}")

        print(f"📡 Macro Keys Captured: {list(macro.keys())}")
        return macro

    def _get_hist_context(self):
        ctx = {}
        for c in self.watchlist:
            try:
                # 使用新浪 K 线接口，比 AkShare 更稳定
                prefix = "sh" if c.startswith(('5', '6')) else "sz"
                url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={prefix}{c}&scale=240&ma=no&datalen=45"
                r = requests.get(url, timeout=5).json()
                if r:
                    ctx[c] = [{
                        "日期": item['day'],
                        "开盘": float(item['open']),
                        "最高": float(item['high']),
                        "最低": float(item['low']),
                        "收盘": float(item['close']),
                        "成交量": float(item['volume']),
                        "unit": "SHARE"
                    } for item in r]
            except: pass
        return ctx

if __name__ == "__main__":
    Harvester().harvest_all()
