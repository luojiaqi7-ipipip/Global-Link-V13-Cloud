import os
import pandas as pd
import akshare as ak
import yfinance as yf
from datetime import datetime
import time

proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(proj_root)
os.makedirs("data/history", exist_ok=True)

def save_csv(key, df, val_col):
    if df is None or df.empty: return
    df = df.reset_index()
    d_col = next((c for c in ['Date', '日期', 'timestamp', '交易日'] if c in df.columns), df.columns[0])
    v_col = val_col if val_col in df.columns else df.columns[1]
    
    df = df[[d_col, v_col]].copy()
    df.columns = ['timestamp', 'value']
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna()
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M')
    df = df.sort_values('timestamp').drop_duplicates('timestamp')
    
    if df['value'].abs().max() > 1e6:
        df['value'] = df['value'] / 1e8
    
    df['value'] = df['value'].round(3)
    df.to_csv(f"data/history/{key}.csv", index=False)
    print(f"[+] {key}: {len(df)} rows")

def main():
    print("🚀 V14.1 PRO: 深度回溯开始...")
    
    # 1-6. YFinance (Global Macro)
    m = {"Nasdaq": "^IXIC", "Gold": "GC=F", "US10Y": "^TNX", "VIX": "^VIX", "HangSeng": "^HSI", "CNH": "USDCNY=X"}
    for k, v in m.items():
        try:
            df = yf.download(v, period="5y", progress=False)
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            save_csv(k, df, 'Close')
        except: pass

    # 7. A50 Proxy (SSE)
    try:
        df = ak.stock_zh_index_daily_em(symbol="sh000001")
        save_csv("A50_Futures", df, 'close')
    except: pass

    # 8. CN10Y
    try:
        df = ak.bond_zh_us_rate()
        save_csv("CN10Y", df, '中国国债收益率10年')
    except: pass

    # 9. SHIBOR
    try:
        df = ak.macro_china_shibor_all()
        save_csv("SHIBOR", df, 'O/N-定价')
    except: pass

    # 10. Margin Debt (Sum of SH/SZ)
    try:
        sh = ak.macro_china_market_margin_sh()
        sz = ak.macro_china_market_margin_sz()
        sh = sh.set_index('统计时间')['融资融券余额'].astype(float)
        sz = sz.set_index('统计时间')['融资融券余额'].astype(float)
        df = (sh + sz).dropna().to_frame()
        save_csv("Margin_Debt", df, '融资融券余额')
    except: pass

    # 11. Southbound (Fund Flow)
    try:
        df = ak.stock_hsgt_fund_flow_summary_em()
        # 这里的 fund_flow_summary 只有最近的，我们需要历史。
        # 尝试使用 stock_hsgt_hist_em
        sh = ak.stock_hsgt_hist_em(symbol="港股通沪")
        sz = ak.stock_hsgt_hist_em(symbol="港股通深")
        sh = sh.set_index('日期')['当日成交净买额'].astype(float)
        sz = sz.set_index('日期')['当日成交净买额'].astype(float)
        df = (sh + sz).dropna().to_frame()
        save_csv("Southbound", df, '当日成交净买额')
    except: pass

    # 12. CSI300 Vol Proxy
    try:
        df = ak.stock_zh_index_daily_em(symbol="sh000300")
        save_csv("CSI300_Vol", df, 'close')
    except: pass

    print("🏁 回溯完成。")

if __name__ == "__main__":
    main()
