import os
import pandas as pd
import akshare as ak
import yfinance as yf
from datetime import datetime
import numpy as np

proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(proj_root)
os.makedirs("data/history", exist_ok=True)

def save_clean_csv(key, df, val_col):
    if df is None or df.empty: return
    df = df.reset_index()
    
    # 1. Date Detection
    date_col = next((c for c in ['日期', 'Date', 'timestamp', '统计时间', '交易日'] if c in df.columns), df.columns[0])
    
    # 2. Value Detection
    v_col = val_col if val_col in df.columns else (df.columns[1] if len(df.columns) > 1 else df.columns[0])
    
    df = df[[date_col, v_col]].copy()
    df.columns = ['timestamp', 'value']
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna()
    
    # 3. Standardize Date
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M')
    df = df.sort_values('timestamp').drop_duplicates('timestamp')
    
    # 4. Unit Normalization (Billions)
    if key in ['Southbound', 'Margin_Debt', 'Northbound']:
        if df['value'].abs().max() > 1e6:
            df['value'] = df['value'] / 1e8
    
    df['value'] = df['value'].round(3)
    df.to_csv(f"data/history/{key}.csv", index=False)
    print(f"[+] {key}: {len(df)} rows")

def main():
    print("🚀 V14.1 PRO: 深度回溯修正版...")
    
    # YFinance Indices
    m = {"Nasdaq": "^IXIC", "Gold": "GC=F", "US10Y": "^TNX", "VIX": "^VIX", "HangSeng": "^HSI", "CNH": "USDCNY=X"}
    for k, v in m.items():
        try:
            df = yf.download(v, period="5y", progress=False)
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            save_clean_csv(k, df, 'Close')
        except: pass

    # A50 Futures Proxy (Use SH Composite as base)
    try:
        df = ak.stock_zh_index_daily_em(symbol="sh000001")
        save_clean_csv("A50_Futures", df, 'close')
    except: pass

    # CSI300 Vol Proxy
    try:
        df = ak.stock_zh_index_daily_em(symbol="sh000300")
        save_clean_csv("CSI300_Vol", df, 'close')
    except: pass

    # CN10Y
    try:
        df = ak.bond_zh_us_rate()
        save_clean_csv("CN10Y", df, '中国国债收益率10年')
    except: pass

    # SHIBOR
    try:
        df = ak.macro_china_shibor_all()
        save_clean_csv("SHIBOR", df, 'O/N-定价')
    except: pass

    # Margin Debt (SH + SZ Sum)
    try:
        sh = ak.macro_china_market_margin_sh()
        sz = ak.macro_china_market_margin_sz()
        sh = sh.set_index('日期')['融资融券余额'].astype(float)
        sz = sz.set_index('日期')['融资融券余额'].astype(float)
        df = (sh + sz).dropna().to_frame()
        save_clean_csv("Margin_Debt", df, '融资融券余额')
    except: pass

    # Southbound
    try:
        sh = ak.stock_hsgt_hist_em(symbol="港股通沪")
        sz = ak.stock_hsgt_hist_em(symbol="港股通深")
        sh = sh.set_index('日期')['当日成交净买额'].astype(float)
        sz = sz.set_index('日期')['当日成交净买额'].astype(float)
        df = (sh + sz).dropna().to_frame()
        save_clean_csv("Southbound", df, '当日成交净买额')
    except: pass

    print("🏁 回溯修正完成。")

if __name__ == "__main__":
    main()
