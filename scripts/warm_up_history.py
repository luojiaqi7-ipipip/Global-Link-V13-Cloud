import os
import pandas as pd
import yfinance as yf
import akshare as ak
from datetime import datetime, timedelta
import time
import numpy as np

# Set working directory to project root
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Create directories
os.makedirs("data/history", exist_ok=True)

def save_to_csv(key, data):
    if data is None:
        print(f"[-] No data for {key}")
        return
    
    if isinstance(data, pd.Series):
        df = data.to_frame()
    else:
        df = data.copy()

    if df.empty:
        print(f"[-] Empty data for {key}")
        return

    # Handle yfinance MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Find column names for date and value
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()
    
    date_col = None
    for col in df.columns:
        if col in ['Date', '日期', 'timestamp', 'index', '交易日', '信用交易日期']:
            date_col = col
            break
    
    if not date_col:
        date_col = df.columns[0]
        
    val_col = None
    # Priority for value columns
    priority_cols = ['Close', 'close', '收盘', 'value', '利率', '中国国债收益率10年', '融资融券余额', 'O/N-定价', '当日成交净买额']
    for p_col in priority_cols:
        if p_col in df.columns:
            val_col = p_col
            break
    
    if not val_col:
        val_col = df.columns[1]
        
    result = df[[date_col, val_col]].copy()
    result.columns = ['timestamp', 'value']
    
    # Clean value
    result['value'] = pd.to_numeric(result['value'], errors='coerce')
    result = result.dropna(subset=['value'])
    
    # Format timestamp
    try:
        result['timestamp'] = pd.to_datetime(result['timestamp']).dt.strftime('%Y-%m-%d %H:%M')
    except:
        pass
        
    file_path = f"data/history/{key}.csv"
    result.to_csv(file_path, index=False)
    print(f"[+] Saved {key} to {file_path} ({len(result)} rows)")

def main():
    print("🚀 Starting warm-up of historical data (5 years)...")
    
    # 1. CNH (yfinance)
    print("Fetching CNH...")
    try:
        df_cnh = yf.download("USDCNY=X", period="5y")
        if df_cnh.empty:
            df_cnh = yf.download("CNH=X", period="5y")
        save_to_csv("CNH", df_cnh)
    except: pass
    
    # 2. Nasdaq (yfinance)
    print("Fetching Nasdaq...")
    try:
        df_nasdaq = yf.download("^IXIC", period="5y")
        save_to_csv("Nasdaq", df_nasdaq)
    except: pass
    
    # 3. Gold (yfinance)
    print("Fetching Gold...")
    try:
        df_gold = yf.download("GC=F", period="5y")
        save_to_csv("Gold", df_gold)
    except: pass
    
    # 4. US10Y (yfinance)
    print("Fetching US10Y...")
    try:
        df_us10y = yf.download("^TNX", period="5y")
        save_to_csv("US10Y", df_us10y)
    except: pass
    
    # 5. VIX (yfinance)
    print("Fetching VIX...")
    try:
        df_vix = yf.download("^VIX", period="5y")
        save_to_csv("VIX", df_vix)
    except: pass
    
    # 6. HangSeng (yfinance)
    print("Fetching HangSeng...")
    try:
        df_hsi = yf.download("^HSI", period="5y")
        save_to_csv("HangSeng", df_hsi)
    except: pass
    
    # 7. CSI300 (AkShare/Sina)
    print("Fetching CSI300...")
    try:
        df_csi300 = ak.stock_zh_index_daily(symbol="sh000300")
        save_to_csv("CSI300_Vol", df_csi300) 
    except Exception as e:
        print(f"[-] CSI300 failed: {e}")

    # 8. CN10Y (AkShare)
    print("Fetching CN10Y...")
    try:
        df_cn10y = ak.bond_zh_us_rate()
        save_to_csv("CN10Y", df_cn10y[['日期', '中国国债收益率10年']])
    except Exception as e:
        print(f"[-] CN10Y failed: {e}")

    # 9. SHIBOR (AkShare)
    print("Fetching SHIBOR...")
    try:
        df_shibor = ak.macro_china_shibor_all()
        save_to_csv("SHIBOR", df_shibor[['日期', 'O/N-定价']])
    except Exception as e:
        print(f"[-] SHIBOR failed: {e}")

    # 10. Margin_Debt (AkShare)
    print("Fetching Margin_Debt...")
    try:
        df_margin = ak.stock_margin_sse(start_date="20200101", end_date=datetime.now().strftime("%Y%m%d"))
        save_to_csv("Margin_Debt", df_margin)
    except Exception as e:
        print(f"[-] Margin_Debt failed: {e}")

    # 11. Southbound (AkShare)
    print("Fetching Southbound...")
    try:
        df_sb_sh = ak.stock_hsgt_hist_em(symbol="港股通沪")
        df_sb_sz = ak.stock_hsgt_hist_em(symbol="港股通深")
        df_sb_sh = df_sb_sh.set_index('日期')['当日成交净买额'].astype(float)
        df_sb_sz = df_sb_sz.set_index('日期')['当日成交净买额'].astype(float)
        df_sb = (df_sb_sh + df_sb_sz).dropna()
        save_to_csv("Southbound", df_sb)
    except Exception as e:
        print(f"[-] Southbound failed: {e}")

    # 12. A50
    print("Fetching A50...")
    try:
        df_a50 = yf.download("FXI", period="5y") # iShares China Large-Cap ETF
        if df_a50.empty:
             df_a50 = ak.stock_zh_index_daily_em(symbol="sh000001") 
        save_to_csv("A50_Futures", df_a50)
    except Exception as e:
        print(f"[-] A50 failed: {e}")

    print("✅ Warm-up complete.")

if __name__ == "__main__":
    main()
