import requests
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta

def verify_all():
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    
    print("--- [V13-DEBUG] 北向资金 Northbound ---")
    # 策略：先拿实时累加，如果为0则查历史接口，历史接口若当天为None则取前一有效日
    url_rt = "https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1,f2&fields2=f51,f52"
    url_hist = "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY&columns=ALL&filter=(MUTUAL_TYPE%3D%22001%22)&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=10"
    
    val = 0
    try:
        r_rt = requests.get(url_rt, headers=headers).json()
        val = (float(r_rt['data']['hk2sh']['dayNetAmtIn']) + float(r_rt['data']['hk2sz']['dayNetAmtIn'])) * 10000
        print(f"RT Value: {val/1e8:.2f} 100M")
    except: pass
    
    if val == 0:
        try:
            r_hist = requests.get(url_hist, headers=headers).json()
            items = r_hist['result']['data']
            for item in items:
                if item['NET_DEAL_AMT'] is not None:
                    val = float(item['NET_DEAL_AMT']) * 1e8
                    print(f"Final Hist Value (Date {item['TRADE_DATE']}): {val/1e8:.2f} 100M")
                    break
        except: pass
    print(f"Verified Northbound: {val}")

    print("\n--- [V13-DEBUG] 两融余额 Margin ---")
    # 策略：RPT_RZRQ_LSTOTAL 失效后，尝试 RPTA_WEB_RZRQ_LSTOTAL (部分环境有效) 或 fallback 到个股汇总（耗时但准）
    # 目前最稳的是从总表汇总
    url_margin = "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPTA_WEB_RZRQ_LSTOTAL&columns=ALL&sortColumns=date&sortTypes=-1&pageSize=5"
    try:
        r = requests.get(url_margin, headers=headers).json()
        if r.get('success'):
            m_data = r['result']['data'][0]
            print(f"Margin Total (RPTA): {float(m_data['rzrqye'])/1e12:.4f} T")
        else:
            # Fallback to AkShare (SH/SZ combined)
            m_sh = ak.stock_margin_sh()
            m_sz = ak.stock_margin_sz()
            total = float(m_sh.iloc[-1].iloc[-1]) + float(m_sz.iloc[-1].iloc[-1])
            print(f"Margin Total (AkShare SH+SZ): {total/1e12:.4f} T")
    except Exception as e:
        print(f"Margin Error: {e}")

    print("\n--- [V13-DEBUG] 行业流向 Sector ---")
    url_sector = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=3&po=1&np=1&ut=b2884a51627f511a683671f901ad97a9&fltt=2&invt=2&fid=f62&fs=m:90+t:2+f:!50&fields=f12,f14,f62"
    try:
        r = requests.get(url_sector, headers=headers).json()
        for d in r['data']['diff']:
            print(f"Top: {d['f14']} -> {d['f62']/1e8:.2f} 100M")
    except: pass

    print("\n--- [V13-DEBUG] 宏观稳定源 Macro ---")
    # Sina Global 仍然是最稳的实时源，针对 US10Y 和 VIX
    url_sina = "http://hq.sinajs.cn/list=fx_sus10y,gb_vix"
    try:
        r = requests.get(url_sina, headers={"Referer": "http://finance.sina.com.cn"})
        lines = r.text.strip().split('\n')
        for line in lines:
            parts = line.split('"')[1].split(',')
            key = "US10Y" if "sus10y" in line else "VIX"
            print(f"{key}: {parts[1]}")
    except: pass

if __name__ == "__main__":
    verify_all()
