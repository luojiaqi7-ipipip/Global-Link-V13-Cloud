import requests
import json

def debug_apis():
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    
    print("--- DEBUG 1: Northbound Hist ---")
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY&columns=ALL&filter=(MUTUAL_TYPE%3D%22001%22)&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=5&pageNumber=1&source=WEB&client=WEB"
    r = requests.get(url, headers=headers)
    print(f"Status: {r.status_code}")
    print(f"Content: {r.text[:500]}")

    print("\n--- DEBUG 2: Margin Total ---")
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPTA_WEB_RZRQ_LSTOTAL&columns=ALL&sortColumns=date&sortTypes=-1&pageSize=5&pageNumber=1&source=WEB&client=WEB"
    r = requests.get(url, headers=headers)
    print(f"Status: {r.status_code}")
    print(f"Content: {r.text[:500]}")

if __name__ == "__main__":
    debug_apis()
