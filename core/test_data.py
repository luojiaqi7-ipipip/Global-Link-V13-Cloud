import sys
import os
import json

# Add current dir to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from harvester import Harvester
    from quant_lab import QuantLab
except ImportError:
    # If running from core/
    from harvester import Harvester
    from quant_lab import QuantLab

def test_full_pipeline():
    print("=== [测试] 数据链路修复验证 ===")
    
    # 1. 模拟抓取
    try:
        harvester = Harvester(data_dir="data/test_raw")
        print("正在执行 _get_macro()...")
        macro_raw = harvester._get_macro()
    except Exception as e:
        print(f"❌ 抓取模块执行失败: {e}")
        return
    
    # 打印关键指标状态
    keys_to_check = ["CNH", "A50_Futures", "Northbound", "SHIBOR", "Margin_Debt"]
    print("\n--- 抓取层 (Harvester) 结果 ---")
    for k in keys_to_check:
        data = macro_raw.get(k, {})
        print(f"[{k}] status: {data.get('status')}, source: {data.get('source')}, value/price: {data.get('value') or data.get('price')}")

    # 2. 模拟计算
    print("\n正在执行 QuantLab 处理...")
    try:
        lab = QuantLab(raw_file="data/test_raw/latest_snap.json", out_dir="data/test_processed")
        m_matrix = lab._calc_macro(macro_raw)
        
        print("\n--- 计算层 (QuantLab) 宏观矩阵 ---")
        print(json.dumps(m_matrix, indent=2, ensure_ascii=False))
        
        check_metrics = [
            "CNH_Price", "A50_Futures_Price", "Northbound_Flow_Billion", 
            "Liquidity_Rate", "Margin_Change_Pct"
        ]
        all_ok = True
        for m in check_metrics:
            val = m_matrix.get(m)
            if val is None:
                print(f"  ⚠️ {m} 为 None (等待同步) - 符合修复逻辑")
            elif val == 0:
                print(f"  ❌ {m} 为 0! (不符合要求)")
                all_ok = False
            else:
                print(f"  ✅ {m}: {val} (正常)")

        if all_ok:
            print("\n🎉 验证通过：核心指标非零或合理为 None。")
        else:
            print("\n❌ 验证失败：存在异常 0 值。")
    except Exception as e:
        print(f"❌ 计算模块执行失败: {e}")

if __name__ == "__main__":
    test_full_pipeline()
