import os
import json
import pandas as pd
import numpy as np
from core.harvester import Harvester
from core.intel_engine import IntelEngine
from core.quant_lab import QuantLab

def verify():
    print("=== Global-Link V14 本地逻辑验证程序 ===")
    
    # 1. 验证 Harvester
    print("\n[1/3] 验证 Harvester 采集逻辑...")
    h = Harvester()
    raw = h.harvest_all()
    ts = raw.get('meta', {}).get('timestamp', '')
    print(f"   - 时间戳格式验证: {ts}")
    if " " not in ts or "-" not in ts:
        print("   [FAILED] 时间戳格式不符合要求 YYYY-MM-DD HH:mm")
        return False
    print("   [PASSED] Harvester 验证通过")

    # 2. 验证 IntelEngine
    print("\n[2/3] 验证 IntelEngine 特征计算逻辑...")
    intel = IntelEngine()
    intel.update_history(raw)
    
    # 检查是否有文件生成
    history_files = os.listdir("data/history")
    if not history_files:
        print("   [FAILED] data/history 目录为空")
        return False
    
    # 检查一个具体指标
    key = "CNH"
    features = intel.get_features(key)
    if features:
        print(f"   - {key} 特征计算结果: {features}")
        for field in ['p_20d', 'z_score', 'slope']:
            if field not in features:
                print(f"   [FAILED] 特征缺失字段: {field}")
                return False
    else:
        print(f"   [WARN] {key} 特征计算结果为空 (可能是首个样本点)")
    print("   [PASSED] IntelEngine 验证通过")

    # 3. 验证 QuantLab
    print("\n[3/3] 验证 QuantLab 矩阵生成逻辑...")
    lab = QuantLab()
    processed = lab.process()
    macro_matrix = processed.get('macro_matrix', {})
    
    if key in macro_matrix:
        m_data = macro_matrix[key]
        print(f"   - 矩阵内 {key} 数据结构: {m_data}")
        for rk in ['value', 'p_20d', 'slope', 'z_score']:
            if rk not in m_data:
                print(f"   [FAILED] 宏观矩阵缺少关键字段: {rk}")
                return False
    else:
        print(f"   [FAILED] 宏观矩阵中找不到关键指标: {key}")
        return False
    print("   [PASSED] QuantLab 验证通过")

    print("\n=== [100% 验证通过] 系统处于就绪状态 ===")
    return True

if __name__ == "__main__":
    if verify():
        exit(0)
    else:
        exit(1)
