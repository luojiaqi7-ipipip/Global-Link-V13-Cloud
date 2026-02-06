from core.harvester import Harvester
from core.quant_lab import QuantLab
from core.general import General
import sys
import traceback

def main():
    print("--- V13 架构: 模块化审计开始 ---")
    
    try:
        # 1. 抓取模块
        harvester = Harvester()
        raw_data = harvester.harvest_all()
        
        if not raw_data.get('etf_spot'):
            print("⚠️ 警告: 实时行情抓取为空，审计可能不准确。")
        
        # 2. 计算模块
        lab = QuantLab()
        lab.process()
        
        # 3. 决策模块
        commander = General()
        decision = commander.audit()
        
        if decision:
            print(f"✅ 审计决策完成: {decision.get('decision', 'N/A')}")
        else:
            print("❌ 审计决策失败 (AI 未能生成结果)")
            
    except Exception as e:
        print(f"💥 系统崩溃: {e}")
        traceback.print_exc()
        sys.exit(1)
    
    print("--- V13 架构: 云端闭环完成 ---")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
