from core.harvester import Harvester
from core.quant_lab import QuantLab
from core.general import General
from core.intel_engine import IntelEngine
import sys
import traceback

def main():
    print("--- Global-Link V13: 宏观特征驱动审计开始 ---")
    
    try:
        # 1. 数据采集
        harvester = Harvester()
        raw_data = harvester.harvest_all()
        
        if not raw_data.get('etf_spot'):
            print("⚠️ 警告: 实时行情为空。")
        
        # 2. 宏观特征引擎更新 (V14)
        intel = IntelEngine()
        intel.update_history(raw_data)
        print("🧠 特征引擎: 历史数据已更新")
        
        # 3. 量化分析
        lab = QuantLab()
        lab.process()
        
        # 4. AI 策略审计
        commander = General()
        decision = commander.audit()
        
        if decision:
            print(f"✅ 策略决策已生成: {decision.get('decision', 'N/A')}")
        else:
            print("❌ 策略决策生成失败")
            
    except Exception as e:
        print(f"💥 系统异常: {e}")
        traceback.print_exc()
        sys.exit(1)
    
    print("--- Global-Link V13: 执行任务已完成 ---")

if __name__ == "__main__":
    main()
