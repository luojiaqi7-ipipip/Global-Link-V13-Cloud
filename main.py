from core.harvester import Harvester
from core.quant_lab import QuantLab
from core.general import General
import time

def main():
    print("--- V13 架构: 模块化审计开始 ---")
    
    # 1. 抓取模块
    harvester = Harvester()
    harvester.harvest_all()
    
    # 2. 计算模块
    lab = QuantLab()
    lab.process()
    
    # 3. 决策模块
    commander = General()
    commander.audit()
    
    print("--- V13 架构: 云端闭环完成 ---")

if __name__ == "__main__":
    main()
