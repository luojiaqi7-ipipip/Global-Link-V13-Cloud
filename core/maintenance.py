import os
import time
from datetime import datetime, timedelta

def clean_directory(directory, days_to_keep, pattern=None):
    """
    清理指定目录下的旧文件。
    """
    if not os.path.exists(directory):
        print(f"Skipping {directory} as it does not exist.")
        return

    now = time.time()
    cutoff = now - (days_to_keep * 86400)
    count = 0
    
    print(f"Cleaning {directory} (keeping last {days_to_keep} days)...")
    
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            if pattern and pattern not in filename:
                continue
            
            file_mtime = os.path.getmtime(filepath)
            if file_mtime < cutoff:
                try:
                    os.remove(filepath)
                    print(f"  Deleted: {filename}")
                    count += 1
                except Exception as e:
                    print(f"  Failed to delete {filename}: {e}")
                    
    print(f"Done. Deleted {count} files from {directory}.")

def main():
    # 获取当前脚本所在目录的父目录作为项目根目录
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 策略定义
    strategies = [
        {"dir": "data/raw", "days": 3},
        {"dir": "data/processed", "days": 7},
        {"dir": "data/audit", "days": 30}
    ]
    
    for strategy in strategies:
        target_dir = os.path.join(base_dir, strategy["dir"])
        clean_directory(target_dir, strategy["days"])

if __name__ == "__main__":
    main()
