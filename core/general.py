import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

class General:
    """
    模块 C: AI 审计中心
    基于 Gemini 3 Flash，对量化矩阵进行最终决策。
    """
    def __init__(self, metrics_file="data/processed/latest_metrics.json", out_dir="data/audit"):
        self.metrics_file = metrics_file
        self.out_dir = out_dir
        os.makedirs(self.out_dir, exist_ok=True)
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        self.model = genai.GenerativeModel('gemini-3-flash-preview')

    def audit(self):
        if not os.path.exists(self.metrics_file):
            return None
            
        with open(self.metrics_file, 'r') as f:
            metrics = json.load(f)

        prompt = f"""
你现在是 Global-Link A-Quant 首席投资官。
请基于“权重金字塔”执行冷酷的量化审计。所有的判断必须严格依据输入的 [量化矩阵]。

[决策指南]
1. 技术面 (1.0): 乖离率 < -2.5% 且 量比 > 1.2。
2. 宏观面 (0.7): 离岸人民币(CNH)变化、流动性(SHIBOR)。

[量化矩阵]
{json.dumps(metrics, ensure_ascii=False, indent=2)}

[审计指令]
- 必须返回 JSON 对象。
- 计算 Factor (0.8-1.2)。
- 决策: BUY/WAIT/HOLD/SELL。
- Rationale 必须包含具体数值引用，使用客观、冷静的中文。

[输出 JSON]
{{
  "decision": "...",
  "target": "...",
  "attack_factor": 1.1,
  "rationale": "基于数据的审计结果...",
  "parameters": {{ "stop_loss": 0.0, "stop_profit": 0.0 }},
  "top_candidates": []
}}
"""
        response = self.model.generate_content(prompt)
        res_json = self._clean_json(response.text)
        
        # 合并时间戳和宏观原始数据供 UI 使用
        res_json['timestamp'] = metrics['timestamp']
        res_json['macro_snapshot'] = metrics['macro_matrix']

        out_path = f"{self.out_dir}/decision_{metrics['timestamp']}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(res_json, f, ensure_ascii=False, indent=2)
            
        # UI 读取的唯一入口
        with open("data/audit_result.json", 'w', encoding='utf-8') as f:
            json.dump(res_json, f, ensure_ascii=False, indent=2)

        return res_json

    def _clean_json(self, text):
        raw = text.strip()
        if "```json" in raw:
            raw = raw.split("```json")[1].split("```")[0].strip()
        elif "```" in raw:
            raw = raw.split("```")[1].split("```")[0].strip()
        return json.loads(raw)

if __name__ == "__main__":
    general = General()
    general.audit()
