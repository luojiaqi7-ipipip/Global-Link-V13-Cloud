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
            print(f"❌ 错误: 找不到指标文件 {self.metrics_file}")
            return None
            
        with open(self.metrics_file, 'r') as f:
            metrics = json.load(f)

        prompt = f"""
你现在是 Global-Link A-Quant 首席投资官。
请基于“权重金字塔”执行冷酷的量化审计。所有的判断必须严格依据输入的 [数据矩阵]。

[决策权重金字塔]
1. 技术面 (1.0): 乖离率 (Bias) < -2.5% 是开火的前提。量比 > 1.2 是动能确认。
2. 宏观对冲 (0.8): 
   - 汇率 (CNH): 贬值压力大时 (Change > 0.1%) 需谨慎。
   - 资金 (Northbound): 外资流入是强心剂。
   - 外盘 (Nasdaq/HSI): 全球风险偏好风向标。
3. 成本/风险 (0.5): SHIBOR 利率反映国内流动性。美债收益率上升压制估值。

[数据矩阵]
{json.dumps(metrics, ensure_ascii=False, indent=2)}

[审计指令]
- 必须返回 JSON 对象。
- 计算 Attack Factor (0.8-1.2)。
- 决策: BUY (共振爆发) / WAIT (观望中) / HOLD (持仓待涨) / SELL (风险离场)。
- Rationale 必须包含具体数值引用，使用专业、客观、冷静的投资经理口吻。

[输出 JSON 格式]
{{
  "decision": "...",
  "target": "...",
  "attack_factor": 1.1,
  "rationale": "基于数据的审计结果...",
  "parameters": {{ "stop_loss": -1.5, "stop_profit": 3.0 }},
  "top_candidates": [
    {{ "code": "...", "name": "...", "bias": 0.0, "vol_ratio": 0.0, "status": "..." }}
  ]
}}
"""
        response = self.model.generate_content(prompt)
        res_json = self._clean_json(response.text)
        
        # 合并元数据供 UI 使用
        res_json['timestamp'] = metrics.get('timestamp', 'unknown')
        res_json['macro_snapshot'] = metrics.get('macro_matrix', {})

        out_path = f"{self.out_dir}/decision_{res_json['timestamp']}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(res_json, f, ensure_ascii=False, indent=2)
            
        # UI 读取的唯一入口
        with open("data/audit_result.json", 'w', encoding='utf-8') as f:
            json.dump(res_json, f, ensure_ascii=False, indent=2)

        print(f"⚖️ AI 审计完成: {out_path}")
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
