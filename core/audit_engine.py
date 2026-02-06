import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

class AuditEngine:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-3-flash-preview')

    def perform_audit(self, market_data):
        prompt = f"""
你现在是 Global-Link A-Quant 首席投资官。
请基于“权重金字塔”执行冷酷的量化审计。

[决策金字塔]
1. 技术面 (权重 1.0): MA5 乖离率 < -2.5% 且 量比 > 1.2 为必选。
2. 宏观面 (权重 0.7): 汇率、VIX、两融、国家队。
3. 政策面 (权重 0.8): 官方对科技创新的支持公文。

[16罗汉技术截面]
{json.dumps(market_data['technical'], ensure_ascii=False, indent=2)}

[全球宏观脉冲]
{json.dumps(market_data['macro'], ensure_ascii=False, indent=2)}

[指令]
必须返回且仅返回一个 JSON 对象。
1. 计算进攻系数 Attack Factor (0.8-1.2)。
2. 给出决策: BUY/WAIT/SELL/HOLD。
3. 严格计算执行参数: 止损线 (1.5% * Factor), 止盈线 (3.0% * Factor)。
4. 所有的 rationale 必须使用中文，语气专业且果断。

[输出格式]
{{
  "timestamp": "{market_data['timestamp']}",
  "decision": "...",
  "target": "...",
  "attack_factor": 1.1,
  "rationale": "用中文简述审计逻辑...",
  "parameters": {{
    "entry": 0.0,
    "stop_loss": 0.0,
    "stop_profit": 0.0,
    "time_limit": "4 天"
  }},
  "macro": {json.dumps(market_data['macro'], ensure_ascii=False)},
  "top_candidates": [
     {{ "code": "...", "bias": -2.5, "vol": 1.2 }}
  ]
}}
"""
        response = self.model.generate_content(prompt)
        raw_text = response.text.strip()
        
        # 清理 Markdown
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0].strip()
            
        return json.loads(raw_text)
