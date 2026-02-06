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
你现在是 Global-Link A-Quant 首席投资决策官。
你的任务是基于 100% 真实的宏观与技术数据进行冷血审计。

[铁律]
1. 严禁幻觉：仅根据输入的数据进行判断。如果数据为空，则决策必须偏向“观望”。
2. 逻辑严密：所有的 Attack Factor 必须有数据支撑。
3. 专业表述：Rationale 必须包含对宏观变量（汇率、流动性、波动率）的具体引用。

[全维度宏观脉冲]
{json.dumps(market_data['macro'], ensure_ascii=False, indent=2)}

[ETF 监测池实时数据]
{json.dumps(market_data['technical'], ensure_ascii=False, indent=2)}

[决策指南]
- 技术面 (1.0): 乖离率 < -2.5% 且 量比 > 1.2 是黄金坑触发器。
- 宏观面 (0.7): 离岸人民币波动、SHIBOR(流动性)、两融(市场杠杆)、北向资金流向。
- 情绪面 (0.1): 市场实际波动率。

[审计指令]
1. 给出决策：BUY/HOLD/SELL/WAIT。
2. 锁定标的：选择监测池中数据表现最极致的 1 个。
3. 计算执行参数：止损 (1.5%*Factor), 止盈 (3.0%*Factor)。
4. top_candidates：展示监测池中乖离率最大的前 5 名。

[输出 JSON]
{{
  "timestamp": "{market_data['timestamp']}",
  "decision": "决策代码",
  "target": "代码 (名称)",
  "attack_factor": 1.0,
  "rationale": "基于数据的专业审计结论...",
  "parameters": {{
    "entry": 0.0,
    "stop_loss": 0.0,
    "stop_profit": 0.0,
    "time_limit": "4 个交易日"
  }},
  "macro": {json.dumps(market_data['macro'], ensure_ascii=False)},
  "top_candidates": [
     {{ "code": "代码", "name": "名称", "bias": "乖离率", "vol": "量比" }}
  ]
}}
"""
        response = self.model.generate_content(prompt)
        raw_text = response.text.strip()
        
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0].strip()
            
        return json.loads(raw_text)
