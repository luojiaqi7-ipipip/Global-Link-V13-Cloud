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
请基于“权重金字塔”执行专业的量化审计。

[决策金字塔]
1. 技术指标 (权重 1.0): 优先考虑 MA5 乖离率 < -2.5% 且 量比 > 1.2 的标的。
2. 宏观环境 (权重 0.7): 汇率稳定性、VIX 恐慌指数、市场杠杆率（两融）、国家队资金流向。
3. 政策导向 (权重 0.8): 重点关注官方发布的硬科技与创新支持政策。

[核心 ETF 监测池技术截面]
{json.dumps(market_data['technical'], ensure_ascii=False, indent=2)}

[宏观经济指标脉冲]
{json.dumps(market_data['macro'], ensure_ascii=False, indent=2)}

[审计指令]
1. 必须返回且仅返回一个 JSON 对象，严禁任何额外文字。
2. 计算风险调节系数 (Attack Factor, 0.8-1.2)。
3. 给出投资决策: BUY (买入) / WAIT (观望) / SELL (卖出) / HOLD (持有)。
4. 计算执行参数: 止损线 (1.5% * Factor), 止盈线 (3.0% * Factor)。
5. Rationale 使用专业、客观的中文字符。
6. top_candidates 必须包含监测池中所有偏离度较大的前 5 名标的，且必须包含其中文名称。

[输出 JSON 结构]
{{
  "timestamp": "{market_data['timestamp']}",
  "decision": "决策代码",
  "target": "标的代码 (名称)",
  "attack_factor": 1.1,
  "rationale": "专业的审计逻辑分析...",
  "parameters": {{
    "entry": 0.0,
    "stop_loss": 0.0,
    "stop_profit": 0.0,
    "time_limit": "4 个交易日"
  }},
  "macro": {json.dumps(market_data['macro'], ensure_ascii=False)},
  "top_candidates": [
     {{ "code": "代码", "name": "中文名称", "bias": -2.5, "vol": 1.2 }}
  ]
}}
"""
        response = self.model.generate_content(prompt)
        raw_text = response.text.strip()
        
        # 清理 Markdown 格式
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0].strip()
            
        return json.loads(raw_text)
