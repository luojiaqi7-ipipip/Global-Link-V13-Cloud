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

[警告]
严禁虚构数据。如果某项宏观指标显示为“获取失败”，请在审计中剔除该因子权重，不要基于猜测做决定。

[决策金字塔]
1. 技术指标 (权重 1.0): 优先考虑 MA5 乖离率 < -2.5% 且 量比 > 1.2 的标的。
2. 宏观环境 (权重 0.7): 汇率、实际波动率、两融余额。
3. 政策导向 (权重 0.8): 官方硬科技支持政策。

[核心 ETF 监测池技术截面]
{json.dumps(market_data['technical'], ensure_ascii=False, indent=2)}

[真实宏观经济指标]
{json.dumps(market_data['macro'], ensure_ascii=False, indent=2)}

[审计指令]
1. 必须返回且仅返回一个 JSON 对象。
2. 计算风险调节系数 (Attack Factor, 0.8-1.2)。
3. 给出投资决策: BUY (买入) / WAIT (观望) / SELL (卖出) / HOLD (持有)。
4. 计算执行参数: 止损线 (1.5% * Factor), 止盈线 (3.0% * Factor)。
5. top_candidates 展示偏离度前 5 名。

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
        
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0].strip()
            
        return json.loads(raw_text)
