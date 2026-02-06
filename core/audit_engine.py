import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

class AuditEngine:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        genai.configure(api_key=self.api_key)
        # Using Gemini 1.5 Flash (closest production equivalent to 'Gemini 3 Flash')
        # We can switch to 'gemini-2.0-flash-exp' for the absolute latest
        self.model = genai.GenerativeModel('gemini-1.5-flash')

    def perform_audit(self, market_data):
        prompt = f"""
[SYSTEM ROLE]
You are the Chief Investment Officer for Global-Link A-Quant V13. 
Perform a cold-blooded, high-stakes audit using the "Weight Pyramid".

[DECISION PYRAMID]
1. Technical (Weight 1.0): MA5 Bias < -2.5% & Volume Ratio > 1.2.
2. Macro (Weight 0.7): US10Y, USD/CNH, VIX, Two-Finance.
3. Policy (Weight 0.8): Official decrees for tech/innovation.

[MARKET PULSE: TECHNICAL]
{json.dumps(market_data['technical'], ensure_ascii=False, indent=2)}

[MARKET PULSE: MACRO]
{json.dumps(market_data['macro'], ensure_ascii=False, indent=2)}

[INSTRUCTION]
Return ONLY a JSON object. No markdown.
- Determine "Attack Factor" (0.8-1.2).
- Final Decision: BUY/WAIT/SELL/HOLD.
- Parameters: Target, Stop-Loss (1.5%*Factor), Stop-Profit (3.0%*Factor), Time-Limit (4 Days).

[JSON STRUCTURE]
{{
  "timestamp": "{market_data['timestamp']}",
  "decision": "...",
  "target": "...",
  "attack_factor": 1.1,
  "rationale": "...",
  "parameters": {{
    "entry": 0.0,
    "stop_loss": 0.0,
    "stop_profit": 0.0,
    "time_limit": "4 Days"
  }},
  "macro": {json.dumps(market_data['macro'])},
  "top_candidates": [
     {{ "code": "...", "bias": -2.5, "vol": 1.2 }}
  ]
}}
"""
        response = self.model.generate_content(prompt)
        raw_text = response.text.strip()
        
        # Clean potential markdown
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0].strip()
            
        return json.loads(raw_text)
