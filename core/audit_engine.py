import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

class AuditEngine:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')

    def perform_audit(self, market_data):
        prompt = f"""
Return ONLY a JSON object representing the V13 Quant Audit.
No markdown, no text before or after.

[DECISION PYRAMID]
1. Technical (Weight 1.0): MA5 Bias < -2.5% & Volume Ratio > 1.2.
2. Macro (Weight 0.7): US10Y, USD/CNH, VIX.
3. Policy (Weight 0.8): Official decrees.

[MARKET DATA]
{json.dumps(market_data, ensure_ascii=False)}

[JSON STRUCTURE]
{{
  "timestamp": "{market_data['timestamp']}",
  "decision": "BUY/HOLD/SELL/WAIT",
  "target": "Code (Name)",
  "attack_factor": 1.1,
  "rationale": "One sentence summary",
  "parameters": {{
    "entry": 0.0,
    "stop_loss": 0.0,
    "stop_profit": 0.0,
    "time_limit": "4 Days"
  }},
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

if __name__ == "__main__":
    # Test stub
    pass
