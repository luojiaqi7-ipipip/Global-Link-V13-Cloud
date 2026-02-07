import os
import json
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

class General:
    """
    模块 C: AI 决策审计中心 - V14 (特征全貌审计)
    基于最新 Gemini 模型，对量化特征矩阵进行深度审计与策略输出。
    """
    def __init__(self, metrics_file="data/processed/latest_metrics.json", out_dir="data/audit"):
        self.metrics_file = metrics_file
        self.out_dir = out_dir
        os.makedirs(self.out_dir, exist_ok=True)
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        self.model_id = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

    def audit(self):
        if not os.path.exists(self.metrics_file):
            print(f"❌ 错误: 找不到指标文件 {self.metrics_file}")
            return None
            
        with open(self.metrics_file, 'r') as f:
            metrics = json.load(f)

        prompt = f"""
你现在是 Global-Link V14-Cloud 的“首席策略官 (CSO)”。
你必须遵循“技术面触发 + 宏观面特征验证”的严密逻辑进行审计。你的决策决定了投资组合的配置方向。

[V14 宏观特征引擎说明]
现在的 macro_matrix 中，每个指标都包含以下多维特征：
1. value: 实时数值。
2. p_20d / p_60d: 该指标在过去 20/60 个采样点的历史分位（0-100）。
   - p_20d > 80: 指标处于近期高位。
   - p_20d < 20: 指标处于近期低位。
3. z_score: 偏离度。反映指标相对于均线的偏离程度。
4. slope: 5日趋势斜率。反映指标近期的变动趋势。

你必须利用这些特征判断宏观环境是在“改善”还是“恶化”，而不仅仅看绝对值。

[核心审计逻辑]
1. 技术触发: 
   - 核心准则: 只有当 乖离率 (Bias) < -2.5% 且 量比 (Vol Ratio) > 1.2 时，才具备“策略入场”基础。
2. 宏观验证:
   - 利用历史分位和趋势斜率判断宏观共振。
   - 例如：汇率 (CNH) 绝对值虽高，但若 slope 为负（升值趋势）且 p_20d 从高位回落，则视为利好。
3. 风险控制:
   - 风险敞口系数 (Attack Factor): 限制在 [0.8, 1.2] 之间。
   - 只有技术面与宏观面形成共振，才能赋予 > 1.0 的风险敞口。

[数据矩阵]
{json.dumps(metrics, ensure_ascii=False, indent=2)}

[审计要求]
- 必须返回纯 JSON 对象。
- Rationale (策略综述): 展现专业策略官风格，点名引用 p_20d 和 slope 等关键特征进行论证。
- Decision: BUY (策略买入) / WAIT (观望等待) / HOLD (坚定持有) / SELL (策略卖出)。

[输出 JSON 格式]
{{
  "decision": "...",
  "target": "...",
  "attack_factor": 1.0,
  "rationale": "策略决策审计报告：鉴于...",
  "parameters": {{ "stop_loss": -1.5, "stop_profit": 3.0, "time_limit": "4天" }},
  "top_candidates": []
}}
"""
        try:
            response = self.client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type='application/json',
                    temperature=0.2,
                )
            )
            
            res_json = json.loads(response.text)
            res_json['timestamp'] = metrics.get('timestamp', 'unknown')
            res_json['macro_snapshot'] = metrics.get('macro_matrix', {})

            out_path = f"{self.out_dir}/decision_{res_json['timestamp']}.json"
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(res_json, f, ensure_ascii=False, indent=2)
                
            with open("data/audit_result.json", 'w', encoding='utf-8') as f:
                json.dump(res_json, f, ensure_ascii=False, indent=2)

            print(f"⚖️ AI 策略审计完成: {out_path}")
            return res_json
        except Exception as e:
            print(f"❌ AI 策略审计失败: {e}")
            return None

if __name__ == "__main__":
    general = General()
    general.audit()
