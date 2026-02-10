import os
import json
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

class General:
    """
    模块 C: AI 决策审计中心 - V17 (特征全貌审计)
    基于最新 Gemini 模型，对量化特征矩阵进行深度审计与策略输出。
    """
    def __init__(self, metrics_file="data/processed/latest_metrics.json", out_dir="data/audit"):
        self.metrics_file = metrics_file
        self.out_dir = out_dir
        os.makedirs(self.out_dir, exist_ok=True)
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        self.model_id = os.getenv("GEMINI_MODEL", "gemini-3-flash")

    def audit(self):
        if not os.path.exists(self.metrics_file):
            print(f"❌ 错误: 找不到指标文件 {self.metrics_file}")
            return None
            
        with open(self.metrics_file, 'r') as f:
            metrics = json.load(f)

        data_time = metrics.get('timestamp', 'unknown')

        # 优化 Token: 仅保留失败的健康状态
        if 'macro_health' in metrics:
            health = metrics['macro_health']
            failed_keys = {k: v for k, v in health.items() if v.get('status') != 'SUCCESS'}
            if failed_keys:
                metrics['macro_health_alerts'] = failed_keys
            del metrics['macro_health']

        prompt = f"""
你现在是 Global-Link V14-Cloud 的“首席策略官 (CSO)”。
你必须遵循“技术面触发 + 宏观面特征验证”的严密逻辑进行审计。

[关键信息]
数据采集时刻 (TIMESTAMP): {data_time}
(你必须在 rationale 的开头明确提到这个数据时间，例如：“基于 {data_time} 的实时行情数据...”)

[V14 宏观特征引擎说明]
现在的 macro_matrix 中，每个指标都包含以下多维特征：
1. value: 实时数值。
2. p_20d / p_60d: 该指标在过去 20/60 个采样点的历史分位（0-100）。
3. z_score: 偏离度。
4. slope: 5日趋势斜率。

[数据完整性说明]
如果 macro_health_alerts 中包含某个指标，说明该指标采集失败，请说明其对决策的影响。

[核心审计逻辑]
1. 技术触发: 乖离率 (Bias) < -2.5% 且 量比 (Vol Ratio) > 1.2。
2. 宏观验证: 利用历史分位和趋势斜率判断宏观共振。
3. 风险控制: 风险敞口系数 (Attack Factor): [0.8, 1.2]。

[数据矩阵]
{json.dumps(metrics, ensure_ascii=False, indent=2)}

[审计要求]
- 必须返回纯 JSON。
- rationale 必须展现专业策略官风格，开头必须声明数据时间戳，并点名引用关键特征进行论证。
- Decision: BUY / WAIT / HOLD / SELL。

[输出 JSON 格式]
{{
  "decision": "...",
  "target": "...",
  "attack_factor": 1.0,
  "rationale": "基于 {data_time} 的数据，审计报告显示...",
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
            res_json['timestamp'] = data_time
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
