import os
import json
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

class General:
    """
    模块 C: AI 审计中心 - V13 Cloud (Upgraded to google-genai SDK)
    基于最新 Gemini 模型，对量化矩阵进行最终决策。
    """
    def __init__(self, metrics_file="data/processed/latest_metrics.json", out_dir="data/audit"):
        self.metrics_file = metrics_file
        self.out_dir = out_dir
        os.makedirs(self.out_dir, exist_ok=True)
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        # 优先使用 gemini-3-flash-preview, 降级使用 gemini-2.0-flash
        self.model_id = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

    def audit(self):
        if not os.path.exists(self.metrics_file):
            print(f"❌ 错误: 找不到指标文件 {self.metrics_file}")
            return None
            
        with open(self.metrics_file, 'r') as f:
            metrics = json.load(f)

        prompt = f"""
你现在是 Global-Link V13-Cloud 的“铁血 CIO”。
你必须遵循“技术面触发 + 宏观面验证”的严密逻辑进行审计。你的决策决定了数亿资金的生死，严禁情感波动。

[核心审计逻辑]
1. 技术触发 (权重 1.0 - 绝对优先级): 
   - 必须以 乖离率 (Bias) 和 量比 (Vol Ratio) 为核心触发器。
   - 核心准则: 只有当 Bias < -2.5% 且 Vol Ratio > 1.2 时，才具备“开火”基础。
2. 宏观验证 (权重 0.8 - 背景修正):
   - 参考 汇率稳定性 (CNH)、北向/行业资金流向、全球恐慌度 (VIX) 作为“故事背景”。
   - 用于修正决策信心及 Attack Factor。
3. 成本与压力 (权重 0.5 - 辅助研判):
   - 参考 10Y 国债收益率 (无风险利率压力)、两融杠杆情绪、商品避险情绪 (黄金/原油)。

[决策约束]
- Attack Factor: 范围严格限制在 [0.8, 1.2] 之间。
- 只有技术面达标，才能赋予 > 1.0 的 Attack Factor。
- 若技术面未达标但宏观环境极佳，Attack Factor 应压制在 1.0 以下或维持 WAIT。

[数据完整性风控]
- 你必须首先审计 [数据矩阵] 中的 `macro_health` 和 `technical_matrix` 的完整性。
- 强制指令 1: 若核心技术指标 (technical_matrix) 缺失或为空，Attack Factor 必须强降至 0.8，决策倾向于 WAIT。
- 强制指令 2: 若宏观背景指标 (macro_matrix) 缺失比例 > 30% (即 FAILED 状态超过 4 项)，Attack Factor 严禁超过 1.0，并须在 Rationale 中明确标注“数据不全预警”。
- 实时性审计: 若 `macro_health` 中任何关键项的 `status` 为 FAILED 或 `last_update` 距离当前时间过久，必须在审计报告中体现，并据此调低 Attack Factor 或触发 WAIT 指令。

[多标的择优原则]
若当前数据矩阵中触发了多个符合“开火”条件的 ETF（即 Bias < -2.5% 且 Vol Ratio > 1.2），必须进行二次筛选：
1. 宏观共振优先：利用“宏观背景层”进行二次过滤。优先选择那些与宏观利好共振最强的标的（例如：如果芯片 ETF 触发了，且 `Inflow_Sectors` 行业流入中包含电子/半导体，则其优先级最高）。
2. 技术指标补位：如果宏观共振程度相同，则按技术指标的“极端程度”排序（优先选择 Bias 更低、Vol Ratio 更大的标的）。
3. 淘汰逻辑：必须在 Rationale 中明确解释为什么选择了 Target A 而放弃了 Target B。

[数据矩阵]
{json.dumps(metrics, ensure_ascii=False, indent=2)}

[审计要求]
- 必须返回纯 JSON 对象。
- Rationale (执行综述): 必须展现“铁血 CIO”风格，点名引用数据矩阵中的多维参数（如：'鉴于 CNH 贬值至 X 且 VIX 升至 Y...'），体现从“技术面触底”到“宏观环境对冲”的逻辑闭环。若触发多标的，必须在 Rationale 中解释为什么选择了 Target A 而放弃了 Target B。
- Decision: BUY (开火) / WAIT (观望) / HOLD (持有) / SELL (撤退)。

[输出 JSON 格式]
{{
  "decision": "...",
  "target": "...",
  "attack_factor": 1.0,
  "rationale": "铁血 CIO 审计报告：...",
  "parameters": {{ "stop_loss": -1.5, "stop_profit": 3.0, "time_limit": "4天" }},
  "top_candidates": [
    {{ "code": "...", "name": "...", "bias": 0.0, "vol_ratio": 0.0, "status": "..." }}
  ]
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
            
            res_text = response.text
            res_json = json.loads(res_text)
            
            # 合并元数据供 UI 使用
            res_json['timestamp'] = metrics.get('timestamp', 'unknown')
            res_json['macro_snapshot'] = metrics.get('macro_matrix', {})

            out_path = f"{self.out_dir}/decision_{res_json['timestamp']}.json"
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(res_json, f, ensure_ascii=False, indent=2)
                
            # UI 读取的唯一入口
            with open("data/audit_result.json", 'w', encoding='utf-8') as f:
                json.dump(res_json, f, ensure_ascii=False, indent=2)

            print(f"⚖️ AI 审计完成 ({self.model_id}): {out_path}")
            return res_json
        except Exception as e:
            print(f"❌ AI 审计失败: {e}")
            return None

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
