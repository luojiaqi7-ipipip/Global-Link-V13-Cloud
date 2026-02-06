# 🛰️ Global-Link V13 Cloud: 架构设计蓝图 (Master Plan)

## 1. 核心设计哲学
- **去状态化 (Stateless)**: 本地电脑仅作为“发射架”，所有逻辑、数据流、评审均在云端闭环。
- **模块化解耦 (Modular)**: 仿照金融机构系统，将抓取、计算、审计、分发彻底分离。
- **透明审计 (Audit Trail)**: 每一轮抓取的数据必须持久化为 JSON 存档，而非仅在内存中流转。

## 2. 系统模块分工 (The Four Pillars)

### A. 情报获取引擎 (The Harvester)
- **职责**: 抓取 A 股实时行情、历史 K 线、全球宏观（汇率/利率/资金流）、政策公文。
- **输入**: 监测池代码列表。
- **输出**: `data/raw/market_snap_YYYYMMDD_HHMM.json` (原始脱水情报)。

### B. 逻辑计算引擎 (The Quant-Lab)
- **职责**: 计算 MA5 Bias、量比、波动率、变化率等衍生指标。
- **输入**: `data/raw/` 中的原始 JSON。
- **输出**: `data/processed/metrics_YYYYMMDD_HHMM.json` (标准量化矩阵)。

### C. AI 审计中心 (The General)
- **职责**: 基于 Gemini 3 Flash，按照“权重金字塔”对量化矩阵进行最终决策。
- **输入**: `metrics.json` + `SOP_Rules`。
- **输出**: `data/audit/decision_YYYYMMDD_HHMM.json` (包含 Attack Factor 和 Rationale)。

### D. 可视化看板 (The Dashboard)
- **职责**: 渲染最新的审计决策与全景地图。
- **技术栈**: Streamlit + 历史轨迹追踪。

## 3. 云端工作流 (SOP 自动化节拍)
1. **唤醒**: GitHub Actions 定时触发（09:15 / 13:30 / 15:15）。
2. **抓取**: 执行 `core/harvester.py`。
3. **存档**: **强制**执行 `git add data/raw/*.json` 保证数据永不丢失。
4. **决策**: 执行 `core/general.py` 调用 Gemini。
5. **发布**: 自动推送至 Streamlit 渲染层。

---
*老板，这是我反思后的顶层架构。如果认可，我将立即按照此蓝图重构仓库目录并分步实现。*
