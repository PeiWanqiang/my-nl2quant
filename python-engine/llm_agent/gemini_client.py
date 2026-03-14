import os
import json
from google import genai
from google.genai import types
from dotenv import load_dotenv
from .protocol import QuantChatResponse, ExtractedCondition, ConditionParameter
from loguru import logger
from typing import Any

load_dotenv()

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

# 使用 Gemini 2.5 Flash 或 Pro 来保证结构化输出的稳定性，由于我们是测试所以用 2.5-flash 会很快
MODEL_NAME = 'gemini-2.5-flash'

NEGOTIATE_PROMPT = """
你是一个名为 NL2Quant 的专业量化策略分析师。你的目标是理解用户的自然语言选股意图，并将其转化为结构化的、可量化的技术或基本面条件。
系统将这些条件在前端渲染为带有滑动条的白盒界面，供用户微调。

当用户提出新的选股需求，或者在已有条件上进行补充时：
1. 深入理解用户的金融意图。
2. 将意图拆解为一个个独立的 `ExtractedCondition` 对象。
3. 对于每个条件，如果包含可变量化的参数（如“N日”、“M倍”、“大于某个阈值”），提取出来放到 `parameters` 中，并给出合理的默认值、类型、最小值和最大值。注意 type 只能是 'int', 'float', 'str'。
4. 如果用户的意图非常模糊，或者你认为需要进一步确认参数，将 `interaction_state` 设置为 "CLARIFYING"，并在 `ai_message` 中礼貌地解释你的理解，并提示用户可以在界面上微调参数。
5. 如果用户明确表示确认、没问题、直接执行等，将 `interaction_state` 设置为 "CONFIRMED"。

当前已有条件树状态：
{current_conditions_json}

用户最新输入：
"{user_input}"
"""

CODE_GEN_PROMPT = """
你是一个专业的 Python 量化工程师。你的任务是基于结构化的选股条件，生成能在 DuckDB/Pandas 环境下执行的代码。
环境假设：
- 全市场日线及基本面数据已经通过 DuckDB 注册为名为 `v_fact_kline` 的视图。
- 视图包含字段: ts_code, trade_date, open, close, high, low, vol, amount, turnover_rate, pct_change, change, amplitude, total_mv, pe_ttm, pb, is_st.
- 你被允许使用 duckdb, pandas, pandas_ta 库。

你的任务是：
1. 编写一个完整的 Python 脚本。
2. 由于数据量极大，建议你先利用 duckdb 的原生 SQL 算子进行初步的指标计算和数据过滤，尽量不要把所有数据拉进 Pandas 计算。例如，使用 DuckDB 的移动窗口函数计算均线。
3. 你的代码不需要额外处理数据库连接等。我们会在沙盒环境中提供一个 `conn` 变量 (代表 duckdb connection)，或者你代码里写 `import duckdb\nconn = duckdb.connect()` 也可以。
4. 将最终符合你逻辑过滤后的最新一天的股票列表 DataFrame 赋值给变量 `final_df`。系统会从沙盒里提取它。

基于以下已经确认的结构化条件，生成纯粹的 Python 代码。请确保代码不要有安全漏洞，不要使用 eval/exec/os.system。

条件列表：
{conditions_json}
"""

def _generate_json(prompt: str) -> dict:
    logger.info(f"Calling Gemini for structured JSON generation")
    
    # We define a simpler JSON schema to ensure stability
    schema = {
        "type": "OBJECT",
        "properties": {
            "interaction_state": {"type": "STRING", "description": "'CLARIFYING' or 'CONFIRMED'"},
            "ai_message": {"type": "STRING", "description": "Response to user"},
            "extracted_conditions": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "id": {"type": "STRING"},
                        "name": {"type": "STRING"},
                        "description": {"type": "STRING"},
                        "parameters": {
                            "type": "OBJECT",
                            "description": "Key is parameter name, value is the parameter object. e.g. {'N_days': {'value': 5, 'type': 'int'}}",
                        }
                    },
                    "required": ["id", "name", "description", "parameters"]
                }
            }
        },
        "required": ["interaction_state", "ai_message", "extracted_conditions"]
    }
    
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=schema,
            temperature=0.2,
        ),
    )
    return json.loads(response.text)

def _generate_code(conditions: list) -> str:
    logger.info("Generating Pandas/DuckDB code via Gemini...")
    cond_list = [c.model_dump() if hasattr(c, 'model_dump') else c for c in conditions]
    prompt = CODE_GEN_PROMPT.format(conditions_json=json.dumps(cond_list, ensure_ascii=False))
    
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.1,
        ),
    )
    
    text = response.text
    if "```python" in text:
        code = text.split("```python")[1].split("```")[0].strip()
    elif "```" in text:
        code = text.split("```")[1].split("```")[0].strip()
    else:
        code = text.strip()
    return code

def negotiate_conditions(user_input: str, current_conditions: list) -> QuantChatResponse:
    try:
        current_cond_dicts = [c.model_dump() if hasattr(c, 'model_dump') else c for c in current_conditions]
        prompt = NEGOTIATE_PROMPT.format(
            current_conditions_json=json.dumps(current_cond_dicts, ensure_ascii=False, indent=2),
            user_input=user_input
        )
        
        # 1. 意图解析 (NL -> JSON)
        raw_json = _generate_json(prompt)
        
        # Build the structured Pydantic model for internal handling
        conds = []
        for c in raw_json.get("extracted_conditions", []):
            params = {}
            for k, v in c.get("parameters", {}).items():
                params[k] = ConditionParameter(**v)
            conds.append(ExtractedCondition(
                id=c["id"],
                name=c["name"],
                description=c["description"],
                parameters=params
            ))
            
        result = QuantChatResponse(
            interaction_state=raw_json["interaction_state"],
            ai_message=raw_json["ai_message"],
            extracted_conditions=conds,
            executable_code=None
        )
        
        # 2. 如果已确认，执行代码生成 (JSON -> Code)
        if result.interaction_state == "CONFIRMED" or "确认" in user_input:
            result.interaction_state = "CONFIRMED" # Force it if user says confirm
            code = _generate_code(result.extracted_conditions)
            result.executable_code = code
            
        return result
        
    except Exception as e:
        logger.error(f"Gemini API Error: {e}")
        # Fallback for MVP
        return QuantChatResponse(
            interaction_state="CLARIFYING",
            ai_message=f"抱歉，解析您的输入遇到困难。请确保输入的自然语言清晰。错误: {e}",
            extracted_conditions=current_conditions,
            executable_code=None
        )
