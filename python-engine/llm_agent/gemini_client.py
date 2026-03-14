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
# =========================================================================
# System Definitions
# =========================================================================

# 动态加载宏函数库
import inspect
import llm_agent.quant_macros as macros
macro_funcs = [name for name, f in inspect.getmembers(macros, inspect.isfunction) if name.startswith('macro_')]

MODEL_NAME = 'gemini-2.5-flash'

NEGOTIATE_PROMPT = f"""
你是一个名为 NL2Quant 的专业量化策略分析师。你的目标是理解用户的自然语言选股意图，并将其转化为结构化的、可量化的技术或基本面条件。
系统将这些条件在前端渲染为带有滑动条的白盒界面，供用户微调。

当用户提出新的选股需求，或者在已有条件上进行补充时：
1. 深入理解用户的金融意图。
2. 意图拆解与路由：
   - 【标准解析】：对于明确的比较运算（如“市盈率小于30”、“今日涨跌幅大于5%”），提取为具体条件，参数中包含具体的阈值。
   - 【黑话路由 (Known Jargon)】：系统内置了 50 个极其稳定的算法宏。你必须**尽可能将用户的自然语言映射到这些宏上**。
     如果用户的意图匹配以下某个宏，必须将该条件设为黑话算子，在 JSON 中记录。可用的宏列表：
     {macro_funcs}
   - 【未知拦截 (Unknown Jargon)】：遇到无法用数学表达式或现有宏定义的极其主观的词汇（如“主力洗盘”、“庄家吸筹”、“龙头股”），必须阻断生成。将 `interaction_state` 设置为 "CLARIFYING"，并在 `ai_message` 中抛出提问，要求用户给出具体的量化指标（如换手率、振幅）。
3. 对于每个条件，如果包含可变量化的参数（如“N日”、“M倍”），提取出来放到 `parameters` 中，给出默认值和最大最小值。
4. 如果用户明确表示确认、没问题、直接执行等，将 `interaction_state` 设置为 "CONFIRMED"。

当前已有条件树状态：
{{current_conditions_json}}

用户最新输入：
"{{user_input}}"
"""

CODE_GEN_PROMPT = """
你是一个专业的 Python 量化工程师。你的任务是将用户在前端确认的 JSON 条件树，零误差地转化为能在 DuckDB/Pandas 下运行的 Python 脚本。

环境假设与规范：
1. `df` 已经是一个包含 `v_fact_kline` 近 N 天数据的 Pandas DataFrame，并已按 `ts_code` 和 `trade_date` 排序。
2. 我们预置了 `import llm_agent.quant_macros as macros`。
3. 对于 JSON 中的每个条件：
   - 如果它对应了黑话路由的宏（名称以 `macro_` 开头），你直接调用 `macros.macro_xxx(df, **parameters)`。该函数会返回一个 Boolean Series。
   - 如果它是普通的条件，请你用 Pandas 向量化运算实现，同样生成一个 Boolean Series。
4. 将所有条件的 Boolean Series 使用按位与 `&` 拼接起来作为最终掩码。
5. 提取最后一天（最新交易日）掩码为 True 的 `ts_code` 列表，赋值给全局变量 `final_codes`（list类型）。

不要生成读取数据库的代码，沙盒会提前把数据准备好传入 `df` 中。你只需要输出纯净的处理逻辑。

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
    
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=schema,
                temperature=0.2,
            ),
        )
        if response.text is None:
            raise ValueError("Empty response text from Gemini")
        return json.loads(response.text)
    except Exception as e:
        logger.error(f"Failed to generate JSON: {e}")
        raise

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
    if text is None:
        return ""
        
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
