import os
import json
import inspect
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from loguru import logger

# 引入我们在 protocol.py 中定义的数据契约
from .protocol import QuantChatResponse, ExtractedCondition, ConditionParameter
from .model_client import get_model_client, BaseModelClient
from sandbox.ast_validator import validate_code

load_dotenv()

# 初始化通用模型客户端 (根据环境变量 MODEL_PROVIDER 自动选择)
# 可选值: gemini, deepseek, minimax
# 也可以在环境变量中指定具体模型，如: MODEL_PROVIDER=gemini-3.1-pro
model_client: BaseModelClient = get_model_client(os.environ.get("MODEL_PROVIDER", "gemini"))
logger.info(f"Model client initialized: {type(model_client).__name__}")

# =========================================================================
# 1. 动态装载底层宏函数库 (Macro Introspection)
# =========================================================================
import llm_agent.quant_macros as macros

def _load_macro_signatures() -> str:
    """动态扫描 quant_macros.py，提取所有宏函数的签名和注释，喂给大模型做上下文"""
    macro_info_list = []
    for name, func in inspect.getmembers(macros, inspect.isfunction):
        if name.startswith('macro_'):
            sig = inspect.signature(func)
            # 提取除 df 之外的所有可调参参数
            params = [p.name for p in sig.parameters.values() if p.name != 'df']
            # 提取函数文档注释 (Docstring)，如果没写注释则使用兜底提示
            doc = inspect.getdoc(func) or "未提供描述，请按字面意思理解"
            
            macro_info_list.append(f"- 【函数名】: {name}({', '.join(params)})\n  【功  能】: {doc}")
    
    return "\n".join(macro_info_list)

MACRO_FUNCS_CONTEXT = _load_macro_signatures()


# =========================================================================
# 2. 系统提示词定义 (System Prompts)
# =========================================================================

NEGOTIATE_PROMPT_TEMPLATE = """
你是一个名为 NL2Quant 的顶级量化策略架构师。你的目标是理解用户的自然语言选股意图，并将其转化为结构化的 JSON 条件树。

【意图路由规则】：
1. 优先映射宏库：系统内置了极其稳定的算法宏。如果用户的意图匹配以下宏，必须优先使用它们：
{MACRO_FUNCS_CONTEXT}
2. 标准指标解析：如果不是复杂的宏，而是基础的比较（如"市盈率小于30"），直接提取条件。
3. 拦截主观黑话：遇到无法量化的词汇（如"主力洗盘"、"龙头股"），将 interaction_state 设为 "CLARIFYING"，并在 ai_message 中要求用户给出具体指标。

【前端 UI 渲染协议 - 极其重要】：
你需要为每个提取出的条件生成 `ui_template` 和 `parameters`。
- `ui_template`: 一句大白话，其中可微调的参数必须用大括号包裹。例如："股票价格连续 $n$ 天上涨" 或 "市盈率小于 $threshold$"。
- `parameters`: 一个数组，包含模板中所有占位符的具体属性（name, value, type, min, max）。

【强制输出格式】：
你必须且只能输出如下 JSON 格式，不要带有 ```json 的 Markdown 标记：
{{
  "interaction_state": "CONFIRMED", // 或者 "CLARIFYING"
  "ai_message": "我已为您提取了以下条件...",
  "extracted_conditions": [
    {{
      "id": "cond_01",
      "name": "连续上涨",
      "description": "调用宏 macro_31_consecutive_up",
      "ui_template": "股票价格连续 $n$ 天上涨",
      "parameters": [
        {{"name": "n", "value": 3, "type": "int", "min": 1, "max": 10}}
      ]
    }}
  ]
}}

当前已存在的条件树：
__CURRENT_CONDITIONS__

用户最新输入：
"__USER_INPUT__"
"""

NEGOTIATE_PROMPT = NEGOTIATE_PROMPT_TEMPLATE.format(MACRO_FUNCS_CONTEXT=MACRO_FUNCS_CONTEXT)

CODE_GEN_PROMPT = """
你是一个严谨的 Python 量化执行引擎。请将用户确认的 JSON 条件树，零误差地翻译为 Pandas 代码。

【重要：数据Schema - 必须严格遵守】：
输入的 df 包含以下列：
- ts_code: 股票代码 (如 '000001')
- trade_date: 交易日期 (datetime格式)
- year: 日历年度 (整数，如 2023, 2024)
- net_profit: 年度净利润 (浮点数，来自财务数据)
其他列可能还包括：open, high, low, close, volume 等标准K线字段。

【执行环境与安全沙盒规范】：
1. 预置导入：`import llm_agent.quant_macros as macros` 已经存在，你可以直接调用。
2. 绝对闭包：你必须输出一个名为 `apply_strategy(df)` 的函数，接收 df，返回 None。必须使用全局变量 `final_codes` 来返回结果。
3. 可用宏白名单：
{macro_funcs_info}
4. 逻辑组合：使用 Pandas 的按位逻辑符 `&` 组合多个条件的掩码 (Boolean Series)。

【标准代码输出模板】：
```python
def apply_strategy(df):
    import pandas as pd
    import numpy as np
    
    # 条件1：宏调用
    cond_1 = macros.macro_31_consecutive_up(df, n=3)
    # 条件2：基础 Pandas 向量化
    cond_2 = df['pe_ttm'] < 30
    
    # 使用 & 组合多个条件
    final_mask = cond_1 & cond_2
    
    # 强制只返回最新一个交易日符合条件的股票，避免买入历史股票
    latest_date = df['trade_date'].max()
    result = df[final_mask & (df['trade_date'] == latest_date)]
    
    # 必须使用 global 声明并设置 final_codes
    global final_codes
    final_codes = result['ts_code'].tolist()

条件列表：
{conditions_json}
"""


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
                    "id": {"type": "STRING", "description": "Unique ID for this condition"},
                    "name": {"type": "STRING", "description": "Human readable name"},
                    "description": {"type": "STRING", "description": "Detailed description"},
                    "ui_template": {
                        "type": "STRING", 
                        "description": "前端渲染模板。必须将可变参数用大括号包裹。例如：'股票价格连续 {n} 天上涨' 或 '今日突破 {n} 日均线'"
                    },
                    "parameters": {
                        "type": "ARRAY",
                        "description": "List of parameters. IMPORTANT: Extract macro arguments (n, m, etc.) as parameters with defaults.",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "name": {"type": "STRING", "description": "Parameter name (e.g., 'n', 'm')"},
                                "value": {"type": "NUMBER", "description": "Extracted default value"},
                                "type": {"type": "STRING", "description": "'int' or 'float'"},
                                "min": {"type": "NUMBER", "description": "Minimum allowed value"},
                                "max": {"type": "NUMBER", "description": "Maximum allowed value"}
                            },
                            "required": ["name", "value", "type"]
                        }
                    }
                },
                "required": ["id", "name", "description", "ui_template", "parameters"]
            }
        }
    },
    "required": ["interaction_state", "ai_message", "extracted_conditions"]
}
def _generate_json(prompt: str) -> dict:
    logger.info(f"Calling model for structured JSON generation")
    
    try:
        result = model_client.generate_json(
            prompt=prompt,
            temperature=0.2,
            json_schema=schema
        )
        logger.info(f"Model raw response: {result}")
        return result
    except Exception as e:
        logger.error(f"Failed to generate JSON: {e}")
        raise

def generate_code(conditions: list, max_retries: int = 3) -> str:
    """
    Generate Pandas code from conditions with retry logic.
    """
    last_error = None
    
    # Log the final conditions
    cond_list = [c.model_dump() if hasattr(c, 'model_dump') else c for c in conditions]
    logger.info(f"=== FINAL QUANT CONDITIONS ===")
    logger.info(f"Conditions: {json.dumps(cond_list, ensure_ascii=False, indent=2)}")
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Generating Pandas/DuckDB code via model (attempt {attempt + 1})...")
            prompt = CODE_GEN_PROMPT.format(
                conditions_json=json.dumps(cond_list, ensure_ascii=False),
                macro_funcs_info=MACRO_FUNCS_CONTEXT
            )
            
            text = model_client.generate_content(
                prompt=prompt,
                temperature=0.1,
            )
            
            if "```python" in text:
                code = text.split("```python")[1].split("```")[0].strip()
            elif "```" in text:
                code = text.split("```")[1].split("```")[0].strip()
            else:
                code = text.strip()
            
            # Log the generated code
            logger.info(f"=== GENERATED PANDAS CODE ===")
            logger.info(code)
            
            # Try to validate the code before returning
            try:
                validate_code(code)
                return code
            except ValueError as e:
                logger.warning(f"Code validation failed on attempt {attempt + 1}: {e}")
                last_error = e
                # Add error context to prompt for retry
                prompt += f"\n\n注意：上次生成的代码有语法/安全错误：{str(e)}，请修正后重新生成。"
                continue
                
        except Exception as e:
            logger.warning(f"Code generation failed on attempt {attempt + 1}: {e}")
            last_error = e
            continue
    
    raise ValueError(f"Failed to generate valid code after {max_retries} attempts: {last_error}")

def negotiate_conditions(user_input: str, current_conditions: list) -> QuantChatResponse:
    try:
        current_cond_dicts = [c.model_dump() if hasattr(c, 'model_dump') else c for c in current_conditions]
        prompt = NEGOTIATE_PROMPT.replace("__CURRENT_CONDITIONS__", json.dumps(current_cond_dicts, ensure_ascii=False, indent=2)).replace("__USER_INPUT__", user_input)
        
        # 1. 意图解析 (NL -> JSON)
        raw_json = _generate_json(prompt)
        
        # Build the structured Pydantic model for internal handling
        conds = []
        logger.info(f"Raw JSON from Gemini: {raw_json}")
        for c in raw_json.get("extracted_conditions", []):
            params = {}
            # Handle both array format (new) and dict format (legacy)
            raw_params = c.get("parameters", {})
            logger.info(f"Processing condition: {c.get('name')}, params type: {type(raw_params)}, value: {raw_params}")
            if isinstance(raw_params, list):
                for p in raw_params:
                    if isinstance(p, str):
                        params[p] = ConditionParameter(value=1, type="int", min=1, max=10)
                    else:
                        params[p["name"]] = ConditionParameter(
                        value=p.get("value"),
                        type=p.get("type", "int"),
                        min=p.get("min"),
                        max=p.get("max")
                    )
            else:
                for k, v in raw_params.items():
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
        # 只有当用户明确确认时（前端会发带有"确认"的字符串），才去执行耗时的代码生成逻辑
        if "确认" in user_input:
            result.interaction_state = "CONFIRMED" # Force it if user says confirm
            code = generate_code(result.extracted_conditions)
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
