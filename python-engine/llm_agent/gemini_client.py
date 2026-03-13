import os
from google import genai
from dotenv import load_dotenv
from .protocol import QuantChatResponse, ExtractedCondition, ConditionParameter

load_dotenv()

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

def negotiate_conditions(user_input: str, current_conditions: list) -> QuantChatResponse:
    # MVP Stub: In reality, we'd craft a complex prompt here with Pydantic output parsing.
    # For now, let's return a dummy structure representing the 'CLARIFYING' state.
    
    if "确认" in user_input or "没问题" in user_input:
        # Mocking the confirmation state
        return QuantChatResponse(
            interaction_state="CONFIRMED",
            ai_message="好的，条件已确认，正在为您生成量化代码并投入沙盒执行...",
            extracted_conditions=current_conditions,
            executable_code="""
import pandas as pd
import duckdb
# Code to execute in sandbox...
print("Hello from Sandbox!")
"""
        )
        
    return QuantChatResponse(
        interaction_state="CLARIFYING",
        ai_message=f"您输入了：'{user_input}'。我理解您可能在寻找某个特征。请问您需要调整以下指标参数吗？",
        extracted_conditions=[
            ExtractedCondition(
                id="cond_001",
                name="放量特征",
                description="今日成交量大于过去N日均量的M倍",
                parameters={
                    "N_days": ConditionParameter(value=5, type="int", min=1, max=60),
                    "M_times": ConditionParameter(value=2.0, type="float", min=1.0, max=10.0)
                }
            )
        ]
    )
