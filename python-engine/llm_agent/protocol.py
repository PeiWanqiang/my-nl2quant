from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class ConditionParameter(BaseModel):
    value: Any
    type: str  # "int", "float", "str"
    min: Optional[float] = None
    max: Optional[float] = None

class ExtractedCondition(BaseModel):
    id: str
    name: str
    description: str
    # 👇 新增这一行：用于前端白盒渲染的带插槽字符串
    ui_template: str = Field(default="", description="带有参数插槽的 UI 渲染模板，如 '连续 {n} 天上涨'")
    parameters: Dict[str, ConditionParameter]

class QuantChatRequest(BaseModel):
    user_input: str
    current_conditions: Optional[List[ExtractedCondition]] = []

class QuantChatResponse(BaseModel):
    interaction_state: str  # "CLARIFYING" or "CONFIRMED"
    ai_message: str
    extracted_conditions: List[ExtractedCondition]
    executable_code: Optional[str] = None

class ExecuteRequest(BaseModel):
    code: str