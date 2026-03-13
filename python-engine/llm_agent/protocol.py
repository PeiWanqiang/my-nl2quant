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
