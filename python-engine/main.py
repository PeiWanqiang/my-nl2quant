from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os

from llm_agent.protocol import QuantChatRequest, QuantChatResponse, ExecuteRequest
from llm_agent.gemini_client import negotiate_conditions
from sandbox.ast_validator import validate_code
# from sandbox.subprocess_exec import execute_in_sandbox

app = FastAPI(title="NL2Quant Python Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health_check():
    return {"status": "ok", "service": "python-engine"}

@app.post("/api/v1/chat/negotiate", response_model=QuantChatResponse)
def chat_negotiate(request: QuantChatRequest):
    """
    Handle natural language input and return structural conditions 
    or final execution payload via Gemini.
    """
    try:
        response = negotiate_conditions(request.user_input, request.current_conditions or [])
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/quant/execute")
def execute_quant_code(request: ExecuteRequest):
    """
    Receives generated Pandas code, validates it, and runs in a sandbox.
    """
    try:
        # 1. AST Static validation
        validate_code(request.code)
        
        # 2. Dynamic Sandbox Execution (Mocked for MVP scaffold)
        # result = execute_in_sandbox(request.code)
        
        return {"status": "success", "message": "Code executed safely", "data": []}
    except ValueError as e:
        raise HTTPException(status_code=403, detail=f"Security exception: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
