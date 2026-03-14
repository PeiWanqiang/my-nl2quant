from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import traceback
import pandas as pd

from llm_agent.protocol import QuantChatRequest, QuantChatResponse, ExecuteRequest
from llm_agent.gemini_client import negotiate_conditions
from sandbox.ast_validator import validate_code
from sandbox.subprocess_exec import execute_in_sandbox
from data_sync.common import get_duckdb_conn

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
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/quant/execute")
def execute_quant_code(request: ExecuteRequest):
    """
    Receives generated Pandas code, validates it, and runs in a sandbox.
    """
    try:
        # 1. AST Static validation
        validate_code(request.code)
        
        # 2. Dynamic Sandbox Execution
        matched_stocks = execute_in_sandbox(request.code)
        
        # 3. Fetch detailed stock info using DuckDB
        data = []
        if matched_stocks:
            target_codes = matched_stocks[:50] # Limit to 50 for UI performance
            codes_str = ", ".join([f"'{str(c).zfill(6)}'" for c in target_codes])
            
            conn = get_duckdb_conn()
            
            # We join the stock list with the latest kline data to get name, price, and pct_change
            query = f"""
            WITH LatestKline AS (
                SELECT ts_code, close as price, pct_change as change,
                       ROW_NUMBER() OVER(PARTITION BY ts_code ORDER BY trade_date DESC) as rn
                FROM v_fact_kline
                WHERE ts_code IN ({codes_str})
            )
            SELECT l.ts_code as code, d.stock_name as name, l.price, l.change
            FROM LatestKline l
            LEFT JOIN read_parquet('data/dim/dim_stock_list.parquet') d ON l.ts_code = d.ts_code
            WHERE l.rn = 1
            """
            try:
                result_df = conn.execute(query).df()
                
                # Format to dictionary
                for _, row in result_df.iterrows():
                    price_val = f"{row['price']:.2f}" if pd.notnull(row['price']) else "N/A"
                    change_val = f"{row['change']:.2f}%" if pd.notnull(row['change']) else "N/A"
                    # Handle case where stock_name might be missing in dimension table
                    name_val = row['name'] if pd.notnull(row['name']) else "Unknown"
                    
                    data.append({
                        "code": row['code'],
                        "name": name_val,
                        "price": price_val,
                        "change": change_val
                    })
            except Exception as e:
                # Fallback if query fails
                print(f"Failed to fetch detailed info: {e}")
                data = [{"code": code, "name": "Unknown", "price": "N/A", "change": "N/A"} for code in target_codes]
                
        return {"status": "success", "message": f"Found {len(matched_stocks)} stocks", "data": data}
        
    except ValueError as e:
        raise HTTPException(status_code=403, detail=f"Security/Runtime exception: {str(e)}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
