import os
import resource
import time
import signal
import psutil
from loguru import logger

def execute_in_sandbox(code: str) -> list:
    """
    Executes the validated Python code in a restricted namespace.
    For an absolute production sandbox, we would spawn a strict subprocess.
    Here we use exec() with a restricted globals dictionary.
    """
    logger.info("Executing code in sandbox environment...")
    
    # Restrict memory using OS capabilities (Works on MacOS/Linux)
    # Set soft limit to ~1GB, hard limit to ~1.5GB
    try:
        mem_limit = 1024 * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (mem_limit, int(mem_limit * 1.5)))
    except Exception as e:
        logger.warning(f"Could not set resource limits: {e}")

    # Set up the restricted environment
    import pandas as pd
    import duckdb
    import llm_agent.quant_macros as macros
    from data_sync.common import get_duckdb_conn
    
    conn = get_duckdb_conn()
    
    # We only inject the necessary recent data to prevent OOM
    # 6 months of data is usually enough for most macro calculations
    query = """
    SELECT * 
    FROM v_fact_kline 
    WHERE trade_date >= CURRENT_DATE - INTERVAL 180 DAY
    ORDER BY ts_code, trade_date
    """
    
    try:
        logger.info("Loading recent data context for sandbox...")
        df = conn.execute(query).df()
    except Exception as e:
        raise ValueError(f"Failed to fetch context data: {e}")

    # Define the namespace
    restricted_globals = {
        "__builtins__": {
            "print": print,
            "len": len,
            "range": range,
            "int": int,
            "float": float,
            "getattr": getattr,
            "bool": bool,
            "str": str,
            "list": list,
            "dict": dict,
            "__import__": __import__ # Required for the import statements in code
        },
        "pd": pd,
        "pandas": pd,
        "duckdb": duckdb,
        "macros": macros,
        "df": df,
        "final_codes": [] # The target variable
    }
    
    # Remove signal alarm since FastAPI handles requests in threads where signal is not allowed
    # A true sandbox would use a separate process, but for MVP we use a simple try-except
    try:
        exec(code, restricted_globals)
        
        result = restricted_globals.get("final_codes", [])
        logger.info(f"Sandbox execution complete. Found {len(result)} stocks.")
        return result
        
    except Exception as e:
        logger.error(f"Sandbox execution failed: {e}")
        raise ValueError(f"Runtime error: {e}")
