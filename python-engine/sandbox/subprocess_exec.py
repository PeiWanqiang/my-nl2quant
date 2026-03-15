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
    
    # Simple query avoiding heavy joins that cause hangs
    # Join only dimension to avoid Cartesian products
    query = """
    WITH Kline AS (
        SELECT * 
        FROM v_fact_kline 
        WHERE trade_date >= '2023-01-01'
    )
    SELECT k.*
    FROM Kline k
    ORDER BY k.ts_code, k.trade_date
    """
    
    try:
        logger.info("Loading recent data context for sandbox...")
        df = conn.execute(query).df()
        
        # Load finance data as a separate frame to avoid massive joins in DuckDB
        # We can pass it into the sandbox if the LLM generated code needs it
        finance_df = conn.execute("SELECT ts_code, report_date, net_profit FROM read_parquet('data/fact_finance/*.parquet') WHERE report_date >= '2022-01-01'").df()
        
        # We merge them in Pandas which is often safer/clearer for memory if sizes are reasonable
        df['year'] = pd.to_datetime(df['trade_date']).dt.year
        finance_df['year'] = pd.to_datetime(finance_df['report_date']).dt.year
        
        # Take the latest report per year per stock
        finance_annual = finance_df.groupby(['ts_code', 'year'])['net_profit'].last().reset_index()
        
        # Merge into main df
        df = pd.merge(df, finance_annual, on=['ts_code', 'year'], how='left')
        
    except Exception as e:
        logger.error(f"Failed to fetch context data: {e}")
        # Fallback to extremely simple query without finance
        try:
             df = conn.execute("SELECT * FROM v_fact_kline WHERE trade_date >= CURRENT_DATE - INTERVAL 180 DAY ORDER BY ts_code, trade_date").df()
             df['net_profit'] = 0 # Dummy to avoid crashes
        except Exception as e2:
             raise ValueError(f"Failed to fetch context data: {e2}")

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
        
        # Auto-call apply_strategy if defined
        if 'apply_strategy' in restricted_globals:
            restricted_globals['apply_strategy'](restricted_globals['df'])
        
        result = restricted_globals.get("final_codes", [])
        logger.info(f"Sandbox execution complete. Found {len(result)} stocks.")
        return result
        
    except Exception as e:
        logger.error(f"Sandbox execution failed: {e}")
        raise ValueError(f"Runtime error: {e}")
