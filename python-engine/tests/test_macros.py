import os
import sys
import duckdb
import pandas as pd
import json
import time
import traceback
from datetime import datetime, timedelta
import inspect

# Add python-engine to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import llm_agent.quant_macros as macros

def run_tests():
    print("Starting Macro Validation Tests...")
    
    # 1. Fetch data slice via DuckDB
    # We use the last 6 months of data
    conn = duckdb.connect()
    
    fact_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "fact_kline")
    
    if not os.path.exists(fact_path):
        print(f"ERROR: No data found at {fact_path}")
        return
        
    print("Creating view and fetching last 6 months of data...")
    conn.execute(f"CREATE OR REPLACE VIEW v_fact_kline AS SELECT * FROM read_parquet('{fact_path}/year=*/data.parquet', hive_partitioning=true)")
    
    six_months_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    query = f"""
    WITH sample_stocks AS (
        SELECT ts_code FROM v_fact_kline 
        WHERE trade_date >= '{six_months_ago}'
        GROUP BY ts_code
        LIMIT 300
    )
    SELECT f.ts_code, f.trade_date, f.open, f.close, f.high, f.low, f.vol, f.pct_change
    FROM v_fact_kline f
    JOIN sample_stocks s ON f.ts_code = s.ts_code
    WHERE f.trade_date >= '{six_months_ago}'
    ORDER BY f.ts_code, f.trade_date
    """
    
    df = conn.execute(query).df()
    print(f"Data slice loaded. Rows: {len(df)}")
    
    if df.empty:
        print("ERROR: Data slice is empty. Cannot run tests.")
        return
        
    latest_date = df['trade_date'].max()
    print(f"Latest trading date in slice: {latest_date}")
    
    # Collect macro functions
    macro_funcs = [f for name, f in inspect.getmembers(macros, inspect.isfunction) if name.startswith('macro_')]
    macro_funcs.sort(key=lambda x: x.__name__)
    
    results = {}
    passed_count = 0
    failed_macros = []
    
    for func in macro_funcs:
        name = func.__name__
        print(f"Testing {name}...")
        
        start_time = time.time()
        try:
            # 2. Execute the macro
            mask = func(df)
            
            exec_time_ms = int((time.time() - start_time) * 1000)
            
            # Validate return type and length
            if not isinstance(mask, pd.Series):
                raise TypeError(f"Expected pd.Series, got {type(mask)}")
            if len(mask) != len(df):
                raise ValueError(f"Length mismatch: {len(mask)} != {len(df)}")
                
            # 3. Extract hits for the latest date
            # Attach mask to df safely
            temp_df = df.copy()
            temp_df['is_hit'] = mask.values
            
            hits = temp_df[(temp_df['trade_date'] == latest_date) & (temp_df['is_hit'] == True)]
            matched_codes = hits['ts_code'].tolist()
            
            results[name] = {
                "status": "PASS",
                "execution_time_ms": exec_time_ms,
                "matched_count": len(matched_codes),
                "matched_codes": matched_codes[:20] # Store up to 20 for preview
            }
            passed_count += 1
            
        except Exception as e:
            err_msg = f"{type(e).__name__}: {str(e)}"
            print(f"  -> FAILED: {err_msg}")
            # traceback.print_exc()
            results[name] = {
                "status": "FAILED",
                "error_message": err_msg
            }
            failed_macros.append((name, err_msg))
            
    # 4. Save results to JSON
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)
    
    report = {
        "test_date": datetime.now().strftime("%Y-%m-%d"),
        "data_slice_info": f"Last 6 months (>= {six_months_ago}), {len(df['ts_code'].unique())} stocks, {len(df)} rows",
        "results": results
    }
    
    out_file = os.path.join(output_dir, "macro_match_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        
    print("\n" + "="*50)
    print(f"Test Execution Summary")
    print("="*50)
    print(f"Total Macros: {len(macro_funcs)}")
    print(f"Passed: {passed_count}")
    print(f"Failed: {len(failed_macros)}")
    
    if failed_macros:
        print("\nFailed Macros & Reasons:")
        for name, err in failed_macros:
            print(f"  - {name}: {err}")
            
    print(f"\nDetailed report saved to: {out_file}")

if __name__ == "__main__":
    run_tests()
