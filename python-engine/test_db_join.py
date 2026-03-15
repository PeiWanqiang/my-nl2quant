from data_sync.common import get_duckdb_conn
conn = get_duckdb_conn()
try:
    query = """
    WITH Kline AS (
        SELECT * 
        FROM v_fact_kline 
        WHERE trade_date >= CURRENT_DATE - INTERVAL 180 DAY
    ), Finance AS (
        SELECT ts_code, net_profit, report_date 
        FROM read_parquet('data/fact_finance/*.parquet')
    )
    SELECT k.*, f.net_profit, f.report_date
    FROM Kline k
    LEFT JOIN Finance f ON k.ts_code = f.ts_code
    ORDER BY k.ts_code, k.trade_date
    """
    df = conn.execute(query).df()
    print("Done")
except Exception as e:
    print(e)
