from data_sync.common import get_duckdb_conn
conn = get_duckdb_conn()
try:
    print(conn.execute("DESCRIBE v_fact_kline").df())
except Exception as e:
    print(e)
