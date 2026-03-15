from data_sync.common import get_duckdb_conn
conn = get_duckdb_conn()
try:
    print(conn.execute("SELECT * FROM read_parquet('data/dim/dim_stock_list.parquet') LIMIT 1;").df())
except Exception as e:
    print(e)
