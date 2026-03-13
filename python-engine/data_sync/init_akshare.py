import akshare as ak
import duckdb
import os
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

def init_data():
    """Fetch basic A-share daily data and store as parquet."""
    print("Starting data initialization...")
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Mocking fetching limited data for MVP
    # In reality, this would fetch historical data for all stocks
    try:
        print("Fetching sample A-share data (000001)...")
        # Fetch Ping An Bank daily data
        df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240101", end_date="20240301", adjust="qfq")
        
        # Save to parquet
        parquet_path = os.path.join(DATA_DIR, "000001.parquet")
        df.to_parquet(parquet_path)
        print(f"Data saved to {parquet_path}")
        
        # Verify with duckdb
        print("Verifying data with DuckDB...")
        res = duckdb.query(f"SELECT * FROM '{parquet_path}' LIMIT 5").df()
        print(res)
        
    except Exception as e:
        print(f"Failed to fetch or save data: {e}")

if __name__ == "__main__":
    init_data()
