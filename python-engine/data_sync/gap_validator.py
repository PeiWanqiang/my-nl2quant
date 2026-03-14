import duckdb
import pandas as pd
import os
import time
import random
import datetime
from loguru import logger
from data_sync.common import get_duckdb_conn, DIM_DIR, FACT_KLINE_DIR, STATUS_DIR
from data_sync.init_fetcher import DataCleaner
import akshare as ak

SUSPENSION_FILE = os.path.join(STATUS_DIR, "known_suspensions.parquet")

def load_suspensions() -> pd.DataFrame:
    if os.path.exists(SUSPENSION_FILE):
        return pd.read_parquet(SUSPENSION_FILE)
    df = pd.DataFrame(columns=["ts_code", "trade_date"])
    # Ensure types for empty DataFrame
    df["ts_code"] = df["ts_code"].astype(str)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df

def save_suspensions(df: pd.DataFrame):
    df.to_parquet(SUSPENSION_FILE)

def validate_gaps():
    logger.info("Starting gap validation (Task C)...")
    conn = get_duckdb_conn()
    
    dim_cal = os.path.join(DIM_DIR, "dim_trade_calendar.parquet")
    
    if not os.path.exists(dim_cal):
        logger.error("Trade calendar missing. Run dim_fetcher.py first.")
        return
        
    try:
        # Load Suspensions into duckdb
        susp_df = load_suspensions()
        conn.register("v_suspensions", susp_df)
        conn.execute(f"CREATE OR REPLACE VIEW v_cal AS SELECT * FROM read_parquet('{dim_cal}')")
        
        # We need to make sure v_fact_kline exists
        try:
            conn.execute("SELECT 1 FROM v_fact_kline LIMIT 1")
        except:
            logger.warning("v_fact_kline view not found or empty. Is fact_kline populated?")
            return

        query = """
        WITH StockDateRange AS (
            SELECT ts_code, MIN(trade_date) as start_date, MAX(trade_date) as end_date
            FROM v_fact_kline
            GROUP BY ts_code
        ),
        ExpectedDates AS (
            SELECT s.ts_code, c.trade_date
            FROM StockDateRange s
            JOIN v_cal c ON c.trade_date >= s.start_date AND c.trade_date <= s.end_date
        ),
        MissingDates AS (
            SELECT e.ts_code, e.trade_date
            FROM ExpectedDates e
            LEFT JOIN v_fact_kline k ON e.ts_code = k.ts_code AND e.trade_date = k.trade_date
            LEFT JOIN v_suspensions s ON e.ts_code = s.ts_code AND e.trade_date = s.trade_date
            WHERE k.trade_date IS NULL AND s.trade_date IS NULL
        )
        SELECT ts_code, MIN(trade_date) as gap_start, MAX(trade_date) as gap_end, COUNT(*) as missing_days
        FROM MissingDates
        GROUP BY ts_code
        ORDER BY missing_days DESC
        """
        
        logger.info("Executing gap detection query...")
        gaps_df = conn.execute(query).df()
        
        if gaps_df.empty:
            logger.info("No gaps found. Data is complete.")
            return
            
        logger.info(f"Found gaps for {len(gaps_df)} stocks. Starting patching...")
        
        # We process each stock
        for _, row in gaps_df.iterrows():
            ts_code = row['ts_code']
            gap_start = row['gap_start']
            gap_end = row['gap_end']
            
            start_str = gap_start.strftime("%Y%m%d")
            end_str = gap_end.strftime("%Y%m%d")
            
            logger.info(f"Patching {ts_code} from {start_str} to {end_str}")
            
            try:
                # Fetch data
                df = ak.stock_zh_a_hist(symbol=ts_code, period="daily", start_date=start_str, end_date=end_str, adjust="qfq")
                
                if df is None or df.empty:
                    logger.warning(f"No data returned for {ts_code} between {start_str} and {end_str}. Registering as suspended.")
                    # It's a suspension. Get all expected dates in this range
                    cal_range = conn.execute(f"SELECT '{ts_code}' as ts_code, trade_date FROM v_cal WHERE trade_date >= '{gap_start}' AND trade_date <= '{gap_end}'").df()
                    susp_df = pd.concat([susp_df, cal_range], ignore_index=True)
                    susp_df.drop_duplicates(subset=["ts_code", "trade_date"], inplace=True)
                    save_suspensions(susp_df)
                    # update registered view
                    conn.register("v_suspensions", susp_df)
                    continue
                    
                # Clean and save
                df = DataCleaner.clean_kline_cols(df)
                df["ts_code"] = ts_code
                df["total_mv"] = pd.NA
                df["pe_ttm"] = pd.NA
                df["pb"] = pd.NA
                df["is_st"] = False
                
                df["year"] = pd.to_datetime(df["trade_date"]).dt.year
                years = df["year"].unique()
                
                for y in years:
                    year_df = df[df["year"] == y].copy()
                    year_df.drop(columns=["year"], inplace=True)
                    
                    year_dir = os.path.join(FACT_KLINE_DIR, f"year={y}")
                    os.makedirs(year_dir, exist_ok=True)
                    parquet_path = os.path.join(year_dir, "data.parquet")
                    
                    if os.path.exists(parquet_path):
                        existing_df = pd.read_parquet(parquet_path)
                        combined = pd.concat([existing_df, year_df])
                        combined.drop_duplicates(subset=["ts_code", "trade_date"], keep="last", inplace=True)
                        combined.to_parquet(parquet_path)
                    else:
                        year_df.to_parquet(parquet_path)
                        
                logger.info(f"Successfully patched gap for {ts_code}")
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                logger.error(f"Failed to fetch gap for {ts_code}: {e}")
                time.sleep(2)
                
    except Exception as e:
        logger.error(f"Gap validation failed: {e}")

if __name__ == "__main__":
    validate_gaps()
