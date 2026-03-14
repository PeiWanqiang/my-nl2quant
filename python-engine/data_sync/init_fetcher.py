import akshare as ak
import pandas as pd
import os
import json
import time
import random
from loguru import logger
from data_sync.common import DIM_DIR, FACT_KLINE_DIR, STATUS_DIR, retry_akshare, DataCleaner

PROGRESS_FILE = os.path.join(STATUS_DIR, "fetch_progress.json")

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)

@retry_akshare
def fetch_stock_hist(ts_code: str):
    # 改为 19900101，获取上市以来的所有数据
    return ak.stock_zh_a_hist(symbol=ts_code, period="daily", start_date="19900101", adjust="qfq")

def init_historical_data():
    logger.info("Starting historical data initialization (Task A)...")
    dim_stock_path = os.path.join(DIM_DIR, "dim_stock_list.parquet")
    if not os.path.exists(dim_stock_path):
        logger.error(f"{dim_stock_path} not found. Run dim_fetcher.py first.")
        return

    stock_list_df = pd.read_parquet(dim_stock_path)
    ts_codes = stock_list_df["ts_code"].tolist()
    
    progress = load_progress()
    
    for ts_code in ts_codes:
        if progress.get(ts_code):
            continue
            
        logger.info(f"Fetching historical data for {ts_code}")
        try:
            df = fetch_stock_hist(ts_code)
            if df.empty:
                logger.warning(f"No historical data for {ts_code}")
                progress[ts_code] = True
                save_progress(progress)
                continue
                
            df = DataCleaner.clean_kline_cols(df)
            df["ts_code"] = ts_code
            
            # 妥协处理：填充 NaN
            df["total_mv"] = pd.NA
            df["pe_ttm"] = pd.NA
            df["pb"] = pd.NA
            df["is_st"] = False
            
            # 按年切片落盘
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
                    
            progress[ts_code] = True
            save_progress(progress)
            
            # 随机休眠防封禁
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            logger.error(f"Failed processing {ts_code}: {e}")
            break # 终止循环，等待重试

if __name__ == "__main__":
    init_historical_data()
