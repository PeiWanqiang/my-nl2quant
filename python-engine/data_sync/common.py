import os
import duckdb
from loguru import logger
from tenacity import retry, wait_exponential, stop_after_attempt
import pandas as pd
import glob

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DIM_DIR = os.path.join(DATA_DIR, "dim")
FACT_KLINE_DIR = os.path.join(DATA_DIR, "fact_kline")
STATUS_DIR = os.path.join(DATA_DIR, "status")

# Setup directories
os.makedirs(DIM_DIR, exist_ok=True)
os.makedirs(FACT_KLINE_DIR, exist_ok=True)
os.makedirs(STATUS_DIR, exist_ok=True)
os.makedirs(os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs"), exist_ok=True)

# Logger setup
log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "data_pipeline.log")
logger.add(log_path, rotation="1 day", retention="7 days")

# Retry decorator for all akshare calls
retry_akshare = retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))

class DataCleaner:
    @staticmethod
    def clean_kline_cols(df: pd.DataFrame) -> pd.DataFrame:
        mapping = {
            "日期": "trade_date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "vol",
            "成交额": "amount",
            "振幅": "amplitude",
            "涨跌幅": "pct_change",
            "涨跌额": "change",
            "换手率": "turnover_rate"
        }
        df.rename(columns=mapping, inplace=True)
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df

def get_duckdb_conn():
    conn = duckdb.connect()
    if glob.glob(f"{FACT_KLINE_DIR}/year=*/data.parquet"):
        try:
            conn.execute(f"CREATE OR REPLACE VIEW v_fact_kline AS SELECT * FROM read_parquet('{FACT_KLINE_DIR}/year=*/data.parquet', hive_partitioning=true);")
        except Exception as e:
            logger.warning(f"Failed to create view: {e}")
    return conn
