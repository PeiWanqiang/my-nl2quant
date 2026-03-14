import akshare as ak
import pandas as pd
import os
import time
import random
from loguru import logger
from data_sync.common import DATA_DIR, retry_akshare

FINANCE_DIR = os.path.join(DATA_DIR, "fact_finance")
os.makedirs(FINANCE_DIR, exist_ok=True)

@retry_akshare
def fetch_quarterly_financials(date_str: str):
    """
    Fetch earnings report (业绩报表) for a specific quarter end date.
    date_str format: YYYYMMDD, e.g., '20230331'
    """
    logger.info(f"Fetching financial report for {date_str}...")
    df = ak.stock_yjbb_em(date=date_str)
    return df

def init_financial_data(start_year: int = 2015, end_year: int = 2024):
    """
    Initialize historical quarterly financial data.
    """
    logger.info("Starting historical financial data initialization...")
    quarters = ["0331", "0630", "0930", "1231"]
    
    for year in range(start_year, end_year + 1):
        for q in quarters:
            date_str = f"{year}{q}"
            
            # Skip future dates
            if pd.to_datetime(date_str).date() > pd.Timestamp.today().date():
                continue
                
            out_path = os.path.join(FINANCE_DIR, f"finance_{date_str}.parquet")
            if os.path.exists(out_path):
                logger.info(f"Financial data for {date_str} already exists, skipping.")
                continue
                
            try:
                df = fetch_quarterly_financials(date_str)
                if df is None or df.empty:
                    logger.warning(f"No financial data for {date_str}")
                    continue
                    
                # Clean columns
                mapping = {
                    "股票代码": "ts_code",
                    "股票简称": "stock_name",
                    "每股收益": "eps",
                    "营业总收入-营业总收入": "total_revenue",
                    "营业总收入-同比增长": "revenue_yoy",
                    "营业总收入-季度环比增长": "revenue_qoq",
                    "净利润-净利润": "net_profit",
                    "净利润-同比增长": "net_profit_yoy",
                    "净利润-季度环比增长": "net_profit_qoq",
                    "每股净资产": "bps",
                    "净资产收益率": "roe",
                    "每股经营现金流量": "cfps",
                    "销售毛利率": "gross_margin",
                    "最新公告日期": "announcement_date"
                }
                
                # Keep only necessary columns and rename
                available_cols = {k: v for k, v in mapping.items() if k in df.columns}
                df = df[list(available_cols.keys())].rename(columns=available_cols)
                
                # Convert string numeric to float safely
                numeric_cols = ["eps", "total_revenue", "revenue_yoy", "revenue_qoq", "net_profit", 
                              "net_profit_yoy", "net_profit_qoq", "bps", "roe", "cfps", "gross_margin"]
                
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        
                if "announcement_date" in df.columns:
                    df["announcement_date"] = pd.to_datetime(df["announcement_date"], errors='coerce').dt.date
                
                df["report_date"] = pd.to_datetime(date_str).date()
                
                df.to_parquet(out_path)
                logger.info(f"Saved {len(df)} records to {out_path}")
                
                # Anti-ban sleep
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                logger.error(f"Failed to fetch financials for {date_str}: {e}")

if __name__ == "__main__":
    # Pull from 2010 to 2024
    init_financial_data(2010, 2024)
