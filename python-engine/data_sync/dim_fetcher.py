import akshare as ak
import pandas as pd
import os
import time
import random
from loguru import logger
from data_sync.common import DIM_DIR, retry_akshare

@retry_akshare
def fetch_stock_list():
    logger.info("Fetching full A-share stock list...")
    df = ak.stock_zh_a_spot_em()
    # Ensure mapping matches schema
    df.rename(columns={"代码": "ts_code", "名称": "stock_name"}, inplace=True)
    df = df[["ts_code", "stock_name"]]
    
    out_path = os.path.join(DIM_DIR, "dim_stock_list.parquet")
    df.to_parquet(out_path)
    logger.info(f"Saved {len(df)} stocks to {out_path}")

@retry_akshare
def fetch_trade_calendar():
    logger.info("Fetching trade calendar...")
    df = ak.tool_trade_date_hist_sina()
    # It returns a dataframe with 'trade_date' column as datetime.date
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    out_path = os.path.join(DIM_DIR, "dim_trade_calendar.parquet")
    df.to_parquet(out_path)
    logger.info(f"Saved trade calendar to {out_path}")

@retry_akshare
def fetch_stock_concept():
    logger.info("Fetching stock concept mapping...")
    try:
        # 获取东方财富行业板块作为概念 MVP 示例（速度比全概念快，且无缝贴合需求）
        # 实际生产中如果要概念，要用 ak.stock_board_concept_name_em()
        industry_df = ak.stock_board_industry_name_em()
        industry_names = industry_df['板块名称'].tolist()
        
        all_mapping = []
        for ind in industry_names:
            try:
                cons_df = ak.stock_board_industry_cons_em(symbol=ind)
                for code in cons_df['代码'].tolist():
                    all_mapping.append({"ts_code": str(code).zfill(6), "concept_name": ind})
                time.sleep(random.uniform(0.5, 1.5)) # 防封禁
            except Exception as e:
                logger.warning(f"Failed to fetch constituents for industry {ind}: {e}")
                continue
                
        final_df = pd.DataFrame(all_mapping)
        out_path = os.path.join(DIM_DIR, "dim_stock_concept.parquet")
        final_df.to_parquet(out_path)
        logger.info(f"Saved {len(final_df)} concept/industry mappings to {out_path}")
        
    except Exception as e:
        logger.error(f"Failed to fetch concept mapping: {e}")

if __name__ == "__main__":
    fetch_stock_list()
    fetch_trade_calendar()
    fetch_stock_concept()
