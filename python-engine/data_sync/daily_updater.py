import akshare as ak
import pandas as pd
import os
import datetime
from loguru import logger
from data_sync.common import FACT_KLINE_DIR, retry_akshare, DataCleaner

@retry_akshare
def fetch_daily_spot():
    return ak.stock_zh_a_spot_em()

def daily_update():
    logger.info("Starting daily increment update (Task B)...")
    today = datetime.date.today()
    current_year = today.year
    
    try:
        df = fetch_daily_spot()
        
        # Mapping EM spot columns
        # ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
        mapping = {
            "代码": "ts_code",
            "名称": "stock_name",
            "最新价": "close",
            "今开": "open",
            "最高": "high",
            "最低": "low",
            "成交量": "vol",
            "成交额": "amount",
            "换手率": "turnover_rate",
            "涨跌幅": "pct_change",
            "涨跌额": "change",
            "振幅": "amplitude",
            "总市值": "total_mv",
            "市盈率-动态": "pe_ttm",
            "市净率": "pb"
        }
        df.rename(columns=mapping, inplace=True)
        
        # Only keep necessary columns
        keep_cols = ["ts_code", "stock_name", "open", "close", "high", "low", "vol", "amount", 
                     "turnover_rate", "pct_change", "change", "amplitude", "total_mv", "pe_ttm", "pb"]
        
        # Some columns might be missing depending on EM API changes, handle safely
        available_cols = [c for c in keep_cols if c in df.columns]
        df = df[available_cols].copy()
        
        df["trade_date"] = today
        
        # ST动态判断
        if "stock_name" in df.columns:
            df["is_st"] = df["stock_name"].astype(str).str.contains("ST|退", regex=True)
            df.drop(columns=["stock_name"], inplace=True) # 不落盘名称
        else:
            df["is_st"] = False
            
        year_dir = os.path.join(FACT_KLINE_DIR, f"year={current_year}")
        os.makedirs(year_dir, exist_ok=True)
        parquet_path = os.path.join(year_dir, "data.parquet")
        
        if os.path.exists(parquet_path):
            existing_df = pd.read_parquet(parquet_path)
            combined = pd.concat([existing_df, df])
            combined.drop_duplicates(subset=["ts_code", "trade_date"], keep="last", inplace=True)
            combined.to_parquet(parquet_path)
            logger.info(f"Updated daily increment for {current_year}, total rows: {len(combined)}")
        else:
            df.to_parquet(parquet_path)
            logger.info(f"Created new partition for {current_year}, total rows: {len(df)}")
            
    except Exception as e:
        logger.error(f"Failed daily update: {e}")

if __name__ == "__main__":
    daily_update()
