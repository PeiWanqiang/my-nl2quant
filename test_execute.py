import requests
import json

code = """
def apply_strategy(df):
    import pandas as pd
    import numpy as np
    
    # Ensure 'trade_date' is in datetime format for date operations
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    # --- 条件1: 连续亏损 (Consecutive Losses) ---
    # 描述: 公司连续N年净利润为负
    # 参数: n=3
    n_years = 3  # 从用户确认的JSON条件中获取参数n的值

    # 1. 提取交易日期中的年份，作为年度标识
    df['year'] = df['trade_date'].dt.year

    # 2. 对于每只股票 (ts_code) 的每个年份，获取其最新的年度净利润。
    #    假设 'net_profit' 列存在于 df 中，代表该交易日所属年份的年度净利润。
    #    如果一年内有多次净利润数据更新，我们取该年份的最后一次报告值。
    annual_net_profit_df = df.groupby(['ts_code', 'year'])['net_profit'].last().reset_index()

    # 3. 判断每年的净利润是否为负
    annual_net_profit_df['is_negative'] = annual_net_profit_df['net_profit'] < 0

    # 4. 按照股票代码和年份进行排序，为后续的滚动计算做准备
    annual_net_profit_df = annual_net_profit_df.sort_values(by=['ts_code', 'year'])

    # 5. 计算连续N年净利润为负的条件
    annual_net_profit_df['cond_consecutive_negative'] = annual_net_profit_df.groupby('ts_code')['is_negative'].rolling(
        window=n_years, min_periods=n_years
    ).apply(lambda x: x.all(), raw=True).reset_index(level=0, drop=True).astype(bool)

    # 6. 填充 NaN 值：对于那些没有足够历史年份来满足 n_years 连续条件的行，其结果为 NaN，我们将其视为 False。
    annual_net_profit_df['cond_consecutive_negative'] = annual_net_profit_df['cond_consecutive_negative'].fillna(False)

    # 7. 将计算出的年度条件合并回原始的日线 DataFrame
    df = pd.merge(df, annual_net_profit_df[['ts_code', 'year', 'cond_consecutive_negative']],
                  on=['ts_code', 'year'], how='left')

    # 最终的条件掩码
    cond_01 = df['cond_consecutive_negative']
    
    # --- 组合所有条件 ---
    # 当前只有一个条件，所以 final_mask 直接等于 cond_01
    final_mask = cond_01
    
    # 强制只返回最新一个交易日符合条件的股票，避免买入历史股票
    latest_date = df['trade_date'].max()
    result = df[final_mask & (df['trade_date'] == latest_date)]
    
    # 必须使用 global 声明并设置 final_codes
    global final_codes
    final_codes = result['ts_code'].tolist()
"""

resp = requests.post("http://127.0.0.1:8080/api/v1/gateway/quant/execute", json={"code": code})
print(resp.status_code)
print(resp.text)
