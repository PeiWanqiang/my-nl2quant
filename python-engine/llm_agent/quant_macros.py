import pandas as pd
import numpy as np

# ---------------------------------------------------------
# Helper Functions (Indicators & Utils)
# ---------------------------------------------------------

def _get_ma(df: pd.DataFrame, n: int) -> pd.Series:
    return df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).mean())

def _get_vol_ma(df: pd.DataFrame, n: int) -> pd.Series:
    return df.groupby('ts_code')['vol'].transform(lambda x: x.rolling(n).mean())

def _get_macd(df: pd.DataFrame):
    ema12 = df.groupby('ts_code')['close'].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = df.groupby('ts_code')['close'].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    dif = ema12 - ema26
    dea = dif.groupby(df['ts_code']).transform(lambda x: x.ewm(span=9, adjust=False).mean())
    macd = (dif - dea) * 2
    return dif, dea, macd

def _get_kdj(df: pd.DataFrame):
    low_min = df.groupby('ts_code')['low'].transform(lambda x: x.rolling(9).min())
    high_max = df.groupby('ts_code')['high'].transform(lambda x: x.rolling(9).max())
    rsv = (df['close'] - low_min) / (high_max - low_min + 1e-8) * 100
    k = rsv.groupby(df['ts_code']).transform(lambda x: x.ewm(com=2, adjust=False).mean())
    d = k.groupby(df['ts_code']).transform(lambda x: x.ewm(com=2, adjust=False).mean())
    j = 3 * k - 2 * d
    return k, d, j

def _get_rsi(df: pd.DataFrame, n: int = 14):
    delta = df.groupby('ts_code')['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    # Using simple moving average of wilder (which is equivalent to ewm with alpha=1/n)
    ema_up = up.groupby(df['ts_code']).transform(lambda x: x.ewm(com=n-1, adjust=False).mean())
    ema_down = down.groupby(df['ts_code']).transform(lambda x: x.ewm(com=n-1, adjust=False).mean())
    rs = ema_up / (ema_down + 1e-8)
    return 100 - (100 / (1 + rs))

def _is_limit_up(df: pd.DataFrame) -> pd.Series:
    is_star_chinext = df['ts_code'].str.startswith(('300', '688'))
    is_bse = df['ts_code'].str.startswith(('43', '83', '87', '92'))
    is_st = df.get('is_st', pd.Series(False, index=df.index)).fillna(False)
    
    limit = pd.Series(9.8, index=df.index)
    limit = limit.mask(is_star_chinext, 19.8)
    limit = limit.mask(is_bse, 29.8)
    limit = limit.mask(is_st & ~is_star_chinext & ~is_bse, 4.8)
    
    return df['pct_change'] >= limit

def _is_limit_down(df: pd.DataFrame) -> pd.Series:
    is_star_chinext = df['ts_code'].str.startswith(('300', '688'))
    is_bse = df['ts_code'].str.startswith(('43', '83', '87', '92'))
    is_st = df.get('is_st', pd.Series(False, index=df.index)).fillna(False)
    
    limit = pd.Series(-9.8, index=df.index)
    limit = limit.mask(is_star_chinext, -19.8)
    limit = limit.mask(is_bse, -29.8)
    limit = limit.mask(is_st & ~is_star_chinext & ~is_bse, -4.8)
    
    return df['pct_change'] <= limit


# ---------------------------------------------------------
# 1. Volume & Price Action
# ---------------------------------------------------------

def macro_01_volume_spike(df: pd.DataFrame, n: int = 5, m: float = 1.5) -> pd.Series:
    avg_vol = df.groupby('ts_code')['vol'].transform(lambda x: x.shift(1).rolling(n).mean())
    return df['vol'] > (avg_vol * m)

def macro_02_volume_shrink(df: pd.DataFrame, n: int = 5, m: float = 0.8) -> pd.Series:
    avg_vol = df.groupby('ts_code')['vol'].transform(lambda x: x.shift(1).rolling(n).mean())
    return df['vol'] < (avg_vol * m)

def macro_03_ultra_low_vol(df: pd.DataFrame, n: int = 60) -> pd.Series:
    min_vol = df.groupby('ts_code')['vol'].transform(lambda x: x.rolling(n).min())
    return df['vol'] == min_vol

def macro_04_peak_vol(df: pd.DataFrame, n: int = 60) -> pd.Series:
    max_vol = df.groupby('ts_code')['vol'].transform(lambda x: x.rolling(n).max())
    return df['vol'] == max_vol

def macro_05_vol_price_up(df: pd.DataFrame, n: int = 3) -> pd.Series:
    cond = (df['close'] > df.groupby('ts_code')['close'].shift(1)) & (df['vol'] > df.groupby('ts_code')['vol'].shift(1))
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_06_shrink_vol_drop(df: pd.DataFrame, n: int = 3) -> pd.Series:
    cond = (df['close'] < df.groupby('ts_code')['close'].shift(1)) & (df['vol'] < df.groupby('ts_code')['vol'].shift(1))
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_07_vol_up_price_flat(df: pd.DataFrame) -> pd.Series:
    vol_up = df['vol'] > df.groupby('ts_code')['vol'].shift(1)
    price_flat = df['pct_change'].abs() < 1.0
    return vol_up & price_flat

def macro_08_spike_and_drop(df: pd.DataFrame, m: float = 50.0) -> pd.Series:
    body = (df['close'] - df['open']).abs()
    upper_shadow = df['high'] - df[['close', 'open']].max(axis=1)
    cond1 = upper_shadow > 2 * body
    cond2 = df['close'] < (df['high'] * (1 - m / 100.0))
    return cond1 & cond2

def macro_09_dip_and_recover(df: pd.DataFrame) -> pd.Series:
    body = (df['close'] - df['open']).abs()
    lower_shadow = df[['close', 'open']].min(axis=1) - df['low']
    return lower_shadow > 2 * body

def macro_10_mild_vol_up(df: pd.DataFrame, n: int = 3) -> pd.Series:
    prev_vol = df.groupby('ts_code')['vol'].shift(1)
    vol_ratio = df['vol'] / (prev_vol + 1e-8) - 1
    cond = (vol_ratio >= 0.10) & (vol_ratio <= 0.30)
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n


# ---------------------------------------------------------
# 2. Moving Averages & Trends
# ---------------------------------------------------------

def macro_11_bullish_ma_array(df: pd.DataFrame, short: int = 5, mid: int = 10, long: int = 20) -> pd.Series:
    ma_s = _get_ma(df, short)
    ma_m = _get_ma(df, mid)
    ma_l = _get_ma(df, long)
    return (ma_s > ma_m) & (ma_m > ma_l)

def macro_12_bearish_ma_array(df: pd.DataFrame, short: int = 5, mid: int = 10, long: int = 20) -> pd.Series:
    ma_s = _get_ma(df, short)
    ma_m = _get_ma(df, mid)
    ma_l = _get_ma(df, long)
    return (ma_s < ma_m) & (ma_m < ma_l)

def macro_13_ma_golden_cross(df: pd.DataFrame, short: int = 5, long: int = 10) -> pd.Series:
    ma_s = _get_ma(df, short)
    ma_l = _get_ma(df, long)
    cross_today = ma_s > ma_l
    cross_yest = ma_s.groupby(df['ts_code']).shift(1) <= ma_l.groupby(df['ts_code']).shift(1)
    return cross_today & cross_yest

def macro_14_ma_death_cross(df: pd.DataFrame, short: int = 5, long: int = 10) -> pd.Series:
    ma_s = _get_ma(df, short)
    ma_l = _get_ma(df, long)
    cross_today = ma_s < ma_l
    cross_yest = ma_s.groupby(df['ts_code']).shift(1) >= ma_l.groupby(df['ts_code']).shift(1)
    return cross_today & cross_yest

def macro_15_ma_consolidation(df: pd.DataFrame, n: int = 5, m: float = 5.0) -> pd.Series:
    ma5 = _get_ma(df, 5)
    ma10 = _get_ma(df, 10)
    ma20 = _get_ma(df, 20)
    mas = pd.concat([ma5, ma10, ma20], axis=1)
    ma_max = mas.max(axis=1)
    ma_min = mas.min(axis=1)
    ma_mean = mas.mean(axis=1)
    consolidation = ((ma_max - ma_min) / ma_mean) * 100 < m
    return consolidation.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_16_ma_divergence(df: pd.DataFrame) -> pd.Series:
    # From consolidation to bullish array and gap increasing
    ma5 = _get_ma(df, 5)
    ma10 = _get_ma(df, 10)
    ma20 = _get_ma(df, 20)
    bullish = (ma5 > ma10) & (ma10 > ma20)
    gap1 = ma5 - ma10
    gap2 = ma10 - ma20
    gap1_increasing = gap1 > gap1.groupby(df['ts_code']).shift(1)
    gap2_increasing = gap2 > gap2.groupby(df['ts_code']).shift(1)
    return bullish & gap1_increasing & gap2_increasing

def macro_17_breakout_ma(df: pd.DataFrame, n: int = 20) -> pd.Series:
    ma = _get_ma(df, n)
    prev_close = df.groupby('ts_code')['close'].shift(1)
    return (prev_close < ma) & (df['close'] > ma) & (df['close'] > df['open'])

def macro_18_pullback_to_ma(df: pd.DataFrame, n: int = 20) -> pd.Series:
    ma = _get_ma(df, n)
    return (df['low'] < ma) & (df['close'] > ma)

def macro_19_high_bias(df: pd.DataFrame, n: int = 20, m: float = 15.0) -> pd.Series:
    ma = _get_ma(df, n)
    bias = (df['close'] - ma) / ma * 100
    return bias > m

def macro_20_ma_turning_up(df: pd.DataFrame, n: int = 20, m: int = 3) -> pd.Series:
    ma = _get_ma(df, n)
    diff = ma.groupby(df['ts_code']).diff()
    was_dropping = (diff.groupby(df['ts_code']).shift(1).rolling(m).max() < 0)
    turning_up = diff > 0
    return was_dropping & turning_up


# ---------------------------------------------------------
# 3. Technical Indicators
# ---------------------------------------------------------

def macro_21_macd_golden_cross(df: pd.DataFrame) -> pd.Series:
    dif, dea, macd = _get_macd(df)
    cross_today = dif > dea
    cross_yest = dif.groupby(df['ts_code']).shift(1) <= dea.groupby(df['ts_code']).shift(1)
    return cross_today & cross_yest

def macro_22_macd_water_golden_cross(df: pd.DataFrame) -> pd.Series:
    dif, dea, macd = _get_macd(df)
    cross = macro_21_macd_golden_cross(df)
    return cross & (dif > 0) & (dea > 0)

def macro_23_macd_top_divergence(df: pd.DataFrame, n: int = 20) -> pd.Series:
    dif, dea, macd = _get_macd(df)
    price_high = df['close'] == df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).max())
    dif_not_high = dif < dif.groupby(df['ts_code']).transform(lambda x: x.rolling(n).max())
    return price_high & dif_not_high

def macro_24_macd_bottom_divergence(df: pd.DataFrame, n: int = 20) -> pd.Series:
    dif, dea, macd = _get_macd(df)
    price_low = df['close'] == df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).min())
    dif_not_low = dif > dif.groupby(df['ts_code']).transform(lambda x: x.rolling(n).min())
    return price_low & dif_not_low

def macro_25_kdj_overbought(df: pd.DataFrame) -> pd.Series:
    k, d, j = _get_kdj(df)
    return (j > 100) | (k > 80)

def macro_26_kdj_oversold(df: pd.DataFrame) -> pd.Series:
    k, d, j = _get_kdj(df)
    return (j < 0) | (k < 20)

def macro_27_kdj_flattening(df: pd.DataFrame, n: int = 3) -> pd.Series:
    ob = macro_25_kdj_overbought(df)
    os = macro_26_kdj_oversold(df)
    ob_n = ob.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n
    os_n = os.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n
    return ob_n | os_n

def macro_28_boll_break_upper(df: pd.DataFrame, n: int = 20) -> pd.Series:
    ma = _get_ma(df, n)
    std = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).std())
    upper = ma + 2 * std
    return df['close'] > upper

def macro_29_boll_touch_lower(df: pd.DataFrame, n: int = 20) -> pd.Series:
    ma = _get_ma(df, n)
    std = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).std())
    lower = ma - 2 * std
    return (df['low'] < lower) & (df['close'] > lower)

def macro_30_rsi_strong(df: pd.DataFrame, n: int = 3) -> pd.Series:
    rsi = _get_rsi(df, 14)
    cond = rsi > 50
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n


# ---------------------------------------------------------
# 4. Candlesticks & Patterns
# ---------------------------------------------------------

def macro_31_consecutive_up(df: pd.DataFrame, n: int = 3) -> pd.Series:
    cond = df['close'] > df['open']
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_32_consecutive_down(df: pd.DataFrame, n: int = 3) -> pd.Series:
    cond = df['close'] < df['open']
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_33_doji(df: pd.DataFrame) -> pd.Series:
    body_pct = (df['close'] - df['open']).abs() / df['open']
    has_upper = df['high'] > df[['close', 'open']].max(axis=1)
    has_lower = df['low'] < df[['close', 'open']].min(axis=1)
    return (body_pct < 0.005) & has_upper & has_lower

def macro_34_whipsaw(df: pd.DataFrame) -> pd.Series:
    yest_upper = df.groupby('ts_code')['high'].shift(1) - df[['close', 'open']].max(axis=1).groupby(df['ts_code']).shift(1)
    yest_body = (df.groupby('ts_code')['close'].shift(1) - df.groupby('ts_code')['open'].shift(1)).abs()
    
    today_lower = df[['close', 'open']].min(axis=1) - df['low']
    today_body = (df['close'] - df['open']).abs()
    
    return (yest_upper > 2 * yest_body) & (today_lower > 2 * today_body) & (today_body / df['open'] < 0.01)

def macro_35_three_white_soldiers(df: pd.DataFrame) -> pd.Series:
    c1 = df['close'] > df['open']
    c2 = c1.groupby(df['ts_code']).shift(1)
    c3 = c1.groupby(df['ts_code']).shift(2)
    
    o_in_prev1 = (df['open'] > df.groupby('ts_code')['open'].shift(1)) & (df['open'] < df.groupby('ts_code')['close'].shift(1))
    o_in_prev2 = (df.groupby('ts_code')['open'].shift(1) > df.groupby('ts_code')['open'].shift(2)) & (df.groupby('ts_code')['open'].shift(1) < df.groupby('ts_code')['close'].shift(2))
    
    high_close1 = df['close'] > df.groupby('ts_code')['close'].shift(1)
    high_close2 = df.groupby('ts_code')['close'].shift(1) > df.groupby('ts_code')['close'].shift(2)
    
    return c1 & c2 & c3 & o_in_prev1 & o_in_prev2 & high_close1 & high_close2

def macro_36_bottom_consolidation(df: pd.DataFrame, n: int = 20, m: float = 15.0) -> pd.Series:
    high_n = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).max())
    low_n = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).min())
    return ((high_n - low_n) / low_n * 100) < m

def macro_37_platform_breakout(df: pd.DataFrame, n: int = 20) -> pd.Series:
    prev_high_n = df.groupby('ts_code')['close'].shift(1).groupby(df['ts_code']).transform(lambda x: x.rolling(n).max())
    return df['close'] > prev_high_n

def macro_38_pullback_stabilize(df: pd.DataFrame, n: int = 3) -> pd.Series:
    # 连跌缩量，不创新低，收阳
    down_n = macro_32_consecutive_down(df, n).groupby(df['ts_code']).shift(1).fillna(False)
    shrink_n = macro_06_shrink_vol_drop(df, n).groupby(df['ts_code']).shift(1).fillna(False)
    prev_low = df.groupby('ts_code')['low'].shift(1)
    not_new_low = df['low'] > prev_low
    up_today = df['close'] > df['open']
    return down_n & shrink_n & not_new_low & up_today

def macro_39_v_reversal(df: pd.DataFrame, n: int = 5, m: float = 10.0) -> pd.Series:
    past_return = df.groupby('ts_code')['close'].shift(n) / df.groupby('ts_code')['close'].shift(2*n) - 1
    recent_return = df['close'] / df.groupby('ts_code')['close'].shift(n) - 1
    return (past_return < -m/100.0) & (recent_return > m/100.0)

def macro_40_double_bottom(df: pd.DataFrame, n: int = 20) -> pd.Series:
    # 简化：过去n天有2个极小值，相差<2%，且当前突破均线(假定颈线)
    min1 = df.groupby('ts_code')['low'].transform(lambda x: x.rolling(n//2).min())
    min2 = df.groupby('ts_code')['low'].shift(n//2).groupby(df['ts_code']).transform(lambda x: x.rolling(n//2).min())
    diff = (min1 - min2).abs() / min2
    ma = _get_ma(df, n)
    return (diff < 0.02) & (df['close'] > ma)


# ---------------------------------------------------------
# 5. A-Share Specifics
# ---------------------------------------------------------

def macro_41_limit_up(df: pd.DataFrame) -> pd.Series:
    return _is_limit_up(df)

def macro_42_limit_down(df: pd.DataFrame) -> pd.Series:
    return _is_limit_down(df)

def macro_43_consecutive_limit_up(df: pd.DataFrame, n: int = 2) -> pd.Series:
    lu = _is_limit_up(df)
    return lu.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_44_broken_limit_up(df: pd.DataFrame) -> pd.Series:
    # Use pct_change for high vs close
    is_star_chinext = df['ts_code'].str.startswith(('300', '688'))
    limit = pd.Series(9.8, index=df.index).mask(is_star_chinext, 19.8)
    
    prev_close = df.groupby('ts_code')['close'].shift(1)
    high_pct = (df['high'] - prev_close) / prev_close * 100
    return (high_pct >= limit) & (~_is_limit_up(df))

def macro_45_floor_to_ceiling(df: pd.DataFrame) -> pd.Series:
    is_star_chinext = df['ts_code'].str.startswith(('300', '688'))
    limit_down_pct = pd.Series(-9.8, index=df.index).mask(is_star_chinext, -19.8)
    
    prev_close = df.groupby('ts_code')['close'].shift(1)
    low_pct = (df['low'] - prev_close) / prev_close * 100
    
    return (low_pct <= limit_down_pct) & _is_limit_up(df)

def macro_46_straight_limit_up(df: pd.DataFrame) -> pd.Series:
    return _is_limit_up(df) & (df['open'] == df['close']) & (df['high'] == df['low'])

def macro_47_t_limit_up(df: pd.DataFrame) -> pd.Series:
    return _is_limit_up(df) & (df['open'] == df['close']) & (df['low'] < df['close'])

def macro_48_first_limit_up(df: pd.DataFrame) -> pd.Series:
    lu_today = _is_limit_up(df)
    lu_yest = _is_limit_up(df).groupby(df['ts_code']).shift(1).fillna(False)
    return lu_today & ~lu_yest

def macro_49_all_time_high(df: pd.DataFrame) -> pd.Series:
    highest_ever = df.groupby('ts_code')['high'].transform('cummax')
    return df['high'] == highest_ever

def macro_50_limit_up_trap(df: pd.DataFrame) -> pd.Series:
    lu_yest = _is_limit_up(df).groupby(df['ts_code']).shift(1).fillna(False)
    big_drop = df['pct_change'] < -5.0
    yin = df['close'] < df['open']
    return lu_yest & big_drop & yin

