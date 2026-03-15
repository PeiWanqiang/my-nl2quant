import pandas as pd
import numpy as np

# ---------------------------------------------------------
# Helper Functions (Indicators & Utils)
# ---------------------------------------------------------

def _get_ma(df: pd.DataFrame, n: int) -> pd.Series:
    """计算简单移动平均线（MA）。通过分组计算指定周期内的收盘价算术平均值，用于平滑价格波动。"""
    return df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).mean())

def _get_vol_ma(df: pd.DataFrame, n: int) -> pd.Series:
    """计算成交量移动平均线。用于衡量市场平均活跃度，判断当前成交量是否处于放量或缩量状态。"""
    return df.groupby('ts_code')['vol'].transform(lambda x: x.rolling(n).mean())

def _get_macd(df: pd.DataFrame):
    """计算MACD指标（指数平滑异同移动平均线）。返回DIF、DEA及MACD柱线，是衡量趋势和动能最常用的指标。"""
    ema12 = df.groupby('ts_code')['close'].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = df.groupby('ts_code')['close'].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    dif = ema12 - ema26
    dea = dif.groupby(df['ts_code']).transform(lambda x: x.ewm(span=9, adjust=False).mean())
    macd = (dif - dea) * 2
    return dif, dea, macd

def _get_kdj(df: pd.DataFrame):
    """计算KDJ随机指标。通过最近9日的最高、最低价与收盘价的比例，反映市场的超买超卖状态及短期波动强弱。"""
    low_min = df.groupby('ts_code')['low'].transform(lambda x: x.rolling(9).min())
    high_max = df.groupby('ts_code')['high'].transform(lambda x: x.rolling(9).max())
    rsv = (df['close'] - low_min) / (high_max - low_min + 1e-8) * 100
    k = rsv.groupby(df['ts_code']).transform(lambda x: x.ewm(com=2, adjust=False).mean())
    d = k.groupby(df['ts_code']).transform(lambda x: x.ewm(com=2, adjust=False).mean())
    j = 3 * k - 2 * d
    return k, d, j

def _get_rsi(df: pd.DataFrame, n: int = 14):
    """计算RSI相对强弱指标。基于上涨和下跌幅度的平均值，衡量多空双方力量对比，数值越大表示市场越强势。"""
    delta = df.groupby('ts_code')['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.groupby(df['ts_code']).transform(lambda x: x.ewm(com=n-1, adjust=False).mean())
    ema_down = down.groupby(df['ts_code']).transform(lambda x: x.ewm(com=n-1, adjust=False).mean())
    rs = ema_up / (ema_down + 1e-8)
    return 100 - (100 / (1 + rs))

def _is_limit_up(df: pd.DataFrame) -> pd.Series:
    """判断是否涨停。根据不同市场板块（主板10%、创/科20%、北交所30%）及ST状态计算涨幅临界点。"""
    is_star_chinext = df['ts_code'].str.startswith(('300', '688'))
    is_bse = df['ts_code'].str.startswith(('43', '83', '87', '92'))
    is_st = df.get('is_st', pd.Series(False, index=df.index)).fillna(False)
    
    limit = pd.Series(9.8, index=df.index)
    limit = limit.mask(is_star_chinext, 19.8)
    limit = limit.mask(is_bse, 29.8)
    limit = limit.mask(is_st & ~is_star_chinext & ~is_bse, 4.8)
    
    return df['pct_change'] >= limit

def _is_limit_down(df: pd.DataFrame) -> pd.Series:
    """判断是否跌停。根据个股所在板块的跌幅限制（-5%、-10%、-20%、-30%），识别该股当天是否触及最低价限制。"""
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
    """成交量异常放量。当日成交量超过过去N日平均成交量的M倍，通常预示着有主力资金介入或重大消息刺激。"""
    avg_vol = df.groupby('ts_code')['vol'].transform(lambda x: x.shift(1).rolling(n).mean())
    return df['vol'] > (avg_vol * m)

def macro_02_volume_shrink(df: pd.DataFrame, n: int = 5, m: float = 0.8) -> pd.Series:
    """成交量明显缩量。当日成交量低于过去N日平均量的M倍，反映市场交投清淡或持股者惜售，常见于回调末端。"""
    avg_vol = df.groupby('ts_code')['vol'].transform(lambda x: x.shift(1).rolling(n).mean())
    return df['vol'] < (avg_vol * m)

def macro_03_ultra_low_vol(df: pd.DataFrame, n: int = 60) -> pd.Series:
    """地量信号。当日成交量为过去N天内的最低水平，代表空头力量枯竭，是股价见底、即将变盘的重要参考。"""
    min_vol = df.groupby('ts_code')['vol'].transform(lambda x: x.rolling(n).min())
    return df['vol'] == min_vol

def macro_04_peak_vol(df: pd.DataFrame, n: int = 60) -> pd.Series:
    """天量信号。当日成交量达到过去N天内的最高峰，在上涨行情中可能意味着高位换手激烈，有见顶回落风险。"""
    max_vol = df.groupby('ts_code')['vol'].transform(lambda x: x.rolling(n).max())
    return df['vol'] == max_vol

def macro_05_vol_price_up(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """量价齐升。连续N个交易日价格上涨且成交量同步放大，这是一种典型的健康多头趋势，表明上涨动力强劲。"""
    cond = (df['close'] > df.groupby('ts_code')['close'].shift(1)) & (df['vol'] > df.groupby('ts_code')['vol'].shift(1))
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_06_shrink_vol_drop(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """缩量下跌。连续N个交易日价格下跌但成交量逐渐缩小，通常被认为是洗盘或调整阶段，卖盘压力正在减弱。"""
    cond = (df['close'] < df.groupby('ts_code')['close'].shift(1)) & (df['vol'] < df.groupby('ts_code')['vol'].shift(1))
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_07_vol_up_price_flat(df: pd.DataFrame) -> pd.Series:
    """量增价平。成交量放大但价格波动极小，常出现在底部吸筹阶段（主力对倒建仓）或顶部派发阶段的滞涨信号。"""
    vol_up = df['vol'] > df.groupby('ts_code')['vol'].shift(1)
    price_flat = df['pct_change'].abs() < 1.0
    return vol_up & price_flat

def macro_08_spike_and_drop(df: pd.DataFrame, m: float = 50.0) -> pd.Series:
    """高位冲高回落。K线带有长上影线且收盘价远离最高点，表示上方抛压沉重，是一种常见的短期见顶信号。"""
    body = (df['close'] - df['open']).abs()
    upper_shadow = df['high'] - df[['close', 'open']].max(axis=1)
    cond1 = upper_shadow > 2 * body
    cond2 = df['close'] < (df['high'] * (1 - m / 100.0))
    return cond1 & cond2

def macro_09_dip_and_recover(df: pd.DataFrame) -> pd.Series:
    """探底回升（锤子线）。K线带有长下影线，反映股价在盘中受支撑明显，多头开始反击，具有一定的止跌意义。"""
    body = (df['close'] - df['open']).abs()
    lower_shadow = df[['close', 'open']].min(axis=1) - df['low']
    return lower_shadow > 2 * body

def macro_10_mild_vol_up(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """温和放量。连续N天成交量以10%-30%的速度稳步增长，表明资金介入节奏稳健，行情具有较好的持续性。"""
    prev_vol = df.groupby('ts_code')['vol'].shift(1)
    vol_ratio = df['vol'] / (prev_vol + 1e-8) - 1
    cond = (vol_ratio >= 0.10) & (vol_ratio <= 0.30)
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n


# ---------------------------------------------------------
# 2. Moving Averages & Trends
# ---------------------------------------------------------

def macro_11_bullish_ma_array(df: pd.DataFrame, short: int = 5, mid: int = 10, long: int = 20) -> pd.Series:
    """均线多头排列。短期、中期、长期均线依次从上到下排列，是典型的强势上升趋势特征，适合持股。"""
    ma_s = _get_ma(df, short)
    ma_m = _get_ma(df, mid)
    ma_l = _get_ma(df, long)
    return (ma_s > ma_m) & (ma_m > ma_l)

def macro_12_bearish_ma_array(df: pd.DataFrame, short: int = 5, mid: int = 10, long: int = 20) -> pd.Series:
    """均线空头排列。短期、中期、长期均线从下到上依次排列，标志着市场处于弱势下跌趋势中，风险较大。"""
    ma_s = _get_ma(df, short)
    ma_m = _get_ma(df, mid)
    ma_l = _get_ma(df, long)
    return (ma_s < ma_m) & (ma_m < ma_l)

def macro_13_ma_golden_cross(df: pd.DataFrame, short: int = 5, long: int = 10) -> pd.Series:
    """均线金叉。短期均线向上穿过长期均线，代表短期买入成本超过长期平均成本，通常作为买入参考信号。"""
    ma_s = _get_ma(df, short)
    ma_l = _get_ma(df, long)
    cross_today = ma_s > ma_l
    cross_yest = ma_s.groupby(df['ts_code']).shift(1) <= ma_l.groupby(df['ts_code']).shift(1)
    return cross_today & cross_yest

def macro_14_ma_death_cross(df: pd.DataFrame, short: int = 5, long: int = 10) -> pd.Series:
    """均线死叉。短期均线向下穿过长期均线，表示短期动能走弱，市场卖压增强，常用于预警趋势转空。"""
    ma_s = _get_ma(df, short)
    ma_l = _get_ma(df, long)
    cross_today = ma_s < ma_l
    cross_yest = ma_s.groupby(df['ts_code']).shift(1) >= ma_l.groupby(df['ts_code']).shift(1)
    return cross_today & cross_yest

def macro_15_ma_consolidation(df: pd.DataFrame, n: int = 5, m: float = 5.0) -> pd.Series:
    """均线粘合。多条均线之间的间距非常接近，表明市场进入震荡筑底或蓄势阶段，即将选择突破方向。"""
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
    """均线发散。由粘合转为多头排列且间距不断扩大，意味着行情脱离震荡区，正处于猛烈的加速上涨阶段。"""
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
    """突破关键均线。股价放量收阳并站上指定周期的均线（如20日线），代表市场短期走强，趋势可能反转。"""
    ma = _get_ma(df, n)
    prev_close = df.groupby('ts_code')['close'].shift(1)
    return (prev_close < ma) & (df['close'] > ma) & (df['close'] > df['open'])

def macro_18_pullback_to_ma(df: pd.DataFrame, n: int = 20) -> pd.Series:
    """回踩均线支撑。股价在下跌中触及均线后收回，表明该均线具有较强的技术支撑力，是潜在的低吸机会。"""
    ma = _get_ma(df, n)
    return (df['low'] < ma) & (df['close'] > ma)

def macro_19_high_bias(df: pd.DataFrame, n: int = 20, m: float = 15.0) -> pd.Series:
    """乖离率过大（超买）。股价偏离均线过远，技术上有修正回踩的需求，警惕股价在极度乐观后的回调风险。"""
    ma = _get_ma(df, n)
    bias = (df['close'] - ma) / ma * 100
    return bias > m

def macro_20_ma_turning_up(df: pd.DataFrame, n: int = 20, m: int = 3) -> pd.Series:
    """均线拐头向上。原本下行的均线止跌并连续多日回升，标志着市场长期成本开始重心上移，趋势转暖。"""
    ma = _get_ma(df, n)
    diff = ma.groupby(df['ts_code']).diff()
    was_dropping = (diff.groupby(df['ts_code']).shift(1).rolling(m).max() < 0)
    turning_up = diff > 0
    return was_dropping & turning_up


# ---------------------------------------------------------
# 3. Technical Indicators
# ---------------------------------------------------------

def macro_21_macd_golden_cross(df: pd.DataFrame) -> pd.Series:
    """MACD金叉。快线DIF从下往上穿过慢线DEA，为常见的买入辅助参考，意味着下跌动力衰减或上涨开始。"""
    dif, dea, macd = _get_macd(df)
    cross_today = dif > dea
    cross_yest = dif.groupby(df['ts_code']).shift(1) <= dea.groupby(df['ts_code']).shift(1)
    return cross_today & cross_yest

def macro_22_macd_water_golden_cross(df: pd.DataFrame) -> pd.Series:
    """水上金叉。在零轴上方发生的MACD金叉，表明市场处于多头强势环境中，属于强力看多信号。"""
    dif, dea, macd = _get_macd(df)
    cross = macro_21_macd_golden_cross(df)
    return cross & (dif > 0) & (dea > 0)

def macro_23_macd_top_divergence(df: pd.DataFrame, n: int = 20) -> pd.Series:
    """MACD顶背离。价格创新高但MACD指标（DIF）未能创新高，暗示上涨动能减弱，极易发生趋势逆转。"""
    dif, dea, macd = _get_macd(df)
    price_high = df['close'] == df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).max())
    dif_not_high = dif < dif.groupby(df['ts_code']).transform(lambda x: x.rolling(n).max())
    return price_high & dif_not_high

def macro_24_macd_bottom_divergence(df: pd.DataFrame, n: int = 20) -> pd.Series:
    """MACD底背离。价格创新低但指标未创新低，说明下跌动能已经枯竭，股价随时可能出现报复性反弹。"""
    dif, dea, macd = _get_macd(df)
    price_low = df['close'] == df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).min())
    dif_not_low = dif > dif.groupby(df['ts_code']).transform(lambda x: x.rolling(n).min())
    return price_low & dif_not_low

def macro_25_kdj_overbought(df: pd.DataFrame) -> pd.Series:
    """KDJ超买。指标进入高分值区域（J>100或K>80），代表市场情绪过热，短线存在回调抛压的风险。"""
    k, d, j = _get_kdj(df)
    return (j > 100) | (k > 80)

def macro_26_kdj_oversold(df: pd.DataFrame) -> pd.Series:
    """KDJ超卖。指标进入低分值区域（J<0或K<20），说明短期跌幅过大，卖方力量疲软，可能存在反弹机会。"""
    k, d, j = _get_kdj(df)
    return (j < 0) | (k < 20)

def macro_27_kdj_flattening(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """KDJ钝化。指标在极高或极低位连续停留，说明行情进入极端强势或极弱状态，指标短期失效，需观察趋势方向。"""
    ob = macro_25_kdj_overbought(df)
    os = macro_26_kdj_oversold(df)
    ob_n = ob.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n
    os_n = os.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n
    return ob_n | os_n

def macro_28_boll_break_upper(df: pd.DataFrame, n: int = 20) -> pd.Series:
    """突破布林带上轨。股价收盘超过上轨线，标志着市场进入异常强势的加速波动期，但也面临均值回归风险。"""
    ma = _get_ma(df, n)
    std = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).std())
    upper = ma + 2 * std
    return df['close'] > upper

def macro_29_boll_touch_lower(df: pd.DataFrame, n: int = 20) -> pd.Series:
    """布林带下轨支撑。股价下探至下轨后止跌回升，通常表示已触及波动下限支撑，有回抽中轨的预期。"""
    ma = _get_ma(df, n)
    std = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).std())
    lower = ma - 2 * std
    return (df['low'] < lower) & (df['close'] > lower)

def macro_30_rsi_strong(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """RSI强势区间。RSI指标持续站在50分界线以上，说明市场多头占据主导地位，行情处于上升通道。"""
    rsi = _get_rsi(df, 14)
    cond = rsi > 50
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n


# ---------------------------------------------------------
# 4. Candlesticks & Patterns
# ---------------------------------------------------------

def macro_31_consecutive_up(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """连阳走势。连续N个交易日收阳线，反映多头情绪持续高涨，市场人气旺盛，资金入场意愿强烈。"""
    cond = df['close'] > df['open']
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_32_consecutive_down(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """连阴走势。连续N个交易日收阴线，反映空方力量占据绝对优势，市场信心匮乏，处于持续抛售状态。"""
    cond = df['close'] < df['open']
    return cond.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_33_doji(df: pd.DataFrame) -> pd.Series:
    """十字星。收盘价与开盘价基本持平，且上下均有影线，预示着多空平衡及市场情绪纠结，常是转势信号。"""
    body_pct = (df['close'] - df['open']).abs() / df['open']
    has_upper = df['high'] > df[['close', 'open']].max(axis=1)
    has_lower = df['low'] < df[['close', 'open']].min(axis=1)
    return (body_pct < 0.005) & has_upper & has_lower

def macro_34_whipsaw(df: pd.DataFrame) -> pd.Series:
    """搓揉线（洗盘形态）。前一日长上影线与今日长下影线组合，通常是主力通过剧烈震荡洗清浮筹的信号。"""
    yest_upper = df.groupby('ts_code')['high'].shift(1) - df[['close', 'open']].max(axis=1).groupby(df['ts_code']).shift(1)
    yest_body = (df.groupby('ts_code')['close'].shift(1) - df.groupby('ts_code')['open'].shift(1)).abs()
    
    today_lower = df[['close', 'open']].min(axis=1) - df['low']
    today_body = (df['close'] - df['open']).abs()
    
    return (yest_upper > 2 * yest_body) & (today_lower > 2 * today_body) & (today_body / df['open'] < 0.01)

def macro_35_three_white_soldiers(df: pd.DataFrame) -> pd.Series:
    """红三兵。连续三根小阳线且收盘价逐日抬高，是底部反转或趋势确认的经典形态，意味着多头稳步推进。"""
    c1 = df['close'] > df['open']
    c2 = c1.groupby(df['ts_code']).shift(1)
    c3 = c1.groupby(df['ts_code']).shift(2)
    
    o_in_prev1 = (df['open'] > df.groupby('ts_code')['open'].shift(1)) & (df['open'] < df.groupby('ts_code')['close'].shift(1))
    o_in_prev2 = (df.groupby('ts_code')['open'].shift(1) > df.groupby('ts_code')['open'].shift(2)) & (df.groupby('ts_code')['open'].shift(1) < df.groupby('ts_code')['close'].shift(2))
    
    high_close1 = df['close'] > df.groupby('ts_code')['close'].shift(1)
    high_close2 = df.groupby('ts_code')['close'].shift(1) > df.groupby('ts_code')['close'].shift(2)
    
    return c1 & c2 & c3 & o_in_prev1 & o_in_prev2 & high_close1 & high_close2

def macro_36_bottom_consolidation(df: pd.DataFrame, n: int = 20, m: float = 15.0) -> pd.Series:
    """底部横盘。股价在过去N天内波动幅度极小，呈现箱体震荡特征，常为大资金正在进行长周期吸筹。"""
    high_n = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).max())
    low_n = df.groupby('ts_code')['close'].transform(lambda x: x.rolling(n).min())
    return ((high_n - low_n) / low_n * 100) < m

def macro_37_platform_breakout(df: pd.DataFrame, n: int = 20) -> pd.Series:
    """平台突破。股价强势突破过去N天形成的横盘整理箱体上缘，通常预示着新一轮拉升行情的开启。"""
    prev_high_n = df.groupby('ts_code')['close'].shift(1).groupby(df['ts_code']).transform(lambda x: x.rolling(n).max())
    return df['close'] > prev_high_n

def macro_38_pullback_stabilize(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """缩量企稳。股价连跌后成交量萎缩，今日不再创新低且收阳，暗示空头衰减，多头开始尝试性反攻。"""
    down_n = macro_32_consecutive_down(df, n).groupby(df['ts_code']).shift(1).fillna(False)
    shrink_n = macro_06_shrink_vol_drop(df, n).groupby(df['ts_code']).shift(1).fillna(False)
    prev_low = df.groupby('ts_code')['low'].shift(1)
    not_new_low = df['low'] > prev_low
    up_today = df['close'] > df['open']
    return down_n & shrink_n & not_new_low & up_today

def macro_39_v_reversal(df: pd.DataFrame, n: int = 5, m: float = 10.0) -> pd.Series:
    """V型反转。股价在经历猛烈下跌后，以同样的剧烈程度迅速收回失地，标志着极端的市场情绪转变。"""
    past_return = df.groupby('ts_code')['close'].shift(n) / df.groupby('ts_code')['close'].shift(2*n) - 1
    recent_return = df['close'] / df.groupby('ts_code')['close'].shift(n) - 1
    return (past_return < -m/100.0) & (recent_return > m/100.0)

def macro_40_double_bottom(df: pd.DataFrame, n: int = 20) -> pd.Series:
    """双底形态。股价两次回探相近的低点不破并向上突破均线，是经典的技术看涨形态，底部支撑扎实。"""
    min1 = df.groupby('ts_code')['low'].transform(lambda x: x.rolling(n//2).min())
    min2 = df.groupby('ts_code')['low'].shift(n//2).groupby(df['ts_code']).transform(lambda x: x.rolling(n//2).min())
    diff = (min1 - min2).abs() / min2
    ma = _get_ma(df, n)
    return (diff < 0.02) & (df['close'] > ma)


# ---------------------------------------------------------
# 5. A-Share Specifics
# ---------------------------------------------------------

def macro_41_limit_up(df: pd.DataFrame) -> pd.Series:
    """当日涨停。根据所在板块精准匹配涨幅上限，识别出当日表现最强劲、被资金封死在最高价的个股。"""
    return _is_limit_up(df)

def macro_42_limit_down(df: pd.DataFrame) -> pd.Series:
    """当日跌停。识别出当日触及板块跌幅下限的个股，通常预示着重大利空、极端恐慌或趋势崩溃。"""
    return _is_limit_down(df)

def macro_43_consecutive_limit_up(df: pd.DataFrame, n: int = 2) -> pd.Series:
    """连板。个股连续N个交易日涨停，是短线极度强势股的象征，通常伴随热门概念炒作和极高关注度。"""
    lu = _is_limit_up(df)
    return lu.groupby(df['ts_code']).rolling(n).sum().reset_index(0, drop=True) == n

def macro_44_broken_limit_up(df: pd.DataFrame) -> pd.Series:
    """炸板。盘中曾触及涨停价但收盘未能封死，反映出涨停价位置抛压巨大或主力封板意愿动摇。"""
    is_star_chinext = df['ts_code'].str.startswith(('300', '688'))
    limit = pd.Series(9.8, index=df.index).mask(is_star_chinext, 19.8)
    
    prev_close = df.groupby('ts_code')['close'].shift(1)
    high_pct = (df['high'] - prev_close) / prev_close * 100
    return (high_pct >= limit) & (~_is_limit_up(df))

def macro_45_floor_to_ceiling(df: pd.DataFrame) -> pd.Series:
    """地天板。股价从跌停价拉升至涨停价收盘，展现了多头惊人的反转能力，通常是妖股或剧烈洗盘特征。"""
    is_star_chinext = df['ts_code'].str.startswith(('300', '688'))
    limit_down_pct = pd.Series(-9.8, index=df.index).mask(is_star_chinext, -19.8)
    
    prev_close = df.groupby('ts_code')['close'].shift(1)
    low_pct = (df['low'] - prev_close) / prev_close * 100
    
    return (low_pct <= limit_down_pct) & _is_limit_up(df)

def macro_46_straight_limit_up(df: pd.DataFrame) -> pd.Series:
    """一字涨停。开盘即涨停且全天未打开，显示了极强的买入意愿和极度的供需失衡，散户极难买入。"""
    return _is_limit_up(df) & (df['open'] == df['close']) & (df['high'] == df['low'])

def macro_47_t_limit_up(df: pd.DataFrame) -> pd.Series:
    """T字涨停。开盘即涨停，盘中曾短暂回落后再次封死涨停，通常表示有一定分歧但最终被多头完全掌控。"""
    return _is_limit_up(df) & (df['open'] == df['close']) & (df['low'] < df['close'])

def macro_48_first_limit_up(df: pd.DataFrame) -> pd.Series:
    """首板。股价在经历非涨停状态后出现的第一个涨停，通常作为强势行情启动或板块跟风的起点。"""
    lu_today = _is_limit_up(df)
    lu_yest = _is_limit_up(df).groupby(df['ts_code']).shift(1).fillna(False)
    return lu_today & ~lu_yest

def macro_49_all_time_high(df: pd.DataFrame) -> pd.Series:
    """创历史新高。股价突破上市以来所有的历史高点，上方无任何套牢盘压力，象征着个股进入星辰大海。"""
    highest_ever = df.groupby('ts_code')['high'].transform('cummax')
    return df['high'] == highest_ever

def macro_50_limit_up_trap(df: pd.DataFrame) -> pd.Series:
    """涨停陷阱（断板大阴）。前一日涨停，今日大幅低开或收大阴线，是典型的诱多后反杀形态，风险极大。"""
    lu_yest = _is_limit_up(df).groupby(df['ts_code']).shift(1).fillna(False)
    big_drop = df['pct_change'] < -5.0
    yin = df['close'] < df['open']
    return lu_yest & big_drop & yin

def macro_51_consecutive_loss(df: pd.DataFrame, n: int = 3) -> pd.Series:
    """连续亏损。公司连续N年净利润为负。该条件基于年度净利润数据(net_profit)进行计算。
    
    注意：df 需要包含 'ts_code', 'year', 'net_profit' 列。
    """
    if 'net_profit' not in df.columns:
        return pd.Series(False, index=df.index)
    
    annual = df[['ts_code', 'year', 'net_profit']].drop_duplicates().sort_values(['ts_code', 'year'])
    annual['is_loss'] = annual['net_profit'] < 0
    
    def count_consecutive_loss(x):
        if len(x) < n:
            return pd.Series([False] * len(x), index=x.index)
        consecutive = pd.Series(False, index=x.index)
        current_streak = 0
        for i in range(len(x) - 1, -1, -1):
            if x.iloc[i]:
                current_streak += 1
                if current_streak >= n:
                    consecutive.iloc[i] = True
            else:
                current_streak = 0
        return consecutive
    
    annual['met'] = annual.groupby('ts_code')['is_loss'].apply(count_consecutive_loss).reset_index(level=0, drop=True)
    
    result = df.merge(annual[['ts_code', 'year', 'met']], on=['ts_code', 'year'], how='left')['met'].fillna(False)
    return result