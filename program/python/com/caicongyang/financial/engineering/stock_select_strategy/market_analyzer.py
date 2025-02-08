def get_market_status(date):
    """判断市场整体状态"""
    # 获取沪深300指数数据
    query = text("""
        SELECT * FROM t_index 
        WHERE index_code = '000300' 
        AND trade_date BETWEEN DATE_SUB(:date, INTERVAL 20 DAY) AND :date
    """)
    
    # 计算均线
    ma5 = df['close'].rolling(5).mean()
    ma20 = df['close'].rolling(20).mean()
    
    # 判断趋势
    if ma5[-1] > ma20[-1] and df['volume'][-1] > df['volume'].mean():
        return 'bull'
    elif ma5[-1] < ma20[-1] and df['volume'][-1] > df['volume'].mean():
        return 'bear'
    else:
        return 'neutral' 