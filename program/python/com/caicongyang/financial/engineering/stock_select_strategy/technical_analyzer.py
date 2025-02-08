def calculate_technical_factors(stock_code, date):
    """计算技术指标"""
    # 获取最近20天的数据
    query = text("""
        SELECT trade_date, close, high, low, volume 
        FROM t_stock 
        WHERE stock_code = :code 
        AND trade_date <= :date 
        ORDER BY trade_date DESC 
        LIMIT 20
    """)
    
    # 计算MACD
    closes = df['close'].values
    exp12 = closes.ewm(span=12, adjust=False).mean()
    exp26 = closes.ewm(span=26, adjust=False).mean()
    macd = exp12 - exp26
    
    # 计算RSI
    delta = closes.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return {
        'macd': macd[-1],
        'rsi': rsi[-1],
        'bollinger_%b': (closes[-1] - lower_band) / (upper_band - lower_band)
    } 