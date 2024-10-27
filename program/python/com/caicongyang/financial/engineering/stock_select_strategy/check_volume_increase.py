# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
检查指定日期的成交量是否是前一个交易日的2倍以上，如果是，则将该股票信息插入到新表中
"""

# 导入所需的库
from sqlalchemy import create_engine, text  # 用于数据库操作
import pandas as pd  # 用于数据处理
from datetime import datetime, timedelta  # 用于日期处理

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '159.138.152.92'
mysql_port = '3333'
mysql_db = 'stock'
source_table = 't_stock'  # 源数据表名
target_table = 't_volume_increase'  # 目标数据表名

# 创建数据库连接
engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

def get_previous_trading_day(current_date):
    # 构建SQL查询，获取指定日期之前的最近交易日
    query = text(f"""
    SELECT MAX(trade_date)
    FROM {source_table}
    WHERE trade_date < :current_date
    """)
    
    # 执行查询并返回结果
    with engine.connect() as conn:
        result = conn.execute(query, {'current_date': current_date})
        previous_date = result.scalar()
    
    return previous_date

def check_and_store_volume_increase(today):
    # 获取前一个交易日
    yesterday = get_previous_trading_day(today)

    # 如果没有找到前一个交易日，打印信息并退出函数
    if not yesterday:
        print(f"No previous trading day found before {today}")
        return

    print(f"Checking volume increase for dates: {yesterday} to {today}")

    # 构建SQL查询，获取今天和昨天的股票数据
    query = text(f"""
    SELECT t1.stock_code, t1.trade_date, t1.volume as today_volume, 
           t2.volume as yesterday_volume, t1.close, t1.open, t1.high, t1.low
    FROM {source_table} t1
    JOIN {source_table} t2 ON t1.stock_code = t2.stock_code
    WHERE t1.trade_date = :today AND t2.trade_date = :yesterday
    """)

    # 执行查询并将结果加载到DataFrame中
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'today': today, 'yesterday': yesterday})

    # 计算成交量增长比例
    df['volume_increase_ratio'] = df['today_volume'] / df['yesterday_volume']

    # 筛选出成交量增加2倍以上的股票
    df_increased = df[df['volume_increase_ratio'] >= 2].copy()

    # 保留需要的列并重命名
    df_increased = df_increased[['stock_code', 'trade_date', 'volume_increase_ratio', 'close', 'open', 'high', 'low']]
    df_increased.columns = ['stock_code', 'trade_date', 'volume_increase_ratio', 'close', 'open', 'high', 'low']

    # 将结果存储到新表中
    if not df_increased.empty:
        try:
            # 将数据插入到目标表中
            df_increased.to_sql(target_table, con=engine, if_exists='append', index=False)
            print(f"{len(df_increased)} stocks with volume increase >= 2 have been inserted into the {target_table} table for {today}.")
        except Exception as e:
            print(f"An error occurred while inserting data for {today}: {e}")
    else:
        print(f"No stocks found with volume increase >= 2 times for {today}.")

def batch_check_volume_increase(date_list):
    for date in date_list:
        try:
            datetime.strptime(date, '%Y-%m-%d')
            check_and_store_volume_increase(date)
        except ValueError:
            print(f"Incorrect date format for {date}, should be YYYY-MM-DD. Skipping this date.")

if __name__ == "__main__":
    # 示例：批量检查多个日期
    dates_to_check = ['2024-10-21','2024-10-22']
    batch_check_volume_increase(dates_to_check)
