# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
检查指定日期的ETF成交量是否是前一个交易日的2倍以上，如果是，则将该ETF信息插入到新表中
"""

from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '101.43.6.49'
mysql_port = '3333'
mysql_db = 'stock'
source_table = 't_etf'  # ETF数据表
target_table = 't_etf_volume_increase'  # ETF成交量增加表

# 创建数据库连接
engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

def check_data_exists(date):
    """检查指定日期的ETF数据是否存在"""
    query = text(f"""
    SELECT COUNT(*) 
    FROM {source_table}
    WHERE trade_date = :date
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {'date': date})
        count = result.scalar()
    
    return count > 0

def get_previous_trading_day(current_date):
    query = text(f"""
    SELECT MAX(trade_date)
    FROM {source_table}
    WHERE trade_date < :current_date
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {'current_date': current_date})
        previous_date = result.scalar()
    
    return previous_date

def check_and_store_volume_increase(today):
    yesterday = get_previous_trading_day(today)

    if not yesterday:
        print(f"No previous trading day found before {today}")
        return

    print(f"Checking ETF volume increase for dates: {yesterday} to {today}")

    query = text(f"""
    SELECT t1.stock_code, t1.stock_name, t1.trade_date, t1.volume as today_volume, 
           t2.volume as yesterday_volume, t1.close, t1.open, t1.high, t1.low
    FROM {source_table} t1
    JOIN {source_table} t2 ON t1.stock_code = t2.stock_code
    WHERE t1.trade_date = :today AND t2.trade_date = :yesterday
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'today': today, 'yesterday': yesterday})

    # 处理前一天成交量为0的情况
    df['volume_increase_ratio'] = df.apply(
        lambda x: float('inf') if x['yesterday_volume'] == 0 and x['today_volume'] > 0
        else 0 if x['yesterday_volume'] == 0 and x['today_volume'] == 0
        else x['today_volume'] / x['yesterday_volume'], 
        axis=1
    )

    # 将无穷大值替换为一个较大的数字，比如999999
    df['volume_increase_ratio'] = df['volume_increase_ratio'].replace([float('inf')], 999999)

    df_increased = df[df['volume_increase_ratio'] >= 2].copy()
    df_increased = df_increased[['stock_code', 'stock_name', 'trade_date', 'volume_increase_ratio', 'close', 'open', 'high', 'low']]
    df_increased.columns = ['stock_code', 'stock_name', 'trade_date', 'volume_increase_ratio', 'close', 'open', 'high', 'low']

    if not df_increased.empty:
        try:
            df_increased.to_sql(target_table, con=engine, if_exists='append', index=False)
            print(f"{len(df_increased)} ETFs with volume increase >= 2 have been inserted into the {target_table} table for {today}.")
        except Exception as e:
            print(f"An error occurred while inserting data for {today}: {e}")
    else:
        print(f"No ETFs found with volume increase >= 2 times for {today}.")

def batch_check_volume_increase(date_list):
    for date in date_list:
        try:
            datetime.strptime(date, '%Y-%m-%d')
            # 检查数据是否存在
            if check_data_exists(date):
                check_and_store_volume_increase(date)
            else:
                print(f"No ETF data found for date: {date}, skipping volume increase check.")
        except ValueError:
            print(f"Incorrect date format for {date}, should be YYYY-MM-DD. Skipping this date.")

if __name__ == "__main__":
    # 示例：批量检查多个日期
    dates_to_check = ['2024-11-14', '2024-11-15', '2024-11-18', '2024-11-19']
    batch_check_volume_increase(dates_to_check)
