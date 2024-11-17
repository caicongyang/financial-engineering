# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
从数据库中读取最近10个交易日的股票数据，计算每个股票的10日均值，并存储到另一个表中
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
source_table = 't_stock'
target_table = 't_stock_10day_avg'

# 创建数据库连接
engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

def check_data_exists(date):
    """检查指定日期的股票数据是否存在"""
    query = text(f"""
    SELECT COUNT(*) 
    FROM {source_table}
    WHERE trade_date = :date
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {'date': date})
        count = result.scalar()
    
    return count > 0

def get_last_10_trading_days(end_date):
    query = text(f"""
    SELECT DISTINCT trade_date
    FROM {source_table}
    WHERE trade_date <= :end_date
    ORDER BY trade_date DESC
    LIMIT 10
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {'end_date': end_date})
        dates = [row[0] for row in result]
    
    return min(dates), max(dates)

def calculate_and_store_10day_average(end_date):
    # 获取最近10个交易日的起始和结束日期
    start_date, _ = get_last_10_trading_days(end_date)

    print(f"Calculating 10-day average for period: {start_date} to {end_date}")

    # SQL查询来获取最近10个交易日的股票数据
    query = text(f"""
    SELECT stock_code, trade_date, close
    FROM {source_table}
    WHERE trade_date BETWEEN :start_date AND :end_date
    ORDER BY stock_code, trade_date
    """)

    # 读取数据
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'start_date': start_date, 'end_date': end_date})

    # 将trade_date列转换为日期类型
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

    # 计算10日均值
    df_avg = df.groupby('stock_code').agg({
        'close': 'mean',
        'trade_date': 'max'
    }).reset_index()

    # 重命名列
    df_avg.columns = ['stock_code', 'avg_10day', 'trade_date']

    # 将结果存储到新表中
    try:
        df_avg.to_sql(target_table, con=engine, if_exists='append', index=False)
        print(f"10-day average data has been successfully inserted into the {target_table} table for {end_date}.")
    except Exception as e:
        print(f"An error occurred while inserting data for {end_date}: {e}")

def batch_calculate_10day_average(date_list):
    for date in date_list:
        try:
            datetime.strptime(date, '%Y-%m-%d')
            # 检查数据是否存在
            if check_data_exists(date):
                calculate_and_store_10day_average(date)
            else:
                print(f"No stock data found for date: {date}, skipping calculation.")
        except ValueError:
            print(f"Incorrect date format for {date}, should be YYYY-MM-DD. Skipping this date.")

if __name__ == "__main__":
    # 示例：批量计算多个日期的10日均值
    dates_to_calculate = ['2024-11-07']
    batch_calculate_10day_average(dates_to_calculate)
