# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
检查指定日期涨幅超过9.5%的股票，并将其插入到t_stock_limit表中
"""

from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from com.caicongyang.financial.engineering.utils.env_loader import load_env

# 加载环境变量 - 使用通用加载模块
load_env()

# 数据库连接信息
mysql_user = os.getenv('DB_USER')
mysql_password = os.getenv('DB_PASSWORD')
mysql_host = os.getenv('DB_HOST')
mysql_port = os.getenv('DB_PORT')
mysql_db = os.getenv('DB_NAME')
source_table = 't_stock'  # 股票数据表
target_table = 't_stock_limit'  # 涨停股票表

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

def check_and_store_limit_stocks(date):
    """检查并存储涨停股票"""
    print(f"Checking limit-up stocks for date: {date}")

    query = text(f"""
    SELECT stock_code, trade_date, pct_chg as gain
    FROM {source_table}
    WHERE trade_date = :date AND pct_chg >= 9.5
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'date': date})

    if not df.empty:
        try:
            df.to_sql(target_table, con=engine, if_exists='append', index=False)
            print(f"{len(df)} stocks with gain >= 9.5% have been inserted into the {target_table} table for {date}.")
        except Exception as e:
            print(f"An error occurred while inserting data for {date}: {e}")
    else:
        print(f"No stocks found with gain >= 9.5% for {date}.")

def batch_check_limit_stocks(date_list):
    """批量处理多个日期的涨停股票数据"""
    for date in date_list:
        try:
            datetime.strptime(date, '%Y-%m-%d')
            # 检查数据是否存在
            if check_data_exists(date):
                check_and_store_limit_stocks(date)
            else:
                print(f"No stock data found for date: {date}, skipping limit stock check.")
        except ValueError:
            print(f"Incorrect date format for {date}, should be YYYY-MM-DD. Skipping this date.")

if __name__ == "__main__":
    # 示例：批量检查多个日期
    dates_to_check = ['2024-11-19']
    batch_check_limit_stocks(dates_to_check) 