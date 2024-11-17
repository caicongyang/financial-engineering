# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把Tushare的数据导入到本地数据库
"""

import sys
from sqlalchemy import create_engine
import tushare as ts
import pandas as pd
from datetime import datetime

# 初始化pro接口
pro = ts.pro_api('6331c623af30880bbe71a753c236208f6b279b5c30250fddb5dfe154')

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '101.43.6.49'
mysql_port = '3333'
mysql_db = 'stock'
table_name = 't_stock'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

# 数据库表字段和 DataFrame 列名的映射
column_mapping = {
    'ts_code': 'stock_code',
    'name': 'stock_name',
    'trade_date': 'trade_date',
    'open': 'open',
    'high': 'high',
    'low': 'low',
    'close': 'close',
    'pre_close': 'pre_close',
    'change': 'chg',
    'pct_chg': 'pct_chg',
    'vol': 'volume',
    'amount': 'amount'
}

def df_to_mysql(df, table_name, column_mapping):
    # 处理ts_code字段，只保留前6位
    df['ts_code'] = df['ts_code'].str[:6]
    
    # 根据映射关系重命名 DataFrame 列
    df = df.rename(columns=column_mapping)
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=500)
        print(f"DataFrame has been successfully inserted into the {table_name} table.")
    except Exception as e:
        print(f"An error occurred: {e}")

def process_stock_data(date):
    """处理指定日期的所有股票数据"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        date = date.replace('-', '')
        
        print(f"Starting to process stock data for date: {date}")
        
        # 获取所有A股股票的基本信息
        stock_basic = pro.query('stock_basic', 
                              exchange='', 
                              list_status='L', 
                              fields='ts_code,symbol,name,industry,list_date')
        
        # 创建股票代码（前6位）和名称的映射字典
        stock_name_dict = dict(zip(stock_basic['ts_code'].str[:6], stock_basic['name']))
        
        # 一次性获取所有股票的行情数据
        df = pro.daily(**{
            "ts_code": "",
            "trade_date": date,
            "start_date": "",
            "end_date": "",
            "offset": "",
            "limit": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount"
        ])
        
        # 判断为空说明是脏数据
        if df.empty:
            print(f"No data found for date: {date}")
            return
            
        # 添加股票名称（使用处理后的ts_code）
        df['name'] = df['ts_code'].str[:6].map(stock_name_dict)
        
        # 删除行索引
        df = df.reset_index(drop=True)
        
        # 保存到数据库
        df_to_mysql(df, table_name, column_mapping)
        
        print(f"Completed processing stock data for date: {date}")
        
    except ValueError:
        print(f"Invalid date format: {date}. Please use YYYY-MM-DD format.")
    except Exception as e:
        print(f"An error occurred while processing data: {e}")

if __name__ == "__main__":
    # 示例：处理指定日期的数据
    date_to_process = '2024-11-08'
    process_stock_data(date_to_process)