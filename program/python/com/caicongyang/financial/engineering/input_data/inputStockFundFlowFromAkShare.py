# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的股票资金流数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd
from datetime import datetime
import numpy as np
import os
from dotenv import load_dotenv
from com.caicongyang.financial.engineering.utils.env_loader import load_env

# 加载环境变量 - 使用通用加载模块
load_env()

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)
# 设置未来行为，避免警告
pd.set_option('future.no_silent_downcasting', True)

# 数据库连接信息
mysql_user = os.getenv('DB_USER')
mysql_password = os.getenv('DB_PASSWORD')
mysql_host = os.getenv('DB_HOST')
mysql_port = os.getenv('DB_PORT')
mysql_db = os.getenv('DB_NAME')
table_name = 't_stock_fund_flow_tsh'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

def pad_stock_code(code):
    """将股票代码补足6位"""
    return str(code).zfill(6)

def check_data_exists(date):
    """检查指定日期的数据是否已存在"""
    query = f"SELECT COUNT(*) FROM {table_name} WHERE trade_date = '{date}'"
    result = pd.read_sql(query, engine)
    return result.iloc[0, 0] > 0

def process_fund_flow_data(date):
    """处理指定日期的资金流数据"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        
        # 检查数据是否已存在
        if check_data_exists(date):
            print(f"Data for {date} already exists in the database. Skipping...")
            return
        
        print(f"Starting to process fund flow data for date: {date}")
        
        # 获取3日排行数据
        df = ak.stock_fund_flow_individual(symbol="3日排行")

        # 删除'序号'列和索引
        df = df.drop(['序号'], axis=1, errors='ignore')
        df = df.reset_index(drop=True)

        # 重命名列
        df = df.rename(columns={
            '股票代码': 'stock_code',
            '股票简称': 'stock_name',
            '最新价': 'price',
            '阶段涨跌幅': 'change_rate',
            '连续换手率': 'turnover_rate',
            '资金流入净额': 'fund_flow'
        })

        # 补足股票代码为6位
        df['stock_code'] = df['stock_code'].astype(str).apply(pad_stock_code)
        
        # 添加日期列
        df['trade_date'] = pd.to_datetime(date)
        
        # 删除包含空值的行
        df = df.dropna(subset=['price'])
        
        # 打印处理后的数据（不显示索引）
        print(df.to_string(index=False))
        
        # 保存到数据库
        try:
            df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=500)
            print(f"Fund flow data has been successfully inserted into the {table_name} table.")
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")
            
        print(f"Completed processing fund flow data for date: {date}")
        
    except ValueError:
        print(f"Invalid date format: {date}. Please use YYYY-MM-DD format.")
    except Exception as e:
        print(f"An error occurred while processing data: {e}")

if __name__ == "__main__":
    # 示例：处理指定日期的数据
    date_to_process = '2024-11-19'
    process_fund_flow_data(date_to_process) 