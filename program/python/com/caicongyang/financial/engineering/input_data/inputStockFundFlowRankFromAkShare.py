# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的东方财富资金流排名数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)
# 设置未来行为，避免警告
pd.set_option('future.no_silent_downcasting', True)

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '101.43.6.49'
mysql_port = '3333'
mysql_db = 'stock'
table_name = 't_stock_fund_flow_rank'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

def clean_numeric_data(df):
    """清理数值型数据，处理特殊字符"""
    # 处理价格列
    df['price'] = df['price'].replace(['-', '--', 'nan', ''], np.nan)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # 处理涨跌幅
    df['change_rate_3d'] = df['change_rate_3d'].replace(['-', '--', 'nan', ''], '0')
    df['change_rate_3d'] = pd.to_numeric(df['change_rate_3d'], errors='coerce')
    
    # 处理资金流相关列
    numeric_columns = [
        'main_net_inflow_3d', 'main_net_inflow_rate_3d',
        'super_net_inflow_3d', 'super_net_inflow_rate_3d',
        'big_net_inflow_3d', 'big_net_inflow_rate_3d',
        'mid_net_inflow_3d', 'mid_net_inflow_rate_3d',
        'small_net_inflow_3d', 'small_net_inflow_rate_3d'
    ]
    
    for col in numeric_columns:
        df[col] = df[col].replace(['-', '--', 'nan', ''], '0')
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def check_data_exists(date):
    """检查指定日期的数据是否已存在"""
    query = f"SELECT COUNT(*) FROM {table_name} WHERE trade_date = '{date}'"
    result = pd.read_sql(query, engine)
    return result.iloc[0, 0] > 0

def process_fund_flow_rank_data(date):
    """处理指定日期的资金流排名数据"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        
        # 检查数据是否已存在
        if check_data_exists(date):
            print(f"Data for {date} already exists in the database. Skipping...")
            return
        
        print(f"Starting to process fund flow rank data for date: {date}")
        
        # 获取3日排名数据
        df = ak.stock_individual_fund_flow_rank(indicator="3日")
        
        # 删除'序号'列
        df = df.drop(['序号'], axis=1, errors='ignore')
        
        # 重命名列
        df = df.rename(columns={
            '代码': 'stock_code',
            '名称': 'stock_name',
            '最新价': 'price',
            '3日涨跌幅': 'change_rate_3d',
            '3日主力净流入-净额': 'main_net_inflow_3d',
            '3日主力净流入-净占比': 'main_net_inflow_rate_3d',
            '3日超大单净流入-净额': 'super_net_inflow_3d',
            '3日超大单净流入-净占比': 'super_net_inflow_rate_3d',
            '3日大单净流入-净额': 'big_net_inflow_3d',
            '3日大单净流入-净占比': 'big_net_inflow_rate_3d',
            '3日中单净流入-净额': 'mid_net_inflow_3d',
            '3日中单净流入-净占比': 'mid_net_inflow_rate_3d',
            '3日小单净流入-净额': 'small_net_inflow_3d',
            '3日小单净流入-净占比': 'small_net_inflow_rate_3d'
        })
        
        # 清理数据
        df = clean_numeric_data(df)
        
        # 添加日期列
        df['trade_date'] = pd.to_datetime(date)
        
        # 删除包含空值的行
        df = df.dropna(subset=['price'])
        
        # 保存到数据库
        try:
            df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=500)
            print(f"Fund flow rank data has been successfully inserted into the {table_name} table.")
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")
            
        print(f"Completed processing fund flow rank data for date: {date}")
        
    except ValueError:
        print(f"Invalid date format: {date}. Please use YYYY-MM-DD format.")
    except Exception as e:
        print(f"An error occurred while processing data: {e}")

if __name__ == "__main__":
    # 示例：处理指定日期的数据
    date_to_process = '2024-11-19'
    process_fund_flow_rank_data(date_to_process) 