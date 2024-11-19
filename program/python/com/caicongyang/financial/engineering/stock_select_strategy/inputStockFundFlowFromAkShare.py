# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的股票资金流数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd
from datetime import datetime
import re

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '101.43.6.49'
mysql_port = '3333'
mysql_db = 'stock'
table_name = 't_stock_fund_flow_tsh'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

def convert_fund_flow(value):
    """将资金流入净额转换为数值（单位：元）"""
    try:
        if isinstance(value, (int, float)):
            return float(value)
        
        # 移除可能的逗号
        value = value.replace(',', '')
        
        # 处理亿级别
        if '亿' in value:
            number = float(value.replace('亿', ''))
            return number * 100000000
        
        # 处理万级别
        if '万' in value:
            number = float(value.replace('万', ''))
            return number * 10000
        
        return float(value)
    except:
        return 0

def convert_percentage(value):
    """将百分比字符串转换为数值"""
    try:
        if isinstance(value, (int, float)):
            return float(value)
        return float(value.strip('%'))
    except:
        return 0

def process_fund_flow_data(date):
    """处理指定日期的资金流数据"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        
        print(f"Starting to process fund flow data for date: {date}")
        
        # 获取3日排行数据
        df = ak.stock_fund_flow_individual(symbol="3日排行")
        
        # 删除'序号'列
        df = df.drop(['序号'], axis=1, errors='ignore')
        
        # 重命名列
        df = df.rename(columns={
            '股票代码': 'stock_code',
            '股票简称': 'stock_name',
            '最新价': 'price',
            '阶段涨跌幅': 'change_rate',
            '连续换手率': 'turnover_rate',
            '资金流入净额': 'fund_flow'
        })
        
        # 转换数据类型
        df['change_rate'] = df['change_rate'].apply(convert_percentage)
        df['turnover_rate'] = df['turnover_rate'].apply(convert_percentage)
        df['fund_flow'] = df['fund_flow'].apply(convert_fund_flow)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # 添加日期列
        df['trade_date'] = pd.to_datetime(date)
        
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