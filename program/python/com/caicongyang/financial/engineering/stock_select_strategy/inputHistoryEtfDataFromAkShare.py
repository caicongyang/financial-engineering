# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的ETF数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd
from datetime import datetime

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '101.43.6.49'
mysql_port = '3333'
mysql_db = 'stock'
table_name = 't_etf'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

# 数据库表字段和 DataFrame 列名的映射
column_mapping = {
    'code': 'stock_code',
    'name': 'stock_name',
    '日期': 'trade_date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'amount'
}

def df_to_mysql(df, table_name, column_mapping):
    # 根据映射关系重命名 DataFrame 列
    df = df.rename(columns=column_mapping)

    # 确保日期列的格式正确
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'])

    try:
        # 如果表已经存在，追加新数据
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=100)
        print(f"DataFrame has been successfully inserted into the {table_name} table.")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_etf_price(code, name, date):
    """获取单个ETF的历史数据"""
    try:
        # 使用 AkShare 的 fund_etf_hist_em 接口获取ETF历史数据
        df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=date.replace('-', ''), 
                                end_date=date.replace('-', ''), adjust="")
        
        # 添加ETF代码和名称
        df['code'] = code
        df['name'] = name
        
        # 删除不需要的列
        columns_to_drop = ['振幅', '涨跌幅', '涨跌额', '换手率']
        df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
        
        # 删除行索引
        df = df.reset_index(drop=True)
        
        # 判断为空说明是脏数据
        if df.empty:
            return
        
        df_to_mysql(df, table_name, column_mapping)
        
    except Exception as e:
        print(f"Error processing ETF {code}: {e}")

def process_etf_data(date):
    """处理指定日期的所有ETF数据"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        
        print(f"Starting to process ETF data for date: {date}")
        
        # 获取ETF列表
        fund_etf_spot_em_df = ak.fund_etf_spot_em()
        # 创建代码和名称的映射字典
        code_name_dict = dict(zip(fund_etf_spot_em_df['代码'], fund_etf_spot_em_df['名称']))
        
        # 处理每个ETF
        for code, name in code_name_dict.items():
            get_etf_price(code, name, date)
            
        print(f"Completed processing ETF data for date: {date}")
        
    except ValueError:
        print(f"Invalid date format: {date}. Please use YYYY-MM-DD format.")
    except Exception as e:
        print(f"An error occurred while processing data: {e}")

if __name__ == "__main__":
    # 示例：处理指定日期的数据
    date_to_process = '2024-11-07'
    process_etf_data(date_to_process)
