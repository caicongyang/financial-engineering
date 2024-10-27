# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '159.138.152.92'
mysql_port = '3333'
mysql_db = 'stock'
table_name = 't_stock'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

# 数据库表字段和 DataFrame 列名的映射
column_mapping = {
    '代码': 'stock_code',
    '名称': 'stock_name',
    '日期': 'trade_date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'amount',
    '涨跌幅': 'pct_chg',
    '涨跌额': 'chg'
}

def df_to_mysql(df, table_name, column_mapping):
    # 根据映射关系重命名 DataFrame 列
    df = df.rename(columns=column_mapping)

    # 确保日期列的格式正确
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'])

    try:
        # 如果表已经存在，追加新数据
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=500)
        print(f"DataFrame has been successfully inserted into the {table_name} table.")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_stock_price(symbol, name, start_date, end_date):
    # 使用 AkShare 的 stock_zh_a_hist 接口获取股票历史数据
    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
    
    # 添加股票代码和名称列
    df['代码'] = symbol
    df['名称'] = name
    
    # 删除 '振幅' 和 '换手率' 列
    df = df.drop(['振幅', '换手率','股票代码'], axis=1, errors='ignore')
    
    # 删除行索引
    df = df.reset_index(drop=True)
    
    # 判断为空说明是脏数据
    if df.empty:
        return
    
    df_to_mysql(df, table_name, column_mapping)

# 获取所有A股股票列表，包括代码和名称
stock_info = ak.stock_zh_a_spot_em()[['代码', '名称']]

# 补偿数据所用
start_date = '20240916'
end_date = '20240918'

for _, row in stock_info.iterrows():
    get_stock_price(row['代码'], row['名称'], start_date, end_date)

print("All stock data has been processed and inserted into the database.")
