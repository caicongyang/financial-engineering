# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
把AkShare的数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd
import pymysql

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '127.0.0.1'
mysql_port = '3306'
mysql_db = 'stock'
table_name = 'T_etf'

# 数据库表字段和 DataFrame 列名的映射
column_mapping = {
    'code': 'stock_code',
    '日期': 'trade_date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'amount'
}

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')


def df_to_mysql(df, table_name, column_mapping, mysql_user, mysql_password, mysql_host, mysql_port, mysql_db):
    # 根据映射关系重命名 DataFrame 列
    df = df.rename(columns=column_mapping)

    # 确保日期列的格式正确
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'])



    # 将 DataFrame 写入 MySQL
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=100)
        print(f"DataFrame has been successfully inserted into the {table_name} table.")
    except Exception as e:
        print(f"An error occurred: {e}")


fund_etf_spot_em_df = ak.fund_etf_spot_em()
code_list = fund_etf_spot_em_df['代码'].tolist()

for x in code_list:
    fund_etf_hist_em_df = ak.fund_etf_hist_em(symbol=x, period="daily", start_date="20240901", end_date="20240930",
                                              adjust="")
    fund_etf_hist_em_df['code'] = x
    # 删除不需要的列
    fund_etf_hist_em_df.drop(columns=['振幅', '涨跌幅', '涨跌额', '换手率'], inplace=True)

    # 插入到数据库
    df_to_mysql(fund_etf_hist_em_df, table_name, column_mapping, mysql_user, mysql_password, mysql_host, mysql_port,
                mysql_db)

