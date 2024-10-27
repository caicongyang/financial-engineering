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
mysql_host = '159.138.152.92'
mysql_port = '3333'
mysql_db = 'stock'
table_name = 't_etf'

# 数据库表字段和 DataFrame 列名的映射
column_mapping = {
    'code': 'stock_code',
    'name': 'stock_name',  # 新增名称字段的映射
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


# 获取ETF列表
fund_etf_spot_em_df = ak.fund_etf_spot_em()
# 创建代码和名称的映射字典
code_name_dict = dict(zip(fund_etf_spot_em_df['代码'], fund_etf_spot_em_df['名称']))

for code in code_name_dict.keys():
    fund_etf_hist_em_df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date="20241023", end_date="20241027",
                                              adjust="")
    fund_etf_hist_em_df['code'] = code
    fund_etf_hist_em_df['name'] = code_name_dict[code]  # 添加名称字段
    
    # 要删除的列
    columns_to_drop = ['振幅', '涨跌幅', '涨跌额', '换手率']

    # 删除 DataFrame 中存在的列
    fund_etf_hist_em_df.drop(columns=[col for col in columns_to_drop if col in fund_etf_hist_em_df.columns],
                             inplace=True)

    # 插入到数据库
    df_to_mysql(fund_etf_hist_em_df, table_name, column_mapping, mysql_user, mysql_password, mysql_host, mysql_port,
                mysql_db)

print("All ETF data has been processed and inserted into the database.")
