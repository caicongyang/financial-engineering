# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
把聚宽的数据导入到本地数据库
"""

import sys

from sqlalchemy import create_engine
import tushare as ts
import pandas as pd

# 初始化pro接口
pro = ts.pro_api('6331c623af30880bbe71a753c236208f6b279b5c30250fddb5dfe154')

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 使用示例
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '127.0.0.1'
mysql_port = '3306'
mysql_db = 'stock'
table_name = 't_stock'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')


def df_to_mysql(df, table_name, mysql_user, mysql_password, mysql_host, mysql_port, mysql_db):
    # 创建 MySQL 连接引擎
    # 将 DataFrame 写入 MySQL
    try:
        # 如果表已经存在，替换旧表的数据（可以选择 'append' 或 'replace'）
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=500)
        print(f"DataFrame has been successfully inserted into the {table_name} table.")
    except Exception as e:
        print(f"An error occurred: {e}")


def getStockPrice(trade_date):
    # 获取所有的股票
    df = pro.daily(**{
        "ts_code": "",
        "trade_date": trade_date,
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
    # 删除行索引
    df = df.reset_index(drop=True)
    # 判断为空说明是脏数据
    if df.empty:
        return
    df_to_mysql(df, table_name, mysql_user, mysql_password, mysql_host, mysql_port, mysql_db)


# 补偿数据所用
for x in ['20240902','20240903','20240904','20240905','20240906','20240909','20240910','20240911','20240912','20240913','20240918','20240919','20240920','20240923','20240924','20240925','20240926','20240927','20240930']:
        getStockPrice(x)