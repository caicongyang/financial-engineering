# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
把聚宽的数据导入到本地数据库
"""

import pandas as pd
import json
from sqlalchemy import create_engine
import sys
import os

print(sys.path)

from program.python.com.caicongyang.financial.engineering.utils.MySQLUtil import *

from jqdatasdk import *

auth('13774598865', '123456')

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 获取所有的股票
stocks_list = list(get_all_securities(['etf']).index)

print(sys.path)


def getStockPrice(engine, stock_code, trading_day):
    """
    获取某一只股票的交易数据
    :param stock_code:
    :param trading_day:
    :return:
    """

    df = get_price(stock_code, start_date=trading_day, end_date=trading_day, frequency='daily', fields=None,
                   skip_paused=False, fq=None)

    df['stock_code'] = stock_code
    df['stock_name'] = ''
    df['trading_day'] = trading_day
    # 删除行索引
    df2 = df.reset_index(drop=True)

    # 将列索引变成df的行索引
    # df2 = df.set_index("stock_code")
    # dataframe转成 json 字符串
    json_str = df2.to_json(orient='index')
    # 转成json 字符串
    json_obj = json.loads(json_str)

    insert_data = json_obj['0']
    inser_sql = 'insert into T_etf(stock_code,stock_name,trading_day,open,close,high,low,volume,money) values(:stock_code,:stock_name,:trading_day,:open,:close,:high,:low,:volume,:money)';
    try:
        engine.insert(inser_sql, insert_data)
    except:
        print("Unexpected error:")


engine = MySQLUtil('49.235.178.21', '3306', 'root', '24777365ccyCCY!', 'stock')

# 补偿数据所用
for y in ['2020-07-14','2020-07-15','2020-07-16','2020-07-17']:
    for x in stocks_list:
        getStockPrice(engine, x, y)
