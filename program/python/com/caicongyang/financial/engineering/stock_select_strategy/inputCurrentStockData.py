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
from program.python.com.caicongyang.financial.engineering.utils.DateTimeUtil import *

from jqdatasdk import *

auth('13774598865', '123456')



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
    inser_sql = 'insert into T_Stock(stock_code,stock_name,trading_day,open,close,high,low,volume,money) values(:stock_code,:stock_name,:trading_day,:open,:close,:high,:low,:volume,:money)';
    try:
        engine.insert(inser_sql, insert_data)
    except:
        print("Unexpected error:")


def getAllStockPrice(trading_day):
    # 数据库连接池
    engine = MySQLUtil('49.235.178.21', '3306', 'root', '24777365ccyCCY!', 'stock')
    # 获取所有的股票
    stocks_list = list(get_all_securities(['stock']).index)
    for x in stocks_list:
        getStockPrice(engine, x, trading_day)


currentDay = get_current_day()
week = datetime.datetime.strptime(currentDay, '%Y-%m-%d').strftime("%w")

# 上证指数 "000001.XSHG"
df = get_price('000001.XSHG', start_date=currentDay, end_date=currentDay, frequency='daily', fields=None,
               skip_paused=False, fq=None)

print(df)
if df.empty:
    print("交易锁暂停交易：" + currentDay)
else:
    getAllStockPrice(currentDay)


