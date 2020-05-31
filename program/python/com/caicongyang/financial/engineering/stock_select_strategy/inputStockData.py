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


sys.path.append("/data/python/program")
sys.path.append(os.path.realpath('/data/python/program'))
sys.path.append(os.path.realpath('/data/python/program/python/com/caicongyang/financial/engineering/utils'))

print(sys.path)


from program.python.com.caicongyang.financial.engineering.utils.MySQLUtil import MySQLUtil


from jqdatasdk import *


auth('13774598865', '123456')

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 获取所有的股票
stocks_list = list(get_all_securities(['stock']).index)

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



engine = MySQLUtil('49.235.178.21', '3306', 'root', '24777365ccyCCY!', 'stock')

for y in ['2020-05-29']:
    for x in stocks_list:
        getStockPrice(engine, x, y)

# print('--------------')

# flow = get_money_flow(stocks_list, start_date='2019-06-28', end_date='2019-06-29',
#                       fields=["date
#
#                       ", "sec_code", "net_pct_main", "net_pct_xl", "net_pct_l"], count=None)
#
# flow = flow[flow['net_pct_main'] > 10]
#
# flow = flow[flow['net_pct_main'] < 50]
#
# df6 = flow.sort_values(by="net_pct_main", ascending=False)
#
# print(df6)

# d = get_industry("000877.XSHE", date="2018-06-01")
# print(d)
# print(d['000877.XSHE']['sw_l1']['industry_name'])
