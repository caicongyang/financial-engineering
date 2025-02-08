# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
获取股票的概念信息
"""
import pandas as pd
import json
import time
from jqdatasdk import *
from MySQLUtil import *

auth('13774598865', '123456')


def get_stock_price_week(engine, stock_code):
    df = get_bars(stock_code, 52, unit='1w', fields=['date', 'open', 'high', 'low', 'close', 'volume'],
                  include_now=True,
                  end_dt=None, fq_ref_date=None, df=True)
    # 判断为空说明是脏数据
    df['stock_code'] = stock_code
    df['stock_name'] = ''
    df['money'] = ''

    # 删除行索引
    df = df.reset_index(drop=True)

    for indexs in df.index:
        indexs_ = df.loc[indexs]
        json_str = indexs_.to_json(orient='index', date_format='iso')

        # 转成json 字符串
        json_obj = json.loads(json_str)
        date_ = json_obj['date']
        print(date_[0:10])
        json_obj['date'] = json_obj['date'][0:10]

        paramsDict = {"stock_code": stock_code, "trading_day": json_obj['date']}
        print(paramsDict)

        result_ = engine.querybyseesion(
            "select * from T_Stock_Week where  stock_code = :stock_code and trading_day = :trading_day ",
            paramsDict)
        if len(result_) == 0:
            inser_sql_ = 'insert into T_Stock_Week(stock_code,stock_name,trading_day,open,close,high,low,volume) values(:stock_code,:stock_name,:date,:open,:close,:high,:low,:volume)';
            engine.insert(inser_sql_, json_obj)
        else:
            print(str(paramsDict) + "当天周数据已经存在")


stocks_list = list(get_all_securities(['stock']).index)
engine = MySQLUtil('49.235.178.21', '3306', 'root', '24777365ccyCCY!', 'stock')
for x in stocks_list:
    get_stock_price_week(engine, x)
