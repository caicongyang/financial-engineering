# !/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
from jqdatasdk import *
from program.python.com.caicongyang.financial.engineering.utils import DateTimeUtil

auth('13774598865', '123456')

pd.set_option('expand_frame_repr', False)


def verify_stock(stock_code, start_date, end_date):
    """
    获取当天时间的字符串时间
    :return:
    """
    last = get_price(stock_code, start_date=start_date, end_date=end_date,
                     frequency='daily', fields=None, skip_paused=False,
                     fq='none',
                     count=None)
    # 收盘价 > 开盘价
    last["compare1"] = last["close"] / last["open"]

    # 收盘价> 最高价
    last["compare2"] = last["close"] / last["high"]


    if float(last["compare2"].tail(1)) < 1 or float(last["compare1"].tail(1)) < 1:
        return False
    else:
        return True


# print(verify_stock("002175.XSHE", '2019-09-06', '2019-09-12'))
