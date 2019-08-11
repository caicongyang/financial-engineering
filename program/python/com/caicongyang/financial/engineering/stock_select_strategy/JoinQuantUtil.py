# !/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
from jqdatasdk import *
from program.python.com.caicongyang.financial.engineering.utils import DateTimeUtil

auth('13774598865', '123456')

pd.set_option('expand_frame_repr', False)


def verify_stock(stock_code):
    """
    获取当天时间的字符串时间
    :return:
    """
    last = get_price(stock_code, start_date=DateTimeUtil.get_last_tran_day(), end_date=DateTimeUtil.get_current_day(),
                     frequency='daily', fields=None, skip_paused=False,
                     fq='pre',
                     count=None)
    # 收盘价 > 开盘价
    last["compare1"] = last["close"] / last["open"]
    # 收盘价> 最高价
    last["compare2"] = last["close"] / last["high"]
    if float(last["compare2"].head(1)) < 1 or float(last["compare1"].head(1)) < 1:
        return False
    else:
        return True


verify_stock("000001.XSHE")
