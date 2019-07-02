# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
时间工具类
"""

import datetime
import time

def get_current_day():
    """
    获取当天时间的字符串时间
    :return:
    """
    var = time.strftime('%Y-%m-%d')
    return var


def get_pre_5days():
    list=[time.strftime("%Y-%m-%d")]


    for i in range(5):
        today = datetime.datetime.now()
        offset = datetime.timedelta(days=-i)
        re_date = (today + offset).strftime('%Y-%m-%d')
        print(re_date)



print(get_current_day)
print ("今日的日期：" + time.strftime("%Y-%m-%d"))
get_pre_5days()
print()
