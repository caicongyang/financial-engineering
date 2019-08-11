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


def get_pre_5days(num):
    """
    获取当前时间的前几天天
    :return:
    """
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-num)
    return (today + offset).strftime('%Y-%m-%d')


def get_pre_tran_day():
    week = datetime.datetime.strptime('2019-08-11', '%Y-%m-%d').strftime("%w")
    # 周天
    if int(week) == 0:
        return get_pre_5days(6)
    # 周六
    elif int(week) == 6:
        return get_pre_5days(5)
    # 周一
    else:
        return get_pre_5days(6)


def get_pre_5day():
    """
    获取当前时间的前五天
    :return:
    """
    list = []
    for i in range(5):
        today = datetime.datetime.now()
        offset = datetime.timedelta(days=-i)
        re_date = (today + offset).strftime('%Y-%m-%d')
        list.append(re_date)
    return list


# print(get_current_day)
# print("今日的日期：" + time.strftime("%Y-%m-%d"))
# print(get_pre_5days())

print(get_pre_tran_day())
