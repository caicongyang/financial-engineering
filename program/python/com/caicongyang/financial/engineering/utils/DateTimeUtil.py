# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
时间工具类
"""
import datetime
import time
import arrow


def get_current_day():
    """
    获取当天时间的字符串时间
    :return:
    """
    var = time.strftime('%Y-%m-%d')
    return var


def get_current_half_day():
    a = datetime.datetime.now().strftime("%Y-%m-%d") + " 12:00:00"
    return a


def get_pre_5days(num):
    """
    获取当前时间的前几天天
    :return:
    """
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-num)
    return (today + offset).strftime('%Y-%m-%d')


def get_pre_tran_day():
    week = datetime.datetime.strptime(get_current_day(), '%Y-%m-%d').strftime("%w")
    # 周天
    if int(week) == 0:
        return get_pre_5days(6)
    # 周六
    elif int(week) == 6:
        return get_pre_5days(5)
    # 周一
    else:
        return get_pre_5days(6)


def get_last_tran_day():
    """
    获取最近的一个交易日
    :return:
    """
    week = datetime.datetime.strptime(get_current_day(), '%Y-%m-%d').strftime("%w")
    # 周天
    if int(week) == 0:
        return get_pre_5days(2)
    # 周六
    elif int(week) == 6:
        return get_pre_5days(1)
    else:
        return get_current_day()


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


def isLeapYear(years):
    '''
    通过判断闰年，获取年份years下一年的总天数
    :param years: 年份，int
    :return:days_sum，一年的总天数
    '''
    # 断言：年份不为整数时，抛出异常。
    assert isinstance(years, int), "请输入整数年，如 2018"

    if ((years % 4 == 0 and years % 100 != 0) or (years % 400 == 0)):  # 判断是否是闰年
        # print(years, "是闰年")
        days_sum = 366
        return days_sum
    else:
        # print(years, '不是闰年')
        days_sum = 365
        return days_sum


def getAllDayPerYear(years):
    '''
    获取一年的所有日期
    :param years:年份
    :return:全部日期列表
    '''
    start_date = '%s-1-1' % years
    a = 0
    all_date_list = []
    days_sum = isLeapYear(int(years))
    while a < days_sum:
        b = arrow.get(start_date).shift(days=a).format("YYYY-MM-DD")
        a += 1
        all_date_list.append(b)
    # print(all_date_list)
    return all_date_list

# print(get_current_day)
# print("今日的日期：" + time.strftime("%Y-%m-%d"))
# print(get_pre_5days())

# print(get_pre_tran_day())

# print(getAllDayPerYear(2020))
