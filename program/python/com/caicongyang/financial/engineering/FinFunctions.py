# coding=utf-8
# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
汇总一些股票市场通用的方法
@author: caicongyang
"""

import pandas as pd
from program.python.com.caicongyang.financial.engineering import config


# 导入函数
def import_stock_data(stock_code):
    """

    :param stock_code:
    :return:
    """
    df = pd.read_csv(config.input_data_path + '/stock_data/' + stock_code + '_utf-8.csv', encoding='utf-8')
    # df.columns = [i.encode('utf8') for i in df.columns]
    print(df.columns)
    df = df[['交易日期', '股票代码', '开盘价', '最高价', '最低价', '收盘价', '涨跌幅']]
    df.sort_values(by=['交易日期'], inplace=True)
    df['交易日期'] = pd.to_datetime(df['交易日期'])
    df.reset_index(inplace=True, drop=True)

    return df


# 计算复权价

def cal_answer_authority(input_stock_data, answer_authority_type='后复权'):
    """
    计算复权收盘价
    :param input_stock_data:
    :param answer_authority_type:  forward 前复权 backward 后复权
    :return:
    """

    df = pd.DataFrame()
    num = {'前复权': 0, '后复权': -1}
    price1 = input_stock_data['收盘价'].iloc[num[answer_authority_type]]  # 定位第一个收盘价；后复权 取的是第一个收盘价 0， 前复权去的是最后一个收盘价 -1
    df['复权因子'] = (1.0 + input_stock_data['涨跌幅']).cumprod()
    price2 = df['复权因子'].iloc[num[answer_authority_type]]  # 定位第一个复权因子，后复权收盘价，等于用等于该股票上市价格的钱，买入该股票后的资金曲线。
    df['收盘价_' + answer_authority_type] = df['复权因子'] * (price1 / price2)  # 收盘价

    # 计算复权的开盘价、最高价、最低价
    df['开盘价_' + answer_authority_type] = input_stock_data['开盘价'] / input_stock_data['收盘价'] * df[
        '收盘价_' + answer_authority_type]
    df['最高价_' + answer_authority_type] = input_stock_data['最高价'] / input_stock_data['收盘价'] * df[
        '收盘价_' + answer_authority_type]
    df['最低价_' + answer_authority_type] = input_stock_data['最低价'] / input_stock_data['收盘价'] * df[
        '收盘价_' + answer_authority_type]

    return df[[i + '_' + answer_authority_type for i in ('开盘价', '最高价', '最低价', '收盘价')]]
