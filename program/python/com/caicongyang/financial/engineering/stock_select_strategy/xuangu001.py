# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
选股策略：
当日成交量 > 前5个交易日成交量均值
当前成交量 > 前一个交易日的成交量
"""

import pandas as pd
from jqdatasdk import *

auth('13774598865', '24777365ccy')

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 获取所有的股票
stocks_list = list(get_all_securities(['stock']).index)
# print(stocks_list)

# 获取某个指数下面的所有股票
# get_index_stocks('000903.XSHG')

panel = get_price(stocks_list, start_date='2019-06-21', end_date='2019-06-29', frequency='daily', fields=None,
                  skip_paused=False, fq=None)

df_volume = panel['volume']  # 获取开盘价的[pandas.DataFrame],  行索引是[datetime.datetime]对象, 列索引是股票代号
# print(df_volume)

df = df_volume.stack().unstack(0)  # 列转行
df.replace('NaN', 0.0, inplace=True)  # 替换空数据
df.fillna(value=0.0, inplace=True)  # 替换空数据
df = df[~df['2019-06-25'].isin([0.0])]
df = df[~df['2019-06-27'].isin([0.0])]
df = df[~df['2019-06-24'].isin([0.0])]
df = df[~df['2019-06-26'].isin([0.0])]
df = df[~df['2019-06-28'].isin([0.0])]
df = df[~df['2019-06-21'].isin([0.0])]

print('--------------------------------------')

# 成交量是过去5个交易日平均值的3倍
df['mean'] = df['2019-06-28'] / (
        (df['2019-06-27'] + df['2019-06-26'] + df['2019-06-25'] + df['2019-06-24'] + df['2019-06-24']) / 5)
df2 = df[df['mean'] > 3]

print('-------------------***')

df5 = df2
df5.columns = df5.columns.map(str)

# 成交量
df5['lastcompare'] = df5['2019-06-28 00:00:00'] / df5['2019-06-27 00:00:00']
df5 = df5[df5['lastcompare'] > 1]

df6 = df5.sort_values(by="mean", ascending=False)
print(df6)
