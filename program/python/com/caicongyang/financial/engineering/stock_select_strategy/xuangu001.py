# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
选股策略：
试用期：牛市追涨策略
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

current_date = '2019-07-01'
start_date = '2019-06-24'
end_date = '2019-07-02'

panel = get_price(stocks_list, start_date=start_date, end_date=end_date, frequency='daily', fields=None,
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
df = df[~df['2019-07-01'].isin([0.0])]

print('--------------------------------------next  mean')

# 成交量是过去5个交易日平均值的3倍
df['mean'] = df['2019-07-01'] / (
        (df['2019-06-27'] + df['2019-06-26'] + df['2019-06-25'] + df['2019-06-24'] + df['2019-06-28']) / 5)
df2 = df[df['mean'] > 2]

print(df2)

print('-------------------***')

df5 = df2
df5.columns = df5.columns.map(str)

# 成交量
df5['lastcompare'] = df5['2019-07-01 00:00:00'] / df5['2019-06-28 00:00:00']
df6 = df5[df5['lastcompare'] > 2]

df7 = df6.sort_values(by="mean", ascending=False)
print(df7)

print('--------------')


print(df7.index.tolist())

"""
选取主力净占比(%) 大于1的股票
"""

flow = get_money_flow(df7.index.tolist(), start_date=current_date, end_date=current_date,
                      fields=["date", "sec_code", "net_pct_main"], count=None)
flow = flow[flow['net_pct_main'] > 20]
flow = flow[flow['net_pct_main'] < 60]

flow = flow.sort_values(by="net_pct_main", ascending=False)

print(flow)

# 所选出股票的行业数据
stock_code_list = flow['sec_code'].tolist()
for stock_code in stock_code_list:
    d = get_industry(stock_code, date=current_date)
    try:
        print(stock_code + ':' + d[stock_code]['sw_l2']['industry_name'])
    except:
        print(stock_code + ':' + '异常')

#
# tolist = flow['sec_code'].tolist()
#
# industry = get_industry(tolist, date="2018-06-01")
#
# print(industry)


# d = get_industry("600519.XSHG", date="2018-06-01")
# print(d['industry_name'])

# 新增成交量大于五日均线
# df['mean'] = df.apply(
#     lambda x: x['2019-06-28'] / (
#             (x['2019-06-21'] + x['2019-06-24'] + x['2019-06-24'] + x['2019-06-26'] + x['2019-06-27']) / 4), axis=1)


# 队列进行操作
# flow['行业'] = flow['sec_code'].map(lambda x: _get_stock_industry(x))
