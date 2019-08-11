# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
选股策略：(查看所有崛起的股票)
试用期：牛市追涨策略
当日成交量 > 前5个交易日成交量均值
当前成交量 > 前一个交易日的成交量
"""
import pandas as pd
from jqdatasdk import *
from program.python.com.caicongyang.financial.engineering.utils import DateTimeUtil
from program.python.com.caicongyang.financial.engineering.stock_select_strategy import JoinQuantUtil

auth('13774598865', '123456')

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 获取所有的股票
stocks_list = list(get_all_securities(['stock']).index)
# ['603797.XSHG','000002.XSHG']


# 当前的时间
current_date = DateTimeUtil.get_current_day()
# 开始时间
start_date = DateTimeUtil.get_pre_tran_day()
# 结束时间
end_date = current_date

panel = get_price(stocks_list, start_date=start_date, end_date=end_date, frequency='daily', fields=None,
                  skip_paused=False, fq=None)

# 获取所有股票的成交量
df_volume = panel['volume']

# 所有的交易时间段
date_list = df_volume.index.strftime('%Y-%m-%d').tolist()

df_volume = df_volume.stack().unstack(0)  # 列转行
df_volume.replace('NaN', 0.0, inplace=True)  # 替换空数据
df_volume.fillna(value=0.0, inplace=True)  # 替换空数据

for date in date_list:
    df_volume = df_volume[~df_volume[date].isin([0.0])]

# print(df_volume)

#  成交量是过去5个交易日平均值的2倍
df_volume['mean'] = df_volume[date_list[4]] / (
        (df_volume[date_list[3]] + df_volume[date_list[2]] + df_volume[date_list[1]] + df_volume[date_list[0]]) / 4)
df_mean = df_volume[df_volume['mean'] > 2]

columns_list = df_mean.columns.tolist()

# print('-------------------以上计算所有股票的最后一天成交量大于前面4天的2倍的股票-------------')


# 当前成交量成交量 与前一天的比较

df_mean['lastDayCompare'] = df_mean[columns_list[4]] / df_mean[columns_list[3]]

# df_mean["lastDayCompare"] = df_mean[[columns_list[4], columns_list[3]]].apply(
#     lambda x: x[columns_list[4]] + x[columns_list[3]], axis=1)

# print(df_mean)

df_mean = df_mean[df_mean['lastDayCompare'] < 70]
#
df_mean = df_mean[df_mean['lastDayCompare'] > 2]

#
df_mean = df_mean.sort_values(by="mean", ascending=False)

print(df_mean)

# 取出所有的不满足条件的股票
df_mean_index_list = list(df_mean.index)
set = []
for x in df_mean_index_list:
    if not JoinQuantUtil.verify_stock(x):
        set.append(x)

# 删除这些不满足条件的股票
df_mean_final = df_mean.drop(set, axis=0)  # 利用drop方法将含有特定数值的列删除

print("-------------final ------------")
print(df_mean_final)
print(df_mean_final.shape)

# df_mean['increase_ratio'] = df_mean.apply(lambda x: JoinQuantUtil.verify_stock(x) == True)

# # pandas显示所有行
# pd.set_option('display.max_rows', None)
# print(df_mean)

#
# """
# 选取主力净占比(%) 大于1的股票
# """
#
# flow = get_money_flow(df7.index.tolist(), start_date='2019-06-20', end_date='2019-06-20',
#                       fields=["date", "sec_code", "net_pct_main", 'net_pct_xl', 'net_pct_l'], count=None)
#
# print(flow)
# flow = flow[flow['net_pct_main'] > 20]
# flow = flow[flow['net_pct_main'] < 60]
#
# flow = flow.sort_values(by="net_pct_main", ascending=False)
#
# print(flow)
#
# print('-------------------flow-------------')
#
# # 所选出股票的行业数据
# stock_code_list = flow['sec_code'].tolist()
# for stock_code in stock_code_list:
#     d = get_industry(stock_code, date=current_date)
#     try:
#         print(stock_code + ':' + d[stock_code]['sw_l2']['industry_name'])

#     except:
#         print(stock_code + ':' + '异常')
