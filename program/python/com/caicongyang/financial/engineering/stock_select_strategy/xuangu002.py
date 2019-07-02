# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
主力净占比(%)
"""

import pandas as pd
from jqdatasdk import *

auth('13774598865', '24777365ccy')

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 获取所有的股票
stocks_list = list(get_all_securities(['stock']).index)

print('--------------')

# flow = get_money_flow(stocks_list, start_date='2019-06-28', end_date='2019-06-29',
#                       fields=["date", "sec_code", "net_pct_main", "net_pct_xl", "net_pct_l"], count=None)
#
# flow = flow[flow['net_pct_main'] > 10]
#
# flow = flow[flow['net_pct_main'] < 50]
#
# df6 = flow.sort_values(by="net_pct_main", ascending=False)
#
# print(df6)

d = get_industry("000877.XSHE", date="2018-06-01")
print(d)
print(d['000877.XSHE']['sw_l1']['industry_name'])



