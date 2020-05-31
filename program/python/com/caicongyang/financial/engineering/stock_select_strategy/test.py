# !/usr/bin/python
# -*- coding: UTF-8 -*-


"""
获取股票的概念信息
"""
import pandas as pd
import json
from jqdatasdk import *


auth('13774598865', '123456')

count = get_query_count()
print(count)

dict = get_concept('000001.XSHE', date='2019-01-29')
print(dict)


