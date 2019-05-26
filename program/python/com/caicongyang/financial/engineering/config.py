#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
配置中心
"""

import os

current_file = __file__

# 程序根路径地址,此除需要改进
root_path = os.path.abspath(
    os.path.join(current_file, os.pardir, os.pardir, os.pardir, os.pardir, os.pardir, os.pardir,os.pardir))

print(root_path)

# 输入数据根路径地址
input_data_path =os.path.abspath(os.path.join(root_path,'data','input_data'))
