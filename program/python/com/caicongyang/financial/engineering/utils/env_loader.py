#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
通用环境变量加载模块
提供优雅的方式加载.env文件
"""

import os
from dotenv import load_dotenv
import pathlib

def get_project_root():
    """
    获取项目根目录的路径
    """
    current_file = pathlib.Path(__file__).resolve()
    # 从当前文件位置向上遍历，直到找到项目根目录
    # 当前文件路径结构: .../program/python/com/caicongyang/financial/engineering/utils/env_loader.py
    # 需要向上7层目录才能到达项目根目录
    return current_file.parents[6]

def load_env():
    """
    加载环境变量的通用函数
    """
    project_root = get_project_root()
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path)
    return dotenv_path 