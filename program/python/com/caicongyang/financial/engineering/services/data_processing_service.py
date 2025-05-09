#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理服务，包含所有数据处理相关的功能
"""

import os
import sys
from datetime import datetime

# 导入日常数据处理相关模块
from com.caicongyang.financial.engineering.input_data import (
    inputHistoryEtfDataFromAkShare as etf_import,
    inputHistoryStockDataFromAkShare as stock_import,
    calculate_stock_10day_average as stock_avg,
    calculate_etf_10day_average as etf_avg,
    inputStockConceptFromAkShare as concept_import,
    check_volume_increase as stock_volume,
    check_etf_volume_increase as etf_volume,
    check_stock_limit as stock_limit
)
from com.caicongyang.financial.engineering.stock_select_strategy import (
    analyze_concept_volume,
    analyze_limit_up_concept
)

class DataProcessingService:
    """
    数据处理服务类，提供所有数据处理相关的方法
    
    主要功能：
    1. 导入股票和ETF历史数据
    2. 检查股票和ETF的成交量增加情况
    3. 检查股票涨停数据
    4. 处理股票概念数据
    5. 分析成交量概念和涨停概念
    
    此服务用于替代原有的daily_data_process.py脚本，提供更好的模块化和代码组织
    """
    
    def __init__(self):
        """初始化数据处理服务"""
        pass
    
    def get_today_date(self):
        """获取当前日期，格式为YYYY-MM-DD"""
        return datetime.now().strftime('%Y-%m-%d')
    
    def is_trading_day(self, date):
        """判断是否为交易日（简单实现，仅判断是否为周末）"""
        day_of_week = datetime.strptime(date, '%Y-%m-%d').weekday()
        return day_of_week < 5  # 0-4 表示周一至周五
    
    def process_daily_data(self, date):
        """按顺序处理每日数据"""
        print(f"\n=== Starting daily data processing for {date} ===\n")

        # 按顺序执行数据处理任务
        self._import_stock_data(date)
        self._import_etf_data(date)
        self._check_stock_volume(date)
        self._check_etf_volume(date)
        self._check_stock_limit(date)
        self._process_stock_concept(date)
        self._analyze_volume_concepts(date)
        self._analyze_limit_up_concepts(date)

        print(f"\n=== Daily data processing completed for {date} ===\n")
    
    def _import_stock_data(self, date):
        """导入股票历史数据"""
        print("\n--- Importing stock historical data ---")
        try:
            stock_import.process_stock_data(date)
            print("Stock historical data import completed successfully")
        except Exception as e:
            print(f"Error importing stock historical data: {e}")
    
    def _import_etf_data(self, date):
        """导入ETF历史数据"""
        print("\n--- Importing ETF historical data ---")
        try:
            etf_import.process_etf_data(date)
            print("ETF historical data import completed successfully")
        except Exception as e:
            print(f"Error importing ETF historical data: {e}")
    
    def _check_stock_volume(self, date):
        """检查股票成交量"""
        print("\n--- Checking stock volume increase ---")
        try:
            stock_volume.batch_check_volume_increase([date])
            print("Stock volume increase check completed successfully")
        except Exception as e:
            print(f"Error checking stock volume increase: {e}")
    
    def _check_etf_volume(self, date):
        """检查ETF成交量"""
        print("\n--- Checking ETF volume increase ---")
        try:
            etf_volume.batch_check_volume_increase([date])
            print("ETF volume increase check completed successfully")
        except Exception as e:
            print(f"Error checking ETF volume increase: {e}")
    
    def _check_stock_limit(self, date):
        """检查股票涨停数据"""
        print("\n--- Checking stock limit  ---")
        try:
            stock_limit.batch_check_limit_stocks([date])
            print("Stock limit check completed successfully")
        except Exception as e:
            print(f"Error checking stock limit: {e}")
    
    def _process_stock_concept(self, date):
        """处理股票概念数据"""
        print("\n--- Processing stock concept data ---")
        try:
            concept_import.process_daily_concept(date)
            print("Stock concept data processing completed successfully")
        except Exception as e:
            print(f"Error processing stock concept data: {e}")
    
    def _analyze_volume_concepts(self, date):
        """分析成交量概念"""
        print("\n--- Analyzing volume concepts ---")
        try:
            analyze_concept_volume.process_concept_volume(date)
            print("Volume concepts analysis completed successfully")
        except Exception as e:
            print(f"Error analyzing volume concepts: {e}")
    
    def _analyze_limit_up_concepts(self, date):
        """分析涨停概念"""
        print("\n--- Analyzing limit up concepts ---")
        try:
            analyze_limit_up_concept.process_limit_up_concept(date)
            print("Limit up concepts analysis completed successfully")
        except Exception as e:
            print(f"Error analyzing limit up concepts: {e}")
    
    def run_daily_job(self):
        """执行每日任务"""
        print(f"Starting scheduled job at {datetime.now()}")
        today = self.get_today_date()
        if self.is_trading_day(today):
            self.process_daily_data(today) 