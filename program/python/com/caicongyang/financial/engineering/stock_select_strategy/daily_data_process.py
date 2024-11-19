# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
日常数据处理的入口文件，按顺序执行以下操作：
1. 导入股票和ETF历史数据
2. 计算股票和ETF的10日均线
3. 检查股票和ETF的成交量增加情况
每天下午6点自动执行
"""

import sys
import time
import schedule
from datetime import datetime, timedelta
import inputHistoryStockDataFromAkShare as stock_import  # 改用Tushare接口
import inputHistoryEtfDataFromAkShare as etf_import
import calculate_stock_10day_average as stock_avg
import calculate_etf_10day_average as etf_avg
import check_volume_increase as stock_volume
import check_etf_volume_increase as etf_volume

def get_today_date():
    """获取当前日期，格式为YYYY-MM-DD"""
    return datetime.now().strftime('%Y-%m-%d')

def process_daily_data(date):
    """按顺序处理每日数据"""
    print(f"\n=== Starting daily data processing for {date} ===\n")

    try:
        # 1. 导入股票历史数据
        print("\n--- Importing stock historical data ---")
        stock_import.process_stock_data(date)

        # 2. 导入ETF历史数据
        print("\n--- Importing ETF historical data ---")
        etf_import.process_etf_data(date)

        # 3. 计算股票10日均线
        print("\n--- Calculating stock 10-day average ---")
        stock_avg.batch_calculate_10day_average([date])

        # 4. 计算ETF10日均线
        print("\n--- Calculating ETF 10-day average ---")
        etf_avg.batch_calculate_10day_average([date])

        # 5. 检查股票成交量
        print("\n--- Checking stock volume increase ---")
        stock_volume.batch_check_volume_increase([date])

        # 6. 检查ETF成交量
        print("\n--- Checking ETF volume increase ---")
        etf_volume.batch_check_volume_increase([date])

        print(f"\n=== Daily data processing completed for {date} ===\n")

    except Exception as e:
        print(f"An error occurred during daily processing: {e}")
        sys.exit(1)

def run_daily_job():
    """执行每日任务"""
    print(f"Starting scheduled job at {datetime.now()}")
    today = get_today_date()
    if is_trading_day(today):
        process_daily_data(today)

def is_trading_day(date):
    """判断是否为交易日（简单实现，仅判断是否为周末）"""
    day_of_week = datetime.strptime(date, '%Y-%m-%d').weekday()
    return day_of_week < 5  # 0-4 表示周一至周五

def main():
    """主函数，设置定时任务"""
    print("Setting up scheduled job to run at 18:00 every day...")
    
    # 设置每天下午6点执行任务
    schedule.every().day.at("18:00").do(run_daily_job)
    
    # 运行定时任务
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    # 如果想立即执行一次，可以取消下面的注释
    # today = get_today_date()
    if is_trading_day("2024-11-18"):
        process_daily_data("2024-11-18")
    
    # 启动定时任务
    main()