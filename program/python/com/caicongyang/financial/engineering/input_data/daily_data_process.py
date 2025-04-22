#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys

# 获取项目根目录
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../../../"))
# 添加 program/python 到 Python 路径
python_root = os.path.join(project_root, "program", "python")
sys.path.append(python_root)

"""
日常数据处理的入口文件，按顺序执行以下操作：
1. 导入股票和ETF历史数据
2. 计算股票和ETF的10日均线
3. 检查股票和ETF的成交量增加情况
每天下午6点自动执行
"""

import time
import schedule
from datetime import datetime
import check_volume_increase as stock_volume
import check_etf_volume_increase as etf_volume
import check_stock_limit as stock_limit
from com.caicongyang.financial.engineering.input_data import (
    inputStockFundFlowFromAkShare as fund_flow,
    inputStockFundFlowRankFromAkShare as fund_flow_rank,
    inputHistoryEtfDataFromAkShare as etf_import,
    inputHistoryStockDataFromAkShare as stock_import,
    calculate_stock_10day_average as stock_avg,
    calculate_etf_10day_average as etf_avg,
    inputStockConceptFromAkShare as concept_import,
    inputStockMinTradeFromAkShare as min_trade_import
)
from com.caicongyang.financial.engineering.stock_select_strategy import (
    analyze_concept_volume,
    analyze_limit_up_concept
)


def get_today_date():
    """获取当前日期，格式为YYYY-MM-DD"""
    return datetime.now().strftime('%Y-%m-%d')

def process_daily_data(date):
    """按顺序处理每日数据"""
    print(f"\n=== Starting daily data processing for {date} ===\n")

    # 1. 导入股票历史数据
    print("\n--- Importing stock historical data ---")
    try:
        stock_import.process_stock_data(date)
        print("Stock historical data import completed successfully")
    except Exception as e:
        print(f"Error importing stock historical data: {e}")
    
    # 2. 导入ETF历史数据
    print("\n--- Importing ETF historical data ---")
    try:
        etf_import.process_etf_data(date)
        print("ETF historical data import completed successfully")
    except Exception as e:
        print(f"Error importing ETF historical data: {e}")

    # # 3. 计算股票10日均线
    # print("\n--- Calculating stock 10-day average ---")
    # try:
    #     stock_avg.batch_calculate_10day_average([date])
    #     print("Stock 10-day average calculation completed successfully")
    # except Exception as e:
    #     print(f"Error calculating stock 10-day average: {e}")
    #
    # # 4. 计算ETF10日均线
    # print("\n--- Calculating ETF 10-day average ---")
    # try:
    #     etf_avg.batch_calculate_10day_average([date])
    #     print("ETF 10-day average calculation completed successfully")
    # except Exception as e:
    #     print(f"Error calculating ETF 10-day average: {e}")

    # 5. 检查股票成交量
    print("\n--- Checking stock volume increase ---")
    try:
        stock_volume.batch_check_volume_increase([date])
        print("Stock volume increase check completed successfully")
    except Exception as e:
        print(f"Error checking stock volume increase: {e}")

    # 6. 检查ETF成交量
    print("\n--- Checking ETF volume increase ---")
    try:
        etf_volume.batch_check_volume_increase([date])
        print("ETF volume increase check completed successfully")
    except Exception as e:
        print(f"Error checking ETF volume increase: {e}")

    # 7. 检查股票涨停数据
    print("\n--- Checking stock limit  ---")
    try:
        stock_limit.batch_check_limit_stocks([date])
        print("Stock limit check completed successfully")
    except Exception as e:
        print(f"Error checking stock limit: {e}")

    # # 8. 检查股票资金流排名
    # print("\n--- Processing stock fund flow rank data ---")
    # try:
    #     fund_flow_rank.process_fund_flow_rank_data(date)
    #     print("Stock fund flow rank data processing completed successfully")
    # except Exception as e:
    #     print(f"Error processing stock fund flow rank data: {e}")

    # # 9. 检查股票资金流
    # print("\n--- Processing stock fund flow data ---")
    # try:
    #     fund_flow.process_fund_flow_data(date)
    #     print("Stock fund flow data processing completed successfully")
    # except Exception as e:
    #     print(f"Error processing stock fund flow data: {e}")

    # 10. 检查股票概念
    print("\n--- Processing stock concept data ---")
    try:
        concept_import.process_daily_concept(date)
        print("Stock concept data processing completed successfully")
    except Exception as e:
        print(f"Error processing stock concept data: {e}")

    # 11. 分析成交量概念
    print("\n--- Analyzing volume concepts ---")
    try:
        analyze_concept_volume.process_concept_volume(date)
        print("Volume concepts analysis completed successfully")
    except Exception as e:
        print(f"Error analyzing volume concepts: {e}")
    
    # 12. 分析涨停概念
    print("\n--- Analyzing limit up concepts ---")
    try:
        analyze_limit_up_concept.process_limit_up_concept(date)
        print("Limit up concepts analysis completed successfully")
    except Exception as e:
        print(f"Error analyzing limit up concepts: {e}")

    # # 13. 处理5分钟交易数据
    # print("\n--- Processing stock 5-min trade data ---")
    # try:
    #     min_trade_import.process_min_trade_data(date)
    #     print("Stock 5-min trade data processing completed successfully")
    # except Exception as e:
    #     print(f"Error processing stock 5-min trade data: {e}")

    print(f"\n=== Daily data processing completed for {date} ===\n")

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
    schedule.every().day.at("18:30").do(run_daily_job)
    
    # 运行定时任务
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    # 如果想立即执行一次，可以取消下面的注释
    # today = get_today_date()
    today = "2025-04-22"
    if is_trading_day(today):
        process_daily_data(today)
    
    # # 启动定时任务
    main()