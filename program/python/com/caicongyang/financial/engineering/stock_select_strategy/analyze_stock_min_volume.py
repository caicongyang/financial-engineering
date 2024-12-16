# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
分析股票分钟级别成交量数据，找出指定日期成交量最大的记录
条件：
1. 日线涨幅为正
2. 成交量最大的前三条记录涨幅都为正
3. 前三条记录中至少有一条成交量超过30万
"""

from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)
# 设置未来行为，避免警告
pd.set_option('future.no_silent_downcasting', True)

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '101.43.6.49'
mysql_port = '3333'
mysql_db = 'stock'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

def analyze_high_volume_stocks(date):
    """分析指定日期成交量最大的记录"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        
        print(f"Analyzing high volume stocks for date: {date}")
        
        # 获取分钟级数据，同时检查日线涨幅
        query = text("""
        SELECT t1.*, t2.stock_name, t2.pct_chg as daily_pct_chg
        FROM t_stock_min_trade t1
        JOIN t_stock t2 ON t1.stock_code = t2.stock_code AND t1.trade_date = t2.trade_date
        WHERE t1.trade_date = :date
            AND t2.pct_chg > 0  -- 只选择当日涨幅为正的股票
        """)
        
        # 读取数据
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'date': date})
        
        if df.empty:
            print(f"No records found for date: {date}")
            return
        
        # 按股票分组处理数据
        result_dfs = []
        for stock_code, group in df.groupby('stock_code'):
            # 获取成交量最大的前三条记录
            top_3 = group.nlargest(3, 'volume')
            
            # 检查条件：
            # 1. 必须有3条记录
            # 2. 3条记录的涨幅都为正
            # 3. 至少有一条记录的成交量超过30万
            # 4. 成交量最大的那条记录必须满足收盘价大于等于最高价
            if (len(top_3) >= 3 and
                all(top_3['change_rate'] > 0) and 
                any(top_3['volume'] > 100000) and
                top_3.iloc[0]['close'] >= top_3.iloc[0]['high']):  # 只检查第一条记录
                result_dfs.append(top_3)
        
        if not result_dfs:
            print(f"No stocks found matching the criteria for date: {date}")
            return
        
        # 合并所有结果
        result_df = pd.concat(result_dfs)
        
        # 按股票代码分组显示结果
        for stock_code, group in result_df.groupby('stock_code'):
            print("\n" + "="*50)
            print(f"Stock: {stock_code} - {group['stock_name'].iloc[0]}")
            print(f"Daily Change Rate: {group['daily_pct_chg'].iloc[0]:.2f}%")
            print(f"Top 3 volume records:")
            display_df = group[['trade_time', 'volume', 'change_rate']].copy()
            display_df['volume'] = display_df['volume'].apply(lambda x: f"{x/10000:.2f}万")
            display_df['change_rate'] = display_df['change_rate'].apply(lambda x: f"{x:.2f}%")
            print(display_df.to_string(index=False))
        
    except ValueError:
        print(f"Invalid date format: {date}. Please use YYYY-MM-DD format.")
    except Exception as e:
        print(f"An error occurred while analyzing data: {e}")

if __name__ == "__main__":
    # 示例：分析指定日期的数据
    date_to_analyze = '2024-12-16'
    analyze_high_volume_stocks(date_to_analyze) 