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

def analyze_continuous_volume_trend(start_date, end_date):
    """
    分析指定日期范围内的成交量趋势
    条件：
    1. 获取每天top3的成交量记录
    2. 统计两天内top3记录（共6条）中上涨的比例
    3. 成交量大于3万
    4. 上涨比例大于80%
    """
    try:
        print(f"Analyzing volume trend from {start_date} to {end_date}")
        
        # 获取分钟级数据
        query = text("""
        SELECT t1.*, t2.stock_name, t2.pct_chg as daily_pct_chg
        FROM t_stock_min_trade t1
        JOIN t_stock t2 ON t1.stock_code = t2.stock_code AND t1.trade_date = t2.trade_date
        WHERE t1.trade_date BETWEEN :start_date AND :end_date
        AND t1.volume > 30000  -- 只选择成交量大于3万的记录
        ORDER BY t1.trade_date, t1.stock_code, t1.volume DESC
        """)
        
        # 读取数据
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                'start_date': start_date,
                'end_date': end_date
            })
        
        if df.empty:
            print(f"No records found between {start_date} and {end_date}")
            return
        
        # 按股票和日期分组，获取每天的top4记录
        result_stocks = []
        for stock_code, stock_group in df.groupby('stock_code'):
            dates = sorted(stock_group['trade_date'].unique())
            if len(dates) < 2:  # 需要两天的数据
                continue
                
            # 获取每天的top4记录
            daily_tops = []
            for date in dates:
                day_data = stock_group[stock_group['trade_date'] == date]
                top_3 = day_data.nlargest(3, 'volume')
                if len(top_3) == 4:  # 确保有4条记录
                    daily_tops.append(top_3)
            
            if len(daily_tops) < 2:  # 确保有两天的数据
                continue
            
            # 合并两天的数据
            combined_data = pd.concat(daily_tops)
            
            # 获取两天内的top3记录（共6条）
            top_6 = combined_data.nlargest(6, 'volume')
            if len(top_6) < 6:
                continue
            
            # 计算上涨比例
            up_count = sum(top_6['change_rate'] > 0)
            up_ratio = up_count / 6
            
            # 如果上涨比例大于80%，添加到结果中
            if up_ratio >= 0.8:
                result_stocks.append({
                    'stock_code': stock_code,
                    'stock_name': stock_group['stock_name'].iloc[0],
                    'up_ratio': up_ratio,
                    'data': top_6
                })
        
        # 打印结果
        if result_stocks:
            print(f"\n找到 {len(result_stocks)} 只满足条件的股票:")
            for stock in result_stocks:
                print("\n" + "="*60)
                print(f"股票: {stock['stock_code']} - {stock['stock_name']}")
                print(f"上涨比例: {stock['up_ratio']*100:.2f}%")
                print("\n大单记录:")
                display_df = stock['data'][['trade_date', 'trade_time', 'volume', 'change_rate']].copy()
                display_df['volume'] = display_df['volume'].apply(lambda x: f"{x/10000:.2f}万")
                display_df['change_rate'] = display_df['change_rate'].apply(lambda x: f"{x:.2f}%")
                print(display_df.to_string(index=False))
        else:
            print("没有找到满足条件的股票")
            
    except ValueError as ve:
        print(f"日期格式错误: {ve}")
    except Exception as e:
        print(f"分析数据时发生错误: {e}")

if __name__ == "__main__":
    # 示例：分析指定日期的数据
    date_to_analyze = '2024-12-16'
    analyze_high_volume_stocks(date_to_analyze) 
    
    # 新增连续两天的分析
    print("\n" + "="*80)
    print("分析连续两天的成交量趋势")
    print("="*80)
    
    # 分析最近两天的数据
    end_date = '2024-12-16'
    start_date = '2024-12-15'
    analyze_continuous_volume_trend(start_date, end_date) 