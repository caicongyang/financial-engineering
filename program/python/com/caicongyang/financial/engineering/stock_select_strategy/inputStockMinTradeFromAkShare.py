# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的股票5分钟交易数据导入到本地数据库
"""

from sqlalchemy import create_engine, text
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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
table_name = 't_stock_min_trade'

# 创建数据库连接池
engine = create_engine(
    f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}',
    pool_size=10,
    max_overflow=20
)

def get_latest_stock_codes():
    """获取最新交易日的所有股票代码"""
    query = text("""
    SELECT DISTINCT stock_code 
    FROM t_stock 
    WHERE trade_date = (
        SELECT MAX(trade_date) 
        FROM t_stock
    )
    """)
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            return df['stock_code'].tolist()
    except Exception as e:
        print(f"Error getting stock codes: {e}")
        return []

def pad_stock_code(code):
    """将股票代码补足6位"""
    return str(code).zfill(6)

def check_data_exists(stock_code, date):
    """检查指定日期的数据是否已存在"""
    query = f"""
    SELECT COUNT(*) 
    FROM {table_name} 
    WHERE stock_code = '{stock_code}' 
    AND DATE(trade_time) = '{date}'
    """
    result = pd.read_sql(query, engine)
    return result.iloc[0, 0] > 0

def process_stock_min_data(stock_code, date):
    """处理指定股票指定日期的5分钟数据"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        
        # 补足股票代码为6位
        stock_code = pad_stock_code(stock_code)
        
        # 检查数据是否已存在
        if check_data_exists(stock_code, date):
            print(f"Data for stock {stock_code} on {date} already exists in the database. Skipping...")
            return
        
        print(f"Starting to process 5-min data for stock {stock_code} on date: {date}")
        
        # 设置时间范围
        start_time = f"{date} 09:30:00"
        end_time = f"{date} 15:00:00"
        
        # 获取5分钟数据
        df = ak.stock_zh_a_hist_min_em(
            symbol=stock_code,
            start_date=start_time,
            end_date=end_time,
            period="5",
            adjust=""
        )
        
        if df.empty:
            print(f"No data found for stock {stock_code} on {date}")
            return
        
        # 重命名列
        df = df.rename(columns={
            '时间': 'trade_time',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'change_rate',
            '涨跌额': 'change_amount',
            '振幅': 'amplitude',
            '换手率': 'turnover_rate'
        })
        
        # 添加股票代码列和交易日期列
        df['stock_code'] = stock_code
        df['trade_date'] = pd.to_datetime(date).date()
        
        # 转换时间列为datetime类型
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        
        # 保存到数据库
        try:
            df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=500)
            print(f"5-min data has been successfully inserted into the {table_name} table.")
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")
            
        print(f"Completed processing 5-min data for stock {stock_code} on date: {date}")
        return f"Successfully processed {stock_code}"
        
    except ValueError:
        print(f"Invalid date format: {date}. Please use YYYY-MM-DD format.")
        return f"Failed to process {stock_code}: Invalid date format"
    except Exception as e:
        print(f"An error occurred while processing data for {stock_code}: {e}")
        return f"Failed to process {stock_code}: {str(e)}"

def process_all_stocks_min_data(date):
    """使用多线程处理所有股票的5分钟数据"""
    # 获取最新的股票代码列表
    stock_codes = get_latest_stock_codes()
    
    if not stock_codes:
        print("No stock codes found.")
        return
    
    print(f"Found {len(stock_codes)} stocks to process.")
    
    # 使用线程池处理数据
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 创建所有任务
        future_to_stock = {
            executor.submit(process_stock_min_data, stock_code, date): stock_code 
            for stock_code in stock_codes
        }
        
        # 处理完成的任务
        completed = 0
        for future in as_completed(future_to_stock):
            stock_code = future_to_stock[future]
            try:
                result = future.result()
                completed += 1
                if completed % 10 == 0:  # 每处理10只股票打印一次进度
                    print(f"Progress: {completed}/{len(stock_codes)} stocks processed")
            except Exception as e:
                print(f"Stock {stock_code} generated an exception: {e}")
    
    end_time = time.time()
    print(f"\nAll stocks processed. Total time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    # 示例：处理指定日期所有股票的数据
    date_to_process = '2024-12-12'
    process_all_stocks_min_data(date_to_process) 