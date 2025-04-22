# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import logging
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))), '.env'))

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 数据库连接信息
mysql_user = os.getenv('DB_USER')
mysql_password = os.getenv('DB_PASSWORD')
mysql_host = os.getenv('DB_HOST')
mysql_port = os.getenv('DB_PORT')
mysql_db = os.getenv('DB_NAME')
table_name = 't_stock'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

# 数据库表字段和 DataFrame 列名的映射
column_mapping = {
    '代码': 'stock_code',
    '名称': 'stock_name',
    '日期': 'trade_date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'amount',
    '涨跌幅': 'pct_chg',
    '涨跌额': 'chg'
}

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 添加线程锁，用于同步日志输出
print_lock = threading.Lock()

def df_to_mysql(df, table_name, column_mapping):
    """将DataFrame保存到MySQL"""
    try:
        # 根据映射关系重命名列
        df = df.rename(columns=column_mapping)

        # 确保日期列格式正确
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])

        # 使用事务保存数据
        with engine.begin() as connection:
            df.to_sql(
                table_name, 
                con=connection, 
                if_exists='append', 
                index=False, 
                chunksize=500
            )
            
    except Exception as e:
        logger.error(f"Error saving to MySQL: {e}")
        raise

def get_stock_price(args):
    """获取单个股票的历史数据"""
    symbol, name, date = args
    try:
        with print_lock:
            logger.debug(f"Fetching data for {symbol} - {name}")
        
        # 使用 AkShare 的 stock_zh_a_hist 接口获取股票历史数据
        df = ak.stock_zh_a_hist(
            symbol=symbol, 
            period="daily",  
            start_date=date.replace('-', ''),
            end_date=date.replace('-', ''), 
            adjust=""
        )

        # 添加股票代码和名称列
        df['代码'] = symbol
        df['名称'] = name
        
        # 删除不需要的列
        df = df.drop(['振幅', '换手率', '股票代码'], axis=1, errors='ignore')
        df = df.reset_index(drop=True)

        
        # 判断为空说明是脏数据
        if df.empty:
            with print_lock:
                logger.warning(f"No data found for {symbol} - {name}")
            return None
        
        df_to_mysql(df, table_name, column_mapping)
        
        with print_lock:
            logger.info(f"Successfully processed {symbol} - {name}")
        return symbol
        
    except Exception as e:
        with print_lock:
            logger.error(f"Error processing stock {symbol}: {e}")
        return None

def process_stock_data(date):
    """使用线程池处理指定日期的所有股票数据"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        
        logger.info(f"Starting to process stock data for date: {date}")
        start_time = time.time()
        
        # 获取所有A股股票列表
        stock_info = ak.stock_zh_a_spot_em()[['代码', '名称']]
        total_stocks = len(stock_info)
        print(f"Total stocks: {total_stocks}")
        
        # 准备任务参数
        tasks = [(row['代码'], row['名称'], date) for _, row in stock_info.iterrows()]
        
        # 创建计数器
        processed_count = 0
        success_count = 0
        
        # 使用线程池处理数据
        with ThreadPoolExecutor(max_workers=10) as executor:
            # 提交所有任务
            future_to_stock = {executor.submit(get_stock_price, task): task[0] for task in tasks}
            
            # 处理完成的任务
            for future in as_completed(future_to_stock):
                processed_count += 1
                stock_code = future_to_stock[future]
                
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    
                    # 每处理100只股票打印一次进度
                    if processed_count % 100 == 0:
                        elapsed_time = time.time() - start_time
                        progress = (processed_count / total_stocks) * 100
                        logger.info(f"Progress: {progress:.2f}% ({processed_count}/{total_stocks}), "
                                  f"Elapsed time: {elapsed_time:.2f}s, "
                                  f"Success rate: {(success_count/processed_count)*100:.2f}%")
                        
                except Exception as e:
                    logger.error(f"Error processing future for stock {stock_code}: {e}")
        
        # 打印最终统计信息
        elapsed_time = time.time() - start_time
        logger.info(f"\nProcessing completed for date: {date}")
        logger.info(f"Total stocks processed: {total_stocks}")
        logger.info(f"Successfully processed: {success_count}")
        logger.info(f"Failed: {total_stocks - success_count}")
        logger.info(f"Total time elapsed: {elapsed_time:.2f} seconds")
        logger.info(f"Average time per stock: {elapsed_time/total_stocks:.2f} seconds")
        
    except ValueError:
        logger.error(f"Invalid date format: {date}. Please use YYYY-MM-DD format.")
    except Exception as e:
        logger.error(f"An error occurred while processing data: {e}")

if __name__ == "__main__":
    # 示例：处理指定日期的数据
    date_to_process = '2025-02-11'
    process_stock_data(date_to_process)
