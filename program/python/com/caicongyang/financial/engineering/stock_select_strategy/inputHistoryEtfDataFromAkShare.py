# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的ETF数据导入到本地数据库
"""

from sqlalchemy import create_engine
import akshare as ak
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import logging

# 列多的时候，不隐藏
pd.set_option('expand_frame_repr', False)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 添加线程锁，用于同步日志输出
print_lock = threading.Lock()

# 数据库连接信息
mysql_user = 'root'
mysql_password = 'root'
mysql_host = '101.43.6.49'
mysql_port = '3333'
mysql_db = 'stock'
table_name = 't_etf'

engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

# 数据库表字段和 DataFrame 列名的映射
column_mapping = {
    'code': 'stock_code',
    'name': 'stock_name',
    '日期': 'trade_date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'amount'
}

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

def get_etf_price(args):
    """获取单个ETF的历史数据"""
    code, name, date = args
    try:
        with print_lock:
            logger.debug(f"Fetching data for ETF {code} - {name}")
        
        # 使用 AkShare 的 fund_etf_hist_em 接口获取ETF历史数据
        df = ak.fund_etf_hist_em(
            symbol=code, 
            period="daily", 
            start_date=date.replace('-', ''), 
            end_date=date.replace('-', ''), 
            adjust=""
        )
        
        # 添加ETF代码和名称
        df['code'] = code
        df['name'] = name
        
        # 删除不需要的列
        columns_to_drop = ['振幅', '涨跌幅', '涨跌额', '换手率']
        df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
        df = df.reset_index(drop=True)
        
        # 判断为空说明是脏数据
        if df.empty:
            with print_lock:
                logger.warning(f"No data found for ETF {code} - {name}")
            return None
        
        df_to_mysql(df, table_name, column_mapping)
        
        with print_lock:
            logger.info(f"Successfully processed ETF {code} - {name}")
        return code
        
    except Exception as e:
        with print_lock:
            logger.error(f"Error processing ETF {code}: {e}")
        return None

def process_etf_data(date):
    """使用线程池处理指定日期的所有ETF数据"""
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
        
        logger.info(f"Starting to process ETF data for date: {date}")
        start_time = time.time()
        
        # 获取ETF列表
        fund_etf_spot_em_df = ak.fund_etf_spot_em()
        total_etfs = len(fund_etf_spot_em_df)
        
        # 准备任务参数
        tasks = [(row['代码'], row['名称'], date) 
                for _, row in fund_etf_spot_em_df[['代码', '名称']].iterrows()]
        
        # 创建计数器
        processed_count = 0
        success_count = 0
        
        # 使用线程池处理数据
        with ThreadPoolExecutor(max_workers=20) as executor:
            # 提交所有任务
            future_to_etf = {executor.submit(get_etf_price, task): task[0] for task in tasks}
            
            # 处理完成的任务
            for future in as_completed(future_to_etf):
                processed_count += 1
                etf_code = future_to_etf[future]
                
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    
                    # 每处理10个ETF打印一次进度
                    if processed_count % 10 == 0:
                        elapsed_time = time.time() - start_time
                        progress = (processed_count / total_etfs) * 100
                        logger.info(f"Progress: {progress:.2f}% ({processed_count}/{total_etfs}), "
                                  f"Elapsed time: {elapsed_time:.2f}s, "
                                  f"Success rate: {(success_count/processed_count)*100:.2f}%")
                        
                except Exception as e:
                    logger.error(f"Error processing future for ETF {etf_code}: {e}")
        
        # 打印最终统计信息
        elapsed_time = time.time() - start_time
        logger.info(f"\nProcessing completed for date: {date}")
        logger.info(f"Total ETFs processed: {total_etfs}")
        logger.info(f"Successfully processed: {success_count}")
        logger.info(f"Failed: {total_etfs - success_count}")
        logger.info(f"Total time elapsed: {elapsed_time:.2f} seconds")
        logger.info(f"Average time per ETF: {elapsed_time/total_etfs:.2f} seconds")
        
    except ValueError:
        logger.error(f"Invalid date format: {date}. Please use YYYY-MM-DD format.")
    except Exception as e:
        logger.error(f"An error occurred while processing data: {e}")

if __name__ == "__main__":
    # 示例：处理指定日期的数据
    date_to_process = '2024-12-23'
    process_etf_data(date_to_process)
