# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
把AkShare的ETF数据导入到本地数据库
"""

from sqlalchemy import create_engine, text
import akshare as ak
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import logging
import sys

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

        # 计算涨跌幅
        for index, row in df.iterrows():
            # 获取前一天收盘价
            prev_close_query = text("""
                SELECT close 
                FROM t_etf 
                WHERE stock_code = :code 
                AND trade_date < :date 
                ORDER BY trade_date DESC 
                LIMIT 1
            """)
            
            with engine.connect() as conn:
                prev_close = conn.execute(
                    prev_close_query, 
                    {'code': row['stock_code'], 'date': row['trade_date']}
                ).scalar()
            
            # 计算涨跌幅
            if prev_close:
                df.at[index, 'pct_chg'] = round((row['close'] - prev_close) / prev_close * 100, 2)
            else:
                df.at[index, 'pct_chg'] = 0.0

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
        print(f"Total etfs: {total_etfs}")
        
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

def process_multiple_days(date_list):
    """
    处理指定日期列表中的ETF数据
    
    参数:
        date_list (list): 日期列表，格式：['YYYY-MM-DD', 'YYYY-MM-DD', ...]
    """
    try:
        # 处理每一天的数据
        total_days = len(date_list)
        for idx, date in enumerate(date_list, 1):
            logger.info(f"Processing day {idx}/{total_days}: {date}")
            try:
                process_etf_data(date)
                logger.info(f"Successfully processed data for {date}")
            except Exception as e:
                logger.error(f"Error processing data for {date}: {e}")
            
            # 添加短暂延迟，避免请求过于频繁
            time.sleep(1)
        
        logger.info("Completed processing all dates")
        
    except Exception as e:
        logger.error(f"Error in process_multiple_days: {e}")
        raise

if __name__ == "__main__":
    # 示例：处理指定日期列表的数据
    try:
        # 设置要处理的日期列表
        dates_to_process = [
            '2025-02-17'
        ]
        
        logger.info(f"Starting batch processing for {len(dates_to_process)} days")
        process_multiple_days(dates_to_process)
        
    except Exception as e:
        logger.error(f"Main process error: {e}")
        sys.exit(1)

    # 如果只想处理单天数据，可以使用：
    # date_to_process = '2024-02-10'
    # process_etf_data(date_to_process)
