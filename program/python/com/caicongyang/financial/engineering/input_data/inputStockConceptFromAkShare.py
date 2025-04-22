"""
把AkShare的数据导入到本地数据库
"""

from sqlalchemy import create_engine, text
import akshare as ak
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))), '.env'))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 添加线程锁，用于同步日志输出和计数
print_lock = threading.Lock()
count_lock = threading.Lock()

class ConceptDataLoader:
    def __init__(self):
        # 数据库连接信息
        self.mysql_config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'db': os.getenv('DB_NAME')
        }
        
        # 表名
        self.concept_table = 't_concept'
        self.concept_stock_table = 't_concept_stock'
        
        # 创建数据库连接
        self.engine = create_engine(
            f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}@"
            f"{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['db']}"
        )
        
        # 数据库表字段映射
        self.concept_mapping = {
    '板块名称': 'concept_name',
    '板块代码': 'concept_code',
    'source': 'source'
}

        self.concept_stock_mapping = {
    '板块代码': 'concept_code',
    '板块名称': 'concept_name',
    '代码': 'stock_code',
    '名称': 'stock_name'
}

    def truncate_tables(self):
        """清空概念相关表"""
        try:
            with self.engine.connect() as conn:
                # 清空概念股票关联表
                conn.execute(text(f"TRUNCATE TABLE {self.concept_stock_table}"))
                logger.info(f"Successfully truncated {self.concept_stock_table}")
                
                # 清空概念表
                conn.execute(text(f"TRUNCATE TABLE {self.concept_table}"))
                logger.info(f"Successfully truncated {self.concept_table}")
                
        except Exception as e:
            logger.error(f"Error truncating tables: {e}")
            raise

    def load_concept_data(self):
        """加载概念数据"""
        try:
            # 获取东方财富概念板块数据
            df = ak.stock_board_concept_name_em()
            logger.info(f"Retrieved {len(df)} concepts from East Money")
            
            # 选择需要的列并添加来源
            selected_columns = df[['板块名称', '板块代码']]
            selected_columns['source'] = 'eastmoney'
            
            # 重命名列
            concept_df = selected_columns.rename(columns=self.concept_mapping)
            
            # 保存到数据库
            concept_df.to_sql(
                self.concept_table, 
                con=self.engine, 
                if_exists='append', 
                index=False, 
                chunksize=100
            )
            
            logger.info(f"Successfully saved {len(concept_df)} concepts to database")
            # 返回原始DataFrame，保留中文列名
            return selected_columns
            
        except Exception as e:
            logger.error(f"Error loading concept data: {e}")
            raise

    def process_concept(self, args):
        """处理单个概念的股票数据"""
        concept_name, concept_code, idx, total_concepts = args
        try:
            # 获取概念成分股
            stocks_df = ak.stock_board_concept_cons_em(symbol=concept_name)
            
            if not stocks_df.empty:
                # 选择并重命名列
                df = stocks_df[['代码', '名称']].copy()
                df['板块代码'] = concept_code
                df['板块名称'] = concept_name
                
                # 重命名列
                df = df.rename(columns=self.concept_stock_mapping)
                
                # 保存到数据库
                df.to_sql(
                    self.concept_stock_table, 
                    con=self.engine, 
                    if_exists='append', 
                    index=False, 
                    chunksize=100
                )
                
                with print_lock:
                    logger.info(f"Progress: {idx}/{total_concepts} - Loaded {len(df)} stocks for concept: {concept_name}")
                
                return len(df)
            
            return 0
            
        except Exception as e:
            with print_lock:
                logger.error(f"Error processing concept {concept_name}: {e}")
            return 0

    def load_concept_stocks(self, concept_df):
        """使用线程池加载概念股票数据"""
        try:
            total_stocks = 0
            concept_list = concept_df['板块名称'].tolist()
            concept_codes = concept_df['板块代码'].tolist()
            total_concepts = len(concept_list)
            
            logger.info(f"Starting to load stocks for {total_concepts} concepts using thread pool")
            
            # 准备任务参数
            tasks = [
                (name, code, idx+1, total_concepts) 
                for idx, (name, code) in enumerate(zip(concept_list, concept_codes))
            ]
            
            # 使用线程池处理数据
            with ThreadPoolExecutor(max_workers=10) as executor:
                # 提交所有任务
                future_to_concept = {
                    executor.submit(self.process_concept, task): task[0] 
                    for task in tasks
                }
                
                # 处理完成的任务
                for future in as_completed(future_to_concept):
                    concept_name = future_to_concept[future]
                    try:
                        stock_count = future.result()
                        with count_lock:
                            total_stocks += stock_count
                        
                    except Exception as e:
                        logger.error(f"Error processing future for concept {concept_name}: {e}")
                    
                    # 添加延时避免请求过快
                    time.sleep(0.2)
            
            logger.info(f"Successfully loaded total {total_stocks} stock-concept relationships")
            
        except Exception as e:
            logger.error(f"Error loading concept stocks: {e}")
            raise

    def process_daily_concept(self, date=None):
        """
        每日概念数据处理入口
        :param date: 日期参数（保持接口统一，实际未使用）
        :return: 处理结果（True/False）
        """
        try:
            logger.info("开始更新概念数据...")
            
            # 1. 清空历史数据
            self.truncate_tables()
            
            # 2. 加载最新概念数据
            concept_df = self.load_concept_data()
            
            # 3. 加载概念股关联数据
            self.load_concept_stocks(concept_df)
            
            logger.info("概念数据更新完成")
            return True
            
        except Exception as e:
            logger.error(f"更新概念数据失败: {str(e)}")
            return False

def process_daily_concept(date=None):
    """
    模块级处理函数（保持接口统一）
    :param date: 日期参数（保持接口统一，实际未使用）
    :return: 处理结果（True/False）
    """
    loader = ConceptDataLoader()
    return loader.process_daily_concept(date)

# 保留命令行调用方式
if __name__ == "__main__":
    success = process_daily_concept()
    if not success:
        sys.exit(1)




