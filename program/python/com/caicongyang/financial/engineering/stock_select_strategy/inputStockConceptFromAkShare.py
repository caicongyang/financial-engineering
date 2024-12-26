"""
把AkShare的数据导入到本地数据库
"""

from sqlalchemy import create_engine, text
import akshare as ak
import pandas as pd
import logging
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConceptDataLoader:
    def __init__(self):
        # 数据库连接信息
        self.mysql_config = {
            'user': 'root',
            'password': 'root',
            'host': '101.43.6.49',
            'port': '3333',
            'db': 'stock'
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

    def load_concept_stocks(self, concept_df):
        """加载概念股票数据"""
        try:
            total_stocks = 0
            # 使用中文列名获取列表
            concept_list = concept_df['板块名称'].tolist()
            total_concepts = len(concept_list)
            
            logger.info(f"Starting to load stocks for {total_concepts} concepts")
            
            for idx, concept_name in enumerate(concept_list, 1):
                try:
                    # 获取概念成分股
                    stocks_df = ak.stock_board_concept_cons_em(symbol=concept_name)
                    
                    if not stocks_df.empty:
                        # 选择并重命名列
                        df = stocks_df[['代码', '名称']].copy()
                        # 使用中文列名获取板块代码
                        df['板块代码'] = concept_df[concept_df['板块名称'] == concept_name]['板块代码'].iloc[0]
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
                        
                        total_stocks += len(df)
                        logger.info(f"Progress: {idx}/{total_concepts} - Loaded {len(df)} stocks for concept: {concept_name}")
                    
                    # 添加延时避免请求过快
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error processing concept {concept_name}: {e}")
                    continue
            
            logger.info(f"Successfully loaded total {total_stocks} stock-concept relationships")
            
        except Exception as e:
            logger.error(f"Error loading concept stocks: {e}")
            raise

def main():
    try:
        loader = ConceptDataLoader()
        
        # 1. 清空历史数据
        logger.info("Clearing historical data...")
        loader.truncate_tables()
        
        # 2. 加载概念数据
        logger.info("Loading concept data...")
        concept_df = loader.load_concept_data()
        
        # 3. 加载概念股票数据
        logger.info("Loading concept stocks data...")
        loader.load_concept_stocks(concept_df)
        
        logger.info("All data loaded successfully")
        
    except Exception as e:
        logger.error(f"Program failed: {e}")

if __name__ == "__main__":
    main()




