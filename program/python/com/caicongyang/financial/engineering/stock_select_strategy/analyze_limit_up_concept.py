# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
分析涨停股票的概念关联性，找出热门概念

策略说明：
1. 数据筛选：
- 获取当日涨幅超过9.9%的涨停股票
- 关联股票所属概念板块
- 不设最小股票数限制（涨停本身已是强筛选）

2. 核心指标：
- 概念内涨停股票数量：反映概念强度
- 概念内平均涨幅：反映整体表现
- 概念内最大涨幅：反映龙头股表现

3. 结果展示：
- 按涨停股数量降序排列
- 展示每个概念的具体股票及其涨幅
"""

from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import logging
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

class LimitUpConceptAnalyzer:
    def __init__(self):
        # 数据库连接信息
        self.mysql_config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'db': os.getenv('DB_NAME')
        }
        
        # 创建数据库连接
        self.engine = create_engine(
            f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}@"
            f"{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['db']}"
        )

    def clear_existing_data(self, date):
        """清空指定日期的数据"""
        try:
            with self.engine.begin() as conn:
                # 清空概念统计数据
                delete_stats = text("""
                    DELETE FROM t_limit_up_concept_stats 
                    WHERE trade_date = :date
                """)
                conn.execute(delete_stats, {'date': date})
                
                # 清空概念详情数据
                delete_details = text("""
                    DELETE FROM t_limit_up_concept_details 
                    WHERE trade_date = :date
                """)
                conn.execute(delete_details, {'date': date})
                
                logger.info(f"Successfully cleared existing data for date {date}")
                
        except Exception as e:
            logger.error(f"Error clearing existing data for date {date}: {e}")
            raise

    def save_analysis_results(self, results, date):
        """保存分析结果到数据库"""
        try:
            if not results:
                return
                
            with self.engine.begin() as conn:
                # 1. 保存概念统计数据
                concept_stats_df = pd.DataFrame.from_dict(results['concept_stats'], orient='index')
                concept_stats_df['trade_date'] = date
                concept_stats_df.to_sql(
                    't_limit_up_concept_stats', 
                    conn, 
                    if_exists='append', 
                    index=True, 
                    index_label='concept_name'
                )
                
                # 2. 保存概念详细数据
                details_rows = []
                for concept, stocks in results['concept_details'].items():
                    for stock in stocks:
                        row = {
                            'trade_date': date,
                            'concept_name': concept,
                            'stock_code': stock['stock_code'],
                            'stock_name': stock['stock_name'],
                            'pct_chg': stock['pct_chg'],
                            'close': stock['close'],
                            'volume': stock['volume']
                        }
                        details_rows.append(row)
                
                if details_rows:
                    details_df = pd.DataFrame(details_rows)
                    details_df.to_sql(
                        't_limit_up_concept_details', 
                        conn, 
                        if_exists='append', 
                        index=False
                    )
                
                logger.info(f"Successfully saved analysis results for date {date}")
                
        except Exception as e:
            logger.error(f"Error saving analysis results for date {date}: {e}")
            raise

    def analyze_limit_up_concepts(self, date):
        """分析指定日期涨停股票的概念关联"""
        try:
            # 清空当天的数据
            self.clear_existing_data(date)
            
            # 1. 获取涨停股票数据
            query = text("""
                SELECT s.stock_code, s.stock_name, s.pct_chg, s.close, s.volume,
                       c.concept_name
                FROM t_stock s
                JOIN t_concept_stock c ON s.stock_code = c.stock_code
                WHERE s.trade_date = :date
                AND s.pct_chg >= 9.9  -- 涨幅大于9.9%视为涨停
                ORDER BY s.pct_chg DESC
            """)
            
            df = pd.read_sql(query, self.engine, params={'date': date})
            
            if df.empty:
                logger.warning(f"No limit up stocks found for date {date}")
                return None
            
            # 2. 按概念分组统计
            concept_stats = df.groupby('concept_name').agg({
                'stock_code': 'count',  # 涨停股票数
                'pct_chg': ['mean', 'max'],  # 平均和最大涨幅
                'volume': 'sum'  # 成交量合计
            }).round(2)
            
            # 重命名列
            concept_stats.columns = [
                'stock_count',
                'avg_increase',
                'max_increase',
                'total_volume'
            ]
            
            # 4. 按涨停股数量排序
            concept_stats = concept_stats.sort_values('stock_count', ascending=False)
            
            # 5. 获取每个概念的具体股票
            concept_details = {}
            for concept in concept_stats.index:
                concept_stocks = df[df['concept_name'] == concept].sort_values('pct_chg', ascending=False)
                concept_details[concept] = concept_stocks[
                    ['stock_code', 'stock_name', 'pct_chg', 'close', 'volume']
                ].to_dict('records')
            
            results = {
                'date': date,
                'concept_stats': concept_stats.to_dict('index'),
                'concept_details': concept_details
            }
            
            # 保存分析结果
            self.save_analysis_results(results, date)
            
            return results
            
        except Exception as e:
            logger.error(f"Error analyzing limit up concepts for date {date}: {e}")
            raise

    def print_analysis_results(self, date, results):
        """打印分析结果"""
        if not results:
            return
            
        print(f"\n涨停概念板块分析 ({date}):")
        print("-" * 80)
        
        concept_stats = pd.DataFrame.from_dict(results['concept_stats'], orient='index')
        top_concepts = concept_stats.head(20)
        
        for concept_name, stats in top_concepts.iterrows():
            print(f"\n概念: {concept_name}")
            print(f"涨停股票数: {stats['stock_count']}")
            print(f"平均涨幅: {stats['avg_increase']:.2f}%")
            print(f"最大涨幅: {stats['max_increase']:.2f}%")
            print(f"总成交量: {stats['total_volume']/100000000:.2f}亿")
            print("\n相关股票:")
            print("代码     名称     涨幅    收盘价    成交量(亿)")
            print("-" * 50)
            
            for stock in results['concept_details'][concept_name]:
                print(f"{stock['stock_code']}  {stock['stock_name']:<8} {stock['pct_chg']:>6.2f}%  "
                      f"{stock['close']:>7.2f}  {stock['volume']/100000000:>8.2f}")
            print("-" * 80)

    def print_concept_stocks(self, date, concept_name):
        """打印概念相关股票详情"""
        query = text("""
            SELECT d.stock_code, d.volume_increase_ratio, s.close, s.open
            FROM t_concept_volume_details d
            JOIN t_stock s ON d.stock_code = s.stock_code AND d.trade_date = s.trade_date
            WHERE d.trade_date = :date
            AND d.concept_name = :concept_name
            ORDER BY d.volume_increase_ratio DESC
        """)
        
        with self.engine.connect() as conn:
            stocks = pd.read_sql(query, conn, params={
                'date': date,
                'concept_name': concept_name
            })
        
        if not stocks.empty:
            print(f"\n{concept_name} 概念相关股票:")
            print("股票代码  成交量增幅    收盘价   开盘价   涨跌幅")
            print("-" * 50)
            for _, stock in stocks.iterrows():
                price_change = ((stock['close'] - stock['open']) / stock['open'] * 100)
                print(f"{stock['stock_code']}  {stock['volume_increase_ratio']:>8.2f}倍  "
                      f"{stock['close']:>7.2f}  {stock['open']:>7.2f}  {price_change:>6.2f}%")
            print()

def process_limit_up_concept(date):
    """
    模块级处理函数（保持接口统一）
    :param date: 日期参数
    :return: 处理结果（True/False）
    """
    try:
        analyzer = LimitUpConceptAnalyzer()
        results = analyzer.analyze_limit_up_concepts(date)
        if results:
            analyzer.print_analysis_results(date, results)
            logger.info(f"涨停概念分析完成，数据已保存到数据库")
            return True
        return False
    except Exception as e:
        logger.error(f"处理涨停概念数据失败: {str(e)}")
        return False

if __name__ == "__main__":
    process_limit_up_concept('2025-02-19')