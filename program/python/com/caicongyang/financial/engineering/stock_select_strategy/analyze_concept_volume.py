# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
分析概念股成交量上涨的关联性，找出热门概念
"""

from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
import logging
import argparse

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConceptVolumeAnalyzer:
    def __init__(self):
        # 数据库连接信息
        self.mysql_config = {
            'user': 'root',
            'password': 'root',
            'host': '101.43.6.49',
            'port': '3333',
            'db': 'stock'
        }
        
        # 创建数据库连接
        self.engine = create_engine(
            f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}@"
            f"{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['db']}"
        )

    def check_data_exists(self, date):
        """检查指定日期的股票数据是否存在"""
        query = text("""
        SELECT COUNT(*) 
        FROM t_volume_increase
        WHERE trade_date = :date
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'date': date})
            count = result.scalar()
        
        return count > 0

    def batch_analyze_concepts(self, dates_to_check):
        """批量分析多个日期的概念股数据"""
        for date in dates_to_check:
            try:
                # 验证日期格式
                analyze_date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
                
                # 检查数据是否存在
                if not self.check_data_exists(analyze_date):
                    logger.warning(f"No stock data found for date: {analyze_date}, skipping concept analysis.")
                    continue
                
                logger.info(f"开始分析 {analyze_date} 的概念股数据...")
                
                # 分析数据
                results = self.analyze_concept_volume(analyze_date)
                if results:
                    # 保存结果
                    self.save_analysis_results(results, analyze_date)
                    
                    # 获取热门概念
                    hot_concepts = self.get_hot_concepts(analyze_date)
                    
                    if not hot_concepts:
                        logger.warning(f"未找到 {analyze_date} 的热门概念数据")
                        continue
                    
                    # 打印结果
                    self.print_analysis_results(analyze_date, hot_concepts)
                else:
                    logger.warning(f"���找到 {analyze_date} 的成交量增加数据")
                    
            except ValueError:
                logger.error(f"日期格式错误: {date}, 请使用YYYY-MM-DD格式")
                continue
            except Exception as e:
                logger.error(f"分析 {date} 数据时发生错误: {e}")
                continue

    def print_analysis_results(self, date, hot_concepts):
        """打印分析结果"""
        print(f"\n热门概念板块分析 ({date}):")
        print("-" * 80)
        
        for concept in hot_concepts:
            print(f"概念: {concept['concept_name']}")
            print(f"成交量增加股票数: {concept['stock_count']}")
            print(f"平均成交量增幅: {concept['avg_volume_increase']:.2f}")
            print(f"最大成交量增幅: {concept['max_volume_increase']:.2f}")
            print(f"上涨股票数: {concept['up_count']}")
            print(f"强度得分: {concept['strength_score']:.2f}")
            print("-" * 80)
            
            # 获取并打印该概念的具体股票信息
            self.print_concept_stocks(date, concept['concept_name'])

    def print_concept_stocks(self, date, concept_name):
        """打印概念相关股票详情"""
        query = text("""
            SELECT stock_code, volume_increase_ratio, close, open
            FROM t_concept_volume_details
            WHERE trade_date = :date
            AND concept_name = :concept_name
            ORDER BY volume_increase_ratio DESC
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

    def analyze_concept_volume(self, date):
        """分析指定日期的概念股成交量上涨情况"""
        try:
            # 1. 获取当天成交量增加的股票
            query = text("""
                SELECT v.stock_code, v.volume_increase_ratio, v.close, v.open,
                       c.concept_name, c.concept_code
                FROM t_volume_increase v
                JOIN t_concept_stock c ON v.stock_code = c.stock_code
                WHERE v.trade_date = :date
                AND v.close >= v.open  -- 只统计收盘价大于开盘价的股票
            """)
            
            df = pd.read_sql(query, self.engine, params={'date': date})
            
            if df.empty:
                logger.warning(f"No volume increase data found for date {date}")
                return None
            
            # 2. 按概念分组统计
            concept_stats = df.groupby('concept_name').agg({
                'stock_code': 'count',  # 概念内上涨股票数
                'volume_increase_ratio': ['mean', 'max'],  # 平均和最大成交量增幅
                'close': lambda x: (x > x.shift(1)).sum()  # 上涨股票数
            }).round(2)
            
            # 重命名列
            concept_stats.columns = [
                'stock_count',
                'avg_volume_increase',
                'max_volume_increase',
                'up_count'
            ]
            
            # 3. 计算概念强度得分
            concept_stats['strength_score'] = (
                concept_stats['stock_count'] * 0.3 +  # 成交量增加的股票数量权重
                concept_stats['avg_volume_increase'] * 0.3 +  # ���均成交量增幅权重
                concept_stats['max_volume_increase'] * 0.2 +  # 最大成交量增幅权重
                concept_stats['up_count'] * 0.2  # 上涨股票数权重
            ).round(2)
            
            # 4. 按强度得分排序
            concept_stats = concept_stats.sort_values('strength_score', ascending=False)
            
            # 5. 获取每个概念的具体股票
            top_concepts = concept_stats.head(10).index.tolist()
            concept_details = {}
            
            for concept in top_concepts:
                concept_stocks = df[df['concept_name'] == concept].sort_values('volume_increase_ratio', ascending=False)
                concept_details[concept] = concept_stocks[['stock_code', 'volume_increase_ratio', 'close', 'open']].to_dict('records')
            
            return {
                'date': date,
                'concept_stats': concept_stats.to_dict('index'),
                'concept_details': concept_details
            }
            
        except Exception as e:
            logger.error(f"Error analyzing concept volume for date {date}: {e}")
            raise

    def save_analysis_results(self, results, date):
        """保存分析结果到数据库"""
        try:
            if not results:
                return
                
            # 1. 保存概念统计数据
            concept_stats_df = pd.DataFrame.from_dict(results['concept_stats'], orient='index')
            concept_stats_df['trade_date'] = date
            concept_stats_df.to_sql('t_concept_volume_stats', self.engine, if_exists='append', index=True, index_label='concept_name')
            
            # 2. 保存概念详细数据
            details_rows = []
            for concept, stocks in results['concept_details'].items():
                for stock in stocks:
                    row = {
                        'trade_date': date,
                        'concept_name': concept,
                        'stock_code': stock['stock_code'],
                        'volume_increase_ratio': stock['volume_increase_ratio'],
                        'close': stock['close'],
                        'open': stock['open']
                    }
                    details_rows.append(row)
            
            if details_rows:
                details_df = pd.DataFrame(details_rows)
                details_df.to_sql('t_concept_volume_details', self.engine, if_exists='append', index=False)
            
            logger.info(f"Successfully saved analysis results for date {date}")
            
        except Exception as e:
            logger.error(f"Error saving analysis results for date {date}: {e}")
            raise

    def get_hot_concepts(self, date, limit=10):
        """获取指定日期的热门概念"""
        try:
            query = text("""
                SELECT concept_name, stock_count, avg_volume_increase, 
                       max_volume_increase, up_count, strength_score
                FROM t_concept_volume_stats
                WHERE trade_date = :date
                ORDER BY strength_score DESC
                LIMIT :limit
            """)
            
            df = pd.read_sql(query, self.engine, params={'date': date, 'limit': limit})
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error getting hot concepts for date {date}: {e}")
            return []

def main():
    try:
        # 要分析的日期列表
        dates_to_check = ['2024-12-26']
        
        analyzer = ConceptVolumeAnalyzer()
        analyzer.batch_analyze_concepts(dates_to_check)
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")

if __name__ == "__main__":
    main() 