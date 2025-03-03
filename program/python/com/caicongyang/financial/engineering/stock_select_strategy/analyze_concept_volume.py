# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
分析概念股成交量上涨的关联性，找出热门概念

策略说明：
1. 数据筛选：
- 获取当日成交量增幅超过100%的股票
- 关联股票所属概念板块
- 过滤股票数量在5-50之间的概念（避免概念太宽泛或太集中）

2. 核心指标：
- 概念内成交量平均增幅：反映整体热度
- 概念内最大成交量增幅：反映龙头股表现
- 涉及股票数量：反映概念覆盖广度

3. 结果展示：
- 按平均成交量增幅降序排列
- 展示每个概念的具体股票及其成交量增幅
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
            'host': '43.133.13.36',
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
                
                # 清空当天的数据
                self.clear_existing_data(analyze_date)
                
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
                    logger.warning(f"未找到 {analyze_date} 的成交量增加数据")
                    
            except ValueError:
                logger.error(f"日期格式错误: {date}, 请使用YYYY-MM-DD格式")
                continue
            except Exception as e:
                logger.error(f"分析 {date} 数据时发生错误: {e}")
                continue

    def print_analysis_results(self, date, hot_concepts):
        """
        打印热门概念分析结果
        
        输出内容：
        - 概念名称
        - 涉及股票数量
        - 平均成交量增幅（倍）
        - 最大成交量增幅（倍）
        - 相关股票列表（按增幅排序）
        """
        print(f"\n热门概念板块分析 ({date}):")
        print("-" * 80)
        
        for concept in hot_concepts:
            print(f"概念: {concept['concept_name']}")
            print(f"成交量增加股票数: {concept['stock_count']}")
            print(f"平均成交量增幅: {concept['avg_increase']:.2f}")
            print(f"最大成交量增幅: {concept['max_increase']:.2f}")
            print("-" * 80)
            
            # 获取并打印该概念的具体股票信息
            self.print_concept_stocks(date, concept['concept_name'])

    def print_concept_stocks(self, date, concept_name):
        """打印概念相关股票详情"""
        query = text("""
            SELECT d.stock_code, d.volume_increase_ratio, 
                   s.stock_name, s.pct_chg, s.close
            FROM t_concept_volume_details d
            JOIN t_stock s ON d.stock_code = s.stock_code 
                 AND d.trade_date = s.trade_date
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
            print("代码     名称     成交量增幅   涨跌幅   收盘价")
            print("-" * 55)
            for _, stock in stocks.iterrows():
                print(f"{stock['stock_code']}  {stock['stock_name']:<8}  "
                      f"{stock['volume_increase_ratio']:>8.2f}倍  "
                      f"{stock['pct_chg']:>6.2f}%  {stock['close']:>7.2f}")
            print()

    def analyze_concept_volume(self, date):
        """
        概念成交量热点分析
        
        实现步骤：
        1. 获取数据：从数据库获取当日成交量增幅>100%的股票及其所属概念
        2. 概念聚类：按概念分组统计股票数量、平均/最大成交量增幅
        3. 结果过滤：保留3-50只股票的概念，避免无效数据
        4. 结果排序：按平均成交量增幅降序排列
        """
        try:
            # 1. 获取当日成交量增加的股票（增幅>50%）
            query = text("""
                SELECT v.stock_code, v.volume_increase_ratio, c.concept_name
                FROM t_volume_increase v
                JOIN t_concept_stock c ON v.stock_code = c.stock_code
                WHERE v.trade_date = :date
                AND v.volume_increase_ratio > 2  -- 成交量增加50%以上
            """)
            
            df = pd.read_sql(query, self.engine, params={'date': date})
            
            if df.empty:
                logger.warning(f"No volume increase data found for date {date}")
                return None
            
            # 2. 按概念分组统计
            concept_stats = df.groupby('concept_name').agg({
                'stock_code': 'count',
                'volume_increase_ratio': ['mean', 'max']
            }).round(2)
            
            # 重命名列
            concept_stats.columns = ['stock_count', 'avg_increase', 'max_increase']
            
            # 3. 过滤无效概念
            concept_stats = concept_stats[
                (concept_stats['stock_count'] > 5) &  # 至少5只股票
                (concept_stats['stock_count'] < 50)   # 最多50只股票
            ]
            
            if concept_stats.empty:
                logger.warning(f"No valid concepts found for date {date}")
                return None
            
            # 4. 按平均增幅排序
            concept_stats = concept_stats.sort_values('avg_increase', ascending=False)
            
            # 5. 获取每个概念的具体股票
            concept_details = {}
            for concept in concept_stats.index:
                concept_stocks = df[df['concept_name'] == concept].sort_values(
                    'volume_increase_ratio', ascending=False
                )
                concept_details[concept] = concept_stocks[['stock_code', 'volume_increase_ratio']].to_dict('records')
            
            return {
                'date': date,
                'concept_stats': concept_stats.to_dict('index'),
                'concept_details': concept_details
            }
            
        except Exception as e:
            logger.error(f"Error analyzing concept volume for date {date}: {e}")
            raise

    def save_analysis_results(self, results, date):
        """
        保存分析结果到数据库
        
        存储结构：
        1. 概念统计表(t_concept_volume_stats)：
           - 概念名称 | 日期 | 股票数量 | 平均增幅 | 最大增幅
           
        2. 概念详情表(t_concept_volume_details)：
           - 日期 | 概念名称 | 股票代码 | 成交量增幅
        """
        try:
            if not results:
                return
                
            with self.engine.begin() as conn:
                # 1. 保存概念统计数据
                concept_stats_df = pd.DataFrame.from_dict(results['concept_stats'], orient='index')
                concept_stats_df['trade_date'] = date
                concept_stats_df.to_sql(
                    't_concept_volume_stats', 
                    conn, 
                    if_exists='append', 
                    index=True, 
                    index_label='concept_name'
                )
                
                # 2. 保存概念详细数据
                details_rows = []
                for concept, stocks in results['concept_details'].items():
                    for stock in stocks:
                        # 获取股票名称和涨跌幅
                        stock_query = text("""
                            SELECT stock_name, pct_chg, close
                            FROM t_stock
                            WHERE stock_code = :code
                            AND trade_date = :date
                        """)
                        stock_info = pd.read_sql(
                            stock_query, 
                            conn, 
                            params={'code': stock['stock_code'], 'date': date}
                        )
                        
                        if not stock_info.empty:
                            row = {
                                'trade_date': date,
                                'concept_name': concept,
                                'stock_code': stock['stock_code'],
                                'stock_name': stock_info['stock_name'].iloc[0],
                                'volume_increase_ratio': stock['volume_increase_ratio'],
                                'pct_chg': stock_info['pct_chg'].iloc[0],
                                'close': stock_info['close'].iloc[0]
                            }
                            details_rows.append(row)
                
                if details_rows:
                    details_df = pd.DataFrame(details_rows)
                    details_df.to_sql(
                        't_concept_volume_details', 
                        conn, 
                        if_exists='append', 
                        index=False
                    )
                
                logger.info(f"Successfully saved analysis results for date {date}")
                
        except Exception as e:
            logger.error(f"Error saving analysis results for date {date}: {e}")
            raise

    def get_hot_concepts(self, date, limit=10):
        """获取指定日期的热门概念"""
        try:
            query = text("""
                SELECT concept_name, stock_count, avg_increase, 
                       max_increase
                FROM t_concept_volume_stats
                WHERE trade_date = :date
                ORDER BY avg_increase DESC
                LIMIT :limit
            """)
            
            df = pd.read_sql(query, self.engine, params={'date': date, 'limit': limit})
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error getting hot concepts for date {date}: {e}")
            return []

    def clear_existing_data(self, date):
        """清空指定日期的数据"""
        try:
            with self.engine.begin() as conn:  # 使用 begin() 来自动处理事务
                # 清空概念统计数据
                delete_stats = text("""
                    DELETE FROM t_concept_volume_stats 
                    WHERE trade_date = :date
                """)
                conn.execute(delete_stats, {'date': date})
                
                # 清空概念详情数据
                delete_details = text("""
                    DELETE FROM t_concept_volume_details 
                    WHERE trade_date = :date
                """)
                conn.execute(delete_details, {'date': date})
                
                # 不需要显式提交，with block 结束时会自动提交
                logger.info(f"Successfully cleared existing data for date {date}")
                
        except Exception as e:
            logger.error(f"Error clearing existing data for date {date}: {e}")
            raise

def process_concept_volume(date):
    """
    模块级处理函数（保持接口统一）
    :param date: 日期参数
    :return: 处理结果（True/False）
    """
    try:
        analyzer = ConceptVolumeAnalyzer()
        results = analyzer.analyze_concept_volume(date)
        if results:
            analyzer.print_analysis_results(date, results)
            logger.info(f"成交量概念分析完成，数据已保存到数据库")
            return True
        return False
    except Exception as e:
        logger.error(f"处理成交量概念数据失败: {str(e)}")
        return False

if __name__ == "__main__":
    process_concept_volume('2025-02-14')