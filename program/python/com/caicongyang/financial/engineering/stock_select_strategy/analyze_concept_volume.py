# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
分析概念股成交量上涨的关联性，找出热门概念

策略说明：
1. 数据筛选：
   - 过滤ST股票、次新股和停牌股
   - 只统计收盘价大于开盘价的股票（保证股价上涨）
   - 剔除概念股数量大于50的概念（避免概念太宽泛）

2. 概念强度评分体系：
   - 股票数量权重：根据市场趋势动态调整（牛市0.4，熊市0.3）
   - 成交量增幅：平均增幅(0.2)和最大增幅(0.2)各占一定权重
   - 上涨股票数：权重0.2，反映概念内股票的整体表现
   - 机构参与度：机构净买入金额权重0.1，反映机构认可度
   - 资金流向：主力净流入权重0.15，散户净流入权重-0.05（主力认可加分，散户追涨减分）

3. 风险控制：
   - 剔除概念股过多的板块（>50只股票）
   - 考虑市场趋势动态调整权重
   - 通过资金流向分析避免跟风炒作

4. 选股逻辑：
   - 先选出强势概念（按强度得分排序）
   - 再从强势概念中选出龙头股（按成交量增幅排序）
   - 结合资金流向和机构参与度验证
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
        """
        分析指定日期的概念股成交量上涨情况
        
        策略步骤：
        1. 数据获取和预处理：
           - 获取成交量增加的股票
           - 关联概念信息
           - 获取机构和资金流向数据
           - 过滤问题股票（ST、次新、停牌）
        
        2. 概念分组统计：
           - 计算每个概念的股票数量
           - 统计成交量增幅（平均值和最大值）
           - 统计上涨股票数量
           - 汇总资金流向数据
        
        3. 概念强度评分：
           - 根据市场趋势动态调整权重
           - 综合考虑多个因子计算得分
           - 特别关注机构资金和主力资金
        
        4. 结果筛选：
           - 按强度得分排序
           - 选取前10个最强概念
           - 获取每个概念的具体股票信息
        """
        try:
            # 1. 获取当天成交量增加的股票
            query = text("""
                SELECT v.stock_code, v.volume_increase_ratio, v.close, v.open,
                       c.concept_name, c.concept_code,
                       s.market_value,  -- 市值数据用于筛选
                       i.institutional_net_buy,  -- 机构净买入：反映机构认可度
                       m.main_net_inflow,  -- 主力净流入：反映主力资金动向
                       r.retail_net_inflow  -- 散户净流入：用于识别跟风盘
                FROM t_volume_increase v
                JOIN t_concept_stock c ON v.stock_code = c.stock_code
                JOIN t_stock_basic s ON v.stock_code = s.stock_code  
                JOIN t_institutional_investment i ON v.stock_code = i.stock_code
                JOIN t_main_net_inflow m ON v.stock_code = m.stock_code
                JOIN t_retail_net_inflow r ON v.stock_code = r.stock_code
                WHERE v.trade_date = :date
                AND v.close >= v.open  -- 确保股价上涨
                AND s.st_status = 1    -- 排除ST股票
                AND s.listing_date < DATE_SUB(:date, INTERVAL 60 DAY)  -- 排除次新股
                AND s.is_suspended = 0  -- 排除停牌股
            """)
            
            df = pd.read_sql(query, self.engine, params={'date': date})
            
            if df.empty:
                logger.warning(f"No volume increase data found for date {date}")
                return None
            
            # 2. 按概念分组统计
            concept_stats = df.groupby('concept_name').agg({
                'stock_code': 'count',  # 概念内股票数：反映概念范围
                'volume_increase_ratio': ['mean', 'max'],  # 成交量增幅：反映活跃度
                'close': lambda x: (x > x.shift(1)).sum(),  # 上涨股票数：反映强度
                'institutional_net_buy': 'sum',  # 机构净买入总额：反映机构参与度
                'main_net_inflow': 'sum',  # 主力净流入总额：反映主力认可度
                'retail_net_inflow': 'sum'  # 散户净流入总额：反映跟风程度
            }).round(2)
            
            # 重命名列
            concept_stats.columns = [
                'stock_count',
                'avg_volume_increase',
                'max_volume_increase',
                'up_count',
                'institutional_net_buy',
                'main_net_inflow',
                'retail_net_inflow'
            ]
            
            # 只保留股票数量小于50的概念
            concept_stats = concept_stats[concept_stats['stock_count'] < 50]
            
            if concept_stats.empty:
                logger.warning(f"No concepts with less than 50 stocks found for date {date}")
                return None
            
            # 3. 计算概念强度得分
            market_trend = 'up' if df['volume_increase_ratio'].mean() > 0 else 'down'
            concept_stats['strength_score'] = (
                # 股票数量权重：牛市加大权重，看重热度
                concept_stats['stock_count'] * (0.4 if market_trend == 'up' else 0.3) +
                # 成交量增幅：反映资金参与度
                concept_stats['avg_volume_increase'] * 0.2 +
                concept_stats['max_volume_increase'] * 0.2 +
                # 上涨股票数：反映概念整体强度
                concept_stats['up_count'] * 0.2 +
                # 机构资金：机构认可度指标
                concept_stats['institutional_net_buy'] * 0.1 +
                # 资金流向：主力加分，散户跟风减分
                concept_stats['main_net_inflow'] * 0.15 -
                concept_stats['retail_net_inflow'] * 0.05
            ).round(2)
            
            # 4. 按强度得分排序
            concept_stats = concept_stats.sort_values('strength_score', ascending=False)
            
            # 5. 获取每个概念的具体股票
            top_concepts = concept_stats.head(10).index.tolist()
            concept_details = {}
            
            for concept in top_concepts:
                # 获取概念内具体股票，按成交量增幅排序找龙头
                concept_stocks = df[df['concept_name'] == concept].sort_values(
                    'volume_increase_ratio', ascending=False
                )
                concept_details[concept] = concept_stocks[
                    ['stock_code', 'volume_increase_ratio', 'close', 'open']
                ].to_dict('records')
            
            logger.info(f"Found {len(concept_stats)} concepts with more than 30 stocks")
            
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
                
            with self.engine.begin() as conn:  # 使用事务
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

def main():
    try:
        # 要分析的日期列表
        dates_to_check = ['2025-02-07']
        
        analyzer = ConceptVolumeAnalyzer()
        analyzer.batch_analyze_concepts(dates_to_check)
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")

if __name__ == "__main__":
    main() 