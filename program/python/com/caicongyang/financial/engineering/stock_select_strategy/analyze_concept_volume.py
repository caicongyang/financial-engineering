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
import time  # Add this import at the top
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

class ConceptVolumeAnalyzer:
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
                    self.print_analysis_results(analyze_date, results)
                else:
                    logger.warning(f"未找到 {analyze_date} 的成交量增加数据")
                    
            except ValueError:
                logger.error(f"日期格式错误: {date}, 请使用YYYY-MM-DD格式")
                continue
            except Exception as e:
                logger.error(f"分析 {date} 数据时发生错误: {e}")
                continue

    def print_analysis_results(self, date, results):
        """打印热门概念分析结果"""
        print(f"\n热门概念板块分析 ({date}):")
        print("-" * 80)
        
        concept_stats = pd.DataFrame.from_dict(results['concept_stats'], orient='index')
        top_concepts = concept_stats.head(30)  # 获取前10个概念
        
        for concept_name, stats in top_concepts.iterrows():
            print(f"概念: {concept_name}")
            print(f"成交量增加股票数: {stats['stock_count']}")
            print(f"平均成交量增幅: {stats['avg_increase']:.2f}")
            print(f"最大成交量增幅: {stats['max_increase']:.2f}")
            print("-" * 80)
            
            # 打印该概念的具体股票信息
            self.print_concept_stocks(date, concept_name)

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
        """分析概念股成交量上涨的关联性，找出热门概念"""
        start_time = time.time()
        logger.info(f"开始分析 {date} 的概念股数据...")
        
        try:
            # 1. 获取当日成交量增加的股票（增幅>50%）
            query_start = time.time()
            query = text("""
                SELECT v.stock_code, v.volume_increase_ratio, c.concept_name
                FROM t_volume_increase v
                JOIN t_concept_stock c ON v.stock_code = c.stock_code
                WHERE v.trade_date = :date
                AND v.volume_increase_ratio > 2  -- 成交量增加50%以上
            """)
            
            df = pd.read_sql(query, self.engine, params={'date': date})
            query_end = time.time()
            logger.info(f"查询数据耗时: {query_end - query_start:.2f}秒, 获取到 {len(df)} 条记录")
            
            if df.empty:
                logger.warning(f"No volume increase data found for date {date}")
                return None
            
            # 2. 按概念分组统计
            groupby_start = time.time()
            concept_stats = df.groupby('concept_name').agg({
                'stock_code': 'count',
                'volume_increase_ratio': ['mean', 'max']
            }).round(2)
            
            # 重命名列
            concept_stats.columns = ['stock_count', 'avg_increase', 'max_increase']
            groupby_end = time.time()
            logger.info(f"概念分组统计耗时: {groupby_end - groupby_start:.2f}秒, 共 {len(concept_stats)} 个概念")
            
            # 3. 过滤无效概念
            filter_start = time.time()
            concept_stats = concept_stats[
                (concept_stats['stock_count'] > 5) &  # 至少5只股票
                (concept_stats['stock_count'] < 50)   # 最多50只股票
            ]
            filter_end = time.time()
            logger.info(f"过滤概念耗时: {filter_end - filter_start:.2f}秒, 过滤后剩余 {len(concept_stats)} 个概念")
            
            if concept_stats.empty:
                logger.warning(f"No valid concepts found for date {date}")
                return None
            
            # 4. 按平均增幅排序
            concept_stats = concept_stats.sort_values('avg_increase', ascending=False)
            
            # 5. 获取每个概念的具体股票
            details_start = time.time()
            concept_details = {}
            for concept in concept_stats.index:
                concept_stocks = df[df['concept_name'] == concept].sort_values(
                    'volume_increase_ratio', ascending=False
                )
                concept_details[concept] = concept_stocks[['stock_code', 'volume_increase_ratio']].to_dict('records')
            details_end = time.time()
            logger.info(f"获取概念详情耗时: {details_end - details_start:.2f}秒")
            
            end_time = time.time()
            logger.info(f"分析 {date} 概念股数据总耗时: {end_time - start_time:.2f}秒")
            
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
        start_time = time.time()
        logger.info(f"开始保存 {date} 的分析结果...")
        
        try:
            if not results:
                logger.warning("结果为空，无需保存")
                return
                
            concept_count = len(results['concept_stats'])
            logger.info(f"需要处理的概念数量: {concept_count}")
            
            with self.engine.begin() as conn:
                # 1. 保存概念统计数据
                stats_start = time.time()
                concept_stats_df = pd.DataFrame.from_dict(results['concept_stats'], orient='index')
                concept_stats_df['trade_date'] = date
                concept_stats_df.to_sql(
                    't_concept_volume_stats', 
                    conn, 
                    if_exists='append', 
                    index=True, 
                    index_label='concept_name'
                )
                stats_end = time.time()
                logger.info(f"保存概念统计数据耗时: {stats_end - stats_start:.2f}秒, 共 {len(concept_stats_df)} 条记录")
                
                # 2. 获取所有需要的股票代码
                all_stock_codes = set()
                for stocks in results['concept_details'].values():
                    for stock in stocks:
                        all_stock_codes.add(stock['stock_code'])
                
                # 3. 预先一次性查询所有股票数据
                prefetch_start = time.time()
                logger.info(f"预查询 {len(all_stock_codes)} 只股票的当日信息...")
                
                # 将所有股票代码转换为字符串列表，用于构建SQL查询
                stock_codes_list = list(all_stock_codes)
                # MySQL的IN操作符需要字符串格式，如：'000001','000002'
                codes_str = "','".join(stock_codes_list)
                codes_str = f"'{codes_str}'"
                
                # 一次查询获取所有股票信息
                stock_query = text(f"""
                    SELECT stock_code, stock_name, pct_chg, close
                    FROM t_stock
                    WHERE stock_code IN ({codes_str})
                    AND trade_date = :date
                """)
                
                stock_info_df = pd.read_sql(stock_query, conn, params={'date': date})
                
                # 转换为字典，以股票代码为键，方便快速查找
                stock_info_map = {}
                for _, row in stock_info_df.iterrows():
                    stock_info_map[row['stock_code']] = {
                        'stock_name': row['stock_name'],
                        'pct_chg': row['pct_chg'],
                        'close': row['close']
                    }
                
                prefetch_end = time.time()
                logger.info(f"预查询股票信息耗时: {prefetch_end - prefetch_start:.2f}秒, 获取到 {len(stock_info_map)} 只股票信息")
                
                # 4. 保存概念详细数据
                details_start = time.time()
                details_rows = []
                
                # 跟踪每个概念的处理进度
                concept_counter = 0
                total_stocks = sum(len(stocks) for stocks in results['concept_details'].values())
                logger.info(f"需要处理的股票总数: {total_stocks}")
                processed_stocks = 0
                not_found_stocks = 0
                
                # 处理每个概念
                for concept, stocks in results['concept_details'].items():
                    concept_counter += 1
                    concept_start = time.time()
                    logger.info(f"处理概念[{concept_counter}/{concept_count}]: {concept}, 包含 {len(stocks)} 只股票")
                    
                    stock_counter = 0
                    for stock in stocks:
                        stock_counter += 1
                        processed_stocks += 1
                        
                        if stock_counter % 20 == 0 or stock_counter == len(stocks):
                            logger.info(f"  - 概念 {concept} 处理进度: {stock_counter}/{len(stocks)} 股票")
                        
                        try:
                            stock_code = stock['stock_code']
                            
                            # 直接从内存中获取股票信息，而不是查询数据库
                            if stock_code in stock_info_map:
                                info = stock_info_map[stock_code]
                                row = {
                                    'trade_date': date,
                                    'concept_name': concept,
                                    'stock_code': stock_code,
                                    'stock_name': info['stock_name'],
                                    'volume_increase_ratio': stock['volume_increase_ratio'],
                                    'pct_chg': info['pct_chg'],
                                    'close': info['close']
                                }
                                details_rows.append(row)
                            else:
                                not_found_stocks += 1
                                if not_found_stocks <= 10:  # 只记录前10个未找到的股票，避免日志过多
                                    logger.warning(f"  未找到股票 {stock_code} 在 {date} 的信息")
                        except Exception as e:
                            logger.error(f"  处理股票 {stock_code} 时出错: {str(e)}")
                    
                    concept_time = time.time() - concept_start
                    if concept_time > 3.0:  # 降低阈值，因为现在处理应该更快
                        logger.warning(f"处理概念 {concept} 耗时较长: {concept_time:.2f}秒")
                    
                    # 每处理20个概念保存一次数据
                    if concept_counter % 20 == 0 and details_rows:
                        batch_save_start = time.time()
                        batch_df = pd.DataFrame(details_rows)
                        logger.info(f"中间保存 {len(batch_df)} 条详情记录...")
                        
                        try:
                            batch_df.to_sql(
                                't_concept_volume_details', 
                                conn, 
                                if_exists='append', 
                                index=False
                            )
                            details_rows = []  # 清空已保存的记录
                            logger.info(f"中间保存完成，耗时: {time.time() - batch_save_start:.2f}秒")
                        except Exception as e:
                            logger.error(f"中间保存数据时出错: {str(e)}")
                
                # 保存剩余记录
                if details_rows:
                    final_save_start = time.time()
                    logger.info(f"最终保存 {len(details_rows)} 条详情记录...")
                    details_df = pd.DataFrame(details_rows)
                    
                    try:
                        details_df.to_sql(
                            't_concept_volume_details', 
                            conn, 
                            if_exists='append', 
                            index=False
                        )
                        logger.info(f"最终保存完成，耗时: {time.time() - final_save_start:.2f}秒")
                    except Exception as e:
                        logger.error(f"最终保存数据时出错: {str(e)}")
                
                details_end = time.time()
                if not_found_stocks > 0:
                    logger.warning(f"有 {not_found_stocks} 只股票在 {date} 无交易数据")
                    
                logger.info(f"保存概念详情数据总耗时: {details_end - details_start:.2f}秒, 总共处理 {processed_stocks} 只股票")
                
                end_time = time.time()
                logger.info(f"保存 {date} 分析结果总耗时: {end_time - start_time:.2f}秒")
                
        except Exception as e:
            import traceback
            logger.error(f"保存数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
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
        # 清空当天的数据
        analyzer.clear_existing_data(date)
        
        results = analyzer.analyze_concept_volume(date)
        if results:
            # 保存分析结果
            analyzer.save_analysis_results(results, date)
            analyzer.print_analysis_results(date, results)
            logger.info(f"成交量概念分析完成，数据已保存到数据库")
            return True
        return False
    except Exception as e:
        logger.error(f"处理成交量概念数据失败: {str(e)}")
        return False

if __name__ == "__main__":
    process_concept_volume('2025-04-09')