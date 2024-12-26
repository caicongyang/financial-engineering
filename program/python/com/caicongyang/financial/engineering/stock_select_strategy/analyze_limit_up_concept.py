# !/usr/bin/python
# -*- coding: UTF-8 -*-

"""
分析涨停股票的概念关联性，找出热门概念
"""

from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import logging

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
        FROM t_stock
        WHERE trade_date = :date
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'date': date})
            count = result.scalar()
        
        return count > 0

    def analyze_limit_up_concepts(self, date):
        """分析指定日期涨停股票的概念关联"""
        try:
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
                'limit_up_count',
                'avg_increase',
                'max_increase',
                'total_volume'
            ]
            
            # 3. 计算概念强度得分
            concept_stats['strength_score'] = (
                concept_stats['limit_up_count'] * 0.8 +  # 涨停股票数量权重
                # concept_stats['avg_increase'] * 0.3 +  # 平均涨幅权重
                concept_stats['max_increase'] * 0.2  # 最大涨幅权重
            ).round(2)
            
            # 4. 按强度得分排序
            concept_stats = concept_stats.sort_values('strength_score', ascending=False)
            
            # 5. 获取每个概念的具体股票
            top_concepts = concept_stats.head(10).index.tolist()
            concept_details = {}
            
            for concept in top_concepts:
                concept_stocks = df[df['concept_name'] == concept].sort_values('pct_chg', ascending=False)
                concept_details[concept] = concept_stocks[['stock_code', 'stock_name', 'pct_chg', 'close', 'volume']].to_dict('records')
            
            return {
                'date': date,
                'concept_stats': concept_stats.to_dict('index'),
                'concept_details': concept_details
            }
            
        except Exception as e:
            logger.error(f"Error analyzing limit up concepts for date {date}: {e}")
            raise

    def print_analysis_results(self, date, results):
        """打印分析结果"""
        if not results:
            return
            
        print(f"\n涨停概念板块分析 ({date}):")
        print("=" * 80)
        
        # 打印概念统计
        concept_stats = pd.DataFrame.from_dict(results['concept_stats'], orient='index')
        top_concepts = concept_stats.head(10)
        
        for concept_name, stats in top_concepts.iterrows():
            print(f"\n概念: {concept_name}")
            print(f"涨停股票数: {stats['limit_up_count']}")
            print(f"平均涨幅: {stats['avg_increase']:.2f}%")
            print(f"最大涨幅: {stats['max_increase']:.2f}%")
            print(f"强度得分: {stats['strength_score']:.2f}")
            print("\n相关股票:")
            print("代码     名称     涨幅    收盘价    成交量(万)")
            print("-" * 50)
            
            for stock in results['concept_details'][concept_name]:
                print(f"{stock['stock_code']}  {stock['stock_name']:<8} {stock['pct_chg']:>6.2f}%  "
                      f"{stock['close']:>7.2f}  {stock['volume']/10000:>8.2f}")
            print("-" * 80)

def main():
    try:
        # 要分析的日期列表
        dates_to_check = ['2024-12-26']
        
        analyzer = LimitUpConceptAnalyzer()
        
        for date in dates_to_check:
            try:
                # 验证日期格式
                analyze_date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
                
                # 检查数据是否存在
                if not analyzer.check_data_exists(analyze_date):
                    logger.warning(f"No stock data found for date: {analyze_date}")
                    continue
                
                logger.info(f"开始分析 {analyze_date} 的涨停概念...")
                
                # 分析数据
                results = analyzer.analyze_limit_up_concepts(analyze_date)
                if results:
                    analyzer.print_analysis_results(analyze_date, results)
                else:
                    logger.warning(f"未找到 {analyze_date} 的涨停数据")
                    
            except ValueError:
                logger.error(f"日期格式错误: {date}, 请使用YYYY-MM-DD格式")
                continue
            except Exception as e:
                logger.error(f"分析 {date} 数据时发生错误: {e}")
                continue
                
    except Exception as e:
        logger.error(f"程序执行失败: {e}")

if __name__ == "__main__":
    main() 